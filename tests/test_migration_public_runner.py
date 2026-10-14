"""Publish only approved roots, preserving checkpoints and download limits."""

import pytest

from app.migration.credentials import CredentialRegistry
from app.migration.errors import MigrationError
from app.migration.runner import MigrationRunner
from app.migration.store import MigrationStore
from tests.test_migration_runner import (
    BODY, CONFIG, DIR, META, Destination, Source, finish, stage_events,
)


class PublicSource(Source):
    def __init__(self):
        super().__init__()
        self.keys = set()
        self.shares = set()
        self.published = []
        self.reads = []
        self.links = []
        self.before_publish = None
        self.after_publish = None
        self.error = None

    def has_publication(self, path):
        return path in self.keys

    def ensure_publication(self, node, *, checkpoint=None):
        path = node["source_path"]
        self.reads.append(path)
        if path in self.shares:
            self.keys.add(path)
            return "existing"
        if node["state"] in {"intent", "published"}:
            raise MigrationError("source_publication_outcome_unknown", uncertain=True)
        if self.before_publish:
            self.before_publish()
        checkpoint()
        self.published.append(path)
        if self.error:
            raise self.error
        self.shares.add(path)
        self.keys.add(path)
        if self.after_publish:
            self.after_publish()
        return "published"

    def public_download_url(self, path):
        assert any(path == root or path.startswith(root + "/") for root in self.keys)
        self.links.append(path)
        return "https://public.example.org/file?secret=never-persist"

    def chunks(self, path, *, download_url=None):
        assert download_url == "https://public.example.org/file?secret=never-persist"
        yield from super().chunks(path)


@pytest.fixture
def public_migration(tmp_path):
    config = dict(CONFIG, strategy="staged", destination_type="shared",
                  destination_vd_hash="shared_1", destination_base="disk:/",
                  destination_root="disk:/owner@example.org", source_public=True)
    store, registry = MigrationStore(tmp_path), CredentialRegistry()
    source, destination = PublicSource(), Destination()
    runner = MigrationRunner(
        store, registry, source_factory=lambda c, a: source,
        destination_factory=lambda c, a: destination, reserve_bytes=0,
    )
    job_id = store.create(config, "public-job")["id"]
    registry.put(job_id, config, {
        "source_client_secret": "secret", "destination_token": "secret",
    })
    yield store, registry, runner, job_id, source, destination
    runner.close()
    store.close()


def test_planning_and_preflight_never_publish(public_migration):
    store, registry, runner, job_id, source, _ = public_migration
    runner.preflight(store.get(job_id)["config"], registry.get(job_id))
    runner.step(job_id)
    assert store.get(job_id)["state"] == "ready"
    assert source.reads == source.published == []
    assert [(p["source_path"], p["state"]) for p in store.publications(job_id)] \
        == [("disk:/Docs", "planned")]


def test_one_folder_share_downloads_child_and_records_safe_journal(public_migration):
    store, _, runner, job_id, source, destination = public_migration
    runner.step(job_id)
    assert finish(store, runner, job_id)["state"] == "completed"
    assert source.published == ["disk:/Docs"]
    assert source.links == ["disk:/Docs/a.txt"]
    assert destination.upload_body == BODY
    assert store.publications(job_id)[0]["state"] == "published"
    assert {e["payload"]["stage"] for e in stage_events(store, job_id)} \
        >= {"publish_source", "public_download_link", "downloading"}
    assert "never-persist" not in str(store.events(job_id, limit=10000))


@pytest.mark.parametrize("action", ["pause", "stop"])
def test_request_between_metadata_and_put_prevents_publication(
    public_migration, action,
):
    store, _, runner, job_id, source, destination = public_migration
    runner.step(job_id)
    store.update_job(job_id, state="running")
    source.before_publish = lambda: (
        store.update_job(job_id, pause_requested=True) if action == "pause"
        else store.request_stop(job_id)
    )
    runner.step(job_id)
    assert source.published == []
    assert store.publications(job_id)[0]["state"] == "planned"
    assert destination.nodes == {}
    if action == "pause":
        assert store.get(job_id)["state"] == "paused"


def test_uncertain_publish_reconciles_without_repeating_put(public_migration):
    store, _, runner, job_id, source, _ = public_migration
    runner.step(job_id)
    source.error = MigrationError("source_publication_outcome_unknown", uncertain=True,
                                  http_status=503)
    assert finish(store, runner, job_id)["state"] == "needs_attention"
    assert store.publications(job_id)[0]["state"] == "intent"
    source.error = None
    runner.retry(job_id, "staged")
    runner.step(job_id)
    assert store.get(job_id)["state"] == "needs_attention"
    assert source.published == ["disk:/Docs"]
    source.shares.add("disk:/Docs")
    runner.retry(job_id, "staged")
    assert finish(store, runner, job_id)["state"] == "completed"
    assert source.published == ["disk:/Docs"]


def test_restart_hydrates_published_root_without_republishing(public_migration):
    store, _, runner, job_id, source, _ = public_migration
    runner.step(job_id)
    store.update_job(job_id, state="running")
    runner.step(job_id)
    assert store.publications(job_id)[0]["state"] == "published"
    source.keys.clear()
    assert finish(store, runner, job_id)["state"] == "completed"
    assert source.reads == ["disk:/Docs", "disk:/Docs"]
    assert source.published == ["disk:/Docs"]


def test_confirmed_publish_error_can_be_retried_and_reason_is_recorded(
    public_migration,
):
    store, _, runner, job_id, source, _ = public_migration
    runner.step(job_id)
    source.error = MigrationError("source_publication_denied", http_status=403)
    assert finish(store, runner, job_id)["state"] == "needs_attention"
    assert store.publications(job_id)[0]["state"] == "failed"
    event = stage_events(store, job_id, "publish_source")[-1]
    assert event["payload"]["error"] == "source_publication_denied"
    assert event["payload"]["http_status"] == 403
    assert event["context"]["source"] == "disk:/Docs"
    source.error = None
    runner.retry(job_id, "staged")
    assert finish(store, runner, job_id)["state"] == "completed"


def test_public_link_failures_skip_after_five_and_preserve_reason(public_migration):
    store, _, runner, job_id, source, destination = public_migration
    runner.step(job_id)

    def denied(path):
        source.links.append(path)
        raise MigrationError("source_public_access_denied", http_status=403)

    source.public_download_url = denied
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    assert len(source.links) == entry["download_attempts"] == 5
    assert entry["error"] == "source_public_access_denied"
    assert destination.upload_body is None
    assert stage_events(store, job_id, "download_retry")[-1]["payload"]["status"] \
        == "skipped"


def test_old_jobs_have_no_publication_or_public_link_reads(tmp_path):
    store, registry = MigrationStore(tmp_path), CredentialRegistry()
    source, destination = Source(), Destination()
    runner = MigrationRunner(store, registry, source_factory=lambda c, a: source,
                             destination_factory=lambda c, a: destination,
                             reserve_bytes=0)
    config = dict(CONFIG, strategy="staged")
    job_id = store.create(config, "old-job")["id"]
    registry.put(job_id, config, {
        "source_client_secret": "secret", "destination_token": "secret",
    })
    runner.step(job_id)
    assert finish(store, runner, job_id)["state"] == "completed"
    assert store.publications(job_id) == []
    assert destination.upload_body == BODY
    runner.close()
    store.close()


@pytest.mark.parametrize("selected, expected", [
    ("disk:/", ["disk:/Docs", "disk:/report.txt"]),
    ("disk:/report.txt", ["disk:/report.txt"]),
])
def test_entire_disk_and_single_file_publish_only_selected_roots(
    public_migration, selected, expected,
):
    store, registry, runner, old_id, source, destination = public_migration
    config = dict(store.get(old_id)["config"], source_path=selected)
    job_id = store.create(config, "other-scope")["id"]
    registry.put(job_id, config, registry.get(old_id))
    tree = [
        dict(DIR, resource_id="folder-1"), dict(META),
        dict(META, path="disk:/report.txt", name="report.txt", resource_id="file-2"),
    ]
    source.walk = lambda path: tree if path == "disk:/" else [tree[-1]]
    source.stat = lambda path: (
        {"type": "dir", "path": "disk:/"} if path == "disk:/"
        else next(n for n in tree if n["path"] == path)
    )
    runner.step(job_id)
    assert finish(store, runner, job_id)["state"] == "completed"
    assert source.published == expected
    assert sorted(source.links) == (
        ["disk:/Docs/a.txt", "disk:/report.txt"]
        if selected == "disk:/" else ["disk:/report.txt"]
    )
    assert destination.nodes["disk:/owner@example.org/report.txt"]["md5"] == META["md5"]


def test_existing_share_is_reused_and_retained_after_stop(public_migration):
    store, _, runner, job_id, source, _ = public_migration
    source.shares.add("disk:/Docs")
    runner.step(job_id)
    store.update_job(job_id, state="running")
    runner.step(job_id)
    runner.stop_job(job_id)
    assert source.shares == {"disk:/Docs"}
    assert source.published == []
    assert store.publications(job_id)[0]["state"] == "published"


def test_public_job_retry_rejects_url_and_keeps_public_staged_source(public_migration):
    store, _, runner, job_id, source, destination = public_migration
    runner.step(job_id)
    original = source.public_download_url

    def denied(path):
        raise MigrationError("source_public_access_denied", http_status=403)

    source.public_download_url = denied
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    before_job = store.get(job_id)
    before_entries = store.entries(job_id)
    before_attempts = store.attempts(job_id)
    with pytest.raises(MigrationError, match="onprem_staged_required"):
        runner.retry(job_id, "url")
    assert store.get(job_id) == before_job
    assert store.entries(job_id) == before_entries
    assert store.attempts(job_id) == before_attempts
    assert destination.imports == destination.uploads == 0
    source.public_download_url = original
    source.download_url = lambda path: pytest.fail("owner URL must not be used")
    runner.retry(job_id, "staged")
    assert finish(store, runner, job_id)["state"] == "completed"
    assert source.links == ["disk:/Docs/a.txt"]
    assert destination.imports == 0 and destination.uploads == 1
    assert source.downloads == 1 and destination.upload_body == BODY


@pytest.mark.parametrize("code, auth", [
    ("source_auth_failed", True), ("source_not_found", False),
    ("source_changed", False),
])
def test_reconciliation_read_error_preserves_unknown_publication_intent(
    public_migration, code, auth,
):
    store, _, runner, job_id, source, _ = public_migration
    runner.step(job_id)
    root = store.publications(job_id)[0]
    store.update_publication(job_id, root["entry_id"], state="intent")

    def unreadable(node, **kwargs):
        raise MigrationError(code, auth=auth)

    source.ensure_publication = unreadable
    finish(store, runner, job_id)
    assert store.publications(job_id)[0]["state"] == "intent"
    assert source.published == []


@pytest.mark.parametrize("phase", ["staged", "waiting_transfer"])
def test_saved_cache_and_remote_operation_do_not_need_public_link_again(
    public_migration, phase,
):
    store, _, runner, job_id, source, destination = public_migration
    runner.step(job_id)
    store.update_job(job_id, state="running")
    runner.step(job_id)
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    attempt = store.new_attempt(entry["id"], "staged")
    if phase == "staged":
        (runner.staging / "saved.part").write_bytes(BODY)
        store.update_attempt(attempt["id"], phase=phase, local_name="saved.part")
        store.update_entry(entry["id"], state="staged")
    else:
        destination.nodes[attempt["remote_path"]] = dict(
            META, path=attempt["remote_path"],
        )
        store.update_attempt(attempt["id"], phase=phase, operation_id="op-1")
        store.update_entry(entry["id"], state="waiting_operation")
    source.keys.clear()

    def unavailable(node, **kwargs):
        raise MigrationError("source_publication_not_confirmed")

    source.ensure_publication = unavailable
    assert finish(store, runner, job_id)["state"] == "completed"
    assert source.links == []
    assert destination.nodes["disk:/owner@example.org/Docs/a.txt"]["md5"] == META["md5"]


def test_pause_during_publish_does_not_start_destination_work(public_migration):
    store, _, runner, job_id, source, destination = public_migration
    runner.step(job_id)
    store.update_job(job_id, state="running")
    source.after_publish = lambda: store.update_job(job_id, pause_requested=True)
    runner.step(job_id)
    assert source.shares == {"disk:/Docs"}
    assert store.get(job_id)["state"] == "paused"
    assert destination.nodes == {}
