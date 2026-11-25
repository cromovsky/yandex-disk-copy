"""Publish only approved roots, preserving checkpoints and download limits."""

from collections import Counter

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
        self.during_publish = None
        self.publish_errors = {}

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
        if self.during_publish:
            self.during_publish()
        if self.publish_errors.get(path):
            raise self.publish_errors[path].pop(0)
        self.shares.add(path)
        self.keys.add(path)
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
    source.publish_errors[DIR["path"]] = [
        MigrationError("source_publication_outcome_unknown", uncertain=True, http_status=503),
    ]
    assert finish(store, runner, job_id)["state"] == "needs_attention"
    assert store.publications(job_id)[0]["state"] == "intent"
    assert source.published == [DIR["path"]]
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


@pytest.mark.parametrize("http,success_attempt", [(403, 2), (423, 3)])
def test_confirmed_denial_retries_until_publication_succeeds(
    public_migration, http, success_attempt,
):
    store, _, runner, job_id, source, destination = public_migration
    source.publish_errors[DIR["path"]] = [
        MigrationError("source_publication_denied", http_status=http)
        for _ in range(success_attempt - 1)
    ]
    runner.step(job_id)
    assert finish(store, runner, job_id)["state"] == "completed"
    assert source.published == [DIR["path"]] * success_attempt
    assert store.publications(job_id)[0]["state"] == "published"
    assert destination.uploads == 1 and destination.upload_body == BODY


@pytest.mark.parametrize("enabled,limit", [(True, 3), (False, 1)])
def test_denied_publications_skip_only_their_own_files_and_folders(
    public_migration, enabled, limit,
):
    store, registry, runner, old_id, source, destination = public_migration
    config = dict(store.get(old_id)["config"], source_path="disk:/", retry_enabled=enabled)
    job_id = store.create(config, "multiple-denials")["id"]
    registry.put(job_id, config, registry.get(old_id))
    separate = dict(META, name="private.txt", path="disk:/private.txt", resource_id="private")
    neighbour = dict(META, name="Docs-copy.txt", path="disk:/Docs-copy.txt", resource_id="copy")
    nested = dict(DIR, name="empty", path="disk:/Docs/empty")
    tree = {n["path"]: n for n in (DIR, META, nested, separate, neighbour)}
    source.walk = lambda path: tree.values()
    source.stat = lambda path: {"type": "dir"} if path == "disk:/" else tree[path]
    denials = {
        DIR["path"]: (423, "DiskResourceLockedError"),
        separate["path"]: (403, "ForbiddenError"),
    }
    for path, (http, api) in denials.items():
        source.publish_errors[path] = [
            MigrationError("source_publication_denied", http_status=http,
                           diagnostics={"error_side": "saas", "api_error": api})
            for _ in range(limit)
        ]
    runner.step(job_id)
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert Counter(source.published) == {
        DIR["path"]: limit, separate["path"]: limit, neighbour["path"]: 1,
    }
    assert source.links == [neighbour["path"]]
    assert destination.uploads == 1 and destination.upload_body == BODY
    assert destination.nodes["disk:/owner@example.org/Docs-copy.txt"]["md5"] == META["md5"]

    entries = {e["source_path"]: e for e in store.entries(job_id)}
    skipped = {
        e["context"]["source"]: e["payload"] for e in stage_events(store, job_id, "publish_source")
        if e["payload"]["status"] == "skipped"
    }
    assert entries[neighbour["path"]]["state"] == "done"
    assert set(skipped) == tree.keys() - {neighbour["path"]}
    for path, event in skipped.items():
        http, api = denials.get(path, denials[DIR["path"]])
        assert entries[path]["state"] == "failed"
        assert entries[path]["error"] == event["error"] == "source_publication_denied"
        assert (event["http_status"], event["api_error"], event["error_side"]) == (http, api, "saas")
        assert event["retry_enabled"] is enabled
        assert event["publication_attempts"] == event["max_publication_attempts"] == limit

    runner.retry(job_id, "staged")
    assert finish(store, runner, job_id)["state"] == "completed"
    assert source.published.count(neighbour["path"]) == 1
    assert destination.uploads == 3


def test_denial_budget_survives_worker_restart(public_migration):
    store, registry, runner, job_id, source, destination = public_migration
    source.publish_errors[DIR["path"]] = [
        MigrationError("source_publication_denied", http_status=403) for _ in range(3)
    ]
    runner.step(job_id)
    store.update_job(job_id, state="running")
    runner.step(job_id)
    assert store.get(job_id)["state"] == "running"
    assert source.published == [DIR["path"]]
    runner.close()
    store.close()
    reopened = MigrationStore(store.root)
    restarted = MigrationRunner(
        reopened, registry, source_factory=lambda c, a: source,
        destination_factory=lambda c, a: destination, reserve_bytes=0,
    )
    try:
        assert finish(reopened, restarted, job_id)["state"] == "completed_with_issues"
        assert source.published == [DIR["path"]] * 3
        assert destination.uploads == 0
    finally:
        restarted.close()
        reopened.close()


@pytest.mark.parametrize("switch_at,attempts,http,api", [
    ("before_publish", 1, 423, "DiskResourceLockedError"),
    ("during_publish", 2, 403, "ForbiddenError"),
])
def test_disabling_retries_stops_new_puts_and_keeps_last_denial(
    public_migration, switch_at, attempts, http, api,
):
    store, _, runner, job_id, source, destination = public_migration
    source.publish_errors[DIR["path"]] = [
        MigrationError("source_publication_denied", http_status=423,
                       diagnostics={"api_error": "DiskResourceLockedError"}),
        MigrationError("source_publication_denied", http_status=403,
                       diagnostics={"api_error": "ForbiddenError"}),
    ]
    runner.step(job_id)
    store.update_job(job_id, state="running")
    runner.step(job_id)
    assert source.published == [DIR["path"]]

    def disable_during_next_publication():
        runner.set_retry_enabled(job_id, False)
        runner.set_retry_enabled(job_id, True)

    setattr(source, switch_at, disable_during_next_publication)
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert source.published == [DIR["path"]] * attempts
    assert destination.uploads == 0
    skipped = [e["payload"] for e in stage_events(store, job_id, "publish_source")
               if e["payload"]["status"] == "skipped"]
    assert skipped
    for event in skipped:
        assert (event["http_status"], event["api_error"]) == (http, api)
        assert event["publication_attempts"] == event["max_publication_attempts"] == attempts
        assert event["retry_enabled"] is False


def test_publication_auth_error_supersedes_pending_denial_when_switch_changes(public_migration):
    store, _, runner, job_id, source, _ = public_migration
    source.publish_errors[DIR["path"]] = [
        MigrationError("source_publication_denied", http_status=403),
        MigrationError("source_auth_failed", http_status=401, auth=True),
    ]
    runner.step(job_id)
    store.update_job(job_id, state="running")
    runner.step(job_id)

    def toggle_during_second_put():
        runner.set_retry_enabled(job_id, False)
        runner.set_retry_enabled(job_id, True)

    source.during_publish = toggle_during_second_put
    runner.step(job_id)
    assert store.get(job_id)["state"] == "awaiting_credentials"
    runner.set_retry_enabled(job_id, False)
    job = store.get(job_id)
    assert job["config"]["retry_enabled"] is False
    assert job["state"] == "awaiting_credentials" and job["error"] == "source_auth_failed"
    assert source.published == [DIR["path"]] * 2
    assert store.publications(job_id)[0]["publication_attempts"] == 1
    assert all(e["state"] == "planned" for e in store.entries(job_id))
    assert all(e["payload"]["status"] != "skipped" for e in stage_events(store, job_id, "publish_source"))


@pytest.mark.parametrize("additional_denials,copied", [(2, True), (3, False)])
def test_point_retry_of_file_under_denied_root_has_its_own_publication_budget(
    public_migration, additional_denials, copied,
):
    store, _, runner, job_id, source, destination = public_migration
    source.publish_errors[DIR["path"]] = [
        MigrationError("source_publication_denied", http_status=403)
        for _ in range(3 + additional_denials)
    ]
    runner.step(job_id)
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    entry = next(e for e in store.entries(job_id) if e["source_path"] == META["path"])
    store.queue_point_retry(job_id, entry["id"], None, store.get(job_id)["revision"])
    finish(store, runner, job_id)
    current = store.entry(entry["id"])
    assert current["state"] == ("done" if copied else "failed")
    assert not current["point_retry_requested"]
    assert source.published == [DIR["path"]] * 6
    assert destination.uploads == int(copied)


@pytest.mark.parametrize("enabled", [True, False])
def test_publication_auth_failure_requires_credentials(public_migration, enabled):
    store, registry, runner, job_id, source, destination = public_migration
    runner.set_retry_enabled(job_id, enabled)
    source.publish_errors[DIR["path"]] = [
        MigrationError("source_auth_failed", http_status=401, auth=True),
    ]
    runner.step(job_id)
    assert finish(store, runner, job_id)["state"] == "awaiting_credentials"
    assert source.published == [DIR["path"]]
    assert not registry.get(job_id)
    assert destination.uploads == 0


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
    source.during_publish = lambda: store.update_job(job_id, pause_requested=True)
    runner.step(job_id)
    assert source.shares == {"disk:/Docs"}
    assert store.get(job_id)["state"] == "paused"
    assert destination.nodes == {}
