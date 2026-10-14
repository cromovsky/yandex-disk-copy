"""Public SaaS links feed local staging; old URL operations are never replayed."""

import json

import pytest

from app.migration.errors import MigrationError
from tests.test_migration_public_runner import public_migration
from tests.test_migration_runner import (
    BODY, DIR, META, Source, finish, legacy_job,
)


@pytest.fixture
def public_download(public_migration):
    store, _, runner, job_id, source, destination = public_migration
    source.download_url = lambda path: pytest.fail("owner URL must not be used")
    links = []

    def link(path):
        assert any(path == root or path.startswith(root + "/") for root in source.keys)
        source.links.append(path)
        return f"https://public.example.org/file?private={len(source.links)}"

    def chunks(path, *, download_url=None):
        assert download_url == f"https://public.example.org/file?private={len(source.links)}"
        links.append((path, download_url))
        yield from Source.chunks(source, path)

    source.public_download_url = link
    source.chunks = chunks
    yield store, runner, job_id, source, destination, links
    assert destination.imports == 0



def accepted_public_url_operation(store, job_id, source, destination):
    """Reconstruct an old accepted operation from this public source's metadata."""
    entry = next(row for row in store.entries(job_id) if row["kind"] == "file")
    for directory in store.entries(job_id):
        if directory["kind"] == "dir":
            store.update_entry(directory["id"], state="done")
    attempt = store.new_attempt(entry["id"], "url")
    store.update_attempt(attempt["id"], phase="waiting_transfer", operation_id="op-1")
    store.update_entry(entry["id"], state="waiting_operation", download_attempts=1)
    store.update_job(job_id, state="running", namespace_state="owned")
    destination.nodes[attempt["remote_path"]] = dict(
        source.stat(entry["source_path"]), path=attempt["remote_path"],
    )
    return store.attempt(attempt["id"])


def test_public_staged_transfer_keeps_tree_and_cleans_local_cache(public_download):
    store, runner, job_id, source, destination, links = public_download
    runner.step(job_id)
    assert store.get(job_id)["state"] == "ready"
    assert source.published == source.links == []
    assert finish(store, runner, job_id)["state"] == "completed"
    assert source.published == ["disk:/Docs"]
    assert source.links == [META["path"]]
    assert destination.uploads == 1
    assert destination.moves == 0
    assert links == [(META["path"], "https://public.example.org/file?private=1")]
    assert destination.nodes["disk:/owner@example.org/Docs/a.txt"]["md5"] == META["md5"]
    assert destination.nodes["disk:/owner@example.org/Docs/empty"]["type"] == "dir"
    assert destination.upload_body == BODY
    assert source.downloads == 1
    assert not list(runner.staging.iterdir())
    attempt = store.attempts(job_id)[0]
    assert attempt["strategy"] == "staged" and attempt["local_name"] is None
    assert attempt["remote_path"] == "disk:/owner@example.org/Docs/a.txt"
    assert not any(".disk-copy" in path for path in destination.nodes)
    events = store.events(job_id, limit=1000, journal=True)
    stages = {e["payload"].get("stage") for e in events}
    assert {"public_download_link", "downloading", "upload_file"} <= stages
    assert "submit_url_upload" not in stages
    assert "private=" not in json.dumps(events)
    assert b"private=" not in store.path.read_bytes()
    assert source.shares == {"disk:/Docs"}


def test_terminal_historical_url_failure_never_refreshes_link_or_replays(
    public_download,
):
    store, runner, job_id, source, destination, links = public_download
    runner.step(job_id)
    previous = accepted_public_url_operation(store, job_id, source, destination)
    destination.operation = "failed"
    runner.step(job_id)
    entry = store.entry(previous["entry_id"])
    assert entry["download_attempts"] == 1
    assert entry["error"] == "destination_url_import_failed"
    assert store.attempt(previous["id"])["phase"] == "failed"
    assert store.attempt(previous["id"])["operation_id"] == "op-1"
    runner.step(job_id)
    assert store.get(job_id)["error"] == "onprem_staged_required"
    assert len(store.attempts(job_id)) == 1
    assert source.published == source.links == links == []
    assert destination.uploads == source.downloads == destination.moves == 0
    failed = [e for e in store.events(job_id, limit=1000, journal=True)
              if e["payload"].get("remote_status") == "failed"]
    assert len(failed) == 1
    assert failed[0]["payload"]["error"] == "destination_url_import_failed"
    assert "On-premises" in failed[0]["error_message"]
    assert not list(runner.staging.iterdir())


def test_accepted_historical_url_operation_needs_no_public_link_after_restart(
    public_download,
):
    store, runner, job_id, source, destination, links = public_download
    runner.step(job_id)
    previous = accepted_public_url_operation(store, job_id, source, destination)
    destination.operation = "in-progress"
    source.keys.clear()
    source.ensure_publication = lambda *args, **kwargs: pytest.fail("no publication")
    source.public_download_url = lambda path: pytest.fail("do not resubmit import")
    source.chunks = lambda *args, **kwargs: pytest.fail("no fresh download")
    for _ in range(3):
        runner.step(job_id)
    assert store.attempt(previous["id"]) == previous
    assert source.published == source.links == links == []
    destination.operation = "success"
    runner.step(job_id)
    assert store.entry(previous["entry_id"])["state"] == "done"
    assert destination.imports == destination.uploads == source.downloads == 0


@pytest.mark.parametrize("legacy", [False, True])
def test_public_upload_outage_stops_retry_and_explicit_retry_reuses_cache(
    public_download, legacy,
):
    store, runner, job_id, source, destination, links = public_download
    if legacy:
        legacy_job(store, job_id)
    destination.upload_error = MigrationError("destination_timeout", uncertain=True)
    runner.step(job_id)
    assert finish(store, runner, job_id)["state"] == "needs_attention"
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    old = store.attempt(entry["attempt_id"])
    assert entry["state"] == "failed"
    assert old["phase"] == "abandoned_transfer"
    assert entry["download_attempts"] == source.downloads == len(links) == 1
    cached = runner._local(old["local_name"])
    assert cached.read_bytes() == BODY
    source.keys.clear()
    source.public_download_url = lambda path: pytest.fail("retry must use saved bytes")
    source.chunks = lambda *args, **kwargs: pytest.fail("retry must not redownload")
    store.queue_point_retry(job_id, entry["id"], old["id"],
                            store.get(job_id)["revision"])
    runner.step(job_id)
    current = store.entry(entry["id"])
    second = store.attempt(current["attempt_id"])
    assert current["point_retry_attempts"] == 1
    assert current["state"] == "failed"
    assert current["error"] == "destination_timeout"
    assert current["retry_at_ms"] is None
    assert not current["point_retry_requested"]
    assert second["phase"] == "abandoned_transfer"
    assert cached.read_bytes() == BODY
    assert destination.uploads == 2
    for _ in range(3):
        runner.step(job_id)
    assert store.get(job_id)["state"] == "needs_attention"
    assert destination.uploads == 2
    destination.upload_error = None
    store.queue_point_retry(job_id, entry["id"], second["id"],
                            store.get(job_id)["revision"])
    runner.step(job_id)
    current = store.entry(entry["id"])
    assert current["point_retry_attempts"] == 1
    assert current["state"] == "done"
    assert not current["point_retry_requested"]
    assert destination.uploads == len(store.attempts(job_id)) == 3
    assert len({attempt["remote_path"] for attempt in store.attempts(job_id)}) \
        == (3 if legacy else 1)
    assert source.downloads == len(links) == 1
    assert runner.final_pending(job_id) == []
    assert source.published == ["disk:/Docs"]
    assert destination.moves == int(legacy)
    assert destination.upload_body == BODY
    assert not cached.exists()
    assert not list(runner.staging.iterdir())
    assert "private=" not in json.dumps(store.events(job_id, limit=1000, journal=True))


@pytest.mark.parametrize("selected, expected", [
    ("disk:/", ["disk:/Docs", "disk:/report.txt"]),
    ("disk:/report.txt", ["disk:/report.txt"]),
])
def test_public_staging_entire_disk_or_file_uses_only_publication_roots(
    public_download, selected, expected,
):
    store, runner, old_id, source, destination, links = public_download
    config = dict(store.get(old_id)["config"], source_path=selected)
    job_id = store.create(config, "other-public-scope")["id"]
    runner.registry.put(job_id, config, runner.registry.get(old_id))
    tree = [dict(DIR), dict(META), dict(
        META, path="disk:/report.txt", name="report.txt", resource_id="file-2",
    )]
    source.walk = lambda path: tree if path == "disk:/" else [tree[-1]]
    source.stat = lambda path: (
        {"type": "dir", "path": "disk:/"} if path == "disk:/"
        else next(n for n in tree if n["path"] == path)
    )
    runner.step(job_id)
    assert finish(store, runner, job_id)["state"] == "completed"
    assert source.published == expected
    assert sorted(source.links) == (
        [META["path"], "disk:/report.txt"]
        if selected == "disk:/" else ["disk:/report.txt"]
    )
    assert destination.nodes["disk:/owner@example.org/report.txt"]["md5"] == META["md5"]
    assert destination.uploads == len(links) == (2 if selected == "disk:/" else 1)


def test_public_link_failure_reports_saas_reason_without_upload(public_download):
    store, runner, job_id, source, destination, links = public_download

    def unavailable(path):
        source.links.append(path)
        raise MigrationError("source_public_download_failed", http_status=503)

    source.public_download_url = unavailable
    runner.step(job_id)
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    assert len(source.links) == entry["download_attempts"] == 5
    assert entry["error"] == "source_public_download_failed"
    assert destination.uploads == source.downloads == 0 and links == []
    assert not list(runner.staging.iterdir())
    failed = [e for e in store.events(job_id, limit=1000, journal=True)
              if e["payload"].get("stage") == "public_download_link"
              and e["payload"].get("status") == "failed"]
    assert len(failed) == 5
    assert all("SaaS" in e["error_message"]
               and e["payload"]["http_status"] == 503 for e in failed)


def test_stop_after_public_link_prevents_staged_upload(public_download):
    store, runner, job_id, source, destination, links = public_download
    original = source.public_download_url

    def stopped(path):
        href = original(path)
        store.request_stop(job_id)
        return href

    source.public_download_url = stopped
    runner.step(job_id)
    finish(store, runner, job_id)
    assert len(source.links) == 1
    assert destination.uploads == 0
    assert source.shares == {"disk:/Docs"}
    cached = list(runner.staging.iterdir())
    assert len(cached) == 1 and cached[0].stat().st_size == 0
    attempt = store.attempts(job_id)[0]
    assert attempt["strategy"] == "staged"
    assert attempt["phase"] == "downloading" and attempt["operation_id"] is None
    assert store.get(job_id)["stop_requested"] is True


def test_new_public_url_job_requires_new_staged_dry_run(public_download):
    store, runner, old_id, source, destination, links = public_download
    config = dict(store.get(old_id)["config"], strategy="url")
    job_id = store.create(config, "retired-public-url-job")["id"]
    runner.registry.put(job_id, config, runner.registry.get(old_id))
    runner.step(job_id)
    assert store.get(job_id)["state"] == "needs_attention"
    assert store.get(job_id)["error"] == "onprem_staged_required"
    assert store.entries(job_id) == store.attempts(job_id) == []
    assert source.published == source.links == links == []
    assert destination.uploads == source.downloads == 0


def test_public_download_retry_refreshes_link_and_removes_partial_bytes(public_download):
    store, runner, job_id, source, destination, links = public_download
    original = source.chunks

    def interrupted(path, *, download_url=None):
        for chunk in original(path, download_url=download_url):
            yield chunk
            if len(links) < 3:
                raise MigrationError("source_stream_interrupted", http_status=503)

    source.chunks = interrupted
    runner.step(job_id)
    assert finish(store, runner, job_id)["state"] == "completed"
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    assert entry["download_attempts"] == source.downloads == len(links) == 3
    assert len({url for _, url in links}) == 3
    assert len(store.attempts(job_id)) == destination.uploads == 1
    assert destination.upload_body == BODY
    assert not list(runner.staging.iterdir())
    failed = [event for event in store.events(job_id, limit=1000, journal=True)
              if event["payload"].get("stage") == "downloading"
              and event["payload"].get("status") == "failed"]
    assert len(failed) == 2
    assert all(event["payload"]["error"] == "source_stream_interrupted"
               and event["payload"]["http_status"] == 503
               and "SaaS" in event["error_message"] for event in failed)
    assert "private=" not in json.dumps(store.events(job_id, limit=1000, journal=True))
