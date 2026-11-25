"""With retries disabled, errors are logged and the queue advances immediately."""

import pytest

from app.migration.errors import MigrationError
from tests import test_migration_file_pause as queue_fixtures
from tests import test_migration_runner as fixtures
from tests import test_migration_public_runner as public_fixtures
from tests.test_migration_point_retry import queue, unknown
from tests.test_migration_runner import CONFIG, META, stage_events


unplanned_migration = fixtures.unplanned_migration
two_files = queue_fixtures.two_files
frozen_clock = queue_fixtures.frozen_clock
direct_migration = fixtures.direct_migration
public_migration = public_fixtures.public_migration


@pytest.fixture
def two_files_no_retries(two_files):
    _, _, runner, job_id, _, _ = two_files
    runner.set_retry_enabled(job_id, False)
    return two_files


def fail_first(migration, failure):
    store, _, runner, job_id, source, destination = migration
    first = next(e for e in store.entries(job_id) if e["source_path"] == META["path"])
    if failure == "source":
        original = source.chunks

        def chunks(path):
            if path == META["path"]:
                raise MigrationError("source_timeout", http_status=504)
            yield from original(path)
        source.chunks = chunks
    elif failure == "destination":
        original = destination.upload

        def upload(path, *args, **kwargs):
            if path.endswith("/a.txt"):
                raise MigrationError("destination_unavailable", http_status=503)
            return original(path, *args, **kwargs)
        destination.upload = upload
    elif failure == "verification":
        original = source.stat
        source.stat = lambda path: dict(META, modified="changed") \
            if path == META["path"] else original(path)
    else:
        raise AssertionError(failure)
    for _ in range(10):
        runner.step(job_id)
        if store.entry(first["id"])["state"] not in {"planned", "transferring"}:
            break
    assert store.entry(first["id"])["state"] in {"failed", "source_changed"}
    assert store.get(job_id)["state"] == "running"
    return first


@pytest.mark.parametrize("failure", ["source", "destination", "verification"])
def test_skipped_error_immediately_advances_to_next_file(two_files_no_retries, frozen_clock, failure):
    store, _, runner, job_id, _, destination = two_files_no_retries
    first = fail_first(two_files_no_retries, failure)
    before = destination.uploads
    assert any(e["payload"].get("entry_id") == first["id"]
               and e["payload"]["status"] == "skipped"
               for e in stage_events(store, job_id))
    runner.step(job_id)
    assert destination.uploads == before + 1
    runner.step(job_id)
    assert store.get(job_id)["state"] == "completed_with_issues"
    assert frozen_clock.monotonic() == 0
    assert not stage_events(store, job_id, "file_pause")


def test_directory_after_error_starts_immediately(two_files_no_retries):
    store, _, runner, job_id, _, destination = two_files_no_retries
    first = fail_first(two_files_no_retries, "source")
    other = next(e for e in store.entries(job_id) if e["source_path"].endswith("b.txt"))
    store.update_entry(other["id"], state="skipped_by_user")
    directory = store.add_entries(job_id, [{
        "kind": "dir", "source_path": "disk:/Docs/later",
        "destination_path": "disk:/owner/Docs/later", "size": 0,
    }], append=True)[0]
    runner.step(job_id)
    assert store.entry(directory)["state"] == "done"
    runner.step(job_id)
    assert store.get(job_id)["state"] == "completed_with_issues"
    assert store.entry(first["id"])["error"] == "source_timeout"


def test_final_error_finishes_immediately(two_files_no_retries):
    store, _, runner, job_id, _, _ = two_files_no_retries
    other = next(e for e in store.entries(job_id) if e["source_path"].endswith("b.txt"))
    store.update_entry(other["id"], state="skipped_by_user")
    fail_first(two_files_no_retries, "source")
    runner.step(job_id)
    assert store.get(job_id)["state"] == "completed_with_issues"
    assert not stage_events(store, job_id, "file_pause")


def test_pause_before_next_file_after_error(two_files_no_retries):
    store, _, runner, job_id, _, destination = two_files_no_retries
    fail_first(two_files_no_retries, "source")
    store.update_job(job_id, pause_requested=True)
    runner.step(job_id)
    assert store.get(job_id)["state"] == "paused"
    store.update_job(job_id, state="running", pause_requested=False)
    runner.step(job_id)
    assert destination.uploads == 1


def test_skipped_error_does_not_delay_another_source(two_files_no_retries):
    store, registry, runner, job_id, source, destination = two_files_no_retries
    fail_first(two_files_no_retries, "source")
    source.walk = lambda path: [dict(fixtures.DIR), dict(META, path="disk:/Docs/b.txt", name="b.txt")]
    config = dict(CONFIG, source_email="other@example.org", destination_root="disk:/other")
    other = store.create(config, "other-source-after-error")["id"]
    registry.put(other, config, {"source_client_secret": "s", "destination_token": "d"})
    runner.step(other)
    store.update_job(other, state="running")
    for _ in range(10):
        runner.step(other)
        if destination.uploads:
            break
    assert destination.uploads == 1


def test_enabled_retries_keep_the_download_retry_schedule(two_files_no_retries):
    store, _, runner, job_id, source, _ = two_files_no_retries
    runner.set_retry_enabled(job_id, True)

    def chunks(path):
        raise MigrationError("source_timeout", http_status=504)
        yield
    source.chunks = chunks
    for _ in range(10):
        runner.step(job_id)
        if stage_events(store, job_id, "download_retry"):
            break
    first = next(e for e in store.entries(job_id) if e["source_path"] == META["path"])
    assert first["retry_at_ms"] is not None
    assert first["download_attempts"] == 1
    assert store.attempt(first["attempt_id"])["phase"] == "download_retry_wait"
    assert not stage_events(store, job_id, "file_pause")


def test_queue_continues_immediately_after_failed_point_retry(direct_migration):
    store, _, runner, job_id, _, destination = direct_migration
    runner.set_retry_enabled(job_id, False)
    entry, attempt = unknown(direct_migration, cache=True)
    destination.upload_error = MigrationError("destination_unavailable", http_status=503)
    queue(direct_migration, entry, attempt)
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] == "failed"
    assert not store.entry(entry["id"])["point_retry_requested"]
    assert store.get(job_id)["state"] == "running"
    directory = next(e for e in store.entries(job_id) if e["kind"] == "dir")
    runner.step(job_id)
    assert store.entry(directory["id"])["state"] == "done"


def test_publication_failure_allows_next_root_immediately(public_migration):
    store, registry, runner, old_id, source, destination = public_migration
    config = dict(store.get(old_id)["config"], source_path="disk:/", retry_enabled=False)
    job_id = store.create(config, "skipped-publication")["id"]
    registry.put(job_id, config, registry.get(old_id))
    nodes = [dict(META, path="disk:/a.txt"), dict(META, path="disk:/b.txt")]
    source.walk = lambda path: nodes
    source.stat = lambda path: {"type": "dir", "path": path} if path == "disk:/" else next(
        dict(node) for node in nodes if node["path"] == path
    )
    original = source.ensure_publication
    calls = []

    def publish(node, **kwargs):
        calls.append(node["source_path"])
        if node["source_path"] == "disk:/a.txt":
            raise MigrationError("source_public_access_denied", http_status=403)
        return original(node, **kwargs)
    source.ensure_publication = publish
    runner.step(job_id)
    store.update_job(job_id, state="running")
    runner.step(job_id)
    assert calls == ["disk:/a.txt", "disk:/b.txt"]
    assert destination.uploads == 1
    runner.step(job_id)
    assert store.get(job_id)["state"] == "completed_with_issues"


def test_failed_subtree_allows_next_directory_immediately(two_files_no_retries):
    store, _, runner, job_id, _, destination = two_files_no_retries
    original = destination.ensure_dir

    def ensure_dir(path):
        if path == "disk:/owner/Docs":
            raise MigrationError("destination_unavailable", http_status=503)
        return original(path)
    destination.ensure_dir = ensure_dir
    runner.step(job_id)
    assert all(e["state"] == "failed" for e in store.entries(job_id))
    other = store.add_entries(job_id, [{
        "kind": "dir", "source_path": "disk:/Elsewhere",
        "destination_path": "disk:/owner/Elsewhere", "size": 0,
    }], append=True)[0]
    runner.step(job_id)
    assert store.entry(other)["state"] == "done"
    runner.step(job_id)
    assert store.get(job_id)["state"] == "completed_with_issues"


def test_accepted_operation_is_polled_after_a_skipped_error(two_files_no_retries):
    store, _, runner, job_id, _, destination = two_files_no_retries
    fail_first(two_files_no_retries, "source")
    second = next(e for e in store.entries(job_id) if e["source_path"].endswith("b.txt"))
    attempt = store.new_attempt(second["id"], "staged")
    store.update_attempt(attempt["id"], phase="waiting_transfer", operation_id="op-1")
    store.update_entry(second["id"], state="waiting_operation")
    destination.nodes[attempt["remote_path"]] = dict(META, path=attempt["remote_path"])
    destination.operation = "in-progress"
    runner.step(job_id)
    assert stage_events(store, job_id, "poll_transfer")
    assert store.entry(second["id"])["state"] == "waiting_operation"
    destination.operation = "success"
    runner.step(job_id)
    assert store.entry(second["id"])["state"] == "done"
    runner.step(job_id)
    assert store.get(job_id)["state"] == "completed_with_issues"
    assert destination.uploads == 0
