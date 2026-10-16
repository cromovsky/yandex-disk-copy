"""A disabled retry skips one object, then paces the next without pausing the job."""

import pytest

from app.file_cooldown import FileCooldown
from app.migration.errors import MigrationError
from tests.fake_clock import FakeClock
from tests import test_migration_runner as fixtures
from tests import test_migration_public_runner as public_fixtures
from tests.test_migration_point_retry import queue, unknown
from tests.test_migration_runner import CONFIG, DIR, META, stage_events


unplanned_migration = fixtures.unplanned_migration
direct_migration = fixtures.direct_migration
public_migration = public_fixtures.public_migration


@pytest.fixture
def paced_errors(unplanned_migration):
    store, _, runner, job_id, source, _ = unplanned_migration
    source.walk = lambda path: [dict(DIR), dict(META), dict(
        META, path="disk:/Docs/b.txt", name="b.txt",
    )]
    clock = FakeClock()
    runner._file_cooldown = FileCooldown(seconds=20, monotonic=clock.monotonic)
    runner.step(job_id)
    runner.set_retry_enabled(job_id, False)
    store.update_job(job_id, state="running")
    return unplanned_migration, clock


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
def test_skipped_error_waits_twenty_seconds_then_continues(paced_errors, failure):
    migration, clock = paced_errors
    store, _, runner, job_id, _, destination = migration
    first = fail_first(migration, failure)
    assert runner._file_cooldown.remaining(job_id) == 20
    before = destination.uploads
    for _ in range(5):
        runner.step(job_id)
    clock.advance(19.999)
    runner.step(job_id)
    assert destination.uploads == before
    assert store.get(job_id)["state"] == "running"
    assert len(stage_events(store, job_id, "file_pause")) == 1
    assert stage_events(store, job_id, "file_pause")[0]["payload"]["retry_delay_ms"] == 20000
    assert any(e["payload"].get("entry_id") == first["id"]
               and e["payload"]["status"] == "skipped"
               for e in stage_events(store, job_id))
    clock.advance(0.001)
    runner.step(job_id)
    assert destination.uploads == before + 1
    runner.step(job_id)
    assert store.get(job_id)["state"] == "completed_with_issues"
    assert clock.sleeps == []


def test_directory_after_error_waits_but_last_error_finishes_without_wait(paced_errors):
    migration, clock = paced_errors
    store, _, runner, job_id, _, destination = migration
    first = fail_first(migration, "source")
    other = next(e for e in store.entries(job_id) if e["source_path"].endswith("b.txt"))
    store.update_entry(other["id"], state="skipped_by_user")
    directory = store.add_entries(job_id, [{
        "kind": "dir", "source_path": "disk:/Docs/later",
        "destination_path": "disk:/owner/Docs/later", "size": 0,
    }], append=True)[0]
    runner.step(job_id)
    assert store.entry(directory)["state"] == "planned"
    assert "disk:/owner/Docs/later" not in destination.nodes
    clock.advance(20)
    runner.step(job_id)
    assert store.entry(directory)["state"] == "done"
    runner.step(job_id)
    assert store.get(job_id)["state"] == "completed_with_issues"
    assert store.entry(first["id"])["error"] == "source_timeout"
    assert clock.sleeps == []


def test_final_error_finishes_immediately(paced_errors):
    migration, clock = paced_errors
    store, _, runner, job_id, _, _ = migration
    other = next(e for e in store.entries(job_id) if e["source_path"].endswith("b.txt"))
    store.update_entry(other["id"], state="skipped_by_user")
    fail_first(migration, "source")
    runner.step(job_id)
    assert store.get(job_id)["state"] == "completed_with_issues"
    assert runner._file_cooldown.remaining(job_id) == 20
    assert not stage_events(store, job_id, "file_pause")
    assert clock.sleeps == []


def test_pause_can_resume_and_stop_interrupts_error_wait(paced_errors):
    migration, clock = paced_errors
    store, _, runner, job_id, _, destination = migration
    fail_first(migration, "source")
    store.update_job(job_id, pause_requested=True)
    runner.step(job_id)
    assert store.get(job_id)["state"] == "paused"
    store.update_job(job_id, state="running", pause_requested=False)
    runner.step(job_id)
    assert destination.uploads == 0
    assert store.get(job_id)["state"] == "running"
    store.request_stop(job_id)
    runner.stop_job(job_id)
    runner.step(job_id)
    assert store.get(job_id)["state"] == "stopped"
    assert runner._file_cooldown.remaining(job_id) == 0
    assert clock.sleeps == []


def test_error_cooldown_does_not_delay_another_source(paced_errors):
    migration, _ = paced_errors
    store, registry, runner, job_id, source, destination = migration
    fail_first(migration, "source")
    source.walk = lambda path: [dict(DIR), dict(META, path="disk:/Docs/b.txt", name="b.txt")]
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
    assert runner._file_cooldown.remaining(job_id) == 20
    assert runner._file_cooldown.remaining(other) == 20


def test_enabled_retries_keep_the_download_retry_schedule(paced_errors):
    migration, _ = paced_errors
    store, _, runner, job_id, source, _ = migration
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
    assert runner._file_cooldown.remaining(job_id) == 0
    assert not stage_events(store, job_id, "file_pause")


def test_point_retry_error_starts_cooldown_before_the_queue_continues(direct_migration):
    store, _, runner, job_id, _, destination = direct_migration
    clock = FakeClock()
    runner._file_cooldown = FileCooldown(seconds=20, monotonic=clock.monotonic)
    runner.set_retry_enabled(job_id, False)
    entry, attempt = unknown(direct_migration, cache=True)
    destination.upload_error = MigrationError("destination_unavailable", http_status=503)
    queue(direct_migration, entry, attempt)
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] == "failed"
    assert not store.entry(entry["id"])["point_retry_requested"]
    assert store.get(job_id)["state"] == "running"
    assert runner._file_cooldown.remaining(job_id) == 20
    directory = next(e for e in store.entries(job_id) if e["kind"] == "dir")
    runner.step(job_id)
    assert store.entry(directory["id"])["state"] == "planned"
    clock.advance(20)
    runner.step(job_id)
    assert store.entry(directory["id"])["state"] == "done"


def test_publication_failure_waits_before_the_next_root(public_migration):
    store, registry, runner, old_id, source, destination = public_migration
    config = dict(store.get(old_id)["config"], source_path="disk:/", retry_enabled=False)
    job_id = store.create(config, "paced-publication-error")["id"]
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
    clock = FakeClock()
    runner._file_cooldown = FileCooldown(seconds=20, monotonic=clock.monotonic)
    runner.step(job_id)
    store.update_job(job_id, state="running")
    runner.step(job_id)
    assert calls == ["disk:/a.txt"]
    assert destination.uploads == 0
    for _ in range(5):
        runner.step(job_id)
    assert calls == ["disk:/a.txt"]
    assert len(stage_events(store, job_id, "file_pause")) == 1
    clock.advance(20)
    runner.step(job_id)
    assert calls == ["disk:/a.txt", "disk:/b.txt"]
    assert destination.uploads == 1
    runner.step(job_id)
    assert store.get(job_id)["state"] == "completed_with_issues"


def test_each_skipped_error_logs_a_new_wait(paced_errors):
    migration, clock = paced_errors
    store, _, runner, job_id, source, _ = migration
    fail_first(migration, "source")
    runner.step(job_id)
    assert len(stage_events(store, job_id, "file_pause")) == 1
    clock.advance(20)

    def chunks(path):
        raise MigrationError("source_timeout", http_status=504)
        yield
    source.chunks = chunks
    runner.step(job_id)
    third = store.add_entries(job_id, [{
        "kind": "dir", "source_path": "disk:/Docs/later",
        "destination_path": "disk:/owner/Docs/later", "size": 0,
    }], append=True)[0]
    runner.step(job_id)
    assert store.entry(third)["state"] == "planned"
    waits = stage_events(store, job_id, "file_pause")
    assert len(waits) == 2
    assert [event["payload"]["retry_delay_ms"] for event in waits] == [20000, 20000]


def test_failed_directory_and_subtree_wait_before_next_directory(paced_errors):
    migration, clock = paced_errors
    store, _, runner, job_id, _, destination = migration
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
    assert store.entry(other)["state"] == "planned"
    assert "disk:/owner/Elsewhere" not in destination.nodes
    clock.advance(20)
    runner.step(job_id)
    assert store.entry(other)["state"] == "done"
    runner.step(job_id)
    assert store.get(job_id)["state"] == "completed_with_issues"


def test_accepted_operation_is_polled_during_error_cooldown(paced_errors):
    migration, _ = paced_errors
    store, _, runner, job_id, _, destination = migration
    fail_first(migration, "source")
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
