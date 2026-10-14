"""Download retry budgets survive outages without replaying accepted uploads."""

import pytest

from app.migration.errors import MigrationError
from app.migration.credentials import CredentialRegistry
from app.migration.runner import MigrationRunner
from app.migration.store import MigrationStore
from tests import test_migration_runner as runner_fixtures
from tests.test_migration_runner import (
    BODY, CONFIG, DIR, META, finish, run_until_attempt, stage_events,
)

migration = runner_fixtures.migration
unplanned_migration = runner_fixtures.unplanned_migration


def test_exhausted_first_file_does_not_prevent_second_file_upload(
    unplanned_migration,
):
    store, _, runner, job_id, source, destination = unplanned_migration
    second = dict(META, path="disk:/Docs/b.txt", name="b.txt", resource_id="file-2")
    source.walk = lambda path: [dict(DIR), dict(META), dict(second)]
    source.stat = lambda path: dict(
        DIR if path == DIR["path"] else META if path == META["path"] else second
    )
    calls = []

    def chunks(path):
        calls.append(path)
        if path == META["path"]:
            yield BODY[:1]
            raise MigrationError("source_stream_interrupted", http_status=503)
        yield BODY

    source.chunks = chunks
    runner.default_strategy = "staged"
    runner.step(job_id)
    result = finish(store, runner, job_id)
    entries = {entry["source_path"]: entry for entry in store.entries(job_id)
               if entry["kind"] == "file"}
    assert result["state"] == "completed_with_issues"
    assert calls == [META["path"]] * 5 + [second["path"]]
    first, last = entries[META["path"]], entries[second["path"]]
    assert (first["state"], first["download_attempts"]) == ("failed", 5)
    assert (last["state"], last["download_attempts"]) == ("done", 1)
    assert first["destination_path"] not in destination.nodes
    assert destination.nodes[last["destination_path"]]["md5"] == second["md5"]
    assert destination.upload_body == BODY
    assert destination.moves == 1
    assert result["verified_bytes"] == len(BODY)
    assert not list(runner.staging.iterdir())
    failures = [event["payload"] for event in stage_events(store, job_id, "downloading")
                if event["payload"]["status"] == "failed"]
    assert [event["download_attempts"] for event in failures] == [1, 2, 3, 4, 5]
    assert all(event["error"] == "source_stream_interrupted"
               and event["http_status"] == 503 for event in failures)
    skipped = stage_events(store, job_id, "download_retry")[-1]["payload"]
    assert skipped["status"] == "skipped" and skipped["entry_id"] == first["id"]


@pytest.mark.parametrize("retry_after,expected_delay", [
    (60, 60000), (None, 2000), (-1, 2000), (True, 2000), ("60", 2000),
    (float("nan"), 2000), (float("inf"), 2000), (10 ** 400, 2000),
    (1e308, None),
])
def test_retry_after_is_honored_or_safely_ignored_and_clamped(
    migration, monkeypatch, retry_after, expected_delay,
):
    store, _, runner, job_id, source, _ = migration
    now = 1700000000
    monkeypatch.setattr("app.migration.runner.time.time", lambda: now)
    monkeypatch.setattr("app.migration.runner.random.uniform", lambda a, b: 0)

    def failing(path):
        raise MigrationError("source_download_failed", http_status=429,
                             retry_after=retry_after)
        yield

    source.chunks = failing
    runner.default_strategy = "staged"
    attempt = run_until_attempt(store, runner, job_id)
    event = stage_events(store, job_id, "download_retry")[-1]["payload"]
    maximum = 2 ** 63 - 1
    if expected_delay is None:
        expected_delay = maximum - now * 1000
    assert event["retry_delay_ms"] == expected_delay
    assert event["retry_at_ms"] == now * 1000 + expected_delay <= maximum
    assert event["download_attempts"] == 1
    assert event["http_status"] == 429
    assert store.entry(attempt["entry_id"])["retry_at_ms"] == event["retry_at_ms"]
    before = len(store.events(job_id, limit=10000))
    runner.step(job_id)
    assert len(store.events(job_id, limit=10000)) == before


@pytest.mark.parametrize("complete_cache", [False, True])
def test_restart_at_fifth_download_never_starts_a_sixth_but_reuses_verified_cache(
    migration, complete_cache,
):
    store, registry, runner, job_id, source, destination = migration
    entry = next(entry for entry in store.entries(job_id) if entry["kind"] == "file")
    attempt = store.new_attempt(entry["id"], "staged")
    name = "f" * 32 + ".part"
    local = runner.staging / name
    local.write_bytes(BODY if complete_cache else BODY[:1])
    store.update_attempt(attempt["id"], phase="downloading", local_name=name)
    store.update_entry(entry["id"], state="downloading", download_attempts=5)
    store.update_job(job_id, state="running")
    reopened = MigrationStore(store.root)
    restarted = MigrationRunner(
        reopened, registry, source_factory=lambda c, a: source,
        destination_factory=lambda c, a: destination, reserve_bytes=0,
    )
    try:
        reopened.recover()
        assert reopened.get(job_id)["state"] == "awaiting_credentials"
        assert reopened.entry(entry["id"])["download_attempts"] == 5
        result = finish(reopened, restarted, job_id)
        final = reopened.entry(entry["id"])
        assert source.downloads == 0
        assert final["download_attempts"] == 5
        assert not local.exists()
        if complete_cache:
            assert result["state"] == "completed"
            assert final["state"] == "done"
            assert destination.upload_body == BODY
            assert destination.moves == 1
        else:
            assert result["state"] == "completed_with_issues"
            assert final["state"] == "failed"
            assert destination.upload_body is None
            assert destination.moves == 0
            skipped = stage_events(reopened, job_id, "download_retry")[-1]["payload"]
            assert skipped["status"] == "skipped"
    finally:
        restarted.close()
        reopened.close()


def test_changed_source_during_wait_prevents_download_retry(migration):
    store, _, runner, job_id, source, destination = migration
    calls = []

    def failing(path):
        calls.append(path)
        yield BODY[:1]
        raise MigrationError("source_download_failed", http_status=503)

    source.chunks = failing
    runner.default_strategy = "staged"
    attempt = run_until_attempt(store, runner, job_id)
    assert store.attempt(attempt["id"])["phase"] == "download_retry_wait"
    source.metadata["modified"] = "2026-10-12T12:00:00Z"
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    entry = store.entry(attempt["entry_id"])
    assert entry["state"] == "source_changed"
    assert entry["download_attempts"] == 1 and entry["retry_at_ms"] is None
    assert calls == [META["path"]]
    assert destination.upload_body is None and destination.moves == 0
    assert not list(runner.staging.iterdir())


def test_auth_failure_on_fifth_download_preserves_last_retry_after_new_credentials(
    migration,
):
    store, registry, runner, job_id, source, destination = migration
    calls = []

    def failing_then_auth(path):
        calls.append(path)
        if len(calls) < 5:
            raise MigrationError("source_download_failed", http_status=503)
        raise MigrationError("source_auth_failed", auth=True, http_status=401)
        yield

    source.chunks = failing_then_auth
    runner.default_strategy = "staged"
    assert finish(store, runner, job_id)["state"] == "awaiting_credentials"
    entry = next(entry for entry in store.entries(job_id) if entry["kind"] == "file")
    assert len(calls) == 5 and entry["download_attempts"] == 4
    assert not any(event["payload"]["status"] == "skipped"
                   for event in stage_events(store, job_id, "download_retry"))
    registry.put(job_id, CONFIG, {
        "source_client_secret": "repaired-source-secret",
        "destination_token": "destination-secret",
    })

    def repaired(path):
        calls.append(path)
        yield BODY

    source.chunks = repaired
    assert finish(store, runner, job_id)["state"] == "completed"
    assert store.entry(entry["id"])["download_attempts"] == 5
    assert len(calls) == 6 and destination.upload_body == BODY


def test_reopened_pending_retry_is_quiet_until_due_then_downloads_once(
    migration, monkeypatch,
):
    store, _, runner, job_id, source, destination = migration
    monkeypatch.setattr("app.migration.runner.time.time", lambda: 1700000000)
    monkeypatch.setattr("app.migration.runner.random.uniform", lambda a, b: 0)
    downloads, metadata_reads = [], []
    original_stat = source.stat

    def stat(path):
        metadata_reads.append(path)
        return original_stat(path)

    def once_fails(path):
        downloads.append(path)
        if len(downloads) == 1:
            yield BODY[:1]
            raise MigrationError("source_stream_interrupted", http_status=503)
        yield BODY

    source.stat, source.chunks = stat, once_fails
    runner.default_strategy = "staged"
    attempt = run_until_attempt(store, runner, job_id)
    entry = store.entry(attempt["entry_id"])
    deadline = entry["retry_at_ms"]
    reopened = MigrationStore(store.root)
    credentials = CredentialRegistry()
    client_calls = []

    def source_factory(config, auth):
        client_calls.append("source")
        return source

    def destination_factory(config, auth):
        client_calls.append("destination")
        return destination

    restarted = MigrationRunner(
        reopened, credentials, source_factory=source_factory,
        destination_factory=destination_factory, reserve_bytes=0,
    )
    try:
        reopened.recover()
        credentials.put(job_id, CONFIG, {
            "source_client_secret": "source-secret",
            "destination_token": "destination-secret",
        })
        reopened.update_job(job_id, state="running")
        event_count = len(reopened.events(job_id, limit=10000))
        reads = list(metadata_reads)
        monkeypatch.setattr("app.migration.runner.time.time",
                            lambda: (deadline - 1) / 1000)
        restarted.step(job_id)
        assert client_calls == [] and metadata_reads == reads
        assert downloads == [META["path"]]
        assert len(reopened.events(job_id, limit=10000)) == event_count
        assert reopened.entry(entry["id"])["retry_at_ms"] == deadline
        monkeypatch.setattr("app.migration.runner.time.time",
                            lambda: deadline / 1000)
        restarted.step(job_id)
        assert downloads == [META["path"]] * 2
        assert reopened.entry(entry["id"])["download_attempts"] == 2
        assert reopened.entry(entry["id"])["retry_at_ms"] is None
        assert destination.upload_body == BODY
        assert finish(reopened, restarted, job_id)["state"] == "completed"
    finally:
        restarted.close()
        reopened.close()


def test_premature_eof_is_retried_five_times_without_upload(migration):
    store, _, runner, job_id, source, destination = migration
    calls = []

    def incomplete(path):
        calls.append(path)
        yield BODY[:1]

    source.chunks = incomplete
    runner.default_strategy = "staged"
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    entry = next(entry for entry in store.entries(job_id) if entry["kind"] == "file")
    assert calls == [META["path"]] * 5
    assert entry["download_attempts"] == 5 and entry["state"] == "failed"
    assert entry["error"] == "source_content_changed"
    retries = stage_events(store, job_id, "download_retry")
    statuses = [event["payload"]["status"] for event in retries]
    assert statuses == ["waiting"] * 4 + ["skipped"]
    assert all(event["payload"]["error"] == "source_content_changed"
               for event in retries)
    assert destination.upload_body is None and destination.moves == 0
    assert not list(runner.staging.iterdir())


def test_fifth_historical_accepted_url_operation_keeps_polling_without_resubmission(migration):
    from tests.test_migration_runner import historical_url_operation

    store, _, runner, job_id, source, destination = migration
    previous = historical_url_operation(store, job_id, destination, count=5)
    original_poll, polls = destination.poll, []

    def poll(reference):
        polls.append(reference)
        return original_poll(reference)

    destination.poll = poll
    destination.operation = "in-progress"
    runner.step(job_id)
    runner.step(job_id)
    entry = store.entry(previous["entry_id"])
    assert entry["download_attempts"] == 5
    assert entry["state"] == "waiting_operation"
    assert polls == ["op-1", "op-1"]
    assert source.downloads == destination.imports == 0
    assert len(store.attempts(job_id)) == 1
    destination.operation = "success"
    runner.step(job_id)
    runner.step(job_id)
    assert store.get(job_id)["state"] == "completed"
    assert store.entry(entry["id"])["state"] == "done"
    assert destination.moves == 1
    assert source.downloads == destination.imports == 0
