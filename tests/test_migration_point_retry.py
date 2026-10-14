"""Targeted retries check the selected disk and abandon historical local waits."""

from app.migration.errors import MigrationError
from app.migration.store import MigrationStore
import pytest
from tests import test_migration_runner as fixtures
from tests.test_migration_runner import BODY, META

migration = fixtures.migration
unplanned_migration = fixtures.unplanned_migration
direct_migration = fixtures.direct_migration


def unknown(migration, *, phase="unknown_transfer", operation=None,
            strategy="staged", cache=False):
    store, _, runner, job_id, _, destination = migration
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    attempt = store.new_attempt(entry["id"], strategy)
    if cache:
        name = attempt["id"] + ".part"
        (runner.staging / name).write_bytes(BODY)
        store.update_attempt(attempt["id"], local_name=name)
    store.update_attempt(attempt["id"], phase=phase, operation_id=operation,
                         error="destination_unavailable")
    store.update_entry(entry["id"], state="outcome_unknown",
                       error="destination_unavailable")
    store.update_job(job_id, state="needs_attention", namespace_state="owned")
    if store.get(job_id)["remote_root"]:
        destination.ensure_dir(store.get(job_id)["remote_root"])
    return entry, store.attempt(attempt["id"])


def queue(migration, entry, attempt):
    store, _, _, job_id, _, _ = migration
    return store.queue_point_retry(job_id, entry["id"], attempt["id"],
                                   store.get(job_id)["revision"])


def test_matching_legacy_move_releases_cache_after_final_verification(migration):
    store, _, runner, job_id, _, destination = migration
    entry, old = unknown(migration, phase="unknown_move", operation="old-op", cache=True)
    destination.nodes[entry["destination_path"]] = dict(META, path=entry["destination_path"])
    destination.poll = lambda *a: pytest.fail("previous operation must not be polled")
    queue(migration, entry, old)
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] == "already_verified"
    previous = store.attempt(old["id"])
    assert previous["phase"] == "abandoned_move"
    assert previous["operation_id"] == "old-op"
    assert previous["local_name"] is None
    assert not list(runner.staging.iterdir())


@pytest.mark.parametrize("fixture,phase", [
    ("direct_migration", "unknown_transfer"),
    ("direct_migration", "transfer_intent"),
    ("direct_migration", "waiting_transfer"),
    ("migration", "unknown_transfer"),
    ("migration", "unknown_move"),
    ("migration", "move_intent"),
    ("migration", "waiting_move"),
])
def test_historical_unknown_or_waiting_attempt_is_abandoned_without_old_polling(
    request, fixture, phase,
):
    selected = request.getfixturevalue(fixture)
    store, _, runner, job_id, source, destination = selected
    entry, old = unknown(selected, phase=phase, operation="old-operation", cache=True)
    source.stat = source.chunks = lambda *a: pytest.fail("verified local cache is the retry source")
    original_poll = destination.poll
    def poll(reference):
        assert reference != "old-operation"
        return original_poll(reference)
    destination.poll = poll
    queue(selected, entry, old)
    runner.step(job_id)
    current = store.entry(entry["id"])
    assert current["state"] == "done"
    assert current["point_retry_attempts"] == 1
    assert not current["point_retry_requested"]
    previous = store.attempt(old["id"])
    assert previous["phase"] == ("abandoned_move" if "move" in phase else "abandoned_transfer")
    assert previous["operation_id"] == "old-operation"
    assert destination.upload_body == BODY
    assert source.downloads == destination.imports == 0
    assert not list(runner.staging.iterdir())



def test_new_accepted_point_upload_keeps_polling_its_own_operation(direct_migration):
    store, _, runner, job_id, source, destination = direct_migration
    entry, old = unknown(direct_migration, operation="old-operation", cache=True)
    status, polls, writes = "in-progress", [], []

    def upload(path, local_path, *, before_upload, on_progress=None, overwrite=False):
        before_upload("new-operation")
        writes.append((path, overwrite))
        destination.nodes[path] = dict(META, path=path)
        return "new-operation"

    def poll(reference):
        assert reference == "new-operation"
        polls.append(reference)
        return status
    destination.upload, destination.poll = upload, poll
    queue(direct_migration, entry, old)
    for _ in range(4):
        runner.step(job_id)
    current = store.entry(entry["id"])
    current_attempt = store.attempt(current["attempt_id"])
    assert current["state"] == "waiting_operation"
    assert current["point_retry_attempts"] == 1
    assert current["point_retry_requested"]
    assert writes == [(entry["destination_path"], True)]
    assert polls == ["new-operation"] * 4
    assert (runner.staging / current_attempt["local_name"]).read_bytes() == BODY
    status = "success"
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] == "done"
    assert len(writes) == 1
    assert not list(runner.staging.iterdir())
    assert source.downloads == destination.moves == 0


def test_historical_url_point_retry_requires_new_dry_run_without_download(direct_migration):
    store, _, runner, job_id, source, destination = direct_migration
    entry, old = unknown(direct_migration, strategy="url", operation="old-operation")
    store.queue_point_retry(job_id, entry["id"], old["id"], store.get(job_id)["revision"])
    runner.step(job_id)
    assert store.get(job_id)["state"] == "needs_attention"
    assert store.get(job_id)["error"] == "onprem_staged_required"
    assert store.attempt(old["id"])["operation_id"] == "old-operation"
    assert destination.imports == destination.uploads == source.downloads == 0
    assert len(store.attempts(job_id)) == 1


def test_unknown_upload_restarts_only_chosen_file_and_reuses_cache(migration):
    store, _, runner, job_id, source, destination = migration
    entry, old = unknown(migration, cache=True)
    untouched = {e["id"]: e for e in store.entries(job_id)
                 if e["id"] != entry["id"]}
    destination.nodes[old["remote_path"]] = dict(META, path=old["remote_path"])
    queue(migration, entry, old)
    runner.step(job_id)
    current = store.entry(entry["id"])
    new = store.attempt(current["attempt_id"])
    assert current["state"] == "done"
    assert current["point_retry_attempts"] == 1
    assert not current["point_retry_requested"]
    assert new["remote_path"] != old["remote_path"]
    assert store.attempt(old["id"])["phase"] == "abandoned_transfer"
    assert old["remote_path"] in destination.nodes
    assert destination.upload_body == BODY
    assert source.downloads == 0
    assert destination.moves == 1
    assert not list(runner.staging.iterdir())
    assert store.get(job_id)["state"] == "running"
    assert not store.get(job_id)["point_retry_active"]
    assert untouched == {e["id"]: e for e in store.entries(job_id)
                         if e["id"] != entry["id"]}
    assert runner.final_pending(job_id) == []


def test_pause_and_stop_during_retry_wait_do_not_submit(migration, monkeypatch):
    store, _, runner, job_id, _, destination = migration
    entry, old = unknown(migration)
    queue(migration, entry, old)
    store.update_job(job_id, pause_requested=True)
    runner.step(job_id)
    assert store.get(job_id)["state"] == "paused"
    assert store.entry(entry["id"])["point_retry_attempts"] == 0
    store.request_stop(job_id)
    runner.stop_job(job_id)
    runner.step(job_id)
    assert destination.imports == destination.moves == 0


def test_other_unknown_entry_does_not_block_or_get_retried(unplanned_migration):
    store, _, runner, job_id, source, destination = unplanned_migration
    source.walk = lambda path: [dict(META), dict(
        META, path="disk:/Docs/b.txt", name="b.txt",
    )]
    runner.step(job_id)
    entry, old = unknown(unplanned_migration, cache=True)
    other = next(e for e in store.entries(job_id)
                 if e["source_path"].endswith("b.txt"))
    attempt = store.new_attempt(other["id"], "staged")
    store.update_attempt(attempt["id"], phase="unknown_transfer")
    store.update_entry(other["id"], state="outcome_unknown")
    before = store.entry(other["id"])
    queue(unplanned_migration, entry, old)
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] == "done"
    assert store.entry(other["id"]) == before
    assert store.get(job_id)["state"] == "needs_attention"
    assert destination.moves == 1


def test_wait_and_counter_survive_restart_without_fourth_attempt(
    migration, monkeypatch,
):
    store, registry, runner, job_id, _, destination = migration
    entry, old = unknown(migration, cache=True)
    queue(migration, entry, old)
    store.update_entry(entry["id"], point_retry_attempts=3)
    store.close()
    reopened = MigrationStore(store.root)
    try:
        reopened.recover()
        runner.store = reopened
        assert reopened.get(job_id)["point_retry_active"]
        reopened.update_job(job_id, state="running")
        destination.info = lambda: (_ for _ in ()).throw(
            AssertionError("exhausted retry must not perform another request"),
        )
        runner.step(job_id)
        assert reopened.entry(entry["id"])["point_retry_attempts"] == 3
        assert not reopened.entry(entry["id"])["point_retry_requested"]
        assert reopened.get(job_id)["state"] == "running"
        assert registry.get(job_id)
    finally:
        reopened.close()


def test_saas_download_failure_is_logged_and_limited_to_three(
    migration, monkeypatch,
):
    store, _, runner, job_id, source, destination = migration
    entry, old = unknown(migration)
    calls = []

    def chunks(path):
        calls.append(path)
        raise MigrationError("source_stream_interrupted", http_status=503)
        yield

    source.chunks = chunks
    now = 1800000000
    monkeypatch.setattr("app.migration.runner.time.time", lambda: now)
    queue(migration, entry, old)
    for _ in range(3):
        runner.step(job_id)
        current = store.entry(entry["id"])
        now = (current.get("retry_at_ms") or now * 1000) // 1000
    assert len(calls) == 3
    assert current["state"] == "failed"
    assert current["error"] == "source_stream_interrupted"
    assert not current["point_retry_requested"]
    assert destination.upload_body is None
    failures = [e for e in store.events(job_id, journal=True, limit=1000)
                if e["payload"].get("error") == "source_stream_interrupted"]
    assert any("SaaS" in e["error_message"] for e in failures)


def test_matching_final_file_releases_cache_but_retains_uncertain_remote_history(
    migration,
):
    store, _, runner, job_id, source, destination = migration
    entry, old = unknown(migration, cache=True)
    destination.nodes[entry["destination_path"]] = dict(
        META, path=entry["destination_path"],
    )
    queue(migration, entry, old)
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] == "already_verified"
    assert store.attempt(old["id"])["phase"] == "abandoned_transfer"
    assert store.attempt(old["id"])["local_name"] is None
    assert not list(runner.staging.iterdir())
    assert destination.moves == source.downloads == 0


@pytest.mark.parametrize("count,expected_uploads", [(1, 1), (3, 0)])
def test_crash_after_upload_intent_consumes_next_budget_slot(
    migration, count, expected_uploads,
):
    store, _, runner, job_id, source, destination = migration
    entry, old = unknown(migration, cache=True, phase="transfer_intent")
    queue(migration, entry, old)
    store.update_entry(entry["id"], state="transferring", point_retry_attempts=count)
    uploads = []
    original = destination.upload

    def upload(*args, **kwargs):
        uploads.append(args[0])
        return original(*args, **kwargs)

    destination.upload = upload
    runner.step(job_id)
    current = store.entry(entry["id"])
    assert len(uploads) == expected_uploads
    assert current["point_retry_attempts"] == min(count + 1, 3)
    assert current["state"] == ("done" if expected_uploads else "outcome_unknown")
    assert not current["point_retry_requested"]
    assert source.downloads == 0


def test_global_retry_cannot_replace_a_paused_point_retry(migration):
    store, _, runner, job_id, _, _ = migration
    entry, old = unknown(migration)
    queue(migration, entry, old)
    store.update_attempt(old["id"], phase="failed")
    store.update_entry(entry["id"], state="failed", point_retry_attempts=1,
                       retry_at_ms=1800000030000)
    store.update_job(job_id, state="paused")
    before = store.entry(entry["id"])
    with pytest.raises(ValueError, match="point_retry_in_progress"):
        runner.retry(job_id, "staged")
    assert store.entry(entry["id"]) == before
    assert len(store.attempts(job_id)) == 1
