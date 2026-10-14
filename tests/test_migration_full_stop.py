"""A full local stop frees the lane while preserving unresolved remote writes."""

import threading

import pytest

from app.migration.errors import MigrationError
from app.migration.runner import _StopRequested
from app.migration.worker import MigrationService
from tests import test_migration_runner as runner_fixtures
from tests import test_migration_worker as worker_fixtures
from tests.test_migration_point_retry import queue, unknown
from tests.test_migration_runner import BODY, DIR, META, finish
from tests.test_migration_worker import ObservedDestination, wait_until


migration = runner_fixtures.migration
unplanned_migration = runner_fixtures.unplanned_migration
direct_migration = runner_fixtures.direct_migration
managed_worker = worker_fixtures.managed_worker


@pytest.mark.parametrize("fixture,phase", [
    ("direct_migration", "unknown_transfer"),
    ("direct_migration", "waiting_transfer"),
    ("migration", "unknown_move"),
    ("migration", "waiting_move"),
])
def test_full_stop_preserves_unknown_history_and_cache_without_polling(request, fixture, phase):
    selected = request.getfixturevalue(fixture)
    store, registry, runner, job_id, _, destination = selected
    entry, attempt = unknown(selected, phase=phase, operation="op-1", cache=True)
    runner.clients(store.get(job_id))
    calls = []
    destination.poll = lambda ref: calls.append(ref) or "success"
    before = store.attempt(attempt["id"])
    store.request_full_stop(job_id)
    runner.stop_job(job_id)
    for _ in range(3):
        runner.step(job_id)
    assert store.get(job_id)["state"] == "stopped"
    assert store.entry(entry["id"])["state"] == "cancelled"
    assert store.entry(entry["id"])["error"] == "local_operation_cancelled"
    assert store.attempt(attempt["id"]) == before
    assert (runner.staging / before["local_name"]).read_bytes() == BODY
    assert runner.final_pending(job_id)
    assert registry.get(job_id) is None
    assert job_id not in runner._clients
    assert calls == []


def test_full_stop_of_already_stopped_job_releases_old_clients_once(tmp_path):
    service = MigrationService(tmp_path, start_worker=False)
    try:
        job = service.store.create(runner_fixtures.CONFIG, "already-stopped")
        job_id = job["id"]
        service.store.add_entries(job_id, [{
            "kind": "file", "source_path": META["path"],
            "destination_path": "disk:/owner/Docs/a.txt", "size": META["size"],
            "md5": META["md5"],
        }])
        entry = service.store.entries(job_id)[0]
        attempt = service.store.new_attempt(entry["id"], "url")
        service.store.update_attempt(attempt["id"], phase="waiting_move", operation_id="op-1")
        credentials = {"source_client_secret": "s", "destination_token": "d"}
        service.registry.put(job_id, job["config"], credentials)
        destination = ObservedDestination()
        service.runner.destination_factory = lambda config, auth: destination
        service.runner.source_factory = lambda config, auth: runner_fixtures.Source()
        service.runner.clients(job)
        service.runner.stop_job(job_id)
        assert service.registry.get(job_id) is not None
        service.store.request_full_stop(job_id)
        service._stop_requested_jobs(service.store.jobs())
        assert service.store.entry(entry["id"])["state"] == "cancelled"
        assert service.registry.get(job_id) is None
        assert job_id not in service.runner._clients
        service.registry.put(job_id, job["config"], credentials)
        service._stop_requested_jobs(service.store.jobs())
        assert service.registry.get(job_id) == credentials
        job = service.store.get(job_id)
        with pytest.raises(_StopRequested):
            service.runner._logged(
                job, entry, "poll_move", destination.poll, "op-1", attempt=attempt,
            )
        assert destination.polls == 0
        assert service.store.attempt(attempt["id"])["phase"] == "waiting_move"
    finally:
        service.close()


def test_worker_waiting_for_guard_keeps_credentials_attached_after_stop_ack(tmp_path):
    service = MigrationService(tmp_path, start_worker=False)
    waiting = threading.Event()
    lock = threading.Lock()

    class ObservedGuard:
        def acquire(self, blocking=True):
            waiting.set()
            return lock.acquire(blocking=blocking)

        def release(self):
            lock.release()

    service.job_guard = lambda job_id: ObservedGuard()
    thread = None
    try:
        job = service.store.create(runner_fixtures.CONFIG, "ack-while-worker-waits")
        job_id = job["id"]
        service.store.update_job(job_id, state="stopped")
        service.store.request_full_stop(job_id)
        credentials = {"source_client_secret": "old", "destination_token": "old"}
        service.registry.put(job_id, job["config"], credentials)
        lock.acquire()
        try:
            thread = threading.Thread(
                target=service._stop_requested_jobs, args=(service.store.jobs(),),
            )
            thread.start()
            assert waiting.wait(5)
            # The credentials endpoint owns the guard while acknowledging stop.
            service.runner.release_clients(job_id)
            service._full_stop_acknowledged.add(job_id)
            fresh = {"source_client_secret": "fresh", "destination_token": "fresh"}
            service.registry.put(job_id, job["config"], fresh)
        finally:
            lock.release()
        thread.join(5)
        assert not thread.is_alive()
        assert service.registry.get(job_id) == fresh
    finally:
        if thread:
            thread.join(5)
        service.close()


@pytest.mark.parametrize("status", ["in-progress", "success", "failed"])
def test_full_stop_during_poll_is_immediate_and_frees_worker_for_another_job(
    managed_worker, status,
):
    service, add_job = managed_worker
    entered, release = threading.Event(), threading.Event()
    destination = ObservedDestination()

    def polling(reference):
        destination.polls += 1
        entered.set()
        assert release.wait(5)
        destination.stat = lambda path: pytest.fail("full stop forbids another request")
        return status

    destination.poll = polling
    first_id, _, first = add_job("full-stop-poll", destination=destination, shared=True)
    try:
        assert entered.wait(5)
        assert not service.job_guard(first_id).acquire(blocking=False)
        before = service.store.attempts(first_id)[0]
        service.store.request_full_stop(first_id)
        service.notify()
        assert service.store.get(first_id)["full_stop_requested"]
        assert service.store.get(first_id)["state"] != "stopped"
    finally:
        release.set()
    wait_until(lambda: service.store.get(first_id)["state"] == "stopped")
    with service.guard:
        assert service.registry.get(first_id) is None
    second_id, _, second = add_job("after-full-stop-poll", shared=True)
    assert second.started.wait(5)
    wait_until(lambda: service.store.get(second_id)["state"] == "completed")
    assert first.polls == first.uploads == 1
    assert first.imports == 0
    actual = service.store.attempt(before["id"])
    assert service.store.entry(before["entry_id"])["state"] == (
        "failed" if status == "failed" else "cancelled"
    )
    assert actual["operation_id"] == before["operation_id"]
    if status == "in-progress":
        assert actual == before
    else:
        assert actual["phase"] == ("verifying" if status == "success" else "failed")
    assert first_id not in service._polls
    assert first_id not in service.runner._clients
    described = service.describe_job(service.store.get(first_id), service.holding_job())
    assert described["blocked_by"] is None
    assert described["stop_blocked"] is False
    assert described["remote_pending"] is (status == "in-progress")
    assert "stop_resolvable_attempt_ids" not in described
    assert service.holding_job() is None



@pytest.mark.parametrize("probe", ["info", "root"])
def test_full_stop_during_point_check_never_continues_to_another_request(
    direct_migration, probe,
):
    store, registry, runner, job_id, source, destination = direct_migration
    entry, attempt = unknown(direct_migration, operation="op-1", cache=True)
    calls = []

    original_info, original_stat = destination.info, destination.stat

    def availability_check():
        store.request_full_stop(job_id)
        destination.stat = lambda path: pytest.fail("stop forbids a subsequent request")
        calls.append("info")
        return original_info()

    def root_check(path):
        assert path == "disk:/"
        store.request_full_stop(job_id)
        destination.stat = lambda path: pytest.fail("stop forbids a subsequent request")
        calls.append(path)
        return original_stat(path)

    if probe == "info":
        destination.info = availability_check
    else:
        destination.stat = root_check
    destination.poll = lambda ref: pytest.fail("point retry must not poll old operation")
    queue(direct_migration, entry, attempt)
    runner.step(job_id)
    runner.stop_job(job_id)
    current = store.get(job_id)
    assert current["state"] == "stopped"
    assert current["point_retry_active"] is False
    assert store.entry(entry["id"])["point_retry_requested"] is False
    assert store.entry(entry["id"])["state"] == "cancelled"
    assert len(store.attempts(job_id)) == 1
    assert store.attempt(attempt["id"])["operation_id"] == "op-1"
    assert registry.get(job_id) is None
    assert source.downloads == destination.imports == destination.moves == 0
    assert destination.upload_body is None
    assert calls == ["info" if probe == "info" else "disk:/"]
    assert (runner.staging / attempt["local_name"]).read_bytes() == BODY


@pytest.mark.parametrize("uncertain", [False, True])
def test_full_stop_during_upload_keeps_result_and_never_polls_or_verifies(
    direct_migration, uncertain,
):
    store, _, runner, job_id, source, destination = direct_migration
    runner.default_strategy = "staged"
    uploads = []

    def upload(path, local_path, *, before_upload, on_progress=None):
        before_upload("accepted-1")
        assert local_path.read_bytes() == BODY
        uploads.append(path)
        store.request_full_stop(job_id)
        destination.poll = lambda ref: pytest.fail("full stop forbids polling")
        destination.stat = lambda path: pytest.fail("full stop forbids verification")
        if uncertain:
            raise MigrationError("destination_connection_failed", uncertain=True)
        return "accepted-1"

    destination.upload = upload
    store.update_job(job_id, state="running")
    for _ in range(10):
        runner.step(job_id)
        if uploads:
            break
    runner.stop_job(job_id)
    attempt = store.attempts(job_id)[0]
    assert len(uploads) == 1
    assert attempt["phase"] == ("abandoned_transfer" if uncertain else "waiting_transfer")
    assert attempt["operation_id"] == "accepted-1"
    assert (runner.staging / attempt["local_name"]).read_bytes() == BODY
    assert store.get(job_id)["state"] == "stopped"
    assert store.entry(attempt["entry_id"])["state"] == (
        "failed" if uncertain else "cancelled"
    )
    assert source.downloads == 1
    assert destination.imports == destination.moves == 0
    assert bool(runner.final_pending(job_id)) is (not uncertain)


@pytest.mark.parametrize("collision", ["file", "matching", "different", "directory", "child"])
def test_locally_cancelled_job_releases_paths_and_keeps_real_file_conflict_checks(
    direct_migration, collision,
):
    store, registry, runner, old_id, source, destination = direct_migration
    old_entry, old = unknown(direct_migration, operation="op-1", cache=True)
    if collision in {"matching", "different"}:
        destination.nodes[old_entry["destination_path"]] = dict(
            META, path=old_entry["destination_path"],
            md5="0" * 32 if collision == "different" else META["md5"],
        )
    store.request_full_stop(old_id)
    runner.stop_job(old_id)
    nodes = [dict(DIR), dict(META, path="disk:/Docs/b.txt", name="b.txt")]
    if collision in {"file", "matching", "different"}:
        nodes.append(dict(META))
    else:
        nodes.append({"path": META["path"], "name": "a.txt", "type": "dir"})
        if collision == "child":
            nodes.append(dict(META, path=META["path"] + "/child.txt", name="child.txt"))
    source.walk = lambda path: nodes
    config = store.get(old_id)["config"]
    new_id = store.create(config, "after-full-stop-" + collision)["id"]
    registry.put(new_id, config, {"source_client_secret": "s", "destination_token": "d"})
    writes = []
    original_upload, original_mkdir = destination.upload, destination.ensure_dir

    def upload(path, local, **kwargs):
        writes.append(path)
        return original_upload(path, local, **kwargs)

    def mkdir(path):
        writes.append(path)
        return original_mkdir(path)

    destination.upload, destination.ensure_dir = upload, mkdir
    runner.step(new_id)
    assert store.get(new_id)["state"] == "ready"
    assert finish(store, runner, new_id)["state"] == (
        "completed_with_issues" if collision == "different" else "completed"
    )
    assert store.entry(old_entry["id"])["state"] == "cancelled"
    assert store.attempt(old["id"])["phase"] == "unknown_transfer"
    assert store.attempt(old["id"])["operation_id"] == "op-1"
    assert (runner.staging / old["local_name"]).read_bytes() == BODY
    released = old_entry["destination_path"]
    assert released.rsplit("/", 1)[0] in writes
    assert released.rsplit("/", 1)[0] + "/b.txt" in writes
    if collision in {"matching", "different"}:
        assert released not in writes
    else:
        assert released in writes
        if collision == "child":
            assert released + "/child.txt" in writes
    conflicts = [e for e in store.entries(new_id) if e["state"] == "conflict"]
    assert len(conflicts) == (1 if collision == "different" else 0)
    assert all(e["error"] == "destination_conflict" for e in conflicts)
    if collision == "different":
        assert destination.nodes[released]["md5"] == "0" * 32
    if collision == "matching":
        matching_entry = next(e for e in store.entries(new_id)
                              if e["destination_path"] == released)
        assert matching_entry["state"] == "already_verified"
    assert len(store.attempts(new_id)) == (
        2 if collision in {"file", "child"} else 1
    )



def test_startup_cancels_pending_local_work_and_releases_only_old_reserved_conflicts(tmp_path):
    service = MigrationService(tmp_path, start_worker=False)
    store = service.store
    job = store.create(runner_fixtures.CONFIG, "cancel-after-restart")
    store.add_entries(job["id"], [{
        "kind": "file", "source_path": META["path"],
        "destination_path": "disk:/owner/Docs/a.txt", "size": META["size"],
        "md5": META["md5"],
    }])
    entry = store.entries(job["id"])[0]
    attempt = store.new_attempt(entry["id"], "staged")
    (service.runner.staging / "pending.part").write_bytes(BODY)
    store.update_attempt(attempt["id"], phase="unknown_move", operation_id="op-1",
                         local_name="pending.part")
    store.update_entry(entry["id"], state="outcome_unknown")
    store.request_full_stop(job["id"])
    store.update_job(job["id"], state="stopped")
    before = store.attempt(attempt["id"])
    newer = store.create(runner_fixtures.CONFIG, "stale-reserved-conflicts")
    store.add_entries(newer["id"], [{
        "kind": "file", "source_path": META["path"],
        "destination_path": "disk:/owner/Docs/a.txt", "size": META["size"],
        "md5": META["md5"],
    }, {
        "kind": "file", "source_path": "disk:/Docs/b.txt",
        "destination_path": "disk:/owner/Docs/b.txt", "size": META["size"],
        "md5": META["md5"],
    }])
    stale, real = store.entries(newer["id"])
    store.update_entry(stale["id"], state="conflict", error="destination_path_reserved")
    store.update_entry(real["id"], state="conflict", error="destination_conflict")
    store.update_job(newer["id"], state="completed_with_issues")
    service.close()
    reopened = MigrationService(tmp_path, start_worker=False)
    try:
        assert reopened.store.entry(entry["id"])["state"] == "cancelled"
        assert reopened.store.attempt(attempt["id"]) == before
        assert (reopened.runner.staging / "pending.part").read_bytes() == BODY
        assert reopened.store.entry(stale["id"])["state"] == "failed"
        assert reopened.store.entry(stale["id"])["error"] == "stopped_job_path_released"
        assert reopened.store.entry(real["id"])["state"] == "conflict"
        assert reopened.store.entry(real["id"])["error"] == "destination_conflict"
        assert reopened.store.get(newer["id"])["state"] == "completed_with_issues"
        assert reopened.holding_job() is None
        assert not reopened.runner._clients
        assert not reopened._polls
        source, destination = runner_fixtures.Source(), runner_fixtures.Destination()
        reopened.runner.source_factory = lambda config, auth: source
        reopened.runner.destination_factory = lambda config, auth: destination
        reopened.registry.put(newer["id"], newer["config"], {
            "source_client_secret": "s", "destination_token": "d",
        })
        reopened.runner.retry(newer["id"], "staged")
        assert finish(reopened.store, reopened.runner, newer["id"])["state"] == (
            "completed_with_issues"
        )
        assert reopened.store.entry(stale["id"])["state"] == "done"
        assert reopened.store.entry(real["id"])["state"] == "conflict"
        assert reopened.store.attempt(attempt["id"]) == before
    finally:
        reopened.close()
