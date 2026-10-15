"""Automatic retry preferences remain live and independent of transfer identity."""
import copy
import threading

import pytest

from app.migration.credentials import CredentialRegistry
from app.migration.errors import MigrationError
from app.migration.store import MigrationStore
from tests.test_migration_api import api, REQUEST
from tests.test_migration_download_retries import unplanned_migration
from tests.test_migration_runner import BODY, META, DIR, finish, stage_events
from tests.test_source_batches import setup_service, create, finish_plan, FakeSource


def test_disabled_download_skips_one_file_and_continues(unplanned_migration):
    store, registry, runner, job_id, source, destination = unplanned_migration
    second = dict(META, path="disk:/Docs/b.txt", name="b.txt", resource_id="file-2")
    source.walk = lambda path: [dict(DIR), dict(META), second]
    source.stat = lambda path: dict(DIR if path == DIR["path"] else META if path == META["path"] else second)
    calls = []
    def chunks(path):
        calls.append(path)
        if path == META["path"]:
            raise MigrationError("source_download_failed", http_status=503)
        yield BODY
    source.chunks = chunks
    runner.set_retry_enabled(job_id, False)
    runner.step(job_id)
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert calls == [META["path"], second["path"]]
    failed = stage_events(store, job_id, "download_retry")[-1]["payload"]
    assert failed["max_download_attempts"] == 1 and failed["status"] == "skipped"


def test_disable_waiting_download_does_not_start_another_attempt(unplanned_migration):
    store, _, runner, job_id, source, _ = unplanned_migration
    calls = []
    def chunks(path):
        calls.append(path)
        raise MigrationError("source_download_failed", http_status=503)
        yield
    source.chunks = chunks
    runner.step(job_id)
    store.update_job(job_id, state="running")
    for _ in range(10):
        runner.step(job_id)
        if any(e.get("retry_at_ms") for e in store.entries(job_id)):
            break
    assert calls == [META["path"]]
    runner.set_retry_enabled(job_id, False)
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert calls == [META["path"]]


def test_live_api_is_strict_and_does_not_take_remote_work_guard(api):
    client, service = api
    job = client.post("/api/onprem/jobs", json=REQUEST).json()
    path = "/api/onprem/jobs/" + job["id"] + "/retries"
    locked = threading.Event()
    release = threading.Event()
    def holder():
        with service.job_guard(job["id"]):
            locked.set()
            release.wait(3)
    thread = threading.Thread(target=holder)
    thread.start()
    try:
        assert locked.wait(1)
        for bad in ["false", 0, None]:
            assert client.post(path, json={"enabled": bad}).status_code == 422
        response = client.post(path, json={"enabled": False})
        assert response.status_code == 200 and response.json() == {"retry_enabled": False}
        assert service.store.get(job["id"])["config"]["retry_enabled"] is False
        assert service.registry.get(job["id"])
        release.set()
        thread.join()
        model = copy.deepcopy(REQUEST)
        model.pop("idempotency_key")
        # A mutable preference must never invalidate reattachment credentials.
        service.store.update_job(job["id"], state="awaiting_credentials")
        service.runner.preflight = lambda *_: {}
        assert client.put("/api/onprem/jobs/" + job["id"] + "/credentials", json=model).status_code == 200
    finally:
        release.set()
        thread.join()


def test_retry_preference_does_not_bind_credentials_or_idempotency(tmp_path):
    from tests.test_migration_runner import CONFIG
    CREDENTIALS = {"source_client_secret": "source-secret", "destination_token": "target-secret"}
    store, registry = MigrationStore(tmp_path), CredentialRegistry()
    try:
        job = store.create({**CONFIG, "retry_enabled": False}, "retry-identity")
        registry.put(job["id"], job["config"], CREDENTIALS)
        registry.put(job["id"], {**CONFIG, "retry_enabled": True}, CREDENTIALS)
        assert store.create({**CONFIG, "retry_enabled": True}, "retry-identity")["id"] == job["id"]
    finally:
        store.close()


def test_batch_policy_shared_by_current_and_future_clients_only(setup_service):
    service, _ = setup_service
    first, second = create(service), create(service)
    first_id, second_id = first["batch"]["id"], second["batch"]["id"]
    first_policy = service.retry_policy(first_id)
    assert first_policy.enabled and service.retry_policy(second_id).enabled
    assert service.set_retry_enabled(first_id, False) == {"retry_enabled": False}
    assert not first_policy.enabled and service.retry_policy(second_id).enabled
    assert service.store.get(first_id)["config"]["retry_enabled"] is False
    source = service.store.sources(first_id)[0]
    assert service._config(service.store.get(first_id), source)["retry_enabled"] is False


def test_batch_local_single_download_attempt(setup_service, monkeypatch):
    service, _ = setup_service
    detail = finish_plan(service, create(service))
    batch_id = detail["batch"]["id"]
    service.set_retry_enabled(batch_id, False)
    calls = []
    def broken(self, path):
        calls.append(path)
        raise MigrationError("source_download_failed", http_status=500)
        yield
    monkeypatch.setattr(FakeSource, "chunks", broken)
    service.start_transfer(batch_id)
    source = service.store.sources(batch_id)[0]
    service._transfer_one(source["id"])
    assert len(calls) == 1
    assert service.store.source(source["id"])["state"] == "completed_with_issues"


def test_saas_http_failure_has_one_attempt_and_retains_rate_limit_barrier(monkeypatch):
    from app.retry_policy import RetryPolicy
    from tests.test_migration_clients import source, token, Response
    for status in (500, 429):
        client, session, limiter = source(monkeypatch, [token(), Response(status, headers={"Retry-After": "9"}), Response(payload={})])
        client.retry_policy = RetryPolicy(False)
        with pytest.raises(MigrationError) as caught:
            client.stat("disk:/file")
        assert caught.value.http_status == status
        assert len(session.calls) == 2
        assert limiter.delays == ([9.0] if status == 429 else [])


def test_saas_token_refresh_is_not_an_automatic_file_retry(monkeypatch):
    from app.retry_policy import RetryPolicy
    from tests.test_migration_clients import source, token, Response
    client, session, _ = source(monkeypatch, [token(), Response(401), token(), Response(payload={"path": "disk:/file", "type": "file"})])
    client.retry_policy = RetryPolicy(False)
    assert client.stat("disk:/file")["type"] == "file"
    assert len(session.calls) == 4


def test_saas_disable_interrupts_current_http_backoff(monkeypatch):
    from app.retry_policy import RetryPolicy
    from tests.test_migration_clients import source, token, Response
    client, session, _ = source(monkeypatch, [token(), Response(503), Response(payload={})])
    client.retry_policy = RetryPolicy(True)
    entered = threading.Event()
    original_wait = client.retry_policy.wait
    def wait(seconds, checkpoint=None, **kwargs):
        entered.set()
        return original_wait(60, checkpoint, **kwargs)
    monkeypatch.setattr(client.retry_policy, "wait", wait)
    failures = []
    def run():
        try:
            client.stat("disk:/file")
        except MigrationError as exc:
            failures.append(exc)
    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert entered.wait(1)
        client.retry_policy.set_enabled(False)
        thread.join(1)
        assert not thread.is_alive()
        assert len(session.calls) == 2 and failures[0].http_status == 503
    finally:
        client.retry_policy.set_enabled(False)
        thread.join(1)


def test_local_batch_disable_interrupts_file_backoff(setup_service, monkeypatch):
    service, _ = setup_service
    detail = finish_plan(service, create(service))
    batch_id, source_id = detail["batch"]["id"], detail["sources"][0]["id"]
    entered = threading.Event()
    calls = []
    def broken(self, path):
        calls.append(path)
        raise MigrationError("source_download_failed", http_status=500)
        yield
    monkeypatch.setattr(FakeSource, "chunks", broken)
    original_event = service._event
    def event(*args, **kwargs):
        original_event(*args, **kwargs)
        if args[2:4] == ("download_retry", "waiting"):
            entered.set()
    monkeypatch.setattr(service, "_event", event)
    service.start_transfer(batch_id)
    thread = threading.Thread(target=service._transfer_one, args=(source_id,))
    thread.start()
    try:
        assert entered.wait(1)
        service.set_retry_enabled(batch_id, False)
        thread.join(1)
        assert not thread.is_alive() and len(calls) == 1
        assert service.store.source(source_id)["state"] == "completed_with_issues"
        with service.export(batch_id, journal=True) as export:
            csv = export.read().decode("utf-8-sig")
        assert "retry_enabled" in csv and "False" in csv and "source_download_failed" in csv
    finally:
        service.set_retry_enabled(batch_id, False)
        thread.join(1)


from tests.test_migration_point_retry import direct_migration, unknown, queue
from tests.test_source_batches_review import paired, ONPREM_CONFIG, ONPREM_CREDS
from tests.test_source_batches import CSV


def test_manual_point_retry_is_one_cycle_and_keeps_cache(direct_migration):
    store, _, runner, job_id, source, destination = direct_migration
    entry, old = unknown(direct_migration, operation="old-operation", cache=True)
    runner.set_retry_enabled(job_id, False)
    calls = []
    def broken(*args, **kwargs):
        calls.append(args)
        raise MigrationError("remote_operation_failed", http_status=500)
    destination.upload = broken
    queue(direct_migration, entry, old)
    runner.step(job_id)
    current = store.entry(entry["id"])
    assert len(calls) == 1 and current["point_retry_attempts"] == 1
    assert not current["point_retry_requested"] and current["retry_at_ms"] is None
    assert list(runner.staging.iterdir()) and source.downloads == 0
    assert stage_events(store, job_id, "point_retry")[-1]["payload"]["max_point_retry_attempts"] == 1


def test_disable_does_not_cancel_polling_an_accepted_operation(direct_migration):
    store, _, runner, job_id, _, destination = direct_migration
    entry, old = unknown(direct_migration, operation="old-operation", cache=True)
    runner.set_retry_enabled(job_id, False)
    polls, status = [], "in-progress"
    def upload(path, local, *, before_upload, **kwargs):
        before_upload("new-operation")
        destination.nodes[path] = dict(META, path=path)
        return "new-operation"
    def poll(operation):
        polls.append(operation)
        return status
    destination.upload, destination.poll = upload, poll
    queue(direct_migration, entry, old)
    for _ in range(3):
        runner.step(job_id)
    assert store.entry(entry["id"])["point_retry_requested"] and polls == ["new-operation"] * 3
    status = "success"
    finish(store, runner, job_id)
    assert store.entry(entry["id"])["state"] == "done"


def test_batch_future_and_existing_children_share_live_policy(paired):
    service, onprem, _ = paired
    csv = CSV + "personal,other@company.ru,,/\n"
    detail = service.create("onprem", csv, ONPREM_CONFIG, ONPREM_CREDS, "batch-policy-children")
    batch_id = detail["batch"]["id"]
    service.set_retry_enabled(batch_id, False)
    detail = finish_plan(service, detail)
    children = [source["child_job_id"] for source in detail["sources"]]
    assert len(children) == 2 and all(children)
    for child_id in children:
        assert onprem.store.get(child_id)["config"]["retry_enabled"] is False
        assert onprem.runner.retry_policy(child_id) is service.retry_policy(batch_id)
        source, destination = onprem.runner.clients(onprem.store.get(child_id))
        assert source.retry_policy is service.retry_policy(batch_id)
        assert destination.retry_policy is service.retry_policy(batch_id)
    assert service.set_child_retry_enabled(children[0], True)
    assert service.store.get(batch_id)["config"]["retry_enabled"] is True
    assert all(onprem.store.get(child)["config"]["retry_enabled"] is True for child in children)


def test_rapid_off_on_cancels_scheduled_download_persistently(unplanned_migration):
    store, _, runner, job_id, source, _ = unplanned_migration
    calls = []
    def broken(path):
        calls.append(path)
        raise MigrationError("source_download_failed", http_status=503)
        yield
    source.chunks = broken
    runner.step(job_id)
    store.update_job(job_id, state="running")
    for _ in range(10):
        runner.step(job_id)
        if any(entry.get("retry_at_ms") for entry in store.entries(job_id)):
            break
    runner.set_retry_enabled(job_id, False)
    runner.set_retry_enabled(job_id, True)
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert calls == [META["path"]]


def test_rapid_off_on_cancels_scheduled_point_retry(direct_migration):
    store, _, runner, job_id, _, destination = direct_migration
    entry, old = unknown(direct_migration, operation="old-operation", cache=True)
    calls = []
    def broken(*args, **kwargs):
        calls.append(args)
        raise MigrationError("remote_operation_failed", http_status=500)
    destination.upload = broken
    queue(direct_migration, entry, old)
    runner.step(job_id)
    assert store.entry(entry["id"])["retry_at_ms"]
    runner.set_retry_enabled(job_id, False)
    runner.set_retry_enabled(job_id, True)
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert len(calls) == 1 and not store.entry(entry["id"])["point_retry_requested"]


def test_429_replay_cancelled_during_later_shared_barrier(monkeypatch):
    from app.retry_policy import RetryPolicy
    from tests.test_migration_clients import source, token, Response
    client, session, limiter = source(monkeypatch, [token(), Response(429, headers={"Retry-After": "7"}), Response(payload={})])
    policy = client.retry_policy = RetryPolicy(True)
    entered, release = threading.Event(), threading.Event()
    original_acquire = limiter.acquire
    calls = []
    def acquire(method, *, checkpoint=None):
        calls.append(method)
        if len(calls) == 3:
            entered.set()
            assert release.wait(2)
        original_acquire(method, checkpoint=checkpoint)
    monkeypatch.setattr(limiter, "acquire", acquire)
    monkeypatch.setattr(client, "_retry_wait", lambda *args: True)
    failures = []
    def run():
        try:
            client.stat("disk:/file")
        except MigrationError as exc:
            failures.append(exc)
    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert entered.wait(1)
        policy.set_enabled(False)
        policy.set_enabled(True)
        release.set()
        thread.join(1)
        assert not thread.is_alive()
        assert len(session.calls) == 2 and failures[0].http_status == 429
        assert limiter.delays == [7.0]
    finally:
        release.set()
        thread.join(1)


def test_local_source_journal_keeps_initial_disabled_preference(setup_service):
    from tests.test_source_batches import CONFIG, CREDENTIALS
    service, _ = setup_service
    detail = service.create("local", CSV, {**CONFIG, "retry_enabled": False}, CREDENTIALS, "journal-initial-disabled")
    detail = finish_plan(service, detail)
    journal_id = detail["sources"][0]["journal_id"]
    assert service.journal.get(journal_id)["retry_enabled"] is False
    settings = [event for event in service.store.events(detail["batch"]["id"]) if event.get("stage") == "retry_settings"]
    assert settings and all(event["retry_enabled"] is False for event in settings)


def test_disable_at_first_persisted_retry_checkpoint_cannot_revive_download(
    unplanned_migration, monkeypatch,
):
    from contextlib import contextmanager
    store, _, runner, job_id, source, _ = unplanned_migration
    calls, switched = [], []
    def broken(path):
        calls.append(path)
        raise MigrationError("source_download_failed", http_status=503)
        yield
    source.chunks = broken
    runner.step(job_id)
    store.update_job(job_id, state="running")
    original_connection = store._connection
    @contextmanager
    def connection(*, write=False):
        with original_connection(write=write) as db:
            yield db
        if write and not switched:
            with original_connection() as db:
                waiting = db.execute(
                    "SELECT entries.retry_at_ms,attempts.phase FROM entries JOIN attempts "
                    "ON entries.attempt_id=attempts.id WHERE entries.job_id=? "
                    "AND entries.retry_at_ms IS NOT NULL LIMIT 1", (job_id,),
                ).fetchone()
            if waiting:
                switched.append(dict(waiting))
                runner.set_retry_enabled(job_id, False)
    monkeypatch.setattr(store, "_connection", connection)
    for _ in range(10):
        runner.step(job_id)
        if switched:
            break
    assert switched
    runner.set_retry_enabled(job_id, True)
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert calls == [META["path"]]
    assert switched[0]["phase"] == "download_retry_wait"
