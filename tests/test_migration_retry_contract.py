"""Local interruption/retry keeps cached data and never requires server evidence."""

import pytest

from app.migration.store import MigrationStore
from app.migration.worker import MigrationService
from tests import test_migration_api as api_fixtures
from tests.test_migration_store import (
    config, direct_unknown_attempt, file_entry, point_retry_job,
)


api = api_fixtures.api
attempted_job = api_fixtures.attempted_job


@pytest.mark.parametrize("phase", [
    "failed", "abandoned_transfer", "abandoned_move", "unknown_transfer", "unknown_move",
])
@pytest.mark.parametrize("state", ["needs_attention", "paused", "completed_with_issues"])
def test_failed_and_historical_files_can_be_retried_without_losing_evidence(tmp_path, phase, state):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store, phase=phase, state=state)
    store.update_entry(entry["id"], state="failed")
    store.update_attempt(attempt["id"], operation_id="old-operation", local_name="cached.ready")
    before = store.attempt(attempt["id"])
    listed = store.describe_entries(job["id"], [store.entry(entry["id"])])[0]
    assert listed["can_retry"] is True
    job = store.get(job["id"])
    queued = store.queue_point_retry(job["id"], entry["id"], attempt["id"], job["revision"])
    assert queued["state"] == "running"
    assert store.attempt(attempt["id"]) == before
    assert not store.describe_entries(job["id"], [store.entry(entry["id"])])[0]["can_retry"]


@pytest.mark.parametrize("phase", ["unknown_transfer", "unknown_move", "abandoned_transfer", "abandoned_move"])
def test_direct_retry_preserves_abandoned_operation_and_transfers_cache_atomically(tmp_path, phase):
    store = MigrationStore(tmp_path)
    job, entry, attempt = direct_unknown_attempt(store, phase=phase)
    store.update_attempt(attempt["id"], operation_id="old-operation", local_name="cached.ready")
    store.queue_point_retry(job["id"], entry["id"], attempt["id"], store.get(job["id"])["revision"])
    newer = store.new_attempt(entry["id"], "url", point_retry=True, reuse_local=True)
    old = store.attempt(attempt["id"])
    assert old["operation_id"] == "old-operation"
    assert old["phase"] == ("abandoned_move" if "move" in phase else "abandoned_transfer")
    assert old["local_name"] is None
    assert newer["local_name"] == "cached.ready"
    assert newer["remote_path"] == entry["destination_path"]


def test_failure_before_attempt_is_retryable_with_null_attempt_cas(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "no-attempt-yet")
    store.add_entries(job["id"], [file_entry()])
    entry = store.entries(job["id"])[0]
    store.update_entry(entry["id"], state="failed", error="destination_timeout")
    job = store.update_job(job["id"], state="needs_attention", namespace_state="owned")
    assert store.describe_entries(job["id"], [store.entry(entry["id"])])[0]["can_retry"]
    store.queue_point_retry(job["id"], entry["id"], None, job["revision"])
    new = store.new_attempt(entry["id"], "staged", point_retry=True)
    assert store.entry(entry["id"])["attempt_id"] == new["id"]
    assert new["phase"] == "prepared"


def test_manual_resolution_endpoint_is_removed_and_entry_retry_is_authoritative(api):
    client, service = api
    job_id, entry, attempt = attempted_job(api)
    base = f"/api/onprem/jobs/{job_id}"
    assert client.post(base + "/resolve", json={}).status_code == 404
    assert not hasattr(service.store, "resolve_attempt")
    exact = client.get(base + f"/entries/{entry['id']}")
    assert exact.status_code == 200
    assert exact.json()["id"] == entry["id"]
    assert exact.json()["can_retry"] is True
    assert client.get(base + "/entries").json()["entries"][0]["can_retry"] is True
    service.store.update_job(job_id, state="stopped")
    assert client.get(base + f"/entries/{entry['id']}").json()["can_retry"] is False


def test_exact_entry_does_not_disclose_foreign_job_entry(api):
    client, service = api
    first, _, _ = attempted_job(api)
    second, entry, _ = attempted_job(api, key="foreign-entry")
    response = client.get(f"/api/onprem/jobs/{first}/entries/{entry['id']}")
    assert response.status_code == 404
    assert response.json()["detail"] == "entry_not_found"
    assert client.get(f"/api/onprem/jobs/{second}/entries/{entry['id']}").status_code == 200


@pytest.mark.parametrize("state", ["needs_attention", "stopped"])
def test_interrupted_or_stopped_job_does_not_hold_worker_lane(tmp_path, state):
    service = MigrationService(tmp_path, start_worker=False)
    try:
        job, entry, attempt = point_retry_job(service.store, phase="unknown_move", state=state)
        before = service.store.attempt(attempt["id"])
        assert service.holding_job() is None
        described = service.describe_job(service.store.get(job["id"]), None)
        assert not described["stop_blocked"]
        assert "stop_resolvable_attempt_ids" not in described
        service._stop_requested_jobs(service.store.jobs())
        assert service.store.attempt(attempt["id"]) == before
    finally:
        service.close()


def test_null_attempt_retry_rejects_a_newer_attempt_without_queuing(api):
    from tests.test_migration_api import request_copy

    client, service = api
    payload = request_copy()
    payload["idempotency_key"] = "file-without-attempt"
    job_id = client.post("/api/onprem/jobs", json=payload).json()["id"]
    service.store.add_entries(job_id, [file_entry()])
    entry = service.store.entries(job_id)[0]
    service.store.update_entry(entry["id"], state="failed", error="destination_timeout")
    service.store.update_job(job_id, state="needs_attention", namespace_state="owned")
    endpoint = f"/api/onprem/jobs/{job_id}/entries/{entry['id']}/retry"
    old_revision = service.store.get(job_id)["revision"]
    attempt = service.store.new_attempt(entry["id"], "staged")
    stale = client.post(endpoint, json={"attempt_id": None, "revision": old_revision})
    assert (stale.status_code, stale.json()["detail"]) == (409, "stale_revision")
    current = client.post(endpoint, json={
        "attempt_id": None, "revision": service.store.get(job_id)["revision"],
    })
    assert (current.status_code, current.json()["detail"]) == (409, "attempt_not_retryable")
    assert service.store.entry(entry["id"])["attempt_id"] == attempt["id"]
    assert service.store.entry(entry["id"])["point_retry_requested"] is False



def test_cached_retry_credential_reattach_does_not_contact_saas(api, monkeypatch):
    from tests.test_migration_api import request_copy

    client, service = api
    job_id, entry, attempt = attempted_job(api)
    service.store.update_attempt(attempt["id"], phase="abandoned_transfer", local_name="cached.ready")
    service.store.update_entry(entry["id"], state="failed", error="destination_timeout")
    service.registry.discard(job_id)
    checked = []
    monkeypatch.setattr(service.runner, "preflight", lambda *args: pytest.fail("SaaS must not be consulted"))
    monkeypatch.setattr(service.runner, "preflight_destination", lambda *args: checked.append(True))
    access = request_copy()
    access.pop("idempotency_key")
    access["point_retry_entry_id"] = entry["id"]
    response = client.put(f"/api/onprem/jobs/{job_id}/credentials", json=access)
    assert response.status_code == 200
    assert checked == [True]
    assert service.registry.get(job_id) is not None
    assert service.store.attempt(attempt["id"])["local_name"] == "cached.ready"


@pytest.mark.parametrize("case", ["foreign", "missing", "ineligible", "changed_during_preflight"])
def test_retry_reattach_requires_current_eligible_entry(api, monkeypatch, case):
    from tests.test_migration_api import request_copy

    client, service = api
    job_id, entry, attempt = attempted_job(api)
    other_job, foreign, _ = attempted_job(api, key="foreign-reattach")
    service.registry.discard(job_id)
    expected = 404 if case in {"foreign", "missing"} else 409
    selected = foreign["id"] if case == "foreign" else "missing" if case == "missing" else entry["id"]
    if case == "ineligible":
        service.store.update_entry(entry["id"], state="done")
    checked = []

    def destination_only(config, credentials):
        checked.append(True)
        if case == "changed_during_preflight":
            service.store.update_entry(entry["id"], state="done")

    monkeypatch.setattr(service.runner, "preflight_destination", destination_only)
    monkeypatch.setattr(service.runner, "preflight", lambda *args: pytest.fail("wrong preflight"))
    access = request_copy()
    access.pop("idempotency_key")
    access["point_retry_entry_id"] = selected
    response = client.put(f"/api/onprem/jobs/{job_id}/credentials", json=access)
    assert response.status_code == expected
    assert checked == ([True] if case == "changed_during_preflight" else [])
    assert service.registry.get(job_id) is None
    assert service.registry.get(other_job) is not None


@pytest.mark.parametrize("explicit_entry", [False, True])
def test_recovered_active_cached_retry_reattaches_without_saas(api, monkeypatch, explicit_entry):
    from tests.test_migration_api import request_copy

    client, service = api
    job_id, entry, attempt = attempted_job(api)
    service.store.queue_point_retry(job_id, entry["id"], attempt["id"], service.store.get(job_id)["revision"])
    service.store.update_job(job_id, state="awaiting_credentials", resume_state="running")
    service.registry.discard(job_id)
    checked = []
    monkeypatch.setattr(service.runner, "preflight", lambda *args: pytest.fail("SaaS must not be consulted"))
    monkeypatch.setattr(service.runner, "preflight_destination", lambda *args: checked.append(True))
    access = request_copy()
    access.pop("idempotency_key")
    if explicit_entry:
        access["point_retry_entry_id"] = entry["id"]
    response = client.put(f"/api/onprem/jobs/{job_id}/credentials", json=access)
    assert response.status_code == 200
    assert checked == [True]
    assert response.json()["state"] == "awaiting_credentials"
    assert response.json()["point_retry_active"] is True
    assert service.registry.get(job_id) is not None
    resumed = client.post(f"/api/onprem/jobs/{job_id}/resume")
    assert resumed.status_code == 200
    assert resumed.json()["state"] == "running"


def test_point_reattach_failure_is_logged_with_onprem_http_code_without_secrets(api, monkeypatch):
    from app.migration.errors import MigrationError
    from tests.test_migration_api import request_copy

    client, service = api
    job_id, entry, _ = attempted_job(api)
    service.registry.discard(job_id)
    monkeypatch.setattr(service.runner, "preflight", lambda *args: pytest.fail("SaaS must not be consulted"))

    def unavailable(*args):
        raise MigrationError("destination_unavailable", http_status=503,
                             diagnostics={"error_side": "onprem", "api_error": "DiskUnavailableError"})

    monkeypatch.setattr(service.runner, "preflight_destination", unavailable)
    access = request_copy()
    access.pop("idempotency_key")
    access["point_retry_entry_id"] = entry["id"]
    response = client.put(f"/api/onprem/jobs/{job_id}/credentials", json=access)
    assert response.status_code == 409
    assert "503" in response.json()["diagnostic_message"]
    events = service.store.events(job_id)
    event = events[-1]["payload"]
    assert event["stage"] == "check_destination_access"
    assert event["status"] == "failed"
    assert event["error"] == "destination_unavailable"
    assert event["http_status"] == 503
    assert event["error_side"] == "onprem"
    assert event["entry_id"] == entry["id"]
    assert "never-return" not in str(events) + response.text
    assert service.registry.get(job_id) is None


def test_stop_during_active_point_reattach_never_saves_credentials(api, monkeypatch):
    from tests.test_migration_api import request_copy

    client, service = api
    job_id, entry, attempt = attempted_job(api)
    service.store.queue_point_retry(job_id, entry["id"], attempt["id"], service.store.get(job_id)["revision"])
    service.store.update_job(job_id, state="awaiting_credentials", resume_state="running")
    service.registry.discard(job_id)
    monkeypatch.setattr(service.runner, "preflight", lambda *args: pytest.fail("SaaS must not be consulted"))
    monkeypatch.setattr(service.runner, "preflight_destination", lambda *args: service.store.request_full_stop(job_id))
    access = request_copy()
    access.pop("idempotency_key")
    response = client.put(f"/api/onprem/jobs/{job_id}/credentials", json=access)
    assert (response.status_code, response.json()["detail"]) == (409, "job_stopped")
    assert service.registry.get(job_id) is None
    assert service.store.get(job_id)["stop_requested"] is True
    assert service.store.get(job_id)["full_stop_requested"] is True
    assert service.store.get(job_id)["state"] != "running"
