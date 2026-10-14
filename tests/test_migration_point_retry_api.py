"""Per-file unknown-outcome retries keep permissions, scope and revision guards."""

import csv
import io

import pytest

from tests import test_migration_api as api_fixtures

api = api_fixtures.api


def request(service, job_id, attempt):
    return {"attempt_id": attempt["id"],
            "revision": service.store.get(job_id)["revision"]}


def test_point_retry_requires_access_and_credentials_and_only_queues_target(api):
    client, service = api
    job_id, entry, attempt = api_fixtures.attempted_job(api)
    endpoint = f"/api/onprem/jobs/{job_id}/entries/{entry['id']}/retry"
    payload = request(service, job_id, attempt)
    denied = client.post(endpoint, json=payload, headers={"X-Onprem-Key": "wrong"})
    assert denied.status_code == 403
    service.registry.discard(job_id)
    response = client.post(endpoint, json=payload)
    assert (response.status_code, response.json()["detail"]) == (
        409, "credentials_required",
    )
    assert not service.store.entry(entry["id"])["point_retry_requested"]
    job = service.store.get(job_id)
    service.registry.put(job_id, job["config"], api_fixtures.REQUEST["credentials"])
    response = client.post(endpoint, json=payload)
    assert response.status_code == 200
    assert response.json()["state"] == "running"
    assert response.json()["point_retry_active"] is True
    assert service.store.entry(entry["id"])["point_retry_requested"] is True
    assert service.store.attempt(attempt["id"])["phase"] == "unknown_transfer"
    assert "never-return" not in response.text


@pytest.mark.parametrize("update", [
    {"revision": True}, {"revision": -1}, {"revision": "1"},
    {"attempt_id": ""}, {"extra": "secret=value"},
])
def test_point_retry_body_is_strict_and_does_not_echo_values(api, update):
    client, service = api
    job_id, entry, attempt = api_fixtures.attempted_job(api)
    endpoint = f"/api/onprem/jobs/{job_id}/entries/{entry['id']}/retry"
    response = client.post(
        endpoint, json={**request(service, job_id, attempt), **update},
    )
    assert response.status_code == 422
    assert response.json()["detail"] == "invalid_request"
    assert "secret=value" not in response.text
    assert not service.store.entry(entry["id"])["point_retry_requested"]


def test_point_retry_rejects_stale_revision_and_foreign_target(api):
    client, service = api
    job_id, entry, attempt = api_fixtures.attempted_job(api)
    endpoint = f"/api/onprem/jobs/{job_id}/entries/{entry['id']}/retry"
    stale = request(service, job_id, attempt)
    service.store.event(job_id, "warning", {"error": "polling_unavailable"})
    response = client.post(endpoint, json=stale)
    assert (response.status_code, response.json()["detail"]) == (409, "stale_revision")
    other_job_id, other_entry, other_attempt = api_fixtures.attempted_job(
        api, key="other-point-retry-job",
    )
    foreign = client.post(
        f"/api/onprem/jobs/{job_id}/entries/{other_entry['id']}/retry",
        json=request(service, job_id, other_attempt),
    )
    assert (foreign.status_code, foreign.json()["detail"]) == (
        409, "attempt_not_retryable",
    )
    assert not service.store.entry(entry["id"])["point_retry_requested"]
    assert not service.store.entry(other_entry["id"])["point_retry_requested"]
    assert service.store.get(other_job_id)["state"] == "needs_attention"


def test_point_retry_missing_job_entry_and_attempt_are_safe_404(api):
    client, service = api
    job_id, entry, attempt = api_fixtures.attempted_job(api)
    payload = request(service, job_id, attempt)
    assert client.post("/api/onprem/jobs/missing/entries/missing/retry",
                       json=payload).status_code == 404
    assert client.post(f"/api/onprem/jobs/{job_id}/entries/missing/retry",
                       json=payload).status_code == 404
    assert client.post(f"/api/onprem/jobs/{job_id}/entries/{entry['id']}/retry",
                       json={**payload, "attempt_id": "missing"}).status_code == 404


def test_point_retry_stopped_job_is_closed(api):
    client, service = api
    job_id, entry, attempt = api_fixtures.attempted_job(api)
    service.store.update_job(job_id, state="stopped")
    response = client.post(f"/api/onprem/jobs/{job_id}/entries/{entry['id']}/retry",
                           json=request(service, job_id, attempt))
    assert (response.status_code, response.json()["detail"]) == (409, "job_stopped")
    assert not service.store.entry(entry["id"])["point_retry_requested"]


def test_point_retry_journal_and_report_export_retry_counters(api):
    client, service = api
    job_id, entry, attempt = api_fixtures.attempted_job(api)
    endpoint = f"/api/onprem/jobs/{job_id}/entries/{entry['id']}/retry"
    response = client.post(endpoint, json=request(service, job_id, attempt))
    assert response.status_code == 200
    service.store.update_entry(entry["id"], point_retry_attempts=2,
                               retry_at_ms=1800000000000)
    service.store.event(job_id, "point_retry_scheduled", {
        "entry_id": entry["id"], "attempt_id": attempt["id"],
        "stage": "point_retry", "status": "waiting", "point_retry_attempts": 2,
        "max_point_retry_attempts": 3, "retry_at_ms": 1800000000000,
    })
    response = client.get(f"/api/onprem/jobs/{job_id}/journal")
    rows = list(csv.DictReader(io.StringIO(response.text.lstrip("\ufeff"))))
    assert rows[-1]["point_retry_attempts"] == "2"
    assert rows[-1]["max_point_retry_attempts"] == "3"
    assert rows[-1]["retry_at_ms"] == "1800000000000"
    response = client.get(f"/api/onprem/jobs/{job_id}/report")
    rows = list(csv.DictReader(io.StringIO(response.text.lstrip("\ufeff"))))
    assert rows[0]["point_retry_attempts"] == "2"
    assert rows[0]["max_point_retry_attempts"] == "3"


def test_pending_point_retry_can_pause_and_resume_without_global_retry(api):
    client, service = api
    job_id, entry, attempt = api_fixtures.attempted_job(api)
    endpoint = f"/api/onprem/jobs/{job_id}/entries/{entry['id']}/retry"
    response = client.post(endpoint, json=request(service, job_id, attempt))
    assert response.status_code == 200
    paused = client.post(f"/api/onprem/jobs/{job_id}/pause", json={})
    assert paused.status_code == 200
    assert paused.json()["pause_requested"] is True
    service.store.update_job(job_id, state="paused", pause_requested=False)
    resumed = client.post(f"/api/onprem/jobs/{job_id}/resume", json={})
    assert resumed.status_code == 200
    assert resumed.json()["state"] == "running"
    assert resumed.json()["point_retry_active"] is True
    assert service.store.entry(entry["id"])["point_retry_attempts"] == 0
    assert service.store.attempt(attempt["id"])["phase"] == "unknown_transfer"


def test_recovered_point_retry_reattaches_then_resumes_selected_file(api, monkeypatch):
    client, service = api
    job_id, entry, attempt = api_fixtures.attempted_job(api)
    service.store.queue_point_retry(
        job_id, entry["id"], attempt["id"], service.store.get(job_id)["revision"],
    )
    service.store.update_entry(entry["id"], point_retry_attempts=2,
                               retry_at_ms=1800000000000)
    service.store.update_job(job_id, state="awaiting_credentials",
                             resume_state="running")
    service.registry.discard(job_id)
    denied = client.post(f"/api/onprem/jobs/{job_id}/resume", json={})
    assert (denied.status_code, denied.json()["detail"]) == (
        409, "credentials_required",
    )
    monkeypatch.setattr(service.runner, "preflight_destination", lambda c, a: {"checked": True})
    monkeypatch.setattr(service.runner, "preflight", lambda *args: pytest.fail("SaaS must not be consulted"))
    access = api_fixtures.request_copy()
    access.pop("idempotency_key")
    attached = client.put(f"/api/onprem/jobs/{job_id}/credentials", json=access)
    assert attached.status_code == 200
    resumed = client.post(f"/api/onprem/jobs/{job_id}/resume", json={})
    assert resumed.status_code == 200
    assert resumed.json()["state"] == "running"
    assert resumed.json()["point_retry_active"] is True
    current = service.store.entry(entry["id"])
    assert current["point_retry_attempts"] == 2
    assert current["retry_at_ms"] == 1800000000000


def test_unknown_file_without_pending_point_retry_still_blocks_resume(api):
    client, service = api
    job_id, entry, _ = api_fixtures.attempted_job(api)
    service.store.update_job(job_id, state="paused")
    response = client.post(f"/api/onprem/jobs/{job_id}/resume", json={})
    assert (response.status_code, response.json()["detail"]) == (
        409, "active_or_unknown_attempt",
    )
    assert service.store.get(job_id)["state"] == "paused"
    assert not service.store.entry(entry["id"])["point_retry_requested"]
