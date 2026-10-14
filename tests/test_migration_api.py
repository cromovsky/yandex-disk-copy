"""API guards, durable history and credential-safe responses."""

import asyncio
import csv
import hashlib
import io
import json
import threading

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.migration.api import AccessRequest, access_data, router
from app.migration.errors import MigrationError
from app.migration.worker import MigrationService


REQUEST = {
    "config": {
        "source_org_id": "org", "source_client_id": "client",
        "source_email": "owner@example.org", "source_path": "/Docs",
        "destination_url": "https://disk.example.org",
        "destination_identity": "receiver", "destination_root": "/owner",
        "identity_confirmed": True,
    },
    "credentials": {"source_client_secret": "never-return-source",
                    "destination_token": "never-return-destination"},
    "idempotency_key": "plan-request-1",
}


@pytest.fixture
def api(tmp_path, monkeypatch):
    monkeypatch.setenv("ONPREM_ENABLED", "1")
    monkeypatch.setenv("ONPREM_ADMIN_TOKEN", "admin-test-only")
    monkeypatch.delenv("ONPREM_ALLOW_INSECURE_HTTP", raising=False)
    service = MigrationService(tmp_path, start_worker=False)
    app = FastAPI()
    app.state.onprem = service
    app.include_router(router)
    with TestClient(app, base_url="https://utility.example.org",
                    headers={"X-Onprem-Key": "admin-test-only"}) as client:
        yield client, service
    service.close()


def test_idempotent_plan_creation_returns_no_credentials(api):
    client, service = api
    first = client.post("/api/onprem/jobs", json=REQUEST)
    again = client.post("/api/onprem/jobs", json=REQUEST)
    assert first.status_code == 200
    assert first.json()["id"] == again.json()["id"]
    assert first.json()["config"]["destination_url"] == (
        "https://disk.example.org/v1/disk"
    )
    result = client.get("/api/onprem/jobs").text
    assert "never-return" not in result
    assert "admin-test-only" not in result
    raw = service.store.path.read_bytes()
    assert b"never-return" not in raw


def test_remote_requests_need_explicit_admin_access(api):
    client, _ = api
    response = client.get("/api/onprem/jobs", headers={"X-Onprem-Key": "wrong"})
    assert response.status_code == 403
    assert response.json()["detail"] == "onprem_access_required"


def test_unprotected_remote_mode_is_closed(api, monkeypatch):
    client, _ = api
    monkeypatch.delenv("ONPREM_ADMIN_TOKEN")
    assert client.get("/api/onprem/jobs").status_code == 403


def test_validation_does_not_echo_submitted_secret(api):
    client, _ = api
    invalid = json.loads(json.dumps(REQUEST))
    invalid["credentials"]["destination_token"] = "secret\nnot-valid"
    response = client.post("/api/onprem/jobs", json=invalid)
    assert response.status_code == 422
    assert "secret" not in response.text


def test_start_requires_reviewable_ready_plan(api):
    client, _ = api
    job_id = client.post("/api/onprem/jobs", json=REQUEST).json()["id"]
    response = client.post(f"/api/onprem/jobs/{job_id}/start", json={})
    assert response.status_code == 409


def test_reattach_rejects_changed_destination_binding(api):
    client, service = api
    job_id = client.post("/api/onprem/jobs", json=REQUEST).json()["id"]
    service.store.update_job(job_id, state="awaiting_credentials",
                             resume_state="ready")
    body = json.loads(json.dumps(REQUEST))
    body["config"]["destination_identity"] = "other-user"
    body.pop("idempotency_key")
    response = client.put(f"/api/onprem/jobs/{job_id}/credentials", json=body)
    assert response.status_code == 409
    assert response.json()["detail"] == "credential_binding_mismatch"


def test_events_replay_is_not_consumed_by_first_viewer(api):
    client, service = api
    job_id = client.post("/api/onprem/jobs", json=REQUEST).json()["id"]
    service.store.event(job_id, "warning", {"error": "quota_unavailable"})
    url = f"/api/onprem/jobs/{job_id}/events?format=json"
    first = client.get(url).json()["events"]
    second = client.get(url).json()["events"]
    assert first == second
    assert client.get(url + f"&after={first[-1]['sequence']}").json()["events"] == []


def test_report_neutralizes_spreadsheet_formulas(api):
    client, service = api
    job_id = client.post("/api/onprem/jobs", json=REQUEST).json()["id"]
    service.store.add_entries(job_id, [{
        "kind": "file", "source_path": "disk:/Docs/=SUM(A1)",
        "destination_path": "disk:/owner/=SUM(A1)", "size": 5,
        "md5": "a" * 32, "modified": "@formula", "resource_id": None,
    }])
    response = client.get(f"/api/onprem/jobs/{job_id}/report")
    assert response.status_code == 200
    assert "'@formula" in response.text
    assert "never-return" not in response.text


def test_volume_lock_refuses_a_second_worker(api, tmp_path):
    _, service = api
    with pytest.raises(RuntimeError, match="migration_volume_in_use"):
        MigrationService(service.root, start_worker=False)


@pytest.mark.parametrize("setting", ["0", " 0 ", "", "typo"])
def test_disabled_mode_blocks_reads_and_mutations_without_changing_history(
    api, monkeypatch, setting,
):
    client, service = api
    job_id, entry, _ = attempted_job(api)
    durable_before = service.store.path.read_bytes()
    credentials_before = service.registry.get(job_id)
    if setting is None:
        monkeypatch.delenv("ONPREM_ENABLED", raising=False)
    else:
        monkeypatch.setenv("ONPREM_ENABLED", setting)

    def forbid_preflight(*args, **kwargs):
        pytest.fail("disabled mode must not contact either disk API")

    monkeypatch.setattr(service.runner, "preflight", forbid_preflight)
    replacement = request_copy()
    replacement["idempotency_key"] = "disabled-create-attempt"
    replacement["credentials"] = {
        "source_client_secret": "disabled-replacement-source",
        "destination_token": "disabled-replacement-destination",
    }
    access = {key: value for key, value in replacement.items()
              if key != "idempotency_key"}
    routes = [
        ("GET", "/jobs", None),
        ("GET", f"/jobs/{job_id}", None),
        ("GET", f"/jobs/{job_id}/entries", None),
        ("GET", f"/jobs/{job_id}/entries/{entry['id']}", None),
        ("POST", "/preflight", access),
        ("POST", "/jobs", replacement),
        ("POST", f"/jobs/{job_id}/restart", replacement),
        ("PUT", f"/jobs/{job_id}/credentials", access),
        ("POST", f"/jobs/{job_id}/start", {}),
        ("POST", f"/jobs/{job_id}/pause", {}),
        ("POST", f"/jobs/{job_id}/resume", {}),
        ("POST", f"/jobs/{job_id}/retry", {"strategy": "url"}),
        ("POST", f"/jobs/{job_id}/cleanup", {}),
        ("POST", "/cache/cleanup", {}),
        ("POST", f"/jobs/{job_id}/stop", {}),
        ("GET", f"/jobs/{job_id}/report", None),
        ("GET", f"/jobs/{job_id}/journal", None),
        ("GET", f"/jobs/{job_id}/events?format=json", None),
        ("GET", f"/jobs/{job_id}/events", None),
    ]
    for method, path, payload in routes:
        kwargs = {"json": payload} if payload is not None else {}
        response = client.request(method, "/api/onprem" + path, **kwargs)
        assert response.status_code == 404, (method, path, response.text)
        assert response.json() == {"detail": "onprem_disabled"}
        assert service.store.path.read_bytes() == durable_before
        assert service.registry.get(job_id) == credentials_before


def request_copy():
    return json.loads(json.dumps(REQUEST))


def test_global_cache_cleanup_needs_no_selected_job_or_disk_credentials(api):
    client, service = api
    (service.runner.staging / "orphan.part").write_bytes(b"cache")
    response = client.post("/api/onprem/cache/cleanup", json={})
    assert response.status_code == 200
    assert response.json() == {"removed": 1, "removed_bytes": 5}
    assert service.store.jobs() == []


@pytest.mark.parametrize("headers,code", [
    ({"X-Onprem-Key": "wrong"}, "onprem_access_required"),
    ({"Origin": "https://other.example.org"}, "onprem_origin_rejected"),
])
def test_global_cache_cleanup_keeps_access_and_origin_protection(api, headers, code):
    client, service = api
    cached = service.runner.staging / "keep.part"
    cached.write_bytes(b"keep")
    response = client.post("/api/onprem/cache/cleanup", json={}, headers=headers)
    assert response.status_code == 403
    assert response.json()["detail"] == code
    assert cached.read_bytes() == b"keep"


@pytest.mark.parametrize("state,pause_requested", [
    ("queued", False), ("planning", False), ("running", False), ("running", True),
])
def test_global_cache_cleanup_refuses_running_or_queued_jobs(api, state, pause_requested):
    client, service = api
    job_id = client.post("/api/onprem/jobs", json=REQUEST).json()["id"]
    service.store.update_job(job_id, state=state, pause_requested=pause_requested)
    cached = service.runner.staging / "keep.part"
    cached.write_bytes(b"keep")
    response = client.post("/api/onprem/cache/cleanup", json={})
    assert response.status_code == 409
    assert response.json()["detail"] == "cache_in_use"
    assert cached.read_bytes() == b"keep"


@pytest.mark.parametrize("state", [
    "ready", "paused", "stopped", "completed", "completed_with_issues",
    "needs_attention", "awaiting_credentials",
])
def test_global_cache_cleanup_allows_idle_jobs(api, state):
    client, service = api
    job_id = client.post("/api/onprem/jobs", json=REQUEST).json()["id"]
    service.store.update_job(job_id, state=state)
    cached = service.runner.staging / "cache.part"
    cached.write_bytes(b"cache")
    response = client.post("/api/onprem/cache/cleanup", json={})
    assert response.status_code == 200
    assert response.json() == {"removed": 1, "removed_bytes": 5}
    assert service.store.get(job_id)["state"] == state


def test_global_cache_cleanup_refuses_worker_guard_without_waiting(api):
    client, service = api
    cached = service.runner.staging / "keep.part"
    cached.write_bytes(b"keep")
    with service.guard:
        response = client.post("/api/onprem/cache/cleanup", json={})
    assert response.status_code == 409
    assert response.json()["detail"] == "cache_in_use"
    assert cached.read_bytes() == b"keep"
    assert not service.guard.locked()


def test_global_cache_cleanup_sanitizes_io_failures_and_releases_guard(api, monkeypatch):
    client, service = api

    def fail(*args, **kwargs):
        raise OSError("private credential path must not be returned")

    monkeypatch.setattr("app.migration.runner.os.unlink", fail)
    (service.runner.staging / "cache.part").write_bytes(b"cache")
    response = client.post("/api/onprem/cache/cleanup", json={})
    assert response.status_code == 409
    assert response.json()["detail"] == "cache_cleanup_failed"
    assert "private" not in response.text
    assert "credential" not in response.text
    assert not service.guard.locked()


def shared_request():
    payload = request_copy()
    payload["config"].pop("destination_root")
    payload["config"].update(
        destination_type="shared", destination_vd_hash="archive_123",
        destination_base="/Archive",
    )
    return payload


@pytest.mark.parametrize("endpoint", ["preflight", "credentials"])
@pytest.mark.parametrize("code,status,details,expected", [
    ("destination_connection_failed", None,
     {"error_side": "onprem", "transport_error": "dns"},
     "Не удалось определить сетевой адрес сервера."),
    ("destination_connection_failed", None,
     {"error_side": "onprem", "transport_error": "tls"},
     "Не удалось установить защищённое TLS-соединение."),
    ("source_unavailable", 500,
     {"error_side": "saas", "api_error": "InternalServerError"},
     "HTTP 500: Внутренняя ошибка сервера."),
    ("destination_unavailable", 503, {"error_side": "onprem"},
     "HTTP 503: Сервис временно недоступен."),
    ("destination_connection_failed", None, {}, ""),
])
def test_access_failure_keeps_safe_diagnostics_without_mutating_job(
    api, monkeypatch, endpoint, code, status, details, expected,
):
    client, service = api
    access = shared_request()
    job_id = None
    if endpoint == "credentials":
        job_id = client.post("/api/onprem/jobs", json=access).json()["id"]
        service.store.update_job(job_id, state="awaiting_credentials",
                                 resume_state="ready")
        service.registry.discard(job_id)
        access["config"] = service.store.get(job_id)["config"]
    access.pop("idempotency_key")
    before_jobs = service.store.jobs()
    before_events = service.store.events(job_id) if job_id else None
    safe_details = ({**details, "request_host": "disk.example.org",
                     "request_id": "0123456789abcdef"} if details else {})
    error = MigrationError(code, http_status=status, diagnostics={
        "request_host": "https://private.test/file?token=raw-host-secret",
        "request_id": "Bearer raw-request-secret",
        "api_error": "Bearer raw-api-secret",
        "message": "Bearer raw-message-secret",
        "description": "https://private.test/file?token=raw-body-secret",
        **safe_details,
    })
    error.args = ("Bearer raw-exception-secret",)

    def unavailable(config, credentials):
        raise error

    monkeypatch.setattr(service.runner, "preflight", unavailable)
    if job_id:
        response = client.put(f"/api/onprem/jobs/{job_id}/credentials", json=access)
    else:
        response = client.post("/api/onprem/preflight", json=access)
    assert response.status_code == 409
    result = response.json()
    assert result["detail"] == code
    if expected:
        assert set(result) == {"detail", "diagnostic_message"}
        assert expected in result["diagnostic_message"]
        assert result["diagnostic_message"].startswith(
            "SaaS." if details["error_side"] == "saas" else "On-premises.",
        )
        assert "Сервер: disk.example.org." in result["diagnostic_message"]
        assert "ID запроса: 0123456789abcdef." in result["diagnostic_message"]
    else:
        assert result == {"detail": code}
    assert "secret" not in response.text
    assert "never-return" not in response.text
    assert "admin-test-only" not in response.text
    assert "private.test" not in response.text
    assert service.store.jobs() == before_jobs
    if job_id:
        assert service.store.events(job_id) == before_events
        assert service.registry.get(job_id) is None
    assert not service.guard.locked()
    monkeypatch.setattr(service.runner, "preflight", lambda c, a: {"checked": True})
    assert client.post("/api/onprem/preflight", json=access).json() == {"checked": True}


def test_false_public_flag_keeps_personal_api_binding_and_credential_reattach(
    api, monkeypatch,
):
    client, service = api
    payload = request_copy()
    original = client.post("/api/onprem/jobs", json=payload).json()
    payload["config"]["source_public"] = False
    duplicate = client.post("/api/onprem/jobs", json=payload)
    assert duplicate.status_code == 200
    assert duplicate.json() == original
    assert "source_public" not in original["config"]
    service.store.update_job(original["id"], state="awaiting_credentials",
                             resume_state="ready")
    service.registry.discard(original["id"])
    validated = []
    monkeypatch.setattr(service.runner, "preflight",
                        lambda c, a: validated.append(c) or {"checked": True})
    payload.pop("idempotency_key")
    attached = client.put(f"/api/onprem/jobs/{original['id']}/credentials",
                          json=payload)
    assert attached.status_code == 200
    assert validated == [original["config"]]
    assert service.registry.get(original["id"]) == REQUEST["credentials"]


@pytest.mark.parametrize("strategy", ["url", "staged"])
def test_public_shared_api_keeps_mode_immutable(api, monkeypatch, strategy):
    client, service = api
    payload = shared_request()
    payload["config"].update(strategy=strategy, source_public=True)
    created = client.post("/api/onprem/jobs", json=payload)
    if strategy == "url":
        assert (created.status_code, created.json()["detail"]) == (422, "onprem_staged_required")
        assert service.store.jobs() == []
        return
    assert created.status_code == 200
    job = created.json()
    assert job["config"]["source_public"] is True
    assert job["config"]["destination_root"] == "disk:/owner@example.org"
    payload["config"] = dict(job["config"])
    service.store.update_job(job["id"], state="awaiting_credentials",
                             resume_state="ready")
    service.registry.discard(job["id"])
    monkeypatch.setattr(service.runner, "preflight", lambda c, a: {"checked": True})
    payload.pop("idempotency_key")
    attached = client.put(f"/api/onprem/jobs/{job['id']}/credentials", json=payload)
    assert attached.status_code == 200
    assert attached.json()["config"]["source_public"] is True
    assert service.registry.get(job["id"]) == REQUEST["credentials"]
    payload["config"]["source_public"] = False
    changed = client.put(f"/api/onprem/jobs/{job['id']}/credentials", json=payload)
    assert (changed.status_code, changed.json()["detail"]) == (
        409, "credential_binding_mismatch",
    )
    assert "never-return" not in attached.text + changed.text



@pytest.mark.parametrize("strategy", ["url", "staged"])
def test_legacy_job_cannot_enable_publication_when_reattaching(api, strategy):
    client, service = api
    payload = shared_request()
    payload["config"]["strategy"] = strategy
    payload.pop("idempotency_key")
    config, credentials = access_data(AccessRequest.model_validate(payload))
    job = service.store.create(config, "legacy-private-shared")
    service.registry.put(job["id"], config, credentials)
    service.store.update_job(job["id"], state="awaiting_credentials",
                             resume_state="ready")
    before = service.registry.get(job["id"])
    payload["config"]["source_public"] = True
    changed = client.put(f"/api/onprem/jobs/{job['id']}/credentials", json=payload)
    assert (changed.status_code, changed.json()["detail"]) == (
        (422 if strategy == "url" else 409),
        ("onprem_staged_required" if strategy == "url" else "credential_binding_mismatch"),
    )
    assert service.registry.get(job["id"]) == before



@pytest.mark.parametrize("factory,strategy", [
    (request_copy, "url"), (request_copy, "staged"),
])
def test_public_api_requires_shared_destination(api, factory, strategy):
    client, service = api
    payload = factory()
    payload["config"].update(source_public=True, strategy=strategy)
    rejected = client.post("/api/onprem/jobs", json=payload)
    assert (rejected.status_code, rejected.json()["detail"]) == (
        422, "onprem_staged_required" if strategy == "url" else "invalid_source_public",
    )
    assert service.store.jobs() == []
    assert "never-return" not in rejected.text



@pytest.mark.parametrize("value", [None, 0, 1, "true", "false", {}, []])
def test_public_api_flag_is_strict_bool_and_does_not_echo_input(api, value):
    client, service = api
    payload = shared_request()
    payload["config"].update(source_public=value, strategy="staged")
    rejected = client.post("/api/onprem/jobs", json=payload)
    assert (rejected.status_code, rejected.json()["detail"]) == (
        422, "invalid_request",
    )
    assert service.store.jobs() == []
    assert "never-return" not in rejected.text


def test_shared_create_derives_email_folder_and_keeps_binding_immutable(api):
    client, service = api
    payload = shared_request()
    response = client.post("/api/onprem/jobs", json=payload)
    assert response.status_code == 200
    job = response.json()
    assert job["config"]["destination_base"] == "disk:/"
    assert job["config"]["destination_root"] == "disk:/owner@example.org"
    assert client.post("/api/onprem/jobs", json=payload).json()["id"] == job["id"]
    payload["config"] = dict(job["config"])
    service.store.update_job(job["id"], state="awaiting_credentials",
                             resume_state="ready")
    payload.pop("idempotency_key")
    payload["config"]["destination_vd_hash"] = "other"
    result = client.put(f"/api/onprem/jobs/{job['id']}/credentials", json=payload)
    assert result.status_code == 409
    assert result.json()["detail"] == "credential_binding_mismatch"


@pytest.mark.parametrize("overrides", [
    {"destination_vd_hash": ""}, {"destination_vd_hash": "wrong/hash"},
    {"destination_base": "/Archive/../Other"},
    {"destination_root": "/Archive/someone@example.org"},
])
def test_shared_create_rejects_invalid_target_without_echoing_input(api, overrides):
    client, service = api
    payload = shared_request()
    payload["config"].update(overrides)
    response = client.post("/api/onprem/jobs", json=payload)
    assert response.status_code == 422
    assert response.json()["detail"] == "invalid_destination_shared_disk"
    assert service.store.jobs() == []


def test_shared_report_qualifies_final_and_technical_paths(api):
    client, service = api
    job = client.post("/api/onprem/jobs", json=shared_request()).json()
    service.store.add_entries(job["id"], [{
        "kind": "file", "source_path": "disk:/Docs/a.txt",
        "destination_path": "disk:/Archive/owner@example.org/Docs/a.txt",
        "size": 1, "md5": "a" * 32, "modified": None, "resource_id": None,
    }])
    entry = service.store.entries(job["id"])[0]
    attempt = service.store.new_attempt(entry["id"], "url")
    report = client.get(f"/api/onprem/jobs/{job['id']}/report")
    rows = list(csv.DictReader(io.StringIO(report.text.lstrip("\ufeff"))))
    assert rows[0]["destination"] == (
        "vd:archive_123:disk:/Archive/owner@example.org/Docs/a.txt"
    )
    assert rows[0]["remote_path"] == "vd:archive_123:" + attempt["remote_path"]


def test_journal_exports_all_events_and_detailed_stages_in_order(api):
    client, service = api
    job = client.post("/api/onprem/jobs", json=shared_request()).json()
    service.store.add_entries(job["id"], [{
        "kind": "dir", "source_path": "disk:/Docs/Архив",
        "destination_path": "disk:/Archive/owner@example.org/Docs/Архив",
        "size": 0, "md5": None, "modified": None, "resource_id": None,
    }])
    entry = service.store.entries(job["id"])[0]
    for _ in range(205):
        service.store.event(job["id"], "warning", {"error": "source_changed"})
    service.store.event(job["id"], "transfer_stage", {
        "entry_id": entry["id"], "stage": "create_directory", "status": "failed",
        "error": "destination_quota_exceeded", "http_status": 507,
        "duration_ms": 2000,
    })
    response = client.get(f"/api/onprem/jobs/{job['id']}/journal")
    assert response.status_code == 200
    assert response.content.startswith(b"\xef\xbb\xbf")
    assert "attachment;" in response.headers["content-disposition"]
    assert "no-store" in response.headers["cache-control"]
    rows = list(csv.DictReader(io.StringIO(response.text.lstrip("\ufeff"))))
    assert len(rows) == len(service.store.events(job["id"], limit=1000))
    assert [int(row["sequence"]) for row in rows] == sorted(
        int(row["sequence"]) for row in rows
    )
    assert rows[-1]["source"] == entry["source_path"]
    assert rows[-1]["destination"] == "vd:archive_123:" + entry["destination_path"]
    assert rows[-1]["kind"] == "dir"
    assert rows[-1]["stage_label"] == "Создание папки"
    assert rows[-1]["http_status"] == "507"
    assert rows[-1]["duration_ms"] == "2000"
    assert "мест" in rows[-1]["error_message"].lower()
    assert "never-return" not in response.text
    assert "admin-test-only" not in response.text


def test_journal_is_protected_and_handles_missing_jobs(api):
    client, _ = api
    assert client.get("/api/onprem/jobs/missing/journal").status_code == 404
    assert client.get("/api/onprem/jobs/missing/journal", headers={
        "X-Onprem-Key": "wrong",
    }).status_code == 403


def test_journal_and_report_export_download_retry_causes_and_counters(api):
    client, service = api
    job = client.post("/api/onprem/jobs", json=REQUEST).json()
    service.store.add_entries(job["id"], [{
        "kind": "file", "source_path": "disk:/Docs/a.txt",
        "destination_path": "disk:/owner/Docs/a.txt", "size": 1,
        "md5": "a" * 32, "modified": None, "resource_id": None,
    }])
    entry = service.store.entries(job["id"])[0]
    retry_at = 1800000000000
    service.store.update_entry(entry["id"], download_attempts=2,
                               retry_at_ms=retry_at, error="source_timeout")
    service.store.event(job["id"], "transfer_stage", {
        "entry_id": entry["id"], "stage": "download_retry", "status": "waiting",
        "download_attempts": 2, "max_download_attempts": 5,
        "retry_delay_ms": 8000, "retry_at_ms": retry_at,
        "error": "source_timeout", "http_status": 504,
    })
    response = client.get(f"/api/onprem/jobs/{job['id']}/journal")
    rows = list(csv.DictReader(io.StringIO(response.text.lstrip("\ufeff"))))
    row = rows[-1]
    assert row["download_attempts"] == "2"
    assert row["max_download_attempts"] == "5"
    assert row["retry_delay_ms"] == "8000"
    assert row["retry_at_ms"] == str(retry_at)
    assert row["error"] == "source_timeout"
    assert row["http_status"] == "504"
    assert "врем" in row["error_message"].lower()
    assert rows[0]["download_attempts"] == ""
    assert "never-return" not in response.text
    report = client.get(f"/api/onprem/jobs/{job['id']}/report")
    rows = list(csv.DictReader(io.StringIO(report.text.lstrip("\ufeff"))))
    assert rows[0]["download_attempts"] == "2"
    assert rows[0]["retry_at_ms"] == str(retry_at)
    assert rows[0]["error"] == "source_timeout"
    service.store.update_entry(entry["id"], download_attempts=5,
                               retry_at_ms=None, state="failed")
    service.store.event(job["id"], "transfer_stage", {
        "entry_id": entry["id"], "stage": "download_retry", "status": "skipped",
        "download_attempts": 5, "max_download_attempts": 5,
        "error": "source_timeout",
    })
    response = client.get(f"/api/onprem/jobs/{job['id']}/journal")
    rows = list(csv.DictReader(io.StringIO(response.text.lstrip("\ufeff"))))
    assert rows[-2]["download_attempts"] == "5"
    assert rows[-1]["status_label"] == "Пропущено"
    assert rows[-1]["error"] == "source_timeout"
    assert rows[-1]["retry_at_ms"] == ""


def test_journal_csv_neutralizes_formula_looking_operation_ids(api):
    client, service = api
    job = client.post("/api/onprem/jobs", json=REQUEST).json()
    service.store.event(job["id"], "transfer_stage", {
        "stage": "poll_transfer", "status": "waiting", "operation_id": "-formula",
    })
    response = client.get(f"/api/onprem/jobs/{job['id']}/journal")
    rows = list(csv.DictReader(io.StringIO(response.text.lstrip("\ufeff"))))
    assert rows[-1]["operation_id"] == "'-formula"


def test_journal_streams_multiple_pages_with_a_stable_cutoff(api, monkeypatch):
    client, service = api
    job = client.post("/api/onprem/jobs", json=REQUEST).json()
    for _ in range(1205):
        service.store.event(job["id"], "warning", {"error": "source_changed"})
    cutoff = service.store.events(job["id"], limit=2000)[-1]["sequence"]
    original = service.store.events
    requests = []

    def append_during_export(job_id, **kwargs):
        requests.append(kwargs)
        if len(requests) == 1:
            service.store.event(job_id, "warning", {"error": "internal_error"})
        return original(job_id, **kwargs)

    monkeypatch.setattr(service.store, "events", append_during_export)
    response = client.get(f"/api/onprem/jobs/{job['id']}/journal")
    rows = list(csv.DictReader(io.StringIO(response.text.lstrip("\ufeff"))))
    assert len(rows) == 1206
    assert int(rows[-1]["sequence"]) == cutoff
    assert len(requests) > 1
    assert all(item["limit"] <= 1000 and item["through"] == cutoff
               for item in requests)
    assert all(row["error"] != "internal_error" for row in rows)


def test_journal_retains_old_attempt_context_after_retry(api):
    client, service = api
    job = client.post("/api/onprem/jobs", json=REQUEST).json()
    service.store.add_entries(job["id"], [{
        "kind": "file", "source_path": "disk:/Docs/a.txt",
        "destination_path": "disk:/owner/Docs/a.txt", "size": 1,
        "md5": "a" * 32, "modified": None, "resource_id": None,
    }])
    entry = service.store.entries(job["id"])[0]
    old = service.store.new_attempt(entry["id"], "url")
    service.store.event(job["id"], "transfer_stage", {
        "entry_id": entry["id"], "attempt_id": old["id"],
        "stage": "upload_file", "status": "failed", "error": "source_changed",
    })
    service.store.new_attempt(entry["id"], "staged")
    response = client.get(f"/api/onprem/jobs/{job['id']}/journal")
    rows = list(csv.DictReader(io.StringIO(response.text.lstrip("\ufeff"))))
    stage = next(row for row in rows if row["event_type"] == "transfer_stage")
    assert stage["attempt_id"] == old["id"]
    assert stage["strategy"] == "url"
    assert stage["remote_path"] == old["remote_path"]


def test_event_tail_is_bounded_chronological_and_resumes_incrementally(api):
    client, service = api
    job = client.post("/api/onprem/jobs", json=REQUEST).json()
    for _ in range(620):
        service.store.event(job["id"], "warning", {"error": "source_changed"})
    raw = service.store.events(job["id"], limit=1000)
    url = f"/api/onprem/jobs/{job['id']}/events?format=json"
    tail = client.get(url + "&tail=500").json()["events"]
    assert len(tail) == 500
    assert [event["sequence"] for event in tail] == [
        event["sequence"] for event in raw[-500:]
    ]
    assert all(event["payload"] == raw_event["payload"]
               for event, raw_event in zip(tail, raw[-500:]))
    assert client.get(url + "&tail=501").status_code == 422
    assert client.get(url + "&tail=-1").status_code == 422
    service.store.event(job["id"], "warning", {"error": "internal_error"})
    new = client.get(url + f"&after={tail[-1]['sequence']}&tail=500").json()["events"]
    assert len(new) == 1
    assert new[0]["payload"] == {"error": "internal_error"}


def test_old_attempt_only_event_gets_safe_immutable_context_without_new_stage(api):
    client, service = api
    job = client.post("/api/onprem/jobs", json=REQUEST).json()
    service.store.add_entries(job["id"], [{
        "kind": "file", "source_path": "disk:/Docs/a.txt",
        "destination_path": "disk:/owner/Docs/a.txt", "size": 1,
        "md5": "a" * 32, "modified": None, "resource_id": None,
    }])
    entry = service.store.entries(job["id"])[0]
    old = service.store.new_attempt(entry["id"], "url")
    payload = {"attempt_id": old["id"], "error": "destination_unavailable"}
    service.store.event(job["id"], "polling_unavailable", payload)
    service.store.new_attempt(entry["id"], "staged")
    events = client.get(
        f"/api/onprem/jobs/{job['id']}/events?format=json&tail=500",
    ).json()["events"]
    event = next(item for item in events if item["type"] == "polling_unavailable")
    assert event["payload"] == payload
    assert "stage" not in event["payload"]
    assert event["stage_label"] == ""
    assert event["context"]["source"] == entry["source_path"]
    assert event["context"]["strategy"] == "url"
    assert event["context"]["remote_path"] == old["remote_path"]
    assert service.store.events(job["id"])[-2]["payload"] == payload
    assert "context" not in service.store.events(job["id"])[-2]


def test_event_tail_handles_short_jobs_and_uses_a_stable_cutoff(api, monkeypatch):
    client, service = api
    job = client.post("/api/onprem/jobs", json=REQUEST).json()
    original = service.store.tail_cursor

    def append_after_cutoff(job_id, limit, through):
        service.store.event(job_id, "warning", {"error": "internal_error"})
        return original(job_id, limit, through)

    monkeypatch.setattr(service.store, "tail_cursor", append_after_cutoff)
    events = client.get(
        f"/api/onprem/jobs/{job['id']}/events?format=json&tail=500",
    ).json()["events"]
    assert len(events) == 1
    assert events[0]["type"] == "job_created"


def test_idempotent_create_cannot_replace_running_credentials(api):
    client, service = api
    job_id = client.post("/api/onprem/jobs", json=REQUEST).json()["id"]
    original = service.registry.get(job_id)
    service.store.update_job(job_id, state="running")
    retry = request_copy()
    retry["credentials"] = {
        "source_client_secret": "replacement-source",
        "destination_token": "replacement-destination",
    }
    response = client.post("/api/onprem/jobs", json=retry)
    assert response.status_code == 200
    assert response.json()["id"] == job_id
    assert service.registry.get(job_id) == original
    assert "replacement" not in response.text


@pytest.mark.parametrize("base,expected", [
    ("http://evil.example.org", 403),
    ("http://localhost", 200),
    ("http://127.0.0.1", 200),
    ("http://[::1]", 200),
])
def test_loopback_mode_rejects_rebinding_but_allows_local_origin(
    api, monkeypatch, base, expected,
):
    client, _ = api
    monkeypatch.delenv("ONPREM_ADMIN_TOKEN")

    async def fetch():
        transport = httpx.ASGITransport(
            app=client.app, client=("127.0.0.1", 12345),
        )
        async with httpx.AsyncClient(transport=transport, base_url=base) as local:
            return await local.get("/api/onprem/jobs", headers={"Origin": base})

    response = asyncio.run(fetch())
    assert response.status_code == expected
    if expected == 403:
        assert response.json()["detail"] == "onprem_access_required"


@pytest.mark.parametrize("allow_http,base,expected", [
    ("1", "http://localhost:8000", 200),
    ("true", "http://127.0.0.1:8000", 200),
    (" TRUE ", "http://localhost:8000", 200),
    ("1", "http://[::1]:8000", 200),
    (None, "http://localhost:8000", 403),
    ("0", "http://localhost:8000", 403),
    ("unexpected", "http://localhost:8000", 403),
    ("1", "http://evil.example.org", 403),
])
def test_insecure_http_opt_in_allows_docker_bridge_for_localhost_only(
    api, monkeypatch, allow_http, base, expected,
):
    client, _ = api
    monkeypatch.delenv("ONPREM_ADMIN_TOKEN")
    if allow_http is not None:
        monkeypatch.setenv("ONPREM_ALLOW_INSECURE_HTTP", allow_http)

    async def fetch():
        transport = httpx.ASGITransport(
            app=client.app, client=("172.18.0.1", 12345),
        )
        async with httpx.AsyncClient(transport=transport, base_url=base) as bridge:
            return await bridge.get("/api/onprem/jobs", headers={"Origin": base})

    assert asyncio.run(fetch()).status_code == expected


@pytest.mark.parametrize("allow_http,key,origin,expected", [
    ("1", "admin-test-only", "http://utility.example.org", 200),
    ("0", "admin-test-only", "http://utility.example.org", 403),
    ("1", "wrong", "http://utility.example.org", 403),
    ("1", "", "http://utility.example.org", 403),
    ("1", "admin-test-only", "http://evil.example.org", 403),
])
def test_insecure_http_keeps_explicit_key_and_origin_checks(
    api, monkeypatch, allow_http, key, origin, expected,
):
    client, _ = api
    monkeypatch.setenv("ONPREM_ALLOW_INSECURE_HTTP", allow_http)

    async def fetch():
        transport = httpx.ASGITransport(
            app=client.app, client=("172.18.0.1", 12345),
        )
        async with httpx.AsyncClient(
            transport=transport, base_url="http://utility.example.org",
        ) as remote:
            return await remote.get("/api/onprem/jobs", headers={
                "X-Onprem-Key": key, "Origin": origin,
            })

    assert asyncio.run(fetch()).status_code == expected


def test_insecure_http_without_key_still_rejects_cross_origin(api, monkeypatch):
    client, _ = api
    monkeypatch.delenv("ONPREM_ADMIN_TOKEN")
    monkeypatch.setenv("ONPREM_ALLOW_INSECURE_HTTP", "1")

    async def fetch():
        transport = httpx.ASGITransport(
            app=client.app, client=("172.18.0.1", 12345),
        )
        async with httpx.AsyncClient(
            transport=transport, base_url="http://localhost:8000",
        ) as bridge:
            return await bridge.get("/api/onprem/jobs", headers={
                "Origin": "http://evil.example.org",
            })

    response = asyncio.run(fetch())
    assert response.status_code == 403
    assert response.json()["detail"] == "onprem_origin_rejected"


BODY = b"verified migration content"
METADATA = {
    "path": "disk:/Docs/a.txt", "type": "file", "name": "a.txt",
    "size": len(BODY), "md5": hashlib.md5(BODY).hexdigest(),
    "modified": "2026-10-11T12:00:00Z", "resource_id": "source-file",
}


class Source:
    def __init__(self):
        self.metadata = dict(METADATA)

    def stat(self, path):
        if path == "disk:/Docs":
            return {"path": path, "type": "dir", "name": "Docs"}
        assert path == METADATA["path"]
        return dict(self.metadata)

    def close(self):
        pass


class Destination:
    def __init__(self):
        self.nodes = {}
        self.poll_error = None
        self.polls = []
        self.imports = 0
        self.moves = 0

    def info(self):
        return {"total_space": 1000000, "used_space": 0,
                "user": {"login": "receiver"}}

    def stat(self, path):
        return self.nodes.get(path)

    def ensure_dir(self, path):
        if path in self.nodes and self.nodes[path]["type"] != "dir":
            raise MigrationError("destination_conflict")
        self.nodes[path] = {"path": path, "type": "dir"}

    def poll(self, operation_id):
        self.polls.append(operation_id)
        if self.poll_error:
            raise self.poll_error
        return "success"

    def import_url(self, path, url):
        self.imports += 1
        raise AssertionError("accepted operation must never be resubmitted")

    def move(self, src, dst):
        assert dst not in self.nodes
        self.moves += 1
        self.nodes[dst] = dict(self.nodes.pop(src), path=dst)
        return None

    def close(self):
        pass


def attempted_job(api, *, key="unknown-attempt-1", phase="unknown_transfer", direct=False):
    client, service = api
    body = shared_request() if direct else request_copy()
    body["idempotency_key"] = key
    job_id = client.post("/api/onprem/jobs", json=body).json()["id"]
    service.store.add_entries(job_id, [{
        "kind": "file", "source_path": METADATA["path"],
        "destination_path": "disk:/owner/Docs/a.txt", "size": METADATA["size"],
        "md5": METADATA["md5"], "modified": METADATA["modified"],
        "resource_id": METADATA["resource_id"],
    }])
    entry = service.store.entries(job_id)[0]
    attempt = service.store.new_attempt(entry["id"], "staged")
    service.store.update_attempt(attempt["id"], phase=phase)
    service.store.update_entry(entry["id"], state="outcome_unknown")
    service.store.update_job(job_id, state="needs_attention", namespace_state="owned")
    return job_id, entry, service.store.attempt(attempt["id"])



def install_clients(service, monkeypatch):
    source, destination = Source(), Destination()
    monkeypatch.setattr(service.runner, "source_factory", lambda c, a: source)
    monkeypatch.setattr(
        service.runner, "destination_factory", lambda c, a: destination,
    )
    return source, destination


@pytest.mark.parametrize("resume_state", [
    "paused", "needs_attention", "completed_with_issues",
])
def test_reattach_then_retry_failed_job_without_intermediate_resume(
    api, monkeypatch, resume_state,
):
    client, service = api
    job_id, entry, attempt = attempted_job(api, phase="failed")
    service.store.update_entry(entry["id"], state="failed", download_attempts=5,
                               error="source_download_failed")
    service.store.update_job(job_id, state="awaiting_credentials",
                             resume_state=resume_state)
    service.registry.discard(job_id)
    before = service.store.entry(entry["id"])

    missing = client.post(f"/api/onprem/jobs/{job_id}/retry",
                          json={"strategy": "staged"})
    assert (missing.status_code, missing.json()["detail"]) == (
        409, "credentials_required",
    )
    assert service.store.entry(entry["id"]) == before
    monkeypatch.setattr(service.runner, "preflight", lambda c, a: {"checked": True})
    access = request_copy()
    access.pop("idempotency_key")
    attached = client.put(f"/api/onprem/jobs/{job_id}/credentials", json=access)
    assert attached.status_code == 200
    assert attached.json()["state"] == "awaiting_credentials"
    retried = client.post(f"/api/onprem/jobs/{job_id}/retry",
                          json={"strategy": "staged"})

    assert retried.status_code == 200
    assert retried.json()["state"] == "running"
    assert retried.json()["resume_state"] is None
    current = service.store.entry(entry["id"])
    assert current["state"] == "planned"
    assert current["download_attempts"] == 0
    assert current["attempt_id"] != attempt["id"]
    assert service.store.attempt(attempt["id"])["phase"] == "failed"
    assert "never-return" not in attached.text + retried.text


def test_job_reads_do_not_block_other_jobs_for_historical_unknown(api):
    client, service = api
    holder_id, holder_entry, holder_attempt = attempted_job(api)
    service.registry.discard(holder_id)
    service.store.update_job(holder_id, state="awaiting_credentials",
                             resume_state="needs_attention")
    model = request_copy()
    model["idempotency_key"] = "blocked-new-job"
    job_id = client.post("/api/onprem/jobs", json=model).json()["id"]
    service.store.add_entries(job_id, [{
        "kind": "dir", "source_path": "disk:/Docs",
        "destination_path": "disk:/owner/Docs", "size": 0,
    }], mark_ready=True)
    assert client.post(f"/api/onprem/jobs/{job_id}/start").status_code == 200
    revision = service.store.get(job_id)["revision"]
    expected = None
    detail = client.get(f"/api/onprem/jobs/{job_id}").json()
    assert detail["state"] == "running"
    assert detail.get("blocked_by") == expected
    listed = client.get("/api/onprem/jobs").json()["jobs"]
    assert next(j for j in listed if j["id"] == job_id).get("blocked_by") == expected
    assert client.get(f"/api/onprem/jobs/{holder_id}").json().get("blocked_by") is None
    assert service.store.get(job_id)["revision"] == revision
    assert service.store.attempt(holder_attempt["id"])["phase"] == "unknown_transfer"
    assert service.store.entries(job_id)[0]["state"] == "planned"
    service.store.update_job(job_id, state="awaiting_credentials",
                             resume_state="running")
    after_restart = client.get(f"/api/onprem/jobs/{job_id}").json()
    assert after_restart["state"] == "awaiting_credentials"
    assert after_restart.get("blocked_by") == expected
    service.store.update_attempt(holder_attempt["id"], phase="failed")
    service.store.update_entry(holder_entry["id"], state="failed")
    service.store.update_job(holder_id, state="completed_with_issues")
    assert client.get(f"/api/onprem/jobs/{job_id}").json().get("blocked_by") is None


@pytest.mark.parametrize("reference", ["accepted-op", "?id=accepted-op"])
@pytest.mark.parametrize("phase", ["waiting_transfer", "waiting_move"])
def test_auth_failure_keeps_operation_evidence_and_retries_by_actual_file_after_reattach(
    api, monkeypatch, reference, phase,
):
    client, service = api
    job_id, entry, attempt = attempted_job(api, phase=phase)
    _, destination = install_clients(service, monkeypatch)
    completed_path = entry["destination_path"]
    destination.nodes["disk:/"] = {"type": "dir", "path": "disk:/"}
    destination.nodes[completed_path] = dict(METADATA, path=completed_path)
    service.store.update_attempt(attempt["id"], operation_id=reference)
    service.store.update_entry(entry["id"], state="waiting_operation")
    service.store.update_job(job_id, state="running")
    destination.poll_error = MigrationError("destination_auth_failed", auth=True)
    service.runner.step(job_id)
    assert service.store.get(job_id)["state"] == "awaiting_credentials"
    assert service.registry.get(job_id) is None
    service.close()
    restarted = MigrationService(service.root, start_worker=False)
    client.app.state.onprem = restarted
    try:
        _, recovered_destination = install_clients(restarted, monkeypatch)
        recovered_destination.nodes = dict(destination.nodes)
        preserved = restarted.store.attempt(attempt["id"])
        assert preserved["operation_id"] == reference
        assert preserved["phase"] == (
            "abandoned_move" if phase == "waiting_move" else "abandoned_transfer"
        )
        assert restarted.store.entry(entry["id"])["state"] == "failed"
        assert restarted.registry.get(job_id) is None
        access = request_copy()
        access.pop("idempotency_key")
        access["credentials"]["destination_token"] = "fresh-reattached-token"
        assert client.put(
            f"/api/onprem/jobs/{job_id}/credentials", json=access,
        ).status_code == 200
        assert restarted.registry.get(job_id)["destination_token"] == (
            "fresh-reattached-token"
        )
        assert restarted.store.get(job_id)["state"] == "awaiting_credentials"
        exact = client.get(f"/api/onprem/jobs/{job_id}/entries/{entry['id']}").json()
        assert exact["can_retry"] is True
        retried = client.post(f"/api/onprem/jobs/{job_id}/entries/{entry['id']}/retry", json={
            "attempt_id": exact["attempt_id"], "revision": restarted.store.get(job_id)["revision"],
        })
        assert retried.status_code == 200
        for _ in range(3):
            restarted.runner.step(job_id)
        assert destination.polls == [reference]
        assert recovered_destination.polls == []
        assert recovered_destination.imports == recovered_destination.moves == 0
        assert restarted.store.entry(entry["id"])["state"] == "already_verified"
        assert restarted.store.get(job_id)["state"] == "completed"
    finally:
        restarted.close()


def test_concurrent_async_preflight_rejects_busy_guard(api, monkeypatch):
    client, service = api
    entered, release = threading.Event(), threading.Event()

    def slow_preflight(config, credentials):
        entered.set()
        assert release.wait(3)
        return {"checked": True}

    monkeypatch.setattr(service.runner, "preflight", slow_preflight)
    access = request_copy()
    access.pop("idempotency_key")

    async def concurrent():
        transport = httpx.ASGITransport(app=client.app)
        async with httpx.AsyncClient(
            transport=transport, base_url="https://utility.example.org",
            headers={"X-Onprem-Key": "admin-test-only"},
        ) as caller:
            first = asyncio.create_task(
                caller.post("/api/onprem/preflight", json=access),
            )
            try:
                assert await asyncio.to_thread(entered.wait, 2)
                second = await caller.post("/api/onprem/preflight", json=access)
                assert second.status_code == 409
                assert second.json()["detail"] == "job_busy"
            finally:
                release.set()
                response = await first
            assert response.status_code == 200

    asyncio.run(concurrent())


def test_stop_is_local_and_works_while_worker_guard_is_held(api, monkeypatch):
    client, service = api
    job_id, entry, attempt = attempted_job(api)
    before_entry = service.store.entry(entry["id"])
    before_attempt = service.store.attempt(attempt["id"])
    notifications = []
    monkeypatch.setattr(service, "notify", lambda: notifications.append(True))
    service.guard.acquire()
    try:
        response = client.post(f"/api/onprem/jobs/{job_id}/stop")
    finally:
        service.guard.release()
    assert response.status_code == 200
    assert response.json()["stop_requested"] is True
    assert response.json()["full_stop_requested"] is True
    assert response.json()["state"] == "needs_attention"
    assert notifications == [True]
    again = client.post(f"/api/onprem/jobs/{job_id}/stop", json={})
    assert again.status_code == 200
    assert again.json() == response.json()
    assert client.get(f"/api/onprem/jobs/{job_id}").json()["stop_requested"] is True
    assert service.store.entry(entry["id"]) == before_entry
    assert service.store.attempt(attempt["id"]) == before_attempt
    assert "never-return" not in response.text
    assert client.post("/api/onprem/jobs/missing/stop").status_code == 404


def test_stop_rejects_credential_payload(api):
    client, _ = api
    job_id = client.post("/api/onprem/jobs", json=REQUEST).json()["id"]
    response = client.post(f"/api/onprem/jobs/{job_id}/stop", json={
        "credentials": REQUEST["credentials"],
    })
    assert response.status_code == 422
    assert "never-return" not in response.text


@pytest.mark.parametrize("acknowledge", [False, True])
def test_stopped_or_stopping_job_rejects_resurrection_actions(
    api, acknowledge, monkeypatch,
):
    client, service = api
    job_id, entry, attempt = attempted_job(api)
    service.store.request_stop(job_id)
    if acknowledge:
        service.store.update_job(job_id, state="stopped")
    before_entry = service.store.entry(entry["id"])
    before_attempt = service.store.attempt(attempt["id"])

    def unexpected_remote_call(*args):
        raise AssertionError("stopped upload must not invoke remote APIs")

    monkeypatch.setattr(service.runner, "preflight", unexpected_remote_call)
    access = request_copy()
    access.pop("idempotency_key")
    responses = [
        client.post(f"/api/onprem/jobs/{job_id}/start"),
        client.post(f"/api/onprem/jobs/{job_id}/pause"),
        client.post(f"/api/onprem/jobs/{job_id}/resume"),
        client.post(f"/api/onprem/jobs/{job_id}/retry", json={"strategy": "staged"}),
        client.put(f"/api/onprem/jobs/{job_id}/credentials", json=access),
    ]
    assert [(r.status_code, r.json()["detail"]) for r in responses] == [
        (409, "job_stopped"),
    ] * len(responses)
    assert service.store.entry(entry["id"]) == before_entry
    assert service.store.attempt(attempt["id"]) == before_attempt



@pytest.mark.parametrize("state", ["stopped", "completed", "completed_with_issues"])
def test_restart_is_fresh_queued_linked_and_never_runs_remote_preflight(
    api, monkeypatch, state,
):
    client, service = api
    job_id, entry, attempt = attempted_job(api)
    if state == "stopped":
        service.store.request_stop(job_id)
    service.store.update_job(job_id, state=state)
    before_entry = service.store.entry(entry["id"])
    before_attempt = service.store.attempt(attempt["id"])

    def unexpected_remote_call(*args):
        raise AssertionError("restart only queues a new planning job")

    monkeypatch.setattr(service.runner, "preflight", unexpected_remote_call)
    model = request_copy()
    model["idempotency_key"] = "restart-child-1"
    response = client.post(f"/api/onprem/jobs/{job_id}/restart", json=model)
    assert response.status_code == 200
    child = response.json()
    assert child["id"] != job_id
    assert child["restart_of"] == job_id
    assert child["state"] == "queued"
    assert child["stop_requested"] is False
    assert child["namespace_state"] == "unclaimed"
    assert child["remote_root"] != service.store.get(job_id)["remote_root"]
    assert service.store.entries(child["id"]) == []
    assert service.registry.get(child["id"]) == REQUEST["credentials"]
    assert service.store.entry(entry["id"]) == before_entry
    assert service.store.attempt(attempt["id"]) == before_attempt
    assert "never-return" not in response.text


@pytest.mark.parametrize("child_state", ["running", "awaiting_credentials", "stopped"])
def test_duplicate_restart_never_rebinds_evolved_child_credentials(api, child_state):
    client, service = api
    parent_id = client.post("/api/onprem/jobs", json=REQUEST).json()["id"]
    service.store.update_job(parent_id, state="completed")
    model = request_copy()
    model["idempotency_key"] = "restart-duplicate-child"
    child = client.post(f"/api/onprem/jobs/{parent_id}/restart", json=model).json()
    service.store.update_job(child["id"], state=child_state)
    service.registry.discard(child["id"])
    model["credentials"]["destination_token"] = "replacement-token"
    response = client.post(f"/api/onprem/jobs/{parent_id}/restart", json=model)
    assert response.status_code == 200
    assert response.json()["id"] == child["id"]
    assert service.registry.get(child["id"]) is None
    assert "replacement-token" not in response.text


def test_restart_rejects_pending_stop_binding_change_and_other_parent_key(api):
    client, service = api
    parent_id = client.post("/api/onprem/jobs", json=REQUEST).json()["id"]
    model = request_copy()
    model["idempotency_key"] = "restart-rejected-child"
    for stop in (False, True):
        if stop:
            service.store.request_stop(parent_id)
        response = client.post(f"/api/onprem/jobs/{parent_id}/restart", json=model)
        assert (response.status_code, response.json()["detail"]) == (
            409, "job_not_restartable",
        )
    service.store.update_job(parent_id, state="stopped")
    changed = request_copy()
    changed["idempotency_key"] = "restart-changed-child"
    changed["config"]["destination_identity"] = "other-receiver"
    response = client.post(f"/api/onprem/jobs/{parent_id}/restart", json=changed)
    assert (response.status_code, response.json()["detail"]) == (
        409, "restart_config_mismatch",
    )
    restarted = client.post(f"/api/onprem/jobs/{parent_id}/restart", json=model)
    assert restarted.status_code == 200
    other = request_copy()
    other["idempotency_key"] = "restart-other-parent"
    other_id = client.post("/api/onprem/jobs", json=other).json()["id"]
    service.store.update_job(other_id, state="completed")
    response = client.post(f"/api/onprem/jobs/{other_id}/restart", json=model)
    assert (response.status_code, response.json()["detail"]) == (
        409, "idempotency_parent_mismatch",
    )



def test_stop_during_credential_validation_cannot_bind_new_credentials(
    api, monkeypatch,
):
    client, service = api
    job_id = client.post("/api/onprem/jobs", json=REQUEST).json()["id"]
    service.store.update_job(job_id, state="awaiting_credentials")
    service.registry.discard(job_id)

    def stop_during_preflight(config, credentials):
        service.store.request_stop(job_id)
        return {"checked": True}

    monkeypatch.setattr(service.runner, "preflight", stop_during_preflight)
    access = request_copy()
    access.pop("idempotency_key")
    response = client.put(f"/api/onprem/jobs/{job_id}/credentials", json=access)
    assert (response.status_code, response.json().get("detail")) == (
        409, "job_stopped",
    )
    assert service.registry.get(job_id) is None


def test_stopped_move_does_not_reattach_or_validate_remote_credentials(
    api, monkeypatch,
):
    client, service = api
    job_id, _, _ = attempted_job(api, phase="waiting_move")
    service.store.request_stop(job_id)
    service.store.update_job(job_id, state="stopped")
    service.registry.discard(job_id)
    source_calls, notifications = [], []
    destination = Destination()

    def unavailable_source(config, credentials):
        source_calls.append(config)
        raise MigrationError("source_auth_failed", auth=True)

    monkeypatch.setattr(service.runner, "source_factory", unavailable_source)
    monkeypatch.setattr(
        service.runner, "destination_factory", lambda config, creds: destination,
    )
    monkeypatch.setattr(service, "notify", lambda: notifications.append(True))
    access = request_copy()
    access.pop("idempotency_key")
    attached = client.put(f"/api/onprem/jobs/{job_id}/credentials", json=access)
    assert attached.status_code == 409
    assert attached.json()["detail"] == "job_stopped"
    assert service.registry.get(job_id) is None
    assert source_calls == []
    assert notifications == []
    assert destination.imports == destination.moves == 0


def test_active_job_credential_reattach_still_checks_source(api, monkeypatch):
    client, service = api
    job_id = client.post("/api/onprem/jobs", json=REQUEST).json()["id"]
    service.store.update_job(job_id, state="awaiting_credentials")
    service.registry.discard(job_id)
    source_calls = []

    def unavailable_source(config, credentials):
        source_calls.append(config)
        raise MigrationError("source_auth_failed", auth=True)

    monkeypatch.setattr(service.runner, "source_factory", unavailable_source)
    access = request_copy()
    access.pop("idempotency_key")
    attached = client.put(f"/api/onprem/jobs/{job_id}/credentials", json=access)
    assert (attached.status_code, attached.json()["detail"]) == (
        409, "source_auth_failed",
    )
    assert len(source_calls) == 1
    assert service.registry.get(job_id) is None


@pytest.mark.parametrize("public", [None, False, True])
@pytest.mark.parametrize("strategy", [None, "url", "staged"])
def test_new_shared_jobs_always_use_root_publication_and_staged_default(
    api, public, strategy,
):
    client, service = api
    payload = shared_request()
    if public is not None:
        payload["config"]["source_public"] = public
    if strategy is not None:
        payload["config"]["strategy"] = strategy
    response = client.post("/api/onprem/jobs", json=payload)
    if strategy == "url":
        assert (response.status_code, response.json()["detail"]) == (422, "onprem_staged_required")
        assert service.store.jobs() == []
        return
    assert response.status_code == 200
    job = response.json()
    assert job["config"]["destination_base"] == "disk:/"
    assert job["config"]["destination_root"] == "disk:/owner@example.org"
    assert job["config"]["source_public"] is True
    assert job["config"]["strategy"] == (strategy or "staged")
    service.registry.put(job["id"], job["config"], REQUEST["credentials"])
    assert client.post("/api/onprem/jobs", json=payload).json() == job



def test_new_shared_preflight_checks_effective_fixed_configuration(api, monkeypatch):
    client, service = api
    payload = shared_request()
    payload.pop("idempotency_key")
    monkeypatch.setattr(service.runner, "preflight", lambda c, a: {"config": c})
    response = client.post("/api/onprem/preflight", json=payload)
    assert response.status_code == 200
    config = response.json()["config"]
    assert config["destination_base"] == "disk:/"
    assert config["destination_root"] == "disk:/owner@example.org"
    assert config["source_public"] is True
    assert config["strategy"] == "staged"
    assert service.store.jobs() == []


@pytest.mark.parametrize("public", [None, False])
def test_historical_private_shared_job_reattaches_and_resumes_unchanged(
    api, monkeypatch, public,
):
    client, service = api
    payload = shared_request()
    payload.pop("idempotency_key")
    if public is not None:
        payload["config"]["source_public"] = public
    config, _ = access_data(AccessRequest.model_validate(payload))
    job = service.store.create(config, "historical-private-job")
    service.store.update_job(job["id"], state="awaiting_credentials", resume_state="ready")
    monkeypatch.setattr(service.runner, "preflight", lambda c, a: {"checked": True})
    attached = client.put(f"/api/onprem/jobs/{job['id']}/credentials", json=payload)
    assert attached.status_code == 200
    assert attached.json()["config"] == config
    assert config["destination_root"] == "disk:/Archive/owner@example.org"
    assert "source_public" not in config
    assert config["strategy"] == "staged"
    service.registry.put(job["id"], config, REQUEST["credentials"])
    resumed = client.post(f"/api/onprem/jobs/{job['id']}/resume", json={})
    assert resumed.status_code == 200
    assert resumed.json()["config"] == config



def test_restart_historical_shared_job_binds_normalized_child_only(api):
    client, service = api
    payload = shared_request()
    access = {key: value for key, value in payload.items() if key != "idempotency_key"}
    config, _ = access_data(AccessRequest.model_validate(access))
    parent = service.store.create(config, "historical-restart-parent")
    service.store.update_job(parent["id"], state="completed")
    payload["idempotency_key"] = "historical-restart-child"
    response = client.post(f"/api/onprem/jobs/{parent['id']}/restart", json=payload)
    assert response.status_code == 200
    child = response.json()
    assert child["config"]["destination_base"] == "disk:/"
    assert child["config"]["destination_root"] == "disk:/owner@example.org"
    assert child["config"]["source_public"] is True
    assert child["config"]["strategy"] == "staged"
    assert service.store.get(parent["id"])["config"] == config
    service.registry.put(child["id"], child["config"], REQUEST["credentials"])
    repeated = client.post(f"/api/onprem/jobs/{parent['id']}/restart", json=payload)
    assert repeated.status_code == 200
    assert repeated.json() == child
