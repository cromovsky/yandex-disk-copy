"""API guards, durable history and credential-safe responses."""

import asyncio
import hashlib
import json
import threading

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.migration.api import router
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


@pytest.mark.parametrize("setting", [None, "0", " 0 ", "", "typo"])
def test_disabled_mode_blocks_reads_and_mutations_without_changing_history(
    api, monkeypatch, setting,
):
    client, service = api
    job_id, _, _ = attempted_job(api)
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
        ("POST", "/preflight", access),
        ("POST", "/jobs", replacement),
        ("POST", f"/jobs/{job_id}/restart", replacement),
        ("PUT", f"/jobs/{job_id}/credentials", access),
        ("POST", f"/jobs/{job_id}/start", {}),
        ("POST", f"/jobs/{job_id}/pause", {}),
        ("POST", f"/jobs/{job_id}/resume", {}),
        ("POST", f"/jobs/{job_id}/retry", {"strategy": "url"}),
        ("POST", f"/jobs/{job_id}/resolve", {}),
        ("POST", f"/jobs/{job_id}/cleanup", {}),
        ("POST", f"/jobs/{job_id}/stop", {}),
        ("GET", f"/jobs/{job_id}/report", None),
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


def attempted_job(api, *, key="unknown-attempt-1", phase="unknown_transfer"):
    client, service = api
    body = request_copy()
    body["idempotency_key"] = key
    job_id = client.post("/api/onprem/jobs", json=body).json()["id"]
    service.store.add_entries(job_id, [{
        "kind": "file", "source_path": METADATA["path"],
        "destination_path": "disk:/owner/Docs/a.txt", "size": METADATA["size"],
        "md5": METADATA["md5"], "modified": METADATA["modified"],
        "resource_id": METADATA["resource_id"],
    }])
    entry = service.store.entries(job_id)[0]
    attempt = service.store.new_attempt(entry["id"], "url")
    service.store.update_attempt(attempt["id"], phase=phase)
    service.store.update_entry(entry["id"], state="outcome_unknown")
    service.store.update_job(job_id, state="needs_attention", namespace_state="owned")
    return job_id, entry, service.store.attempt(attempt["id"])


def resolution(service, job_id, attempt, **changes):
    return {
        "attempt_id": attempt["id"],
        "revision": service.store.get(job_id)["revision"],
        "terminal_status": "success", "confirmed": True,
        "evidence": "Server task registry proves this resource completed successfully",
        **changes,
    }


def install_clients(service, monkeypatch):
    source, destination = Source(), Destination()
    monkeypatch.setattr(service.runner, "source_factory", lambda c, a: source)
    monkeypatch.setattr(
        service.runner, "destination_factory", lambda c, a: destination,
    )
    return source, destination


def test_unknown_resolution_rejects_stale_revision(api):
    client, service = api
    job_id, entry, attempt = attempted_job(api)
    model = resolution(service, job_id, attempt)
    service.store.event(job_id, "warning", {"error": "quota_unavailable"})
    response = client.post(f"/api/onprem/jobs/{job_id}/resolve", json=model)
    assert response.status_code == 409
    assert response.json()["detail"] == "stale_revision"
    assert service.store.entry(entry["id"])["state"] == "outcome_unknown"
    assert service.store.attempt(attempt["id"])["phase"] == "unknown_transfer"


def test_job_reads_explain_unknown_holder_without_changing_or_releasing_it(api):
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
    expected = {"job_id": holder_id, "state": "awaiting_credentials",
                "reason": "operation_outcome_unknown"}
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


def test_unknown_resolution_rejects_another_jobs_attempt(api):
    client, service = api
    job_id, _, _ = attempted_job(api)
    other_id, other_entry, other_attempt = attempted_job(api, key="another-attempt-1")
    response = client.post(
        f"/api/onprem/jobs/{job_id}/resolve",
        json=resolution(service, job_id, other_attempt),
    )
    assert response.status_code == 409
    assert response.json()["detail"] == "attempt_not_unknown"
    assert service.store.entry(other_entry["id"])["state"] == "outcome_unknown"
    assert service.store.get(other_id)["state"] == "needs_attention"


@pytest.mark.parametrize("evidence", [
    "", "The timeout elapsed so this should have stopped",
    "Server task returned 404 and no entry can be found",
    "https://disk.example.org/operations/secret?token=private",
])
def test_unknown_resolution_rejects_missing_or_nonterminal_evidence(api, evidence):
    client, service = api
    job_id, entry, attempt = attempted_job(api)
    response = client.post(
        f"/api/onprem/jobs/{job_id}/resolve",
        json=resolution(service, job_id, attempt, evidence=evidence),
    )
    assert response.status_code == 422
    assert response.json()["detail"] == "terminal_server_evidence_required"
    assert service.store.entry(entry["id"])["state"] == "outcome_unknown"
    assert "private" not in response.text


@pytest.mark.parametrize("reference", ["accepted-op", "?id=accepted-op"])
def test_restored_operation_preserves_route_and_resumes_without_import(
    api, monkeypatch, reference,
):
    client, service = api
    job_id, entry, attempt = attempted_job(api)
    _, destination = install_clients(service, monkeypatch)
    destination.nodes[attempt["remote_path"]] = dict(
        METADATA, path=attempt["remote_path"],
    )
    response = client.post(
        f"/api/onprem/jobs/{job_id}/resolve",
        json=resolution(
            service, job_id, attempt, operation_id=reference,
            terminal_status=None, evidence="",
        ),
    )
    assert response.status_code == 200
    assert service.store.attempt(attempt["id"])["operation_id"] == reference
    assert service.store.attempt(attempt["id"])["phase"] == "waiting_transfer"
    assert service.store.events(job_id)[-1]["payload"]["operation_id"] == reference
    assert destination.imports == 0
    assert client.post(f"/api/onprem/jobs/{job_id}/resume").status_code == 200
    for _ in range(3):
        service.runner.step(job_id)
    assert destination.polls == [reference, reference]
    assert destination.imports == 0
    assert destination.moves == 1
    assert service.store.entry(entry["id"])["state"] == "done"


@pytest.mark.parametrize("reference", [
    "?id=op_123&token=secret", "?id=one&id=two",
    "https://host/operations?id=op_123", "x" * 257,
])
def test_invalid_restored_reference_is_input_error_without_poll_or_state_change(
    api, monkeypatch, reference,
):
    client, service = api
    job_id, entry, attempt = attempted_job(api)
    _, destination = install_clients(service, monkeypatch)
    revision = service.store.get(job_id)["revision"]
    response = client.post(
        f"/api/onprem/jobs/{job_id}/resolve",
        json=resolution(
            service, job_id, attempt, operation_id=reference,
            terminal_status=None, evidence="",
        ),
    )
    assert response.status_code == 422
    assert response.json()["detail"] == "invalid_operation_id"
    assert service.store.get(job_id)["revision"] == revision
    assert service.store.entry(entry["id"])["state"] == "outcome_unknown"
    assert destination.polls == []
    assert "secret" not in response.text


@pytest.mark.parametrize("outcome", ["matching", "corrupt", "source_changed"])
def test_proven_terminal_resolution_still_requires_content_checks(
    api, monkeypatch, outcome,
):
    client, service = api
    job_id, entry, attempt = attempted_job(api)
    source, destination = install_clients(service, monkeypatch)
    destination.nodes[attempt["remote_path"]] = dict(
        METADATA, path=attempt["remote_path"],
    )
    if outcome == "corrupt":
        destination.nodes[attempt["remote_path"]]["md5"] = "0" * 32
    elif outcome == "source_changed":
        source.metadata["modified"] = "2026-10-11T13:00:00Z"
    model = resolution(service, job_id, attempt)
    response = client.post(f"/api/onprem/jobs/{job_id}/resolve", json=model)
    assert response.status_code == 200
    assert service.store.entry(entry["id"])["state"] == "verifying"
    assert service.store.attempt(attempt["id"])["phase"] == "verifying"
    assert service.store.get(job_id)["state"] == "needs_attention"
    events = service.store.events(job_id)
    resolution_event = next(
        e for e in events if e["type"] == "administrative_resolution"
    )
    assert resolution_event["payload"]["evidence_digest"] == hashlib.sha256(
        model["evidence"].encode(),
    ).hexdigest()
    assert model["evidence"] not in json.dumps(events)
    assert client.post(f"/api/onprem/jobs/{job_id}/resume").status_code == 200
    for _ in range(3):
        service.runner.step(job_id)
    result = service.store.entry(entry["id"])
    assert destination.imports == 0
    if outcome == "matching":
        assert result["state"] == "done"
        assert destination.moves == 1
        assert destination.nodes[entry["destination_path"]]["md5"] == METADATA["md5"]
        assert service.store.get(job_id)["state"] == "completed"
    else:
        expected = "failed" if outcome == "corrupt" else "source_changed"
        assert result["state"] == expected
        assert destination.moves == 0
        assert entry["destination_path"] not in destination.nodes
        assert service.store.get(job_id)["state"] == "completed_with_issues"


@pytest.mark.parametrize("reference", ["accepted-op", "?id=accepted-op"])
@pytest.mark.parametrize("phase", ["waiting_transfer", "waiting_move"])
def test_known_operation_survives_auth_failure_restart_and_reattach(
    api, monkeypatch, reference, phase,
):
    client, service = api
    job_id, entry, attempt = attempted_job(api, phase=phase)
    _, destination = install_clients(service, monkeypatch)
    completed_path = (entry["destination_path"] if phase == "waiting_move"
                      else attempt["remote_path"])
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
        assert preserved["phase"] == phase
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
        assert client.post(f"/api/onprem/jobs/{job_id}/resume").status_code == 200
        for _ in range(3):
            restarted.runner.step(job_id)
        assert destination.polls == [reference]
        assert recovered_destination.polls == [reference]
        assert recovered_destination.imports == 0
        assert recovered_destination.moves == (0 if phase == "waiting_move" else 1)
        assert restarted.store.entry(entry["id"])["state"] == "done"
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
        client.post(f"/api/onprem/jobs/{job_id}/retry", json={"strategy": "url"}),
        client.post(f"/api/onprem/jobs/{job_id}/resolve", json=resolution(
            service, job_id, attempt,
        )),
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
    changed["config"]["strategy"] = "staged"
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


@pytest.mark.parametrize("phase", ["move_intent", "waiting_move", "unknown_move"])
def test_stopped_final_move_can_reattach_and_resolve_without_resuming(
    api, monkeypatch, phase,
):
    client, service = api
    job_id, entry, attempt = attempted_job(api, phase=phase)
    service.store.request_stop(job_id)
    service.store.update_job(job_id, state="stopped")
    service.registry.discard(job_id)
    _, destination = install_clients(service, monkeypatch)
    notifications = []
    monkeypatch.setattr(service, "notify", lambda: notifications.append(True))
    access = request_copy()
    access.pop("idempotency_key")
    access["credentials"]["destination_token"] = "read-only-resolution-token"
    attached = client.put(f"/api/onprem/jobs/{job_id}/credentials", json=access)
    assert attached.status_code == 200
    assert attached.json()["state"] == "stopped"
    assert notifications == [True]
    response = client.post(f"/api/onprem/jobs/{job_id}/resolve", json=resolution(
        service, job_id, attempt,
    ))
    assert response.status_code == 200
    assert response.json()["state"] == "stopped"
    assert response.json()["stop_requested"] is True
    assert service.store.attempt(attempt["id"])["phase"] == "verifying_final"
    assert service.store.entry(entry["id"])["state"] == "committing"
    assert destination.imports == destination.moves == 0
    assert client.post(f"/api/onprem/jobs/{job_id}/resume").json()["detail"] == (
        "job_stopped"
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


def test_pending_stop_rejects_final_move_resolution_until_local_ack(api):
    client, service = api
    job_id, _, attempt = attempted_job(api, phase="unknown_move")
    service.store.request_stop(job_id)
    response = client.post(f"/api/onprem/jobs/{job_id}/resolve", json=resolution(
        service, job_id, attempt,
    ))
    assert (response.status_code, response.json()["detail"]) == (409, "job_stopped")
    assert service.store.attempt(attempt["id"])["phase"] == "unknown_move"


def test_stopped_move_credentials_validate_destination_when_source_is_unavailable(
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
    assert attached.status_code == 200
    assert attached.json()["state"] == "stopped"
    assert service.registry.get(job_id) == REQUEST["credentials"]
    assert source_calls == []
    assert notifications == [True]
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


@pytest.mark.parametrize("phase", ["move_intent", "waiting_move", "unknown_move"])
@pytest.mark.parametrize("known_reference", [False, True])
@pytest.mark.parametrize("status,expected_phase", [
    ("success", "verifying_final"), ("failed", "failed"),
    ("in-progress", "waiting_move"),
])
def test_stopped_move_operation_resolution_only_polls_destination_and_keeps_link(
    api, monkeypatch, phase, known_reference, status, expected_phase,
):
    client, service = api
    job_id, entry, attempt = attempted_job(api, phase=phase)
    service.store.request_stop(job_id)
    service.store.update_job(job_id, state="stopped")
    source_calls = []
    destination = Destination()
    reference = "?id=stopped-server-op"
    if known_reference:
        service.store.update_attempt(attempt["id"], operation_id=reference)

    def unavailable_source(config, credentials):
        source_calls.append(config)
        raise MigrationError("source_auth_failed", auth=True)

    def operation_status(operation_id):
        destination.polls.append(operation_id)
        return status

    monkeypatch.setattr(service.runner, "source_factory", unavailable_source)
    monkeypatch.setattr(
        service.runner, "destination_factory", lambda config, creds: destination,
    )
    monkeypatch.setattr(destination, "poll", operation_status)
    resolved = client.post(f"/api/onprem/jobs/{job_id}/resolve", json=resolution(
        service, job_id, attempt, operation_id=reference,
        terminal_status=None, evidence="",
    ))
    assert resolved.status_code == 200
    assert resolved.json()["state"] == "stopped"
    assert resolved.json()["stop_requested"] is True
    saved_attempt = service.store.attempt(attempt["id"])
    assert saved_attempt["phase"] == expected_phase
    assert saved_attempt["operation_id"] == reference
    assert service.store.entry(entry["id"])["state"] == (
        "failed" if status == "failed" else "committing"
    )
    audit = service.store.events(job_id)[-1]
    assert audit["type"] == "administrative_resolution"
    assert audit["payload"]["status"] == status
    assert audit["payload"]["operation_id"] == reference
    assert destination.polls == [reference]
    assert destination.moves == destination.imports == 0
    assert source_calls == []


def test_active_waiting_move_still_rejects_manual_resolution(api):
    client, service = api
    job_id, _, attempt = attempted_job(api, phase="waiting_move")
    before = service.store.attempt(attempt["id"])
    resolved = client.post(f"/api/onprem/jobs/{job_id}/resolve", json=resolution(
        service, job_id, attempt,
    ))
    assert (resolved.status_code, resolved.json()["detail"]) == (
        409, "attempt_not_unknown",
    )
    assert service.store.attempt(attempt["id"]) == before


@pytest.mark.parametrize("phase", ["move_intent", "waiting_move", "unknown_move"])
def test_stopped_final_move_rejects_substitution_of_known_operation(
    api, monkeypatch, phase,
):
    client, service = api
    job_id, _, attempt = attempted_job(api, phase=phase)
    service.store.update_attempt(attempt["id"], operation_id="?id=original-operation")
    service.store.request_stop(job_id)
    service.store.update_job(job_id, state="stopped")
    before = service.store.attempt(attempt["id"])
    destination = Destination()
    monkeypatch.setattr(
        service.runner, "destination_factory", lambda config, creds: destination,
    )
    resolved = client.post(f"/api/onprem/jobs/{job_id}/resolve", json=resolution(
        service, job_id, attempt, operation_id="?id=unrelated-operation",
        terminal_status=None, evidence="",
    ))
    assert (resolved.status_code, resolved.json().get("detail")) == (
        409, "operation_reference_mismatch",
    )
    assert service.store.attempt(attempt["id"]) == before
    assert destination.polls == []
