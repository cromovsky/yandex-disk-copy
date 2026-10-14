"""Administrative source credentials survive only in process memory."""

import copy
import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.migration.credentials import CredentialRegistry
from app.source_batches.api import router
from tests.test_migration_api import REQUEST, api
from tests.test_migration_store import config
from tests.test_source_batches import CONFIG, CSV, setup_service

ADMIN = "source-admin-memory-only-value"


def test_single_onprem_accepts_admin_token_without_persisting_or_returning_it(api, monkeypatch):
    client, service = api
    request = copy.deepcopy(REQUEST)
    request["credentials"]["source_admin_token"] = ADMIN
    response = client.post("/api/onprem/jobs", json=request)
    assert response.status_code == 200
    job_id = response.json()["id"]
    assert service.registry.get(job_id)["source_admin_token"] == ADMIN
    assert ADMIN not in response.text
    assert ADMIN.encode() not in service.store.path.read_bytes()
    service.store.update_job(job_id, state="awaiting_credentials", resume_state="ready")
    request.pop("idempotency_key")
    request["config"] = response.json()["config"]
    monkeypatch.setattr(service.runner, "preflight", lambda config, credentials: {})
    request["credentials"]["source_admin_token"] = "replacement-admin-memory-only"
    attached = client.put(f"/api/onprem/jobs/{job_id}/credentials", json=request)
    assert attached.status_code == 200
    assert service.registry.get(job_id)["source_admin_token"] == "replacement-admin-memory-only"
    assert "replacement-admin-memory-only" not in attached.text
    assert b"replacement-admin-memory-only" not in service.store.path.read_bytes()


def test_local_list_passes_admin_to_source_and_recovers_without_serializing_secret(setup_service):
    service, _ = setup_service
    app = FastAPI()
    app.state.source_batches = service
    app.include_router(router)
    credentials = {"source_client_secret": "source-memory-only", "source_admin_token": ADMIN}
    with TestClient(app) as client:
        response = client.post("/api/source-batches", json={
            "mode": "local", "sources_csv": CSV, "config": CONFIG,
            "credentials": credentials, "idempotency_key": "admin-access-test",
        })
        assert response.status_code == 200
        batch_id = response.json()["batch"]["id"]
        assert service._credentials_for(batch_id)["source_admin_token"] == ADMIN
        assert ADMIN not in response.text
        assert ADMIN.encode() not in service.store.path.read_bytes()
        assert "source_admin_token" not in response.json()["batch"]["config"]
        service.store.update(batch_id, state="awaiting_credentials", resume_state="ready")
        credentials["source_admin_token"] = "replacement-list-admin"
        response = client.post(f"/api/source-batches/{batch_id}/credentials", json={"credentials": credentials})
        assert response.status_code == 200
        assert service._credentials_for(batch_id)["source_admin_token"] == "replacement-list-admin"
        assert "replacement-list-admin" not in response.text
        assert b"replacement-list-admin" not in service.store.path.read_bytes()


def test_local_list_admin_errors_are_redacted_from_stored_journal(setup_service):
    service, _ = setup_service
    detail = service.create("local", CSV, CONFIG,
                            {"source_client_secret": "source-secret", "source_admin_token": ADMIN},
                            "admin-journal-redaction")
    batch = service.store.get(detail["batch"]["id"])
    source = service.store.sources(batch["id"])[0]
    service._event(batch, source, "planning", "failed", message="Rejected " + ADMIN)
    assert ADMIN.encode() not in service.store.path.read_bytes()
    exported = service.export(batch["id"], journal=True)
    try:
        assert ADMIN.encode() not in exported.read()
    finally:
        exported.close()


def test_optional_admin_does_not_break_old_migration_credentials():
    registry = CredentialRegistry()
    original = {"source_client_secret": "source-value", "destination_token": "destination-value"}
    registry.put("job", config(), original)
    assert registry.get("job") == original
    with_admin = {**original, "source_admin_token": ADMIN}
    registry.put("job", config(), with_admin)
    assert registry.get("job") == with_admin
    registry.put("job", config(), original)
    assert registry.get("job") == original


@pytest.mark.parametrize("value", ["", "bad\nvalue", 123, "x" * 16385])
def test_registry_rejects_invalid_optional_admin_credentials(value):
    registry = CredentialRegistry()
    with pytest.raises(ValueError):
        registry.put("job", config(), {"source_client_secret": "source-value",
                                     "destination_token": "destination-value",
                                     "source_admin_token": value})


def test_admin_cannot_enter_immutable_migration_configuration(api):
    client, _ = api
    request = json.loads(json.dumps(REQUEST))
    request["config"]["source_admin_token"] = ADMIN
    response = client.post("/api/onprem/jobs", json=request)
    assert response.status_code == 422
    assert ADMIN not in response.text
