"""The list API keeps administrative source access bound to every child in memory."""

import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.source_batches.api import router
from tests.test_source_batches import (
    FakeDestination,
    FakeSource,
    ONPREM_CONFIG,
    ONPREM_CREDS,
    onprem_service,
)


@pytest.mark.parametrize("kind", ["personal", "shared"])
def test_onprem_list_admin_reaches_plan_and_child_after_reattachment(
    onprem_service, monkeypatch, kind,
):
    service, onprem = onprem_service
    source_clients, destination_credentials = [], []

    class RecordingSource(FakeSource):
        def __init__(self, config, credentials):
            super().__init__(config, credentials)
            self.credentials = dict(credentials)
            self.closed = False
            source_clients.append(self)

        def close(self):
            self.closed = True

    class RecordingDestination(FakeDestination):
        def __init__(self, config, credentials):
            super().__init__(config, credentials)
            destination_credentials.append(dict(credentials))

    # Every boundary uses fake clients; no source or destination HTTP is allowed.
    monkeypatch.setattr("requests.sessions.Session.request", lambda *a, **k: pytest.fail(
        "the access integration regression must not contact a real disk",
    ))
    monkeypatch.setenv("ONPREM_ENABLED", "1")
    monkeypatch.setenv("ONPREM_ADMIN_TOKEN", "integration-utility-access")
    service.source_factory = RecordingSource
    onprem.runner.source_factory = RecordingSource
    onprem.runner.destination_factory = RecordingDestination

    app = FastAPI()
    app.state.source_batches = service
    app.include_router(router)
    credentials = {**ONPREM_CREDS, "source_admin_token": "first-list-admin-memory-only"}
    config = {**ONPREM_CONFIG, "source_org_id": "12345"}
    vd_hash = "source-disk" if kind == "shared" else ""
    sources_csv = "type,email,vd_hash,path\n" + f"{kind},user@company.ru,{vd_hash},/\n"
    all_secrets = set(credentials.values())

    with TestClient(app, base_url="https://utility.example.org",
                    headers={"X-Onprem-Key": "integration-utility-access"}) as client:
        created = client.post("/api/source-batches", json={
            "mode": "onprem", "sources_csv": sources_csv, "config": config,
            "credentials": credentials, "idempotency_key": "integration-admin-" + kind,
        })
        assert created.status_code == 200
        batch_id = created.json()["batch"]["id"]
        assert service._credentials_for(batch_id) == credentials
        assert "source_admin_token" not in created.json()["batch"]["config"]

        planned = client.post(f"/api/source-batches/{batch_id}/plan")
        assert planned.status_code == 200
        source_id = planned.json()["sources"][0]["id"]
        service._plan_one(source_id)
        service._finish_planning(batch_id)
        ready = client.get(f"/api/source-batches/{batch_id}")
        assert ready.json()["batch"]["state"] == "ready", ready.text
        child_id = ready.json()["sources"][0]["child_job_id"]
        child = onprem.store.get(child_id)
        assert onprem.registry.get(child_id) == credentials
        assert child["config"]["source_org_id"] == "12345"
        assert "source_admin_token" not in child["config"]
        assert source_clients and all(c.credentials == credentials for c in source_clients)
        assert all(c.config["source_org_id"] == "12345" for c in source_clients)
        assert all(c == credentials for c in destination_credentials)

        # Simulate process-memory loss while retaining the completed Dry Run.
        prior_child_source = onprem.runner.clients(child)[0]
        service._credentials.clear()
        onprem.registry.discard(child_id)
        service.store.update(batch_id, state="awaiting_credentials", resume_state="ready")
        onprem.store.update_job(child_id, state="awaiting_credentials", resume_state="ready")
        replacement = {**credentials, "source_admin_token": "reattached-list-admin-memory-only"}
        all_secrets.update(replacement.values())
        attached = client.post(f"/api/source-batches/{batch_id}/credentials", json={
            "credentials": replacement,
        })
        assert attached.status_code == 200
        assert attached.json()["batch"]["state"] == "ready"
        assert onprem.store.get(child_id)["state"] == "ready"
        assert prior_child_source.closed
        assert service._credentials_for(batch_id) == replacement
        assert onprem.registry.get(child_id) == replacement
        refreshed_source = onprem.runner.clients(onprem.store.get(child_id))[0]
        assert refreshed_source is not prior_child_source
        assert refreshed_source.credentials == replacement

        # Responses, both CSV downloads and all persistent files remain secret-free.
        responses = [created, planned, ready, attached,
                     client.get(f"/api/source-batches/{batch_id}/report.csv"),
                     client.get(f"/api/source-batches/{batch_id}/journal.csv")]
        for response in responses:
            assert response.status_code == 200
            assert not any(secret in response.text for secret in all_secrets)
        assert "source_admin_token" not in json.dumps(service.detail(batch_id))
        for path in onprem.root.rglob("*"):
            if path.is_file():
                raw = path.read_bytes()
                assert not any(secret.encode() in raw for secret in all_secrets), path
