"""Exercise the staged transfer through FastAPI, SQLite and both real Disk clients."""

import hashlib

import pytest
import requests

from app.migration.source import API, TOKEN_URL
from tests.test_migration_api import api
from tests.test_migration_clients import Response
from tests.test_migration_runner import BODY, DIR, META


@pytest.mark.parametrize("subject_kind,wrong_owner,shared", [
    ("email", False, False), ("uid", False, False), ("uid", True, False), ("email", False, True),
], ids=["email-transfer-with-uid-only", "uid-transfer", "uid-mismatch", "shared-operator"])
def test_single_file_with_exchanged_token(api, monkeypatch, subject_kind, wrong_owner, shared):
    client, service = api
    uid = "9007199254740993"
    recipient = "employee@office.example" if subject_kind == "email" else uid
    destination = "https://cloud-api.office.example/v1/disk"
    oauth = "https://oauth.office.example/token"
    download = "https://files.cloud.example/d?key=fixture-capability"
    upload = "https://files.office.example/u?key=fixture-capability"
    path = META["path"]
    namespace = "vd:team-disk:" if shared else ""
    root = namespace + "disk:/"
    target = root + "aivanov@cloud.example/a.txt" if shared else path
    resources = destination + ("/virtual-disks/resources" if shared else "/resources")
    nodes = {root: {"path": root, "type": "dir"}}
    operator_uid = "42" if wrong_owner else uid
    permissions = ["read", "write"]
    published = []
    uploads = []

    def request(session, method, url, **kwargs):
        auth = kwargs.get("headers", {}).get("Authorization")
        params = kwargs.get("params", {})
        if url == oauth:
            assert method == "POST" and kwargs["data"]["subject_token"] == recipient
            assert kwargs["data"]["subject_token_type"] == "urn:yandex:params:oauth:token-type:" + subject_kind
            assert kwargs["data"]["access_token"] == "fixture-owner-credential"
            assert kwargs["data"]["client_secret"] == "fixture-service-secret"
            return Response(payload={"access_token": "fixture-temporary", "expires_in": 3600})
        if url == TOKEN_URL:
            return Response(payload={"access_token": "fixture-saas-temporary", "expires_in": 3600})
        if url == API + "/resources":
            assert auth == "OAuth fixture-saas-temporary" and params["path"] == path
            info = dict(META)
            if published:
                info["public_key"] = "fixture-public-key"
            return Response(payload=info)
        if url == API + "/resources/publish":
            assert method == "PUT" and params["path"] == path
            published.append(path)
            return Response(payload={})
        if url == API + "/public/resources/download":
            assert params["public_key"] == "fixture-public-key" and not auth
            return Response(payload={"href": download})
        if url == download:
            assert method == "GET" and not auth
            return Response(body=BODY)
        if url == upload:
            assert method == "PUT" and not auth
            assert any(f.is_file() and f.read_bytes() == BODY for f in service.runner.staging.iterdir())
            content = kwargs["data"].read()
            uploads.append(content)
            nodes[target] = dict(META, path=target, size=len(content), md5=hashlib.md5(content).hexdigest())
            return Response(201)
        assert auth == "OAuth fixture-temporary"
        if url == destination + "/":
            return Response(payload={"user": {"uid": operator_uid},
                                     "total_space": 0 if shared else 1000000, "used_space": 0})
        if shared and url == destination + "/virtual-disks":
            assert params == {"vd_hash": "team-disk"}
            return Response(payload={"vd_hash": "team-disk", "permissions": permissions,
                                     "total_space": 1000000, "used_space": 0})
        if url == resources:
            key = params["path"]
            assert key.startswith(root)
            if method == "PUT":
                assert key == target.rsplit("/", 1)[0]
                nodes[key] = dict(DIR, path=key)
                return Response(201)
            assert method == "GET"
            return Response(payload=nodes[key]) if key in nodes else Response(404)
        assert url == resources + "/upload" and method == "GET"
        assert params == {"path": target, "overwrite": "false"}
        return Response(payload={"href": upload, "method": "PUT"})

    monkeypatch.setattr(requests.Session, "request", request)
    data = {
        "config": {"source_email": "aivanov@cloud.example", "source_path": path,
                   "source_org_id": "cloud", "source_client_id": "cloud-app",
                   "destination_type": "personal", "destination_auth": "service_application",
                   "destination_identity": recipient, "source_public": True, "strategy": "staged",
                   "destination_url": destination,
                   "destination_client_id": "registered-app"},
        "credentials": {"source_client_secret": "fixture-saas-secret",
                        "destination_client_secret": "fixture-service-secret",
                        "destination_owner_token": "fixture-owner-credential"},
    }
    if shared:
        data["config"].update(destination_type="shared", destination_vd_hash="team-disk")
        preview = client.post("/api/onprem/preflight", json=data)
        assert preview.status_code == 200, preview.text
        assert preview.json()["destination"]["verified_uid"] == uid
        assert preview.json()["destination"]["free_bytes"] == 1000000
    response = client.post("/api/onprem/jobs", json={**data, "idempotency_key": "uid-transfer"})
    assert response.status_code == 200
    job_id = response.json()["id"]
    base = f"/api/onprem/jobs/{job_id}"
    service.runner.step(job_id)
    plan = client.get(base).json()
    assert published == [] and list(nodes) == [root] and uploads == []
    if wrong_owner:
        assert plan["error"] == "destination_identity_mismatch" and not plan["plan_complete"]
        assert client.post(base + "/start").status_code == 409
    else:
        assert plan["state"] == "ready" and plan["destination_verified_uid"] == uid
        if shared:
            operator_uid = "777"
            assert client.post(base + "/start").json()["detail"] == "destination_identity_mismatch"
            assert client.put(base + "/credentials", json=data).json()["detail"] == "destination_identity_mismatch"
            assert client.post(base + "/start").json()["detail"] == "credentials_required"
            operator_uid = uid
            assert client.put(base + "/credentials", json=data).status_code == 200
            permissions.remove("write")
            assert client.post(base + "/start").json()["detail"] == "destination_shared_access_required"
            assert published == [] and uploads == [] and list(nodes) == [root]
            permissions.append("write")
        assert client.post(base + "/start").status_code == 200
        for _ in range(20):
            service.runner.step(job_id)
            if service.store.get(job_id)["state"] == "completed":
                break
        assert service.store.get(job_id)["state"] == "completed"
        assert published == [path] and uploads == [BODY]
        assert nodes[target]["md5"] == META["md5"]
        assert not list(service.runner.staging.iterdir())
    for suffix in ("/report", "/journal"):
        result = client.get(base + suffix)
        assert result.status_code == 200 and result.content
        assert b"fixture-" not in result.content
    assert service.store.path.is_file()
    for database_file in service.store.path.parent.glob("jobs.db*"):
        assert b"fixture-" not in database_file.read_bytes()
