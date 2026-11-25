"""CSV sources share an operator on one On-premises Disk across planning and recovery."""

from pathlib import Path

import pytest
import requests
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.migration.destination import OnPremDiskDestination
from app.migration.errors import MigrationError
from app.source_batches.api import router
from app.source_batches.store import clean_config
from tests.test_migration_clients import Response
from tests.test_source_batches import BODY, CONFIG, CREDENTIALS, MD5, onprem_service, wait_for


OPERATOR = "operator@office.example"
API = "https://cloud-api.office.example/v1/disk"
CONFIG_SHARED = {**CONFIG, "destination_url": API, "destination_type": "shared",
                 "destination_identity": OPERATOR, "destination_vd_hash": "target",
                 "destination_auth": "service_application", "destination_client_id": "office-app"}
SECRETS = {**CREDENTIALS, "destination_client_secret": "office-secret",
           "destination_owner_token": "owner-token"}


def test_shared_csv_service_contract():
    saved = clean_config("onprem", CONFIG_SHARED)
    assert saved["identity_confirmed"] is False
    assert saved["destination_oauth_url"] == "https://oauth.office.example/token"
    assert clean_config("onprem", saved) == saved


@pytest.mark.parametrize("change", [{"destination_identity": "bare-login"},
                                    {"destination_identity": "1234"}, {"identity_confirmed": True},
                                    {"destination_auth": "admin_token"},
                                    {"destination_oauth_url": "https://other.example/token"}])
def test_shared_csv_rejects_unbound_identity_or_grant(change):
    with pytest.raises(ValueError):
        clean_config("onprem", dict(CONFIG_SHARED, **change))


@pytest.fixture
def shared_list(onprem_service, monkeypatch):
    from app.migration.source import API as CLOUD_API, TOKEN_URL, SaasDiskSource
    from tests.test_migration_clients import Limiter

    batches, migration = onprem_service
    batches.source_factory = migration.runner.source_factory = SaasDiskSource
    migration.runner.destination_factory = OnPremDiskDestination
    monkeypatch.setattr("app.migration.destination.get_onprem_limiter", lambda _: Limiter())
    monkeypatch.setenv("ONPREM_ENABLED", "1")
    monkeypatch.setenv("ONPREM_ADMIN_TOKEN", "test-key")
    app = FastAPI()
    app.state.source_batches, app.state.onprem = batches, migration
    app.include_router(router)
    client = TestClient(app, base_url="https://utility.example", headers={"X-Onprem-Key": "test-key"})
    sources = ["user@company.ru", "second@company.ru"]
    meta = {"type": "file", "path": "disk:/note.txt", "name": "note.txt", "size": len(BODY), "md5": MD5,
            "resource_id": "source-file"}
    server = {"uid": "9007199254740993", "permissions": ["read", "write"], "writes": [], "quota": 1000}
    disk = {"vd:target:disk:/": {"type": "dir", "path": "vd:target:disk:/"}}
    published = set()
    transfer = "https://files.example"

    def request(session, method, url, **kwargs):
        auth = kwargs.get("headers", {}).get("Authorization")
        args = kwargs.get("params", {})
        if url in (TOKEN_URL, "https://oauth.office.example/token"):
            form = kwargs["data"]
            assert method == "POST" and form["subject_token_type"].endswith(":email")
            email = form["subject_token"]
            if url == TOKEN_URL:
                assert email in sources and form["client_secret"] == CREDENTIALS["source_client_secret"]
            else:
                assert email == OPERATOR and form["client_secret"] == SECRETS["destination_client_secret"]
                assert form["access_token"] == SECRETS["destination_owner_token"]
                assert set(form["scope"].split()) == {"cloud_api:disk.read", "cloud_api:disk.write", "cloud_api:disk.info"}
            return Response(payload={"access_token": email, "expires_in": 3600})
        if url == CLOUD_API + "/public/resources/download":
            assert not auth
            return Response(payload={"href": transfer + "/get/" + args["public_key"]})
        if url.startswith(CLOUD_API + "/"):
            email = auth.removeprefix("OAuth ")
            assert email in sources and args["path"] == "disk:/note.txt"
            if url.endswith("/publish"):
                assert method == "PUT"
                published.add(email)
                return Response()
            return Response(payload={**meta, **({"public_key": email} if email in published else {})})
        if url.startswith(transfer):
            assert not auth
            if method == "GET":
                assert url.rsplit("/", 1)[-1] in published
                return Response(body=BODY)
            assert method == "PUT" and Path(kwargs["data"].name).parent == migration.runner.staging
            assert kwargs["data"].read() == BODY
            path = "vd:target:disk:/" + url.removeprefix(transfer + "/put/")
            disk[path] = {**meta, "path": path}
            server["writes"].append(path)
            return Response(201)
        assert auth == "OAuth " + OPERATOR
        if url == API + "/":
            return Response(payload={"user": {"uid": server["uid"]}, "total_space": 0, "used_space": 0})
        if url == API + "/virtual-disks":
            assert args["vd_hash"] == "target"
            return Response(payload={"vd_hash": "target", "permissions": server["permissions"],
                                     "total_space": server["quota"], "used_space": 0})
        path = args["path"]
        assert path.startswith("vd:target:disk:/")
        if url == API + "/virtual-disks/resources/upload":
            assert args["overwrite"] == "false"
            return Response(payload={"href": transfer + "/put/" + path.removeprefix("vd:target:disk:/")})
        assert url == API + "/virtual-disks/resources"
        if method == "PUT":
            disk[path] = {"path": path, "type": "dir"}
            return Response(201)
        return Response(payload=disk[path]) if path in disk else Response(404)

    monkeypatch.setattr(requests.Session, "request", request)
    result = client.post("/api/source-batches", json={
        "mode": "onprem", "sources_csv": "type,email,vd_hash,path\n" + "\n".join(
            f"personal,{email},,/note.txt" for email in sources),
        "config": CONFIG_SHARED, "credentials": SECRETS, "idempotency_key": "two-shared-rows",
    })
    assert result.status_code == 200, result.text
    batch_id = result.json()["batch"]["id"]
    url = "/api/source-batches/" + batch_id
    assert client.post(url + "/plan").status_code == 200
    return client, batches, migration, batch_id, url, server


def test_two_shared_service_rows_transfer_to_their_own_folders(shared_list):
    client, batches, migration, batch_id, url, server = shared_list
    batches.start()
    planned = wait_for(batches, batch_id, "ready")
    assert planned["batch"]["shared_operator_uid"] == server["uid"]
    assert {r["destination_verified_uid"] for r in planned["sources"]} == {server["uid"]}
    assert not server["writes"]
    response = client.post(url + "/start")
    assert response.status_code == 200, response.text
    wait_for(batches, batch_id, "completed", timeout=10)
    assert sorted(server["writes"]) == ["vd:target:disk:/second@company.ru/note.txt",
                                        "vd:target:disk:/user@company.ru/note.txt"]
    assert not list(migration.runner.staging.iterdir())


@pytest.mark.parametrize("reopen", [False, True])
def test_resume_polls_accepted_upload_after_unavailable_operation(shared_list, monkeypatch, reopen):
    client, batches, migration, batch_id, url, server = shared_list
    source = batches.store.sources(batch_id)[0]
    batches.selection(batch_id, [source["id"]])
    batches._plan_one(source["id"])
    batches._finish_planning(batch_id)
    child = batches.store.source(source["id"])["child_job_id"]
    operation = "accepted-upload"
    polls = []
    request = requests.Session.request

    def async_upload(session, method, endpoint, **kwargs):
        if endpoint == API + "/operations/" + operation:
            polls.append(endpoint)
            return Response(503) if len(polls) == 1 else Response(payload={"status": "success"})
        result = request(session, method, endpoint, **kwargs)
        if endpoint == API + "/virtual-disks/resources/upload":
            result.payload["operation_id"] = operation
        elif method == "PUT" and endpoint.startswith("https://files.example/put/"):
            result.status_code = 202
        return result

    monkeypatch.setattr(requests.Session, "request", async_upload)
    assert client.post(url + "/start").status_code == 200
    batches._transfer_one(source["id"])
    attempt, = migration.store.attempts(child)
    assert (attempt["phase"], attempt["operation_id"]) == ("waiting_transfer", operation)
    paused = batches.detail(batch_id)
    assert paused["batch"]["state"] == paused["sources"][0]["state"] == "paused"
    assert paused["batch"]["error"] == "destination_unavailable"
    if reopen:
        batches.store.recover()
        migration.store.recover()
        batches._credentials.clear()
        migration.registry.clear()
        assert client.post(url + "/credentials", json={"credentials": SECRETS}).json()["batch"]["state"] == "paused"
    assert client.post(url + "/resume").status_code == 200
    batches.start()
    wait_for(batches, batch_id, "completed")
    assert len(polls) == 2 and len(server["writes"]) == 1
    finished, = migration.store.attempts(child)
    assert (finished["id"], finished["phase"]) == (attempt["id"], "succeeded")


def test_shared_operator_reassigned_between_rows_and_before_start(shared_list):
    client, batches, migration, batch_id, url, server = shared_list
    first, second = batches.store.sources(batch_id)
    batches._plan_one(first["id"])
    uid = server["uid"]
    server["uid"] = "9007199254740994"
    batches._plan_one(second["id"])
    assert batches.store.source(second["id"])["error"] == "destination_identity_mismatch"
    assert batches.store.get(batch_id)["shared_operator_uid"] == uid
    server["uid"] = uid
    response = client.post(url + "/sources/" + second["id"] + "/plan")
    assert response.status_code == 200
    batches._plan_one(second["id"])
    batches._finish_planning(batch_id)
    assert batches.store.get(batch_id)["state"] == "ready"
    server["uid"] = "9007199254740994"
    assert client.post(url + "/start").json()["detail"] == "destination_identity_mismatch"
    batches._credentials.clear()
    migration.registry.clear()
    result = client.post(url + "/credentials", json={"credentials": SECRETS})
    assert result.status_code == 409 and result.json()["detail"] == "destination_identity_mismatch"
    assert not server["writes"] and batch_id not in batches._credentials
    assert not any(migration.registry.get(s["child_job_id"]) for s in batches.store.sources(batch_id))


def test_shared_recovery_rechecks_acl_and_point_retry_rechecks_operator(shared_list):
    client, batches, migration, batch_id, url, server = shared_list
    for row in batches.store.sources(batch_id):
        batches._plan_one(row["id"])
    batches._finish_planning(batch_id)
    assert client.post(url + "/start").status_code == 200
    child = batches.store.sources(batch_id)[0]["child_job_id"]
    batches.store.recover()
    migration.store.recover()
    batches._credentials.clear()
    migration.registry.clear()
    assert batches.store.get(batch_id)["shared_operator_uid"] == server["uid"]
    server["permissions"] = ["read"]
    response = client.post(url + "/credentials", json={"credentials": SECRETS})
    assert response.json()["detail"] == "destination_shared_access_required"
    assert batch_id not in batches._credentials and not migration.registry.get(child)
    server["permissions"] = ["read", "write"]
    assert client.post(url + "/credentials", json={"credentials": SECRETS}).json()["batch"]["state"] == "paused"
    server["uid"] = "999"
    assert client.post(url + "/resume").json()["detail"] == "destination_identity_mismatch"
    server["uid"] = batches.store.get(batch_id)["shared_operator_uid"]
    assert client.post(url + "/resume").json()["batch"]["state"] == "running"
    server["uid"] = "998"
    with pytest.raises(MigrationError, match="destination_identity_mismatch"):
        batches.prepare_child_retry(child)
    assert not server["writes"]
