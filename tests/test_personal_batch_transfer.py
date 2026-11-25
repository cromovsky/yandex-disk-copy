"""Two CSV routes through the batch API, SQLite, both OAuth clients and staged file I/O."""

import csv
import hashlib
import io
from pathlib import Path

import pytest
import requests
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.migration.api import router as job_router
from app.migration.destination import OnPremDiskDestination
from app.migration.source import API, TOKEN_URL, SaasDiskSource
from app.source_batches.api import router
from tests.test_migration_clients import Response
from tests.test_source_batches import onprem_service, wait_for


@pytest.mark.parametrize("with_folder,source_folder,deny_publication", [
    (False, "", False), (True, "", False), (True, "/Проекты/Пилот", False), (True, "/Проекты/Пилот", True),
], ids=["two-columns", "destination-folder", "selected-source-and-destination-folders", "publication-locked"])
def test_two_personal_pairs_staged_transfer(onprem_service, monkeypatch, with_folder, source_folder, deny_publication):
    batches, migration = onprem_service
    batches.source_factory = migration.runner.source_factory = SaasDiskSource
    migration.runner.destination_factory = OnPremDiskDestination
    monkeypatch.setenv("ONPREM_ENABLED", "1")
    monkeypatch.setenv("ONPREM_ADMIN_TOKEN", "test-key")
    app = FastAPI()
    app.state.source_batches, app.state.onprem = batches, migration
    app.include_router(router)
    app.include_router(job_router)
    client = TestClient(app, base_url="https://utility.example", headers={"X-Onprem-Key": "test-key"})

    cloud = ["one@cloud.example", "two@cloud.example"]
    office = ["first@office.example", "second@office.example"]
    user_ids = ["9007199254740993", "9007199254740995"]
    contents = [b"first file", b"second file"]
    destination = "https://cloud-api.office.example/v1/disk"
    oauth = "https://oauth.office.example/token"
    transfers = "https://files.example"
    root = {"type": "dir", "path": "disk:/"}
    source_dirs = [root, {"type": "dir", "path": "disk:" + (source_folder or "/")}]
    paths = ["disk:/note.txt", f"disk:{source_folder}/note.txt"]
    targets = [paths[0], f"disk:/Архив{source_folder}/note.txt" if with_folder else paths[1]]
    files = [
        {"type": "file", "path": paths[index], "name": "note.txt", "size": len(body),
         "md5": hashlib.md5(body).hexdigest(), "resource_id": f"note-{index}"}
        for index, body in enumerate(contents)
    ]
    stored = [{"disk:/": root}, {"disk:/": root}]
    published, uploads, exchanges = [], [], []
    denied = []
    credentials = {"source_client_secret": "fixture-cloud-secret",
                   "destination_client_secret": "fixture-office-secret",
                   "destination_owner_token": "fixture-owner-token"}

    def request(session, method, url, **kwargs):
        auth = kwargs.get("headers", {}).get("Authorization", "")
        params = kwargs.get("params", {})
        if url in (TOKEN_URL, oauth):
            form = kwargs["data"]
            assert method == "POST" and form["subject_token_type"].endswith(":email")
            assert form["grant_type"] == "urn:ietf:params:oauth:grant-type:token-exchange"
            email = form["subject_token"]
            exchanges.append((url, email))
            if url == oauth:
                index = office.index(email)
                assert form["access_token"] == credentials["destination_owner_token"]
                assert set(form["scope"].split()) == {"cloud_api:disk.read", "cloud_api:disk.write", "cloud_api:disk.info"}
                assert form["client_secret"] == credentials["destination_client_secret"]
                token = f"fixture-office-{index}"
            else:
                index = cloud.index(email)
                assert form["client_secret"] == credentials["source_client_secret"]
                token = f"fixture-cloud-{index}"
            return Response(payload={"access_token": token, "expires_in": 3600})
        if url == API + "/public/resources/download":
            assert not auth
            index = int(params["public_key"].removeprefix("public-"))
            if index == 1 and source_folder:
                assert params["path"] == "/note.txt"
            return Response(payload={"href": f"{transfers}/download/{index}"})
        if url.startswith(API + "/"):
            index = ["OAuth fixture-cloud-0", "OAuth fixture-cloud-1"].index(auth)
            folder = source_dirs[index]["path"]
            share_path = paths[index] if folder == "disk:/" else folder
            if url.endswith("/publish"):
                assert method == "PUT" and params["path"] == share_path
                if deny_publication and index == 1:
                    denied.append(share_path)
                    return Response(423, payload={"error": "DiskReadOnlyError"})
                published.append(index)
                return Response(payload={})
            assert method == "GET" and url == API + "/resources"
            if "offset" in params:
                assert params["path"] == folder  # A filtered route may only list the selected folder.
                return Response(payload={"_embedded": {"total": 1, "items": [files[index]]}})
            assert params["path"] in (folder, paths[index])
            metadata = source_dirs[index] if params["path"] == folder else files[index]
            if params["path"] == share_path and index in published:
                metadata = {**metadata, "public_key": f"public-{index}"}
            return Response(payload=metadata)
        if url.startswith(transfers + "/"):
            assert not auth
            index = int(url.split("/")[-1])
            if method == "GET":
                assert url == f"{transfers}/download/{index}"
                return Response(body=contents[index])
            assert method == "PUT" and url == f"{transfers}/upload/{index}"
            cache_file = Path(kwargs["data"].name)
            assert cache_file.parent == migration.runner.staging
            body = kwargs["data"].read()
            assert cache_file.read_bytes() == body
            assert body == contents[index]
            uploads.append(index)
            target = targets[index]
            stored[index][target] = {**files[index], "path": target}
            return Response(201)
        index = ["OAuth fixture-office-0", "OAuth fixture-office-1"].index(auth)
        if url == destination + "/":
            return Response(payload={"user": {"uid": user_ids[index]}, "total_space": 10000, "used_space": 0})
        if url == destination + "/resources/upload":
            assert params == {"path": targets[index], "overwrite": "false"}
            return Response(payload={"href": f"{transfers}/upload/{index}", "method": "PUT"})
        assert url == destination + "/resources"
        key = params["path"]
        if method == "PUT":
            assert with_folder and index == 1 and targets[index].startswith(key + "/")
            stored[index][key] = {"path": key, "type": "dir"}
            return Response(201)
        assert method == "GET"
        return Response(payload=stored[index][key]) if key in stored[index] else Response(404)

    monkeypatch.setattr(requests.Session, "request", request)
    header = ["source_email", "destination_email"]
    if source_folder:
        header.append("path")
    if with_folder:
        header.append("destination_folder")
    manifest = [",".join(header), ""]
    for index, (source, target) in enumerate(zip(cloud, office)):
        row = [source, target]
        if source_folder:
            row.append(source_folder if index else "")
        if with_folder:
            row.append("/Архив" if index else "")
        manifest.append(",".join(row))
        manifest.append("")
    reply = client.post("/api/source-batches", json={
        "mode": "onprem", "sources_csv": "\n".join(manifest), "idempotency_key": "two-user-transfer",
        "config": {"source_org_id": "1", "source_client_id": "cloud-app",
                   "destination_type": "personal", "destination_auth": "service_application",
                   "destination_url": destination, "destination_client_id": "office-app"},
        "credentials": credentials,
    })
    assert reply.status_code == 200, reply.text
    assert all("planned_destination_path" not in row for row in reply.json()["sources"])
    batch_id = reply.json()["batch"]["id"]
    base = f"/api/source-batches/{batch_id}"
    assert client.post(base + "/plan").status_code == 200
    batches.start()
    planned = wait_for(batches, batch_id, "ready", timeout=12)
    assert [row["destination_verified_uid"] for row in planned["sources"]] == user_ids
    assert [row["csv_row"] for row in planned["sources"]] == [1, 2]
    assert [row["planned_destination_path"] for row in planned["sources"]] == [
        "disk:/", f"disk:/Архив{source_folder}" if with_folder else "disk:/"]
    assert not published and not denied and not uploads and all(list(nodes) == ["disk:/"] for nodes in stored)
    assert client.post(base + "/start").status_code == 200
    final_state = "completed_with_issues" if deny_publication else "completed"
    wait_for(batches, batch_id, final_state, timeout=15)
    assert sorted(published) == sorted(uploads) == ([0] if deny_publication else [0, 1])
    if deny_publication:
        assert denied == [source_dirs[1]["path"]] * 3
        assert list(stored[1]) == ["disk:/"]
    assert set(exchanges) == {(TOKEN_URL, email) for email in cloud} | {(oauth, email) for email in office}
    assert not list(migration.runner.staging.iterdir())
    job_url = "/api/onprem/jobs/" + planned["sources"][1]["child_job_id"]
    child = client.get(job_url).json()
    assert child["managed_by_batch"] and child["state"] == final_state
    restart = client.post(job_url + "/restart", json={"config": child["config"], "credentials": credentials,
                                                       "idempotency_key": "standalone-restart"})
    assert (restart.status_code, restart.json()["detail"]) == (409, "managed_batch_start_required")

    for endpoint in ("report", "journal"):
        response = client.get(f"{base}/{endpoint}.csv")
        assert response.status_code == 200 and "fixture-" not in response.text
        rows = list(csv.DictReader(io.StringIO(response.content.decode("utf-8-sig"))))
        assert {(row["csv_row"], row["destination_email"], row["destination_verified_uid"]) for row in rows} == {
            (str(i+1), office[i], user_ids[i]) for i in range(2)
        }
        if endpoint == "report":
            assert {(row["csv_row"], row["source_path"], row["destination_path"]) for row in rows
                    if row["kind"] == "file"} == {(str(i+1), paths[i], targets[i]) for i in range(2)}
            if deny_publication:
                assert {row["state"] for row in rows if row["csv_row"] == "2"} == {"failed"}
        elif deny_publication:
            single = client.get(job_url + "/journal")
            assert single.status_code == 200
            child_events = list(csv.DictReader(io.StringIO(single.content.decode("utf-8-sig"))))
            for journal in ([row for row in rows if row["csv_row"] == "2"], child_events):
                retries = [row for row in journal if row["stage"] == "publish_source" and
                           row["status"] in {"waiting", "skipped"} and row["api_error"] == "DiskReadOnlyError"]
                assert {(row["publication_attempts"], row["max_publication_attempts"], row["http_status"])
                        for row in retries} == {(str(n), "3", "423") for n in (1, 2, 3)}
