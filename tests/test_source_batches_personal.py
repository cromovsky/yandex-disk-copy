import csv
import io
import json
import sqlite3

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.migration.destination import OnPremDiskDestination
from app.source_batches.api import router
from app.source_batches.store import BatchStore, parse_sources
from tests.test_source_batches import CONFIG, CSV, ONPREM_CONFIG, ONPREM_CREDS, onprem_service


HEADER = "source_email,destination_email"
PAIR = "A@cloud.example,b@office.example"
PAIRS = HEADER + "\n" + PAIR + "\n"
PERSONAL = dict(CONFIG, destination_type="personal", destination_auth="service_application",
                destination_client_id="office-app", destination_url="https://CLOUD-API.office.example:443")
SECRETS = {"source_client_secret": "fixture-saas-app", "source_admin_token": "fixture-admin-token",
           "destination_client_secret": "fixture-office-app", "destination_owner_token": "fixture-owner-token"}


@pytest.mark.parametrize("columns,values,path,folder", [
    ("destination_email", " b@office.example ", "disk:/", "disk:/"),
    ("destination_email,path", "b@office.example,/Фото", "disk:/Фото", "disk:/"),
    ("destination_email,destination_folder", 'b@office.example,"/Архив, 2026"', "disk:/", "disk:/Архив, 2026"),
    ("destination_email,destination_folder,path", "b@office.example,,/Документы", "disk:/Документы", "disk:/"),
    ("destination_email,path,destination_folder", 'b@office.example,,"/Архив, 2026"', "disk:/", "disk:/Архив, 2026"),
    ("destination_email,path,destination_folder", "b@office.example,/Документы,/Архив", "disk:/Документы", "disk:/Архив"),
    ("path,destination_email,destination_folder", ',b@office.example,"/Архив, 2026"', "disk:/", "disk:/Архив, 2026"),
    ("path,destination_email,destination_folder", '"/Фото, 2026",b@office.example,/Архив', "disk:/Фото, 2026", "disk:/Архив"),
])
def test_personal_csv_paths(columns, values, path, folder):
    content = f"\ufeff\r\nsource_email,{columns}\r\n\r\n  A@cloud.example ,{values}\r\n"
    assert parse_sources(content, personal=True) == [
        {"type": "personal", "email": "A@cloud.example", "vd_hash": "", "path": path,
         "destination_email": "b@office.example", "destination_folder": folder, "csv_row": 1},
    ]


@pytest.mark.parametrize("content,code,row", [
    ("destination_email,path\n", "invalid_sources_columns", None),
    ('"unclosed header', "invalid_sources_columns", None),
    (HEADER + ",path,path\n", "invalid_sources_columns", None),
    (HEADER + "\n\n", "empty_source_list", None),
    (HEADER + '\n"unclosed', "invalid_sources_row", 1),
    (PAIRS + "a@cloud.example,login\n", "invalid_sources_row", 2),
    (PAIRS + '\n\na@cloud.example,"unclosed', "invalid_sources_row", 2),
    (PAIRS + "a@cloud.example\n", "invalid_sources_row", 2),
    (HEADER + ",destination_folder\n" + PAIR + ",relative\n", "invalid_disk_path", 1),
    (HEADER + ",path\n" + PAIR + ",/bad/../path\n", "invalid_disk_path", 1),
    pytest.param(HEADER + "\n" + (PAIR + "\n") * 1001, "invalid_sources_row", 1001, id="row-limit"),
    pytest.param(HEADER + "\n" + "я" * (1024**2), "invalid_sources_csv", None, id="byte-limit"),
])
def test_personal_csv_errors(content, code, row):
    with pytest.raises(ValueError, match=code) as caught:
        parse_sources(content, personal=True)
    assert caught.value.csv_row == row


def test_old_shared_history_and_idempotency_survive_schema_upgrade(onprem_service):
    service, _ = onprem_service
    batch = service.create("onprem", CSV, ONPREM_CONFIG, ONPREM_CREDS, "old-shared-list")["batch"]
    store = service.store
    store.close()
    with sqlite3.connect(store.path) as old:
        for col in ("csv_row", "destination_folder", "destination_email"):
            old.execute(f"ALTER TABLE sources DROP COLUMN {col}")
        old.execute("ALTER TABLE batches DROP COLUMN mapping_issues")
    service.store = BatchStore(store.root)
    restored = service.create("onprem", CSV, ONPREM_CONFIG, ONPREM_CREDS, "old-shared-list")
    assert restored["batch"]["id"] == batch["id"]
    assert restored["batch"]["mapping_issues"] == {}
    assert restored["batch"]["config"]["destination_type"] == "shared"
    assert restored["sources"][0]["email"] == "user@company.ru"


def test_old_personal_history_preserves_issues_exports_and_idempotency(personal_api):
    _, service = personal_api
    content = HEADER + "\n\n" + PAIR + "\n\n" + PAIR + "\n"
    key = "old-personal-list"
    result = service.create("onprem", content, PERSONAL, SECRETS, key)
    batch_id = result["batch"]["id"]
    first_id = result["sources"][0]["id"]
    store = service.store
    with sqlite3.connect(store.path) as old:
        old.execute("UPDATE sources SET csv_row=3+position*2 WHERE batch_id=?", (batch_id,))
        issue = {"mapping_error": "overlapping_source_paths", "conflict_rows": [3, 5], "conflict_path": "disk:/3/5"}
        old.execute("UPDATE batches SET mapping_issues=?,state='needs_attention' WHERE id=?",
                    (json.dumps({first_id: issue}), batch_id))

    for _ in range(2):
        service.store.close()
        service.store = BatchStore(store.root)
        restored = service.create("onprem", content, PERSONAL, SECRETS, key)
        assert restored["batch"]["id"] == batch_id
        assert [row["csv_row"] for row in restored["sources"]] == [1, 2]
        assert restored["batch"]["mapping_issues"][first_id] == dict(issue, conflict_rows=[1, 2])
        assert restored["sources"][0]["conflict_rows"] == [1, 2]
        for journal in (False, True):
            with service.export(batch_id, journal=journal) as report:
                first = next(csv.DictReader(io.StringIO(report.read().decode("utf-8-sig"))))
            assert (first["csv_row"], first["conflict_rows"], first["conflict_path"]) == ("1", "1, 2", "disk:/3/5")


@pytest.fixture
def personal_api(onprem_service, monkeypatch):
    service, onprem = onprem_service
    onprem.runner.destination_factory = OnPremDiskDestination
    monkeypatch.setenv("ONPREM_ENABLED", "1")
    monkeypatch.setenv("ONPREM_ALLOW_INSECURE_HTTP", "1")
    monkeypatch.delenv("ONPREM_ADMIN_TOKEN", raising=False)
    app = FastAPI()
    app.state.source_batches = service
    app.include_router(router)
    with TestClient(app, base_url="http://localhost") as client:
        yield client, service


def test_personal_api_persists_routes_and_rejects_changed_idempotent_requests(personal_api):
    client, service = personal_api
    url = "/api/source-batches"
    # Conflicting rows can be individually excluded from the later Dry Run.
    content = PAIRS + "a@cloud.example,other@office.example\n"
    payload = dict(mode="onprem", config=PERSONAL, sources_csv=content, credentials=SECRETS,
                   idempotency_key="personal-pairs")
    response = client.post(url, json=payload)
    assert response.status_code == 200
    result = response.json()
    batch = result["batch"]
    assert batch["config"]["destination_oauth_url"] == "https://oauth.office.example/token"
    assert batch["config"]["destination_url"] == "https://cloud-api.office.example/v1/disk"
    assert [s["destination_email"] for s in result["sources"]] == ["b@office.example", "other@office.example"]
    assert client.post(url, json=payload).json()["batch"]["id"] == batch["id"]
    changed_folder = (content.replace(HEADER, HEADER + ",destination_folder")
                      .replace("b@office.example\n", "b@office.example,/Archive\n")
                      .replace("other@office.example\n", "other@office.example,\n"))
    for changed in (content.replace("b@office.example", "x@office.example"), changed_folder):
        conflict = client.post(url, json=dict(payload, sources_csv=changed))
        assert (conflict.status_code, conflict.json()["detail"]) == (409, "idempotency_conflict")
    invalid = client.post(url, json=dict(payload, sources_csv=PAIRS + "bad\n"))
    assert (invalid.status_code, invalid.json()) == (422, {"detail": {"code": "invalid_sources_row", "csv_row": 2}})
    service._credentials.clear()
    attach = url + "/" + batch["id"] + "/credentials"
    assert client.post(attach, json={"credentials": ONPREM_CREDS}).json()["detail"] == "invalid_credentials"
    assert client.post(attach, json={"credentials": SECRETS}).json()["batch"]["credentials_available"]
    evidence = (response.content + invalid.content + repr(service.store.events(batch["id"])).encode()
                + b"".join(p.read_bytes() for p in service.store.root.glob("source-batches.db*")))
    for secret in SECRETS.values():
        assert secret.encode() not in evidence


@pytest.mark.parametrize("config,credentials", [
    (dict(PERSONAL, destination_auth="admin_token"), SECRETS),
    (dict(PERSONAL, destination_identity="b@office.example"), SECRETS),
    (dict(PERSONAL, destination_oauth_url="https://other.example/token"), SECRETS),
    (dict(PERSONAL, destination_client_id="bad/id"), SECRETS),
    (PERSONAL, ONPREM_CREDS),
    (PERSONAL, {k: v for k, v in SECRETS.items() if k != "destination_owner_token"}),
    (PERSONAL, dict(SECRETS, destination_token="fixture-shared-token")),
])
def test_personal_rejects_unbound_config_or_credentials(personal_api, config, credentials):
    client, service = personal_api
    response = client.post("/api/source-batches", json=dict(mode="onprem", config=config,
                           sources_csv=PAIRS, credentials=credentials, idempotency_key="invalid-pairs"))
    assert 400 <= response.status_code < 500
    assert service.store.batches()["batches"] == []


@pytest.mark.parametrize("mismatch", ["batch_oauth", "missing_oauth", "child_installation"])
def test_batch_reattachment_checks_stored_oauth_binding(personal_api, mismatch):
    client, service = personal_api
    created = client.post("/api/source-batches", json=dict(mode="onprem", config=PERSONAL, sources_csv=PAIRS,
                          credentials=SECRETS, idempotency_key="stored-endpoint"))
    batch = created.json()["batch"]
    batch_id = batch["id"]
    service._credentials.clear()
    if mismatch == "batch_oauth":
        with service.store.connection() as connection:
            connection.execute("UPDATE batches SET config=json_set(config,'$.destination_oauth_url',?) WHERE id=?",
                               ("https://oauth.different.example/token", batch_id))
    elif mismatch == "missing_oauth":
        with service.store.connection() as connection:
            connection.execute("UPDATE batches SET config=json_remove(config,'$.destination_oauth_url') WHERE id=?",
                               (batch_id,))
    else:
        source, = service.store.sources(batch_id)
        config = dict(service._child_config(batch, source), destination_url="https://cloud-api.different.example/v1/disk",
                      destination_oauth_url="https://oauth.different.example/token")
        child = service.onprem.store.create(config, "separate-installation", batch_child=True)
        service.store.update_source(source["id"], child_job_id=child["id"])
    result = client.post(f"/api/source-batches/{batch_id}/credentials", json={"credentials": SECRETS})
    assert (result.status_code, result.json()["detail"]) == (409, "invalid_destination_oauth_url")
    assert not service._credentials


@pytest.mark.parametrize("csv_text,expected", [
    ("", {"code": "invalid_sources_columns", "csv_row": None}),
    pytest.param("x" * (2 * 1024**2 + 1), {"code": "invalid_sources_csv", "csv_row": None}, id="over-limit"),
    pytest.param(HEADER + ",destination_folder\n" + PAIR + "," + ("/" + "a" * 255) * 16,
                 {"code": "invalid_disk_path", "csv_row": 1}, id="canonical-path-too-long"),
])
def test_personal_csv_boundaries_report_row(personal_api, csv_text, expected):
    client, service = personal_api
    response = client.post("/api/source-batches", json=dict(
        mode="onprem", config=PERSONAL, sources_csv=csv_text, credentials=SECRETS, idempotency_key="boundary-check",
    ))
    assert (response.status_code, response.json()["detail"]) == (422, expected)
    assert service.store.batches()["batches"] == []
