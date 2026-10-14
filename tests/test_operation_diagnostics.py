"""Operation parsing explains safe rejection reasons without storing responses."""

import csv
import io

import pytest

from app.migration.errors import MigrationError
from app.migration.journal import enrich_event
from tests import test_migration_api as api_fixtures
from tests import test_migration_runner as runner_fixtures
from tests.test_migration_api import REQUEST
from tests.test_migration_clients import Response, destination
from tests.test_migration_runner import finish, stage_events


api = api_fixtures.api
migration = runner_fixtures.migration


@pytest.mark.parametrize("payload,issue", [
    ({"operation_id": "op", "method": "POST"}, "metadata"),
    ({"href": "http://disk.example.com/v1/disk/operations/op"}, "url"),
    ({"href": "https://internal.example/v1/disk/operations/op"}, "origin"),
    ({"href": "/other/operations/op"}, "route"),
    ({"href": "/v1/disk/operations?id=op&token=private"}, "query"),
    ({"href": "/v1/disk/operations/op", "operation_id": "other"}, "id_mismatch"),
    ({}, "missing_id"),
    ({"operation_id": "id:private"}, "id_format"),
])
def test_accepted_import_reports_specific_safe_rejection(monkeypatch, payload, issue):
    client, session, _ = destination(monkeypatch, [Response(202, payload)])
    with pytest.raises(MigrationError) as caught:
        client.import_url("/tmp/a", "https://signed.example/?token=private")
    exc = caught.value
    assert exc.code == "invalid_operation_reference" and exc.uncertain
    assert exc.http_status == 202
    assert exc.operation_reference_issue == issue
    assert "private" not in str(exc)
    assert len(session.calls) == 1


@pytest.mark.parametrize("issue", ["private", "https://signed/?token=private", [], None])
def test_error_ignores_unapproved_operation_diagnostics(issue):
    exc = MigrationError("invalid_operation_reference", operation_reference_issue=issue)
    assert exc.operation_reference_issue is None


def test_runner_keeps_specific_reason_in_journal(migration):
    store, _, runner, job_id, _, destination = migration
    destination.upload_error = MigrationError(
        "invalid_operation_reference", uncertain=True, http_status=202,
        operation_reference_issue="origin",
    )
    assert finish(store, runner, job_id)["state"] == "needs_attention"
    event = stage_events(store, job_id, "upload_file")[-1]
    assert event["payload"]["operation_reference_issue"] == "origin"
    assert "адресом API" in event["error_message"]
    assert "принят" in event["error_message"]



def test_csv_explains_accepted_request_and_safe_reason(api):
    client, service = api
    job = client.post("/api/onprem/jobs", json=REQUEST).json()
    service.store.event(job["id"], "transfer_stage", {
        "stage": "submit_url_upload", "status": "outcome_unknown",
        "error": "invalid_operation_reference", "http_status": 202,
        "operation_reference_issue": "origin",
    })
    response = client.get(f"/api/onprem/jobs/{job['id']}/journal")
    row = list(csv.DictReader(io.StringIO(response.text.lstrip("\ufeff"))))[-1]
    assert "202" in row["error_message"] and "принят" in row["error_message"]
    assert "адресом API" in row["error_message"]
    assert "некорректный идентификатор" not in row["error_message"]
    for value in ["private", "https://internal.example", "admin-test-only"]:
        assert value not in response.text


@pytest.mark.parametrize("stage", ["submit_url_upload", "move_file"])
def test_historical_unknown_link_does_not_invent_id_failure(stage):
    event = enrich_event({"payload": {
        "stage": stage, "status": "outcome_unknown",
        "error": "invalid_operation_reference", "http_status": 202,
    }}, dict(REQUEST["config"], strategy="url"), "disk:/temporary")
    assert "принят" in event["error_message"]
    assert "ссылку" in event["error_message"]
    assert "некорректный идентификатор" not in event["error_message"]
    if stage == "move_file":
        assert "загрузки" not in event["error_message"]


@pytest.mark.parametrize("error,issue", [
    ("invalid_operation_reference", "private"),
    ("invalid_operation_reference", []),
    ("invalid_operation_reference", None),
    ("source_unavailable", "origin"),
])
def test_store_rejects_unapproved_operation_detail(api, error, issue):
    client, service = api
    job = client.post("/api/onprem/jobs", json=REQUEST).json()
    with pytest.raises(ValueError):
        service.store.event(job["id"], "transfer_stage", {
            "stage": "submit_url_upload", "status": "outcome_unknown",
            "error": error, "operation_reference_issue": issue,
        })
