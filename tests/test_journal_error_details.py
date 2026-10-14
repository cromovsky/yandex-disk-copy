import csv
import io

import pytest

from app.migration.errors import MigrationError
from app.migration.journal import enrich_event
from tests import test_migration_api as api_fixtures
from tests.test_migration_api import REQUEST


api = api_fixtures.api


def test_historical_http_failure_explains_side_and_status_without_inventing_code():
    event = enrich_event({"payload": {
        "stage": "downloading", "status": "failed",
        "error": "source_public_download_failed", "http_status": 500,
    }}, dict(REQUEST["config"], strategy="url"), "disk:/tmp")
    assert "SaaS" in event["diagnostic_message"]
    assert "HTTP 500" in event["diagnostic_message"]
    assert "Внутренняя ошибка сервера" in event["error_message"]
    assert "Код API" not in event["diagnostic_message"]


def test_new_details_survive_runner_store_journal_and_csv(api):
    client, service = api
    job = client.post('/api/onprem/jobs', json=REQUEST).json()
    details = {"error_side": "onprem", "api_error": "UPLOAD_FILE_SIZE_LIMIT_EXCEEDED",
               "request_host": "upload.example", "request_id": "a" * 32}

    def fail():
        raise MigrationError("destination_file_too_large", http_status=413,
                             diagnostics=details)
    with pytest.raises(MigrationError):
        service.runner._logged(service.store.get(job['id']), None, "upload_file", fail)
    event = service.store.events(job['id'], journal=True)[-1]
    for key, value in details.items():
        assert event['payload'][key] == value
    response = client.get(f"/api/onprem/jobs/{job['id']}/journal")
    row = list(csv.DictReader(io.StringIO(response.text.lstrip('\ufeff'))))[-1]
    for key, value in details.items():
        assert row[key] == value
    assert "HTTP 413" in row['error_message']
    assert "лимит размера" in row['diagnostic_message']


@pytest.mark.parametrize('details', [
    {'error_side': 'https://private'}, {'api_error': 'OAuth private'},
    {'api_error': 'SECRET_TOKEN'}, {'transport_error': 'raw private'},
    {'request_host': 'https://server/?token=private'}, {'request_id': 'secret'},
])
def test_store_rejects_unsafe_diagnostics(api, details):
    client, service = api
    job = client.post('/api/onprem/jobs', json=REQUEST).json()
    with pytest.raises(ValueError):
        service.store.event(job['id'], 'transfer_stage', {
            'stage': 'downloading', 'status': 'failed', 'error': 'source_timeout',
            **details,
        })


def test_remote_failed_status_keeps_server_error_without_changing_poll_result(api):
    client, service = api
    job = client.post('/api/onprem/jobs', json=REQUEST).json()

    class Destination:
        last_operation_diagnostics = {
            'error_side': 'onprem', 'http_status': 200,
            'api_error': 'DiskOperationFailedError',
        }

        def poll(self):
            return 'failed'
    destination = Destination()
    assert service.runner._logged(service.store.get(job['id']), None,
                                  'poll_transfer', destination.poll) == 'failed'
    event = service.store.events(job['id'], journal=True)[-1]
    assert event['payload']['api_error'] == 'DiskOperationFailedError'
    assert 'операция завершилась ошибкой' in event['diagnostic_message']


def test_async_failure_with_api_code_does_not_claim_the_cause_is_missing():
    event = enrich_event({"payload": {
        "stage": "poll_transfer", "status": "failed",
        "error": "destination_url_import_failed", "http_status": 200,
        "remote_status": "failed", "error_side": "onprem",
        "api_error": "UPLOAD_TRAFFIC_LIMIT_EXCEEDED",
    }}, dict(REQUEST["config"], strategy="url"), "disk:/tmp")
    assert "лимит трафика" in event["error_message"]
    assert "API не сообщил" not in event["error_message"]


def test_historical_unconfirmed_upload_offers_file_retry_in_journal_and_csv(api):
    client, service = api
    job = client.post("/api/onprem/jobs", json=api_fixtures.shared_request()).json()
    service.store.event(job["id"], "transfer_stage", {
        "stage": "poll_transfer", "status": "outcome_unknown",
        "error": "final_upload_confirmation_required",
    })
    event = service.store.events(job["id"], journal=True)[-1]
    assert "On-premises не подтвердил завершение загрузки" in event["error_message"]
    assert "«Проверить и повторить» у файла" in event["error_message"]
    response = client.get(f"/api/onprem/jobs/{job['id']}/journal")
    row = list(csv.DictReader(io.StringIO(response.text.lstrip("\ufeff"))))[-1]
    assert row["error_message"] == event["error_message"]


def test_historical_pending_operation_offers_file_retry_and_keeps_id_in_csv(api):
    client, service = api
    job = client.post("/api/onprem/jobs", json=api_fixtures.shared_request()).json()
    service.store.event(job["id"], "transfer_stage", {
        "stage": "point_retry", "status": "failed",
        "error": "remote_operation_still_pending", "remote_status": "in-progress",
        "operation_id": "pending-operation", "error_side": "onprem",
        "point_retry_attempts": 3, "max_point_retry_attempts": 3,
    })
    event = service.store.events(job["id"], journal=True)[-1]
    assert "прежняя операция ещё выполняется" in event["error_message"]
    assert "«Проверить и повторить» у файла" in event["error_message"]
    assert "Ошибка переноса:" not in event["error_message"]
    response = client.get(f"/api/onprem/jobs/{job['id']}/journal")
    row = list(csv.DictReader(io.StringIO(response.text.lstrip("\ufeff"))))[-1]
    assert row["error_message"] == event["error_message"]
    assert row["operation_id"] == "pending-operation"
    assert row["remote_status"] == "in-progress"
    assert row["point_retry_attempts"] == row["max_point_retry_attempts"] == "3"
