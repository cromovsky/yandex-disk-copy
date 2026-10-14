"""Transport causes stay safe and identify the disk that did not respond."""

import csv
import io
import socket
from http.client import RemoteDisconnected

import pytest
import requests
from urllib3.exceptions import (
    MaxRetryError, NameResolutionError, NewConnectionError, ProtocolError, ReadTimeoutError,
)

from app.migration.diagnostics import request_not_sent, transport_details
from app.migration.errors import MigrationError
from app.migration.journal import ERROR_MESSAGES, enrich_event
from tests import test_migration_api as api_fixtures
from tests.test_migration_api import REQUEST
from tests.test_migration_clients import Response, destination


api = api_fixtures.api


@pytest.mark.parametrize("exception,code", [
    (requests.Timeout, "destination_timeout"),
    (requests.ConnectTimeout, "destination_timeout"),
    (requests.ConnectionError, "destination_connection_failed"),
    (requests.exceptions.ChunkedEncodingError, "destination_stream_interrupted"),
    (requests.exceptions.ContentDecodingError, "destination_stream_interrupted"),
])
def test_mutation_transport_cause_is_safe_and_never_replayed(
    monkeypatch, exception, code,
):
    client, session, limiter = destination(monkeypatch, [
        exception("https://signed.example/?token=private OAuth secret"),
    ])
    with pytest.raises(MigrationError) as caught:
        client.move("/temp/a", "/final/a")
    assert caught.value.code == code
    assert caught.value.uncertain is (exception is not requests.ConnectTimeout)
    assert caught.value.http_status is None
    assert len(session.calls) == 1
    assert limiter.delays == []
    assert "private" not in str(caught.value)
    assert "secret" not in str(caught.value)


def test_preconnect_reason_is_not_replaced_by_previous_exception_context():
    previous = requests.ReadTimeout("token=private")
    error = requests.ConnectionError(MaxRetryError(
        None, "https://upload.example/put?token=private",
        NewConnectionError(None, "token=private"),
    ))
    error.__context__ = previous
    assert request_not_sent(error)
    details = transport_details(error, "onprem", "https://upload.example/put?token=private")
    assert details == {
        "error_side": "onprem", "request_host": "upload.example",
        "transport_error": "connect_failed",
    }


@pytest.mark.parametrize("side,expected", [("onprem", "connect_failed"), ("saas", "connection")])
def test_preconnect_no_send_label_is_scoped_to_onprem(side, expected):
    error = requests.ConnectionError(MaxRetryError(
        None, "/private", NewConnectionError(None, "private"),
    ))
    assert request_not_sent(error)
    assert transport_details(error, side)["transport_error"] == expected


@pytest.mark.parametrize("error,expected", [
    (requests.ReadTimeout("private"), "read_timeout"),
    (requests.ConnectionError(ProtocolError("private", RemoteDisconnected("private"))),
     "remote_closed"),
    (requests.ConnectionError("private"), "connection"),
    (requests.exceptions.SSLError("private"), "tls"),
    (requests.ConnectionError(MaxRetryError(
        None, "/private", ReadTimeoutError(None, "/private", "private"),
    )), "read_timeout"),
])
@pytest.mark.parametrize("attribute", ["__context__", "__cause__"])
def test_transport_category_uses_current_error_instead_of_previous_connect_failure(
    error, expected, attribute,
):
    previous = requests.ConnectionError(MaxRetryError(
        None, "/private", NameResolutionError(
            "private", None, socket.gaierror(-2, "private"),
        ),
    ))
    setattr(error, attribute, previous)
    details = transport_details(error, "onprem", "https://upload.example/put?token=private")
    assert details["transport_error"] == expected
    assert not request_not_sent(error)
    assert "private" not in repr(details)


def test_csv_explains_definite_preconnect_failure_without_raw_details(api):
    client, service = api
    job = client.post("/api/onprem/jobs", json=REQUEST).json()
    service.store.event(job["id"], "transfer_stage", {
        "stage": "upload_file", "status": "failed",
        "error": "destination_connection_failed", "error_side": "onprem",
        "request_host": "upload.example", "transport_error": "connect_failed",
    })
    response = client.get(f"/api/onprem/jobs/{job['id']}/journal")
    row = list(csv.DictReader(io.StringIO(response.text.lstrip("\ufeff"))))[-1]
    assert row["transport_error"] == "connect_failed"
    assert "HTTP-запрос не отправлен" in row["diagnostic_message"]
    assert "token=" not in response.text


def test_read_transport_failure_does_not_hide_an_internal_retry(monkeypatch):
    client, session, limiter = destination(monkeypatch, [
        requests.Timeout("private"), requests.ConnectionError("private"),
        Response(payload={"total_space": 123}),
    ])
    with pytest.raises(MigrationError) as caught:
        client.info()
    assert caught.value.code == "destination_timeout"
    assert not caught.value.uncertain
    assert len(session.calls) == 1
    assert limiter.delays == []


def test_single_read_preserves_first_cause_without_uncertain_mutation(monkeypatch):
    client, session, limiter = destination(monkeypatch, [
        requests.ReadTimeout("private"), requests.ConnectionError("private"),
    ])
    with pytest.raises(MigrationError) as caught:
        client.info()
    assert caught.value.code == "destination_timeout"
    assert not caught.value.uncertain
    assert caught.value.diagnostics["transport_error"] == "read_timeout"
    assert len(session.calls) == 1
    assert limiter.delays == []


@pytest.mark.parametrize("code", [
    "source_timeout", "source_connection_failed", "source_unavailable",
    "source_download_timeout", "source_stream_interrupted",
    "source_public_download_failed", "source_publication_outcome_unknown",
])
def test_source_failure_messages_identify_saas(code):
    assert "SaaS" in ERROR_MESSAGES[code]
    assert "On-premises" not in ERROR_MESSAGES[code]


@pytest.mark.parametrize("stage,code,side,operation", [
    ("upload_file", "destination_timeout", "On-premises", "загрузки"),
    ("move_file", "destination_connection_failed", "On-premises", "перемещения"),
    ("publish_source", "source_publication_outcome_unknown", "SaaS", "публикации"),
])
def test_unknown_outcome_explains_stage_and_cause(stage, code, side, operation):
    event = enrich_event({"payload": {
        "stage": stage, "status": "outcome_unknown", "error": code,
    }}, dict(REQUEST["config"], strategy="url"), "disk:/temporary")
    message = event["error_message"]
    assert side in message
    assert operation in message
    assert "подтвержден" in message
    assert ERROR_MESSAGES[code] in message


def test_csv_preserves_side_cause_and_unknown_outcome_explanation(api):
    client, service = api
    job = client.post("/api/onprem/jobs", json=REQUEST).json()
    for stage, status, error in [
        ("upload_file", "outcome_unknown", "destination_timeout"),
        ("downloading", "failed", "source_connection_failed"),
        ("point_retry", "waiting", "destination_connection_failed"),
    ]:
        service.store.event(job["id"], "transfer_stage", {
            "stage": stage, "status": status, "error": error,
        })
    response = client.get(f"/api/onprem/jobs/{job['id']}/journal")
    assert response.status_code == 200
    rows = list(csv.DictReader(io.StringIO(response.text.lstrip("\ufeff"))))[-3:]
    assert rows[0]["error"] == "destination_timeout"
    assert "On-premises" in rows[0]["error_message"]
    assert "подтвержден" in rows[0]["error_message"]
    assert "SaaS" in rows[1]["error_message"]
    assert rows[2]["stage_label"] == "Проверка и повтор файла"


def test_url_import_failure_reports_only_what_onprem_confirmed(api):
    client, service = api
    job = client.post("/api/onprem/jobs", json=REQUEST).json()
    service.store.event(job["id"], "transfer_stage", {
        "stage": "poll_transfer", "status": "failed",
        "error": "destination_url_import_failed",
    })
    response = client.get(f"/api/onprem/jobs/{job['id']}/journal")
    rows = list(csv.DictReader(io.StringIO(response.text.lstrip("\ufeff"))))
    failure = rows[-1]
    assert failure["stage_label"] == "Ожидание загрузки"
    assert failure["error"] == "destination_url_import_failed"
    assert failure["error_message"] == (
        "On-premises сообщил об ошибке скачивания файла по ссылке; "
        "API не сообщил подробную причину."
    )
    assert "SaaS" not in failure["error_message"]
    assert "never-return" not in response.text
    assert "admin-test-only" not in response.text
