"""Destination diagnostics preserve safe causes without replaying mutations."""

import pytest
import requests

from app.migration.destination import OnPremDiskDestination
from app.migration.errors import MigrationError
from tests.test_migration_clients import Response, destination


def reply(status=500, payload=None, *, host="disk.example.com"):
    response = Response(status, payload, headers={"X-Request-Id": "0123456789abcdef"})
    response.url = f"https://{host}/private/file?token=private"
    return response


@pytest.mark.parametrize("status,code,auth,uncertain", [
    (400, "destination_unavailable", False, False),
    (401, "destination_auth_failed", True, False),
    (403, "destination_auth_failed", True, False),
    (404, "destination_not_found", False, False),
    (409, "destination_conflict", False, False),
    (413, "destination_file_too_large", False, False),
    (423, "destination_read_only", False, False),
    (429, "destination_rate_limited", False, False),
    (500, "destination_unavailable", False, True),
    (502, "destination_unavailable", False, True),
    (503, "destination_unavailable", False, True),
    (504, "destination_unavailable", False, True),
    (507, "destination_quota_exceeded", False, True),
])
def test_http_details_do_not_change_mutation_outcome(status, code, auth, uncertain):
    response = reply(status, {"error": "InternalServerError",
                              "message": "private token=private"})
    with pytest.raises(MigrationError) as caught:
        OnPremDiskDestination._status(response, (201,), mutation=True)
    exc = caught.value
    assert (exc.code, exc.auth, exc.uncertain) == (code, auth, uncertain)
    assert exc.http_status == status
    assert exc.diagnostics == {
        "error_side": "onprem", "api_error": "InternalServerError",
        "request_id": "0123456789abcdef", "request_host": "disk.example.com",
    }
    assert "private" not in repr(exc.diagnostics)


@pytest.mark.parametrize("error,kind,code", [
    (requests.ConnectTimeout("token=private"), "connect_timeout", "destination_timeout"),
    (requests.ReadTimeout("token=private"), "read_timeout", "destination_timeout"),
    (requests.exceptions.SSLError("token=private"), "tls", "destination_connection_failed"),
    (requests.ConnectionError("token=private"), "connection", "destination_connection_failed"),
    (requests.exceptions.ChunkedEncodingError("private"), "stream",
     "destination_stream_interrupted"),
])
def test_transport_cause_is_safe_and_mutation_is_never_replayed(monkeypatch, error, kind, code):
    client, session, _ = destination(monkeypatch, [error])
    with pytest.raises(MigrationError) as caught:
        client.claim_dir("/archive")
    exc = caught.value
    assert exc.code == code
    assert exc.uncertain is (not isinstance(error, requests.ConnectTimeout))
    assert exc.diagnostics["transport_error"] == kind
    assert exc.diagnostics["error_side"] == "onprem"
    assert exc.diagnostics["request_host"] == "disk.example.com"
    assert "private" not in repr(exc.diagnostics)
    assert len(session.calls) == 1


def test_custom_conflict_keeps_http_and_api_cause(monkeypatch):
    client, session, _ = destination(monkeypatch, [reply(409, {
        "error": "DiskResourceAlreadyExistsError", "message": "private",
    })])
    with pytest.raises(MigrationError) as caught:
        client.claim_dir("/archive")
    exc = caught.value
    assert exc.code == "technical_namespace_exists" and not exc.uncertain
    assert exc.http_status == 409
    assert exc.diagnostics["api_error"] == "DiskResourceAlreadyExistsError"
    assert len(session.calls) == 1


def test_mkdir_conflict_details_survive_followup_stat(monkeypatch):
    client, _, _ = destination(monkeypatch, [Response(404), reply(409, {
        "error": "DiskResourceAlreadyExistsError",
    }), Response(payload={"path": "disk:/archive", "type": "file"})])
    with pytest.raises(MigrationError) as caught:
        client.ensure_dir("/archive")
    assert caught.value.http_status == 409
    assert caught.value.diagnostics["api_error"] == "DiskResourceAlreadyExistsError"


def test_malformed_json_retains_safe_http_context():
    with pytest.raises(MigrationError) as caught:
        OnPremDiskDestination._json(reply(202), mutation=True)
    assert caught.value.uncertain and caught.value.http_status == 202
    assert caught.value.diagnostics["error_side"] == "onprem"
    assert caught.value.diagnostics["request_id"] == "0123456789abcdef"
    assert "private" not in repr(caught.value.diagnostics)


def test_foreign_shared_response_path_retains_safe_http_context(monkeypatch):
    client, _, _ = destination(monkeypatch, [reply(200, {
        "path": "vd:other:disk:/archive", "type": "dir",
    })], config={"destination_type": "shared", "destination_vd_hash": "selected"})
    with pytest.raises(MigrationError) as caught:
        client.stat("/archive")
    assert caught.value.code == "destination_invalid_response"
    assert caught.value.http_status == 200
    assert caught.value.diagnostics["request_host"] == "disk.example.com"
    assert "other" not in repr(caught.value.diagnostics)


def test_rejected_operation_reference_retains_response_context(monkeypatch):
    client, session, _ = destination(monkeypatch, [reply(202, {
        "href": "https://other.example/v1/disk/operations/private",
    })])
    with pytest.raises(MigrationError) as caught:
        client.import_url("/archive/file", "https://signed.example/file?token=private")
    exc = caught.value
    assert exc.uncertain and exc.http_status == 202
    assert exc.operation_reference_issue == "origin"
    assert exc.diagnostics["request_host"] == "disk.example.com"
    assert "other.example" not in repr(exc.diagnostics)
    assert len(session.calls) == 1


def test_upload_http_failure_reports_upload_host_without_capability(monkeypatch, tmp_path):
    local = tmp_path / "file"
    local.write_bytes(b"data")
    client, session, _ = destination(monkeypatch, [Response(payload={
        "href": "https://upload.example/file?token=private",
    }), reply(500, {"error": "InternalServerError"}, host="upload.example")])
    with pytest.raises(MigrationError) as caught:
        client.upload("/archive/file", local)
    exc = caught.value
    assert exc.uncertain and exc.http_status == 500
    assert exc.diagnostics["request_host"] == "upload.example"
    assert exc.diagnostics["api_error"] == "InternalServerError"
    assert "private" not in repr(exc.diagnostics)
    assert len(session.calls) == 2


def test_invalid_upload_link_keeps_get_context_before_any_upload(monkeypatch, tmp_path):
    local = tmp_path / "file"
    local.write_bytes(b"data")
    client, session, _ = destination(monkeypatch, [reply(200, {
        "href": "http://upload.example/file?token=private",
    })])
    with pytest.raises(MigrationError) as caught:
        client.upload("/archive/file", local)
    assert caught.value.code == "invalid_upload_url"
    assert caught.value.http_status == 200
    assert caught.value.diagnostics["request_id"] == "0123456789abcdef"
    assert len(session.calls) == 1


def test_poll_failed_cause_is_sidecar_and_cleared_for_next_operation(monkeypatch):
    client, _, _ = destination(monkeypatch, [reply(200, {
        "status": "failed", "error": "InternalServerError", "message": "private",
    }), reply(200, {"status": "success"})])
    assert client.poll("failed-op") == "failed"
    assert client.last_operation_diagnostics == {
        "error_side": "onprem", "api_error": "InternalServerError",
        "request_id": "0123456789abcdef", "request_host": "disk.example.com", "http_status": 200,
    }
    assert client.poll("next-op") == "success"
    assert client.last_operation_diagnostics == {}


def test_poll_failed_without_server_cause_does_not_invent_it(monkeypatch):
    client, _, _ = destination(monkeypatch, [reply(200, {"status": "failed"})])
    assert client.poll("failed-op") == "failed"
    assert "api_error" not in client.last_operation_diagnostics
    assert client.last_operation_diagnostics["http_status"] == 200


def test_poll_resets_old_remote_cause_even_when_next_request_fails(monkeypatch):
    client, _, _ = destination(monkeypatch, [reply(200, {
        "status": "failed", "error": "InternalServerError",
    }), reply(403, {"error": "ForbiddenError"})])
    assert client.poll("failed-op") == "failed"
    with pytest.raises(MigrationError) as caught:
        client.poll("next-op")
    assert caught.value.auth
    assert client.last_operation_diagnostics == {}
