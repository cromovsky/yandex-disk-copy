"""Source failures retain safe server facts without reading binary errors."""

import json
import socket

import pytest
import requests
from urllib3.exceptions import ReadTimeoutError

from app.migration.errors import MigrationError
from tests.test_migration_clients import Response, source, token
from tests.test_migration_public_source import FILE, metadata


REQUEST_ID = "0123456789abcdef0123456789abcdef"


def test_source_api_500_keeps_documented_error_and_request_context(monkeypatch):
    failure = Response(500, {
        "error": "DiskOperationFailedError",
        "message": "secret=https://signed.example/file?token=private",
        "description": "private source details",
    }, headers={"X-Request-Id": REQUEST_ID, "Retry-After": "13"})
    client, session, limiter = source(monkeypatch, [token(), failure])

    with pytest.raises(MigrationError) as caught:
        client.download_url("/file")

    error = caught.value
    assert error.http_status == 500
    assert error.retry_after == 13
    assert error.diagnostics == {
        "error_side": "saas", "api_error": "DiskOperationFailedError",
        "request_host": "cloud-api.yandex.net", "request_id": REQUEST_ID,
    }
    assert "private" not in json.dumps(error.diagnostics)
    assert str(error) == "source_unavailable"
    assert failure.closed and len(session.calls) == 2
    assert limiter.delays == []


def test_oauth_denial_logs_safe_error_without_token_payload(monkeypatch):
    response = Response(400, {
        "error": "invalid_grant", "error_description": "private user or secret",
    })
    client, session, _ = source(monkeypatch, [response])

    with pytest.raises(MigrationError) as caught:
        client.download_url("/file")

    assert caught.value.auth
    assert caught.value.http_status == 400
    assert caught.value.diagnostics == {
        "error_side": "saas", "api_error": "invalid_grant",
        "request_host": "oauth.yandex.ru",
    }
    assert response.closed and len(session.calls) == 1


@pytest.mark.parametrize("failure,kind,code", [
    (requests.ConnectTimeout("private signed URL"), "connect_timeout", "source_timeout"),
    (requests.ReadTimeout("private signed URL"), "read_timeout", "source_timeout"),
    (requests.exceptions.SSLError("private certificate"), "tls", "source_connection_failed"),
    (requests.ConnectionError(socket.gaierror(-2, "private hostname")), "dns",
     "source_connection_failed"),
])
def test_source_transport_subtype_keeps_host_without_exception_text(
    monkeypatch, failure, kind, code,
):
    client, session, limiter = source(monkeypatch, [failure])
    with pytest.raises(MigrationError) as caught:
        list(client.chunks("/file", download_url="https://storage.example/f?token=private"))

    assert caught.value.code == code
    assert caught.value.diagnostics == {
        "error_side": "saas", "request_host": "storage.example", "transport_error": kind,
    }
    assert "private" not in json.dumps(caught.value.diagnostics)
    assert len(session.calls) == 1 and limiter.delays == []


def test_public_stream_http_error_is_not_read_as_an_unbounded_json(monkeypatch):
    class BinaryError(Response):
        def json(self):
            raise AssertionError("must not load a streaming binary error body")

        def iter_content(self, chunk_size):
            raise AssertionError("must not read a binary error stream")

    failure = BinaryError(500, headers={
        "Content-Type": "application/octet-stream", "X-Request-Id": REQUEST_ID,
    })
    client, session, _ = source(monkeypatch, [failure])
    client.config = dict(client.config, source_public=True)

    with pytest.raises(MigrationError) as caught:
        list(client.chunks("/file", download_url="https://storage.example/f?token=private"))

    assert caught.value.code == "source_public_download_failed"
    assert caught.value.http_status == 500
    assert caught.value.diagnostics == {
        "error_side": "saas", "request_host": "storage.example", "request_id": REQUEST_ID,
    }
    assert failure.closed and len(session.calls) == 1


def test_public_stream_json_download_limit_preserves_existing_classification(monkeypatch):
    payload = {"error": "DiskDownloadLimitExceededError", "description": "private details"}
    failure = Response(409, body=json.dumps(payload).encode(), headers={
        "Content-Type": "application/json", "Retry-After": "20",
    })
    client, session, _ = source(monkeypatch, [failure])
    client.config = dict(client.config, source_public=True)

    with pytest.raises(MigrationError) as caught:
        list(client.chunks("/file", download_url="https://storage.example/f?token=private"))

    assert caught.value.code == "source_public_download_limit"
    assert caught.value.diagnostics["api_error"] == "DiskDownloadLimitExceededError"
    assert caught.value.http_status == 409 and caught.value.retry_after == 20
    assert failure.closed and len(session.calls) == 1


def test_partial_stream_error_reports_final_redirect_host_and_safe_subtype(monkeypatch):
    class BrokenStream(Response):
        def json(self):
            raise AssertionError("successful binary body must not be JSON-inspected")

        def iter_content(self, chunk_size):
            yield b"partial"
            raise requests.exceptions.ChunkedEncodingError("private signed URL")

    stream = BrokenStream()
    client, session, _ = source(monkeypatch, [
        Response(302, headers={"Location": "https://final-storage.example/f?token=private"}),
        stream,
    ])
    chunks = client.chunks("/file", download_url="https://first-storage.example/f?token=private")
    assert next(chunks) == b"partial"
    with pytest.raises(MigrationError) as caught:
        next(chunks)

    assert caught.value.code == "source_stream_interrupted"
    assert caught.value.http_status == 200
    assert caught.value.diagnostics == {
        "error_side": "saas", "request_host": "final-storage.example", "transport_error": "stream",
    }
    assert stream.closed and len(session.calls) == 2


def test_stream_read_timeout_wrapped_by_requests_keeps_precise_reason(monkeypatch):
    class TimeoutStream(Response):
        def iter_content(self, chunk_size):
            raise requests.ConnectionError(ReadTimeoutError(
                None, "/file?token=private", "private timeout text",
            ))

    stream = TimeoutStream()
    client, session, _ = source(monkeypatch, [stream])
    with pytest.raises(MigrationError) as caught:
        list(client.chunks("/file", download_url="https://storage.example/f?token=private"))

    assert caught.value.code == "source_stream_interrupted"
    assert caught.value.http_status == 200
    assert caught.value.diagnostics["transport_error"] == "read_timeout"
    assert "private" not in json.dumps(caught.value.diagnostics)
    assert stream.closed and len(session.calls) == 1


def test_publication_transport_wrapper_retains_reason_and_unknown_outcome(monkeypatch):
    client, session, _ = source(monkeypatch, [
        token(), Response(payload=metadata(FILE)), requests.ReadTimeout("private secret"),
    ])
    client.config = dict(client.config, source_public=True)
    with pytest.raises(MigrationError) as caught:
        client.ensure_publication(FILE)

    assert caught.value.code == "source_publication_outcome_unknown"
    assert caught.value.uncertain
    assert caught.value.diagnostics == {
        "error_side": "saas", "request_host": "cloud-api.yandex.net",
        "transport_error": "read_timeout",
    }
    assert len(session.calls) == 3


def test_publication_confirmation_wrapper_retains_original_api_error(monkeypatch):
    failure = Response(403, {"error": "ForbiddenError", "message": "private secret"})
    client, session, _ = source(monkeypatch, [
        token(), Response(payload=metadata(FILE)), Response(), failure,
    ])
    client.config = dict(client.config, source_public=True)
    with pytest.raises(MigrationError) as caught:
        client.ensure_publication(FILE)

    assert caught.value.code == "source_publication_outcome_unknown"
    assert caught.value.uncertain and caught.value.auth
    assert caught.value.http_status == 403
    assert caught.value.diagnostics["api_error"] == "ForbiddenError"
    assert caught.value.diagnostics["request_host"] == "cloud-api.yandex.net"
    assert failure.closed and len(session.calls) == 4
