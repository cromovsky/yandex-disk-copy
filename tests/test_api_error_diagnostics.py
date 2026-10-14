import json
from http.client import RemoteDisconnected
from types import SimpleNamespace

import pytest
import requests
from urllib3.exceptions import ProtocolError

from app.migration.diagnostics import (
    describe_diagnostics, diagnostic_fields, response_details, transport_details,
)
from app.migration.errors import MigrationError


def response(payload, **headers):
    return SimpleNamespace(
        content=json.dumps(payload).encode(), headers=headers,
        url="https://download.example/file?token=never-return",
    )


def test_response_keeps_known_code_and_safe_request_context():
    details = response_details(response(
        {"error": "DiskNotFoundError", "message": "OAuth never-return"},
        **{"X-Request-Id": "b0e6b8d3-5bbf-4b3c-9457-9e06c4c591c8"},
    ), "saas")
    assert details == {
        "error_side": "saas", "api_error": "DiskNotFoundError",
        "request_host": "download.example",
        "request_id": "b0e6b8d3-5bbf-4b3c-9457-9e06c4c591c8",
    }
    assert "never-return" not in json.dumps(details)


def test_unknown_server_text_and_request_ids_are_not_persisted():
    details = response_details(response(
        {"error": "never-return", "description": "private"},
        **{"X-Request-Id": "OAuth never-return"},
    ), "onprem")
    assert details["api_error"] == "unrecognized_api_error"
    assert "request_id" not in details
    assert "never-return" not in json.dumps(details)


def test_non_json_stream_is_not_read_for_diagnostics():
    class Stream:
        headers = {"Content-Type": "text/html"}
        url = "https://download.example/?secret=never-return"

        def iter_content(self, **kwargs):
            raise AssertionError("must not consume HTML or binary error bodies")
    assert response_details(Stream(), "saas", streaming=True) == {
        "error_side": "saas", "request_host": "download.example",
    }


def test_streaming_json_has_a_strict_size_limit():
    class Stream:
        headers = {"Content-Type": "application/json"}
        read_chunks = 0

        def iter_content(self, **kwargs):
            while True:
                self.read_chunks += 1
                yield b"x" * 8192
    stream = Stream()
    assert response_details(stream, "saas", streaming=True) == {"error_side": "saas"}
    assert stream.read_chunks <= 3


@pytest.mark.parametrize("exception,expected", [
    (requests.ConnectTimeout, "connect_timeout"),
    (requests.ReadTimeout, "read_timeout"),
    (requests.Timeout, "timeout"),
    (requests.exceptions.SSLError, "tls"),
    (requests.ConnectionError, "connection"),
    (requests.exceptions.ChunkedEncodingError, "stream"),
])
def test_transport_details_never_use_exception_text(exception, expected):
    details = transport_details(exception("OAuth never-return"), "onprem",
                                "https://upload.example/path?token=private")
    assert details == {"error_side": "onprem", "transport_error": expected,
                       "request_host": "upload.example"}


def test_wrapped_peer_close_is_distinguished_without_exception_text():
    error = requests.ConnectionError(ProtocolError(
        "private token=never-return", RemoteDisconnected("OAuth never-return"),
    ))
    details = transport_details(error, "onprem", "https://disk.example/v1/disk")
    assert details["transport_error"] == "remote_closed"
    text = describe_diagnostics(details)
    assert "HTTP-ответа" in text
    assert "never-return" not in text
    assert "HTTP 409" not in text


def test_migration_error_filters_unsafe_details():
    error = MigrationError("source_timeout", diagnostics={
        "error_side": "saas", "transport_error": "read_timeout",
        "api_error": "never-return", "request_id": "OAuth private",
        "request_host": "https://bad.example/?token=private",
        "raw_body": "secret",
    })
    assert error.diagnostics == {"error_side": "saas", "transport_error": "read_timeout"}
    assert str(error) == "source_timeout"


@pytest.mark.parametrize("code", [
    "SECRET_TOKEN", "AnyPrivateSecretError", "PasswordError",
    "BearerCredentialError",
])
def test_secret_shaped_identifiers_are_redacted(code):
    details = response_details(response({"error": code}), "saas")
    assert details["api_error"] == "unrecognized_api_error"
    assert code not in describe_diagnostics(details)


def test_unknown_conventional_api_code_is_shown_without_inventing_a_cause():
    details = response_details(response({"error": "NewBackendError"}), "onprem")
    assert details["api_error"] == "NewBackendError"
    assert "отсутствует в справочнике" in describe_diagnostics(details)


@pytest.mark.parametrize("payload", [
    {"error": "ForbiddenError", "reason": "UPLOAD_TRAFFIC_LIMIT_EXCEEDED"},
    {"error": "ForbiddenError",
     "details": {"reason": "UPLOAD_TRAFFIC_LIMIT_EXCEEDED"}},
])
def test_documented_limit_reason_is_kept_separately_from_error_code(payload):
    details = response_details(response(payload), "onprem")
    assert details["api_error"] == "ForbiddenError"
    assert details["api_reason"] == "UPLOAD_TRAFFIC_LIMIT_EXCEEDED"
    assert "лимит трафика" in describe_diagnostics(details)


def test_malformed_nested_reason_and_raw_messages_are_ignored():
    details = response_details(response({
        "error": {"code": "InternalServerError"},
        "details": {"reason": ["OAuth private"]},
        "message": "private", "description": "private",
    }), "saas")
    assert details["api_error"] == "InternalServerError"
    assert "api_reason" not in details
    assert "private" not in describe_diagnostics(details)


def test_broken_error_body_cannot_mask_http_failure(monkeypatch):
    server_response = response({"error": "InternalServerError"})

    def too_deep(*args, **kwargs):
        raise RecursionError
    monkeypatch.setattr("app.migration.diagnostics.json.loads", too_deep)
    assert response_details(server_response, "saas") == {
        "error_side": "saas", "request_host": "download.example",
    }


def test_ipv6_scope_cannot_smuggle_secrets_into_host():
    assert diagnostic_fields({"request_host": "fe80::1%token=private"}) == {}
    server_response = response({"error": "InternalServerError"})
    server_response.url = "https://[fe80::1%25token=private]/f"
    assert "request_host" not in response_details(server_response, "saas")
    assert diagnostic_fields({"request_host": "2001:db8::1"}) == {
        "request_host": "2001:db8::1",
    }


@pytest.mark.parametrize("status", [
    400, 401, 403, 404, 406, 409, 412, 413, 415, 423, 429, 500, 503, 507,
])
def test_documented_http_errors_have_a_readable_explanation(status):
    text = describe_diagnostics({"error_side": "saas", "http_status": status})
    assert f"HTTP {status}" in text
    assert "SaaS" in text
    assert "Сервер вернул этот HTTP-статус" not in text


def test_async_failure_retry_keeps_http200_from_looking_like_success():
    text = describe_diagnostics({
        "stage": "download_retry", "http_status": 200,
        "error": "destination_url_import_failed",
    })
    assert "On-premises" in text
    assert "операция завершилась ошибкой" in text
    assert "Запрос обработан" not in text
