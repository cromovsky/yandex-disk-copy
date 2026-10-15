"""Migration adapters use mocked HTTP only; no real Disk access."""

import socket
from http.client import RemoteDisconnected

import pytest
import requests
from urllib3.exceptions import (
    ConnectTimeoutError, MaxRetryError, NameResolutionError, NewConnectionError,
    ProtocolError, ReadTimeoutError,
)

from app.migration.errors import MigrationError
from app.migration.source import SaasDiskSource
from app.migration.destination import (
    OnPremDiskDestination,
    normalize_destination_url,
)


class Response:
    def __init__(self, status=200, payload=None, body=b"", headers=None):
        self.status_code = status
        self.payload = payload
        self.headers = headers or {}
        self.body = body
        self.closed = False

    def json(self):
        if self.payload is None:
            raise ValueError("malformed secret=https://signed/?token=secret")
        return self.payload

    def iter_content(self, chunk_size):
        yield self.body

    def close(self):
        self.closed = True


class Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.closed = False

    def request(self, method, url, **kwargs):
        data = kwargs.get("data")
        if hasattr(data, "read"):
            kwargs["body"] = data.read()
        self.calls.append((method, url, kwargs))
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def close(self):
        self.closed = True


class Limiter:
    def __init__(self):
        self.calls = []
        self.delays = []

    def acquire(self, method, *, checkpoint=None):
        if checkpoint is not None:
            checkpoint()
        self.calls.append(method)

    def penalize(self, delay):
        self.delays.append(delay)
        return delay

    def pause(self, delay):
        self.delays.append(delay)


SOURCE_CONFIG = {
    "source_org_id": "org", "source_client_id": "client",
    "source_email": "source@example.com", "source_path": "disk:/",
}
DEST_CONFIG = {"destination_url": "https://disk.example.com"}
CREDENTIALS = {"source_client_secret": "secret", "destination_token": "target"}


def source(monkeypatch, responses):
    session, limiter = Session(responses), Limiter()
    monkeypatch.setattr("app.migration.source.requests.Session", lambda: session)
    monkeypatch.setattr("app.migration.source.get_limiter", lambda: limiter)
    return SaasDiskSource(SOURCE_CONFIG, CREDENTIALS), session, limiter


def destination(monkeypatch, responses, *, config=None):
    session, limiter = Session(responses), Limiter()
    monkeypatch.setattr("app.migration.destination.requests.Session", lambda: session)
    monkeypatch.setattr(
        "app.migration.destination.get_onprem_limiter", lambda _url: limiter,
    )
    return OnPremDiskDestination(
        dict(DEST_CONFIG, **(config or {})), CREDENTIALS,
    ), session, limiter


def token():
    return Response(payload={"access_token": "owner", "expires_in": 3600})


def test_journal_http_diagnostics_are_numbers_without_raw_response(monkeypatch):
    client, _, _ = destination(monkeypatch, [Response(503, {"token": "private"})])
    with pytest.raises(MigrationError) as caught:
        client.import_url("/a", "https://signed.example/file?token=private")
    assert caught.value.http_status == 503
    assert caught.value.uncertain
    assert str(caught.value) == "destination_unavailable"


def test_journal_identifies_source_folder_that_failed_during_planning(monkeypatch):
    root = {"path": "disk:/selected", "type": "dir"}
    folder = {"path": "disk:/selected/child", "type": "dir"}
    client, _, _ = source(monkeypatch, [
        token(), Response(payload=root), Response(payload={
            "_embedded": {"total": 1, "items": [folder]},
        }), Response(403, {"error_description": "private"}),
    ])
    with pytest.raises(MigrationError) as caught:
        list(client.walk("/selected"))
    assert caught.value.http_status == 403
    assert caught.value.source_path == "disk:/selected/child"
    assert str(caught.value) == "source_auth_failed"


@pytest.mark.parametrize("page,expected", [
    ({"_embedded": {"total": 1, "items": None}}, "disk:/selected"),
    ({"_embedded": {"total": 1, "items": [{
        "path": "disk:/selected/a.txt", "type": "file", "size": -1,
    }]}}, "disk:/selected/a.txt"),
])
def test_journal_identifies_invalid_page_or_file_metadata(monkeypatch, page, expected):
    client, _, _ = source(monkeypatch, [token(), Response(payload={
        "path": "disk:/selected", "type": "dir",
    }), Response(payload=page)])
    with pytest.raises(MigrationError) as caught:
        list(client.walk("/selected"))
    assert caught.value.source_path == expected


@pytest.mark.parametrize("status", [True, "503", 99, 600, "https://secret"])
def test_journal_rejects_unsafe_http_diagnostics(status):
    error = MigrationError("source_unavailable", http_status=status,
                           source_path="https://signed/?token=private")
    assert error.http_status is None
    assert error.source_path is None


def test_error_never_includes_external_text():
    error = MigrationError("destination_unavailable", uncertain=True)
    assert str(error) == "destination_unavailable"
    assert error.uncertain and not error.auth


@pytest.mark.parametrize("value", [
    True, "12", "https://signed/?token=private", -1, float("inf"), float("nan"),
])
def test_error_discards_unsafe_retry_after(value):
    error = MigrationError("source_unavailable", retry_after=value)
    assert error.retry_after is None
    assert str(error) == "source_unavailable"


@pytest.mark.parametrize("value", [0, 12, 12.5])
def test_error_keeps_finite_nonnegative_retry_after(value):
    error = MigrationError("source_unavailable", retry_after=value)
    assert error.retry_after == float(value)


@pytest.mark.parametrize("url", [
    "http://disk.example.com", "https://user:secret@disk.example.com",
    "https://disk.example.com/other", "https://disk.example.com?token=secret",
    "https://disk.example.com/#x", "https://disk.example.com/\n",
    "https://@disk.example.com", "https://disk.example.com:/",
])
def test_destination_rejects_unsafe_base_url(url):
    with pytest.raises(MigrationError, match="invalid_destination_url"):
        normalize_destination_url(url)


def test_destination_url_normalization():
    assert normalize_destination_url("https://DISK.example.com/") == (
        "https://disk.example.com/v1/disk"
    )
    assert normalize_destination_url("https://disk.example.com/v1/disk/") == (
        "https://disk.example.com/v1/disk"
    )


def test_source_walk_paginates_and_keeps_empty_folder(monkeypatch):
    node = {"path": "disk:/selected", "type": "dir", "name": "selected"}
    empty = {"path": "disk:/selected/empty", "type": "dir", "name": "empty"}
    file = {"path": "disk:/selected/a", "type": "file", "name": "a",
            "size": 5, "md5": "123", "modified": "now"}
    client, session, _ = source(monkeypatch, [
        token(), Response(payload=node),
        Response(payload={"_embedded": {"total": 2, "items": [empty]}}),
        Response(payload={"_embedded": {"total": 2, "items": [file]}}),
        Response(payload={"_embedded": {"total": 0, "items": []}}),
    ])
    client.page_limit = 1
    assert list(client.walk("/selected")) == [node, empty, file]
    assert [c[2]["params"]["offset"] for c in session.calls[2:]] == [0, 1, 0]
    assert all("publish" not in c[1] for c in session.calls)


def test_source_walk_root_not_emitted_and_selected_file_emitted(monkeypatch):
    client, _, _ = source(monkeypatch, [
        token(), Response(payload={"path": "disk:/", "type": "dir"}),
        Response(payload={"_embedded": {"total": 0, "items": []}}),
        Response(payload={"path": "disk:/a", "type": "file", "size": 0}),
    ])
    assert list(client.walk("/")) == []
    assert list(client.walk("/a")) == [{"path": "disk:/a", "type": "file", "size": 0}]


def test_source_token_refresh_and_download_has_no_oauth(monkeypatch):
    stream = Response(body=b"data")
    client, session, _ = source(monkeypatch, [
        token(), Response(payload={"href": "https://signed.example/a?secret=x"}),
        stream, token(), Response(payload={"path": "disk:/a", "type": "file"}),
    ])
    assert b"".join(client.chunks("/a")) == b"data"
    assert stream.closed
    assert "Authorization" not in session.calls[2][2].get("headers", {})
    client._token_deadline = 0
    assert client.stat("/a")["type"] == "file"
    assert [c[0] for c in session.calls].count("POST") == 2


def test_source_retries_read_but_not_oauth(monkeypatch):
    client, session, limiter = source(monkeypatch, [
        token(), Response(429, headers={"Retry-After": "2"}),
        Response(payload={"path": "disk:/a", "type": "file"}),
    ])
    assert client.stat("/a")["type"] == "file"
    assert limiter.delays == [2.0]
    assert len(session.calls) == 3


@pytest.mark.parametrize("stage", ["url", "body"])
def test_source_download_http_failure_has_one_transport_attempt(monkeypatch, stage):
    failure = Response(503, headers={"Retry-After": "13"})
    responses = [token()]
    if stage == "body":
        responses.append(Response(payload={"href": "https://signed.example/a"}))
    responses.extend([failure, Response(body=b"must-not-retry")])
    client, session, limiter = source(monkeypatch, responses)
    with pytest.raises(MigrationError) as caught:
        list(client.chunks("/a"))
    assert caught.value.http_status == 503
    assert caught.value.retry_after == 13.0
    assert caught.value.code == (
        "source_unavailable" if stage == "url" else "source_download_failed"
    )
    assert len(session.calls) == (2 if stage == "url" else 3)
    assert len(session.responses) == 1
    assert limiter.delays == []
    assert failure.closed


@pytest.mark.parametrize("failure,code", [
    (requests.Timeout("https://signed/?secret=value"), "source_timeout"),
    (requests.ConnectionError("https://signed/?secret=value"),
     "source_connection_failed"),
])
@pytest.mark.parametrize("stage", ["url", "body"])
def test_source_download_connection_failure_is_safe_without_hidden_retry(
    monkeypatch, failure, code, stage,
):
    responses = [token()]
    if stage == "body":
        responses.append(Response(payload={"href": "https://signed.example/a"}))
    responses.extend([failure] * 5)
    client, session, limiter = source(monkeypatch, responses)
    with pytest.raises(MigrationError) as caught:
        list(client.chunks("/a"))
    assert caught.value.code == code
    assert "secret" not in str(caught.value)
    assert caught.value.http_status is None
    assert len(session.calls) == (2 if stage == "url" else 3)
    assert len(session.responses) == 4
    assert limiter.delays == []


def test_manual_download_retry_gets_a_fresh_signed_url(monkeypatch):
    first = Response(503)
    second = Response(body=b"data")
    client, session, _ = source(monkeypatch, [
        token(), Response(payload={"href": "https://signed.example/first"}), first,
        Response(payload={"href": "https://signed.example/second"}), second,
    ])
    with pytest.raises(MigrationError, match="source_download_failed"):
        list(client.chunks("/a"))
    assert b"".join(client.chunks("/a")) == b"data"
    urls = [call[1] for call in session.calls]
    assert urls.count("https://cloud-api.yandex.net/v1/disk/resources/download") == 2
    assert urls.count("https://signed.example/first") == 1
    assert urls.count("https://signed.example/second") == 1
    assert first.closed and second.closed


@pytest.mark.parametrize("header", ["inf", "nan", "private-token", "-2"])
def test_download_retry_after_diagnostics_reject_nonfinite_headers(monkeypatch, header):
    client, session, limiter = source(monkeypatch, [
        token(), Response(503, headers={"Retry-After": header}),
    ])
    with pytest.raises(MigrationError) as caught:
        client.download_url("/a")
    assert caught.value.retry_after in {None, 0.0}
    assert len(session.calls) == 2
    assert limiter.delays == []


def test_stream_exception_is_safe_and_stream_closed(monkeypatch):
    class BrokenStream(Response):
        def iter_content(self, chunk_size):
            raise requests.ConnectionError("https://signed/?secret=value")

    stream = BrokenStream()
    client, _, _ = source(monkeypatch, [
        token(), Response(payload={"href": "https://signed.example/a"}), stream,
    ])
    with pytest.raises(MigrationError, match="source_stream_interrupted") as caught:
        list(client.chunks("/a"))
    assert "secret" not in str(caught.value)
    assert caught.value.http_status == 200
    assert stream.closed


@pytest.mark.parametrize("failure,code", [
    (requests.ReadTimeout("https://signed/?secret=value"), "source_download_timeout"),
    (requests.exceptions.ChunkedEncodingError("https://signed/?secret=value"),
     "source_stream_interrupted"),
])
def test_source_partial_stream_preserves_safe_reason_and_closes(
    monkeypatch, failure, code,
):
    class PartialStream(Response):
        def iter_content(self, chunk_size):
            yield b"partial"
            raise failure

    stream = PartialStream(headers={"Retry-After": "17"})
    client, session, _ = source(monkeypatch, [
        token(), Response(payload={"href": "https://signed.example/a"}), stream,
    ])
    chunks = client.chunks("/a")
    assert next(chunks) == b"partial"
    with pytest.raises(MigrationError) as caught:
        next(chunks)
    assert caught.value.code == code
    assert caught.value.http_status == 200
    assert caught.value.retry_after == 17.0
    assert len(session.calls) == 3
    assert "secret" not in str(caught.value)
    assert stream.closed


def test_stat_only_returns_none_on_not_found(monkeypatch):
    client, _, _ = destination(monkeypatch, [Response(404), Response(403)])
    assert client.stat("/absent") is None
    with pytest.raises(MigrationError) as caught:
        client.stat("/denied")
    assert caught.value.auth


def test_ensure_dir_checks_conflict_type(monkeypatch):
    client, session, _ = destination(monkeypatch, [
        Response(404), Response(201), Response(404), Response(409),
        Response(payload={"path": "disk:/parent/file", "type": "file"}),
    ])
    with pytest.raises(MigrationError, match="destination_conflict"):
        client.ensure_dir("/parent/file")
    assert [c[2]["params"]["path"] for c in session.calls if c[0] == "PUT"] == [
        "disk:/parent", "disk:/parent/file",
    ]


def test_import_url_uses_owner_url_and_does_not_replay_mutation(monkeypatch):
    client, session, _ = destination(monkeypatch, [Response(503)])
    with pytest.raises(MigrationError) as caught:
        client.import_url("/temp/a", "https://signed.example/a?token=secret")
    assert caught.value.uncertain
    assert len(session.calls) == 1
    assert session.calls[0][2]["allow_redirects"] is False
    assert session.calls[0][2]["params"]["overwrite"] == "false"


@pytest.mark.parametrize("href", [
    "https://evil.example/v1/disk/operations/abc",
    "https://disk.example.com/v1/disk/operations/abc?token=secret",
    "https://disk.example.com/v1/disk/operations/../../abc",
    "https://user:secret@disk.example.com/v1/disk/operations/abc",
    "http://disk.example.com/v1/disk/operations/abc",
])
def test_import_rejects_unsafe_operation_reference(monkeypatch, href):
    client, _, _ = destination(monkeypatch, [Response(202, {"href": href})])
    with pytest.raises(MigrationError) as caught:
        client.import_url("/temp/a", "https://signed.example/a")
    assert caught.value.uncertain
    assert "secret" not in str(caught.value)


def test_import_and_poll_use_safe_operation_id(monkeypatch):
    client, session, _ = destination(monkeypatch, [
        Response(202, {"href": "/v1/disk/operations/op_123"}),
        Response(payload={"status": "in-progress"}),
    ])
    assert client.import_url("/tmp/a", "https://signed.example/a") == "op_123"
    assert client.poll("op_123") == "in-progress"
    assert session.calls[-1][1] == "https://disk.example.com/v1/disk/operations/op_123"


@pytest.mark.parametrize("method", ["import", "move"])
@pytest.mark.parametrize("href", [
    "https://disk.example.com/v1/disk/operations?id=op_123",
    "/v1/disk/operations?id=op_123",
    "https://disk.example.com:443/v1/disk/operations?id=op_123",
])
def test_query_operation_link_preserves_polling_route(monkeypatch, method, href):
    client, session, _ = destination(monkeypatch, [
        Response(202, {"href": href, "method": "GET", "templated": False}),
        Response(payload={"status": "in-progress"}),
    ])
    if method == "import":
        reference = client.import_url("/tmp/a", "https://signed.example/a")
    else:
        reference = client.move("/tmp/a", "/final/a")
    assert reference == "?id=op_123"
    assert client.poll(reference) == "in-progress"
    assert session.calls[-1][1] == (
        "https://disk.example.com/v1/disk/operations?id=op_123"
    )
    assert session.calls[-1][2]["headers"] == {"Authorization": "OAuth target"}
    assert session.calls[-1][2]["allow_redirects"] is False


@pytest.mark.parametrize("payload", [
    {"href": "/v1/disk/operations?id=op_123&id=op_123"},
    {"href": "/v1/disk/operations?id=op_123&token=secret"},
    {"href": "/v1/disk/operations?operation_id=op_123"},
    {"href": "/v1/disk/operations?id="},
    {"href": "/v1/disk/operations?id=../secret"},
    {"href": "/v1/disk/operations?id=op%2F123"},
    {"href": "/v1/disk/operations?id=op_123#secret"},
    {"href": "https://evil.example/v1/disk/operations?id=op_123"},
    {"href": "https://disk.example.com:444/v1/disk/operations?id=op_123"},
    {"href": "https://user:secret@disk.example.com/v1/disk/operations?id=op_123"},
    {"href": "//disk.example.com/v1/disk/operations?id=op_123"},
    {"href": "/v1/disk/operations?id=op_123", "operation_id": "other"},
    {"href": "/v1/disk/operations?id=op_123", "method": "POST"},
    {"href": "/v1/disk/operations/op_123", "templated": True},
])
def test_operation_link_rejects_unsafe_query_and_metadata(monkeypatch, payload):
    client, session, _ = destination(monkeypatch, [Response(202, payload)])
    with pytest.raises(MigrationError, match="invalid_operation_reference") as caught:
        client.import_url("/tmp/a", "https://signed.example/a")
    assert caught.value.uncertain
    assert len(session.calls) == 1


@pytest.mark.parametrize("reference", [
    "?id=op_123&token=secret", "?id=op_123&id=other",
    "https://evil.example/operations?id=op_123", "/resources", "?id=../x",
])
def test_poll_rejects_unsafe_restored_reference_without_http(monkeypatch, reference):
    client, session, _ = destination(monkeypatch, [])
    with pytest.raises(MigrationError, match="invalid_operation_reference"):
        client.poll(reference)
    assert session.calls == []


def test_upload_gets_fresh_link_and_op_id_without_sending_oauth(monkeypatch, tmp_path):
    local = tmp_path / "object"
    local.write_bytes(b"data")
    client, session, _ = destination(monkeypatch, [
        Response(payload={"href": "https://upload.example/put?secret=x",
                          "operation_id": "upload_123"}), Response(202),
    ])
    assert client.upload("/tmp/a", local) == "upload_123"
    assert session.calls[0][2]["params"]["overwrite"] == "false"
    assert session.calls[1][2]["body"] == b"data"
    assert session.calls[1][2]["headers"] == {"Content-Length": "4"}
    assert session.calls[1][2]["allow_redirects"] is False


def test_move_uses_no_overwrite_and_network_failure_is_uncertain(monkeypatch):
    client, session, _ = destination(monkeypatch, [
        requests.ConnectionError("https://disk.example.com?secret=x"),
    ])
    with pytest.raises(MigrationError) as caught:
        client.move("/temp/a", "/final/a")
    assert caught.value.uncertain
    assert str(caught.value) == "destination_connection_failed"
    assert session.calls[0][2]["params"]["overwrite"] == "false"


def preconnect_error(reason):
    return requests.ConnectionError(MaxRetryError(
        None, "https://upload.example/put?token=private", reason,
    ))


@pytest.mark.parametrize("error,code,transport", [
    (requests.ConnectTimeout("private"), "destination_timeout", "connect_timeout"),
    (preconnect_error(ConnectTimeoutError("private")),
     "destination_connection_failed", "connect_timeout"),
    (preconnect_error(NewConnectionError(None, "private")),
     "destination_connection_failed", "connect_failed"),
    (preconnect_error(NameResolutionError(
        "upload.example", None, socket.gaierror(-2, "private"),
    )), "destination_connection_failed", "dns"),
])
def test_known_preconnect_mutation_failure_is_definite_without_replaying(
    monkeypatch, error, code, transport,
):
    client, session, _ = destination(monkeypatch, [error])
    with pytest.raises(MigrationError) as caught:
        client.move("/tmp/a", "/owner/a")
    assert caught.value.code == code
    assert not caught.value.uncertain
    assert caught.value.diagnostics["transport_error"] == transport
    assert "private" not in repr(caught.value.diagnostics)
    assert len(session.calls) == 1
    assert session.calls[0][2]["params"]["overwrite"] == "false"


@pytest.mark.parametrize("error", [
    requests.ReadTimeout("private"),
    requests.ConnectionError(ProtocolError("private", RemoteDisconnected("private"))),
    requests.ConnectionError(ProtocolError("private", BrokenPipeError(32, "private"))),
    preconnect_error(ReadTimeoutError(None, "/private", "private")),
    requests.ConnectionError(NewConnectionError(None, "private")),
    requests.exceptions.SSLError(MaxRetryError(
        None, "/private", NewConnectionError(None, "private"),
    )),
    requests.exceptions.ProxyError(MaxRetryError(
        None, "/private", NewConnectionError(None, "private"),
    )),
    requests.ConnectionError(MaxRetryError(
        None, "/private", NewConnectionError(None, "private"),
    ), "extra"),
])
def test_ambiguous_transport_error_does_not_adopt_previous_preconnect_context(
    monkeypatch, error,
):
    previous = preconnect_error(NewConnectionError(None, "previous private"))
    error.__context__ = previous
    error.__cause__ = previous
    client, session, _ = destination(monkeypatch, [error])
    with pytest.raises(MigrationError) as caught:
        client.move("/tmp/a", "/owner/a")
    assert caught.value.uncertain
    assert caught.value.diagnostics["transport_error"] != "connect_failed"
    assert len(session.calls) == 1
    assert "private" not in repr(caught.value.diagnostics)


def test_preconnect_put_keeps_preallocated_operation_without_hidden_retry(
    monkeypatch, tmp_path,
):
    local = tmp_path / "object"
    local.write_bytes(b"data")
    client, session, _ = destination(monkeypatch, [Response(payload={
        "href": "https://upload.example/put?token=private",
        "operation_id": "preallocated-operation",
    }), preconnect_error(NewConnectionError(None, "private"))], config=SHARED_CONFIG)
    checkpoints = []
    with pytest.raises(MigrationError) as caught:
        client.upload("/owner/a", local, before_upload=checkpoints.append)
    assert not caught.value.uncertain
    assert checkpoints == ["preallocated-operation"]
    assert [call[0] for call in session.calls] == ["GET", "PUT"]
    assert session.calls[0][2]["params"]["overwrite"] == "false"
    assert caught.value.diagnostics["request_host"] == "upload.example"
    assert "private" not in repr(caught.value.diagnostics)


def test_corporate_ca_is_verified(monkeypatch):
    monkeypatch.setenv("ONPREM_CA_BUNDLE", "/etc/company-ca.pem")
    client, session, _ = destination(monkeypatch, [
        Response(payload={"total_space": 10}),
    ])
    assert client.info() == {"total_space": 10}
    assert session.calls[0][2]["verify"] == "/etc/company-ca.pem"


def test_terminal_mutation_4xx_and_malformed_success(monkeypatch):
    client, _, _ = destination(monkeypatch, [Response(409), Response(202, {})])
    with pytest.raises(MigrationError) as conflict:
        client.move("/temp/a", "/final/a")
    assert not conflict.value.uncertain
    with pytest.raises(MigrationError) as malformed:
        client.import_url("/temp/a", "https://signed.example/a")
    assert malformed.value.uncertain


def test_clients_close_sessions(monkeypatch):
    client, session, _ = destination(monkeypatch, [])
    client.close()
    assert session.closed


def test_stream_closes_when_caller_stops_generator(monkeypatch):
    stream = Response(body=b"data")
    client, _, _ = source(monkeypatch, [
        token(), Response(payload={"href": "https://signed.example/a"}), stream,
    ])
    iterator = client.chunks("/a")
    assert next(iterator) == b"data"
    iterator.close()
    assert stream.closed


def test_owner_401_refreshes_once_then_surfaces_auth_error(monkeypatch):
    client, session, _ = source(monkeypatch, [
        token(), Response(401), token(), Response(401),
    ])
    with pytest.raises(MigrationError) as caught:
        client.stat("/a")
    assert caught.value.auth
    assert len(session.calls) == 4


def test_download_url_401_refreshes_once_without_transport_retry(monkeypatch):
    first = Response(401)
    client, session, _ = source(monkeypatch, [
        token(), first, token(), Response(payload={"href": "https://signed.example/a"}),
    ])
    assert client.download_url("/a") == "https://signed.example/a"
    assert len(session.calls) == 4
    assert first.closed


def test_source_walk_rejects_pagination_loss_and_path_escape(monkeypatch):
    client, _, _ = source(monkeypatch, [
        token(), Response(payload={"path": "disk:/folder", "type": "dir"}),
        Response(payload={"_embedded": {"total": 1, "items": []}}),
    ])
    with pytest.raises(MigrationError, match="source_invalid_response"):
        list(client.walk("/folder"))
    client, _, _ = source(monkeypatch, [
        token(), Response(payload={"path": "disk:/folder", "type": "dir"}),
        Response(payload={"_embedded": {"total": 1, "items": [
            {"path": "disk:/outside", "type": "file"},
        ]}}),
    ])
    with pytest.raises(MigrationError, match="source_invalid_response"):
        list(client.walk("/folder"))


def test_source_stat_must_match_requested_path(monkeypatch):
    client, _, _ = source(monkeypatch, [
        token(), Response(payload={"path": "disk:/other", "type": "file"}),
    ])
    with pytest.raises(MigrationError, match="source_invalid_response"):
        client.stat("/requested")


def test_signed_download_only_follows_https_without_auth(monkeypatch):
    client, session, _ = source(monkeypatch, [
        token(), Response(payload={"href": "https://signed.example/a"}),
        Response(302, headers={"Location": "https://cdn.example/file?signature=x"}),
        Response(body=b"data"),
    ])
    assert b"".join(client.chunks("/a")) == b"data"
    assert all("Authorization" not in c[2].get("headers", {})
               for c in session.calls[2:])
    client, _, _ = source(monkeypatch, [
        token(), Response(payload={"href": "https://signed.example/a"}),
        Response(302, headers={"Location": "http://unsafe.example/file"}),
    ])
    with pytest.raises(MigrationError, match="invalid_download_url"):
        list(client.chunks("/a"))


@pytest.mark.parametrize("response", [
    Response(429), Response(302, headers={"Location": "https://evil.example"}),
    Response(403),
])
def test_mutations_never_replay_or_follow_redirect(monkeypatch, response):
    client, session, _ = destination(monkeypatch, [response])
    with pytest.raises(MigrationError):
        client.import_url("/tmp/a", "https://signed.example/a")
    assert len(session.calls) == 1


def test_upload_manual_retry_reopens_body_and_gets_fresh_url(monkeypatch, tmp_path):
    local = tmp_path / "object"
    local.write_bytes(b"data")
    client, session, _ = destination(monkeypatch, [
        Response(payload={"href": "https://upload.example/first"}), Response(503),
        Response(payload={"href": "https://upload.example/second"}), Response(201),
    ])
    with pytest.raises(MigrationError) as caught:
        client.upload("/tmp/a", local)
    assert caught.value.uncertain
    assert client.upload("/tmp/new-attempt/a", local) is None
    uploads = [call for call in session.calls if call[0] == "PUT"]
    assert [call[2]["body"] for call in uploads] == [b"data", b"data"]
    assert [call[1] for call in uploads] == [
        "https://upload.example/first", "https://upload.example/second",
    ]


def test_missing_upload_operation_is_uncertain_only_after_put(monkeypatch, tmp_path):
    local = tmp_path / "object"
    local.write_bytes(b"data")
    client, _, _ = destination(monkeypatch, [
        Response(payload={"href": "https://upload.example/put"}), Response(202),
    ])
    with pytest.raises(MigrationError) as caught:
        client.upload("/tmp/a", local)
    assert caught.value.uncertain


def test_source_and_destination_hashes_lowercase(monkeypatch):
    metadata = {"path": "disk:/a", "type": "file", "md5": "ABC", "size": 3}
    client, _, _ = source(monkeypatch, [token(), Response(payload=metadata)])
    assert client.stat("/a")["md5"] == "abc"
    client, _, _ = destination(monkeypatch, [Response(payload=metadata)])
    assert client.stat("/a")["md5"] == "abc"


def test_onprem_limiter_shared_per_installation_and_separate_from_source():
    from app.migration.destination import get_onprem_limiter
    from app.throttle import get_limiter

    first = get_onprem_limiter("https://one.example/v1/disk")
    assert first is get_onprem_limiter("https://one.example/v1/disk")
    assert first is not get_onprem_limiter("https://two.example/v1/disk")
    assert first is not get_limiter()


def test_destination_stat_must_match_requested_path(monkeypatch):
    client, _, _ = destination(monkeypatch, [
        Response(payload={"path": "disk:/other", "type": "file"}),
    ])
    with pytest.raises(MigrationError, match="destination_invalid_response"):
        client.stat("/requested")


def test_signed_download_resolves_relative_https_redirect(monkeypatch):
    client, session, _ = source(monkeypatch, [
        token(), Response(payload={"href": "https://signed.example/a"}),
        Response(302, headers={"Location": "/file?signature=x"}),
        Response(body=b"data"),
    ])
    assert b"".join(client.chunks("/a")) == b"data"
    assert session.calls[-1][1] == "https://signed.example/file?signature=x"


def test_oauth_bad_credentials_are_auth_failure_without_replay(monkeypatch):
    client, session, _ = source(monkeypatch, [Response(400)])
    with pytest.raises(MigrationError) as caught:
        client.stat("/a")
    assert caught.value.auth
    assert len(session.calls) == 1


def test_read_backoff_uses_jitter_without_changing_retry_after(monkeypatch):
    monkeypatch.setattr(
        "app.migration.source.random.uniform", lambda low, high: high / 2,
    )
    client, _, limiter = source(monkeypatch, [
        token(), requests.ConnectionError("secret"),
        Response(payload={"path": "disk:/a", "type": "file"}),
    ])
    client.stat("/a")
    assert limiter.delays == [0.5]
    client, session, limiter = destination(monkeypatch, [
        Response(503), Response(payload={"total_space": 10}),
    ])
    with pytest.raises(MigrationError, match="destination_unavailable"):
        client.info()
    assert len(session.calls) == 1
    assert limiter.delays == []


def test_onprem_limiter_uses_configured_budgets(monkeypatch):
    from app.migration.destination import get_onprem_limiter

    monkeypatch.setenv("ONPREM_API_READ_RPS", "7")
    monkeypatch.setenv("ONPREM_API_WRITE_RPS", "1")
    limiter = get_onprem_limiter("https://configured.example/v1/disk")
    assert limiter.read.rps == 7
    assert limiter.write.rps == 1


def test_claim_dir_requires_exclusive_creation_without_stat_or_replay(monkeypatch):
    client, session, _ = destination(monkeypatch, [Response(201), Response(409)])
    client.claim_dir("/.disk-copy-private")
    with pytest.raises(MigrationError, match="technical_namespace_exists") as caught:
        client.claim_dir("/.disk-copy-private")
    assert not caught.value.uncertain
    assert [call[0] for call in session.calls] == ["PUT", "PUT"]


def test_claim_dir_lost_response_is_uncertain_without_replay(monkeypatch):
    client, session, _ = destination(monkeypatch, [
        requests.ConnectionError("https://disk.example.com?token=secret"),
    ])
    with pytest.raises(MigrationError) as caught:
        client.claim_dir("/.disk-copy-private")
    assert caught.value.uncertain
    assert len(session.calls) == 1


SHARED_CONFIG = {"destination_type": "shared", "destination_vd_hash": "hash_1"}
SHARED_INFO = {
    "vd_hash": "hash_1", "name": "Existing shared disk", "resource_id": "vd-1",
    "total_space": 100, "used_space": 30, "trash_size": 5,
    "permissions": ["read", "write"],
}


@pytest.mark.parametrize("hash_value", [
    "", None, 123, "a" * 257, "other/hash", "hash:disk", "hash%2F", "hash x",
    "хеш", "hash\n",
])
def test_shared_destination_rejects_invalid_hash_before_http(monkeypatch, hash_value):
    with pytest.raises(MigrationError, match="invalid_destination_shared_disk"):
        destination(monkeypatch, [], config={
            "destination_type": "shared", "destination_vd_hash": hash_value,
        })


def test_destination_rejects_unknown_destination_type(monkeypatch):
    with pytest.raises(MigrationError, match="invalid_destination_type"):
        destination(monkeypatch, [], config={"destination_type": "other"})


@pytest.mark.parametrize("returned_path", [
    "vd:hash_1:disk:/Docs/a.txt", "vd:/hash_1/disk/Docs/a.txt",
    "/Docs/a.txt", "disk:/Docs/a.txt",
])
def test_shared_stat_normalizes_selected_disk_path(monkeypatch, returned_path):
    client, session, _ = destination(monkeypatch, [Response(payload={
        "path": returned_path, "type": "file", "md5": "ABC", "size": 3,
    })], config=SHARED_CONFIG)
    assert client.stat("disk:/Docs/a.txt") == {
        "path": "disk:/Docs/a.txt", "type": "file", "md5": "abc", "size": 3,
    }
    assert session.calls[0][1] == (
        "https://disk.example.com/v1/disk/virtual-disks/resources"
    )
    assert session.calls[0][2]["params"]["path"] == "vd:hash_1:disk:/Docs/a.txt"


@pytest.mark.parametrize("returned_path", [
    "vd:other:disk:/Docs/a.txt", "vd:/other/disk/Docs/a.txt",
    "vd:hash_11:disk:/Docs/a.txt", "vd:/hash_11/disk/Docs/a.txt",
    "/Docs/other.txt",
])
def test_shared_stat_rejects_other_disk_or_path(monkeypatch, returned_path):
    client, _, _ = destination(monkeypatch, [Response(payload={
        "path": returned_path, "type": "file",
    })], config=SHARED_CONFIG)
    with pytest.raises(MigrationError, match="destination_invalid_response"):
        client.stat("disk:/Docs/a.txt")


def test_shared_stat_rejects_conflicting_hash_in_metadata(monkeypatch):
    client, _, _ = destination(monkeypatch, [Response(payload={
        "path": "/Docs/a.txt", "type": "file", "vd_hash": "other",
    })], config=SHARED_CONFIG)
    with pytest.raises(MigrationError, match="destination_invalid_response"):
        client.stat("disk:/Docs/a.txt")


@pytest.mark.parametrize("method", ["stat", "ensure_dir", "claim_dir", "import_url",
                                    "upload", "move"])
def test_shared_resource_rejects_qualified_input_before_http(
    monkeypatch, tmp_path, method,
):
    client, session, _ = destination(monkeypatch, [Response(404)] * 10,
                                     config=SHARED_CONFIG)
    local = tmp_path / "object"
    local.write_bytes(b"data")
    with pytest.raises(MigrationError, match="invalid_disk_path"):
        if method == "import_url":
            client.import_url("vd:other:disk:/a", "https://signed.example/a")
        elif method == "upload":
            client.upload("vd:other:disk:/a", local)
        elif method == "move":
            client.move("vd:other:disk:/a", "disk:/owner/a")
        else:
            getattr(client, method)("vd:other:disk:/a")
    assert session.calls == []


@pytest.mark.parametrize("config, logical, expected", [
    ({}, "/Docs/a.txt", "disk:/Docs/a.txt"),
    ({"destination_vd_hash": "unused"}, "disk:/Docs/a.txt", "disk:/Docs/a.txt"),
    (SHARED_CONFIG, "disk:/Docs/a.txt", "vd:hash_1:disk:/Docs/a.txt"),
    (SHARED_CONFIG, "/", "vd:hash_1:disk:/"),
])
def test_destination_path_export_qualifies_without_credentials(config, logical,
                                                               expected):
    from app.migration import destination as destination_module

    assert destination_module.destination_path(config, logical) == expected


def test_shared_info_uses_shared_quota_and_token_owner(monkeypatch):
    shared = dict(SHARED_INFO, user={"login": "wrong-shared-owner"})
    client, session, _ = destination(monkeypatch, [
        Response(payload={"user": {"login": "token-owner"},
                          "total_space": 10000, "used_space": 1}),
        Response(payload=shared),
    ], config=SHARED_CONFIG)
    assert client.info() == dict(SHARED_INFO, user={"login": "token-owner"})
    assert [(call[0], call[1]) for call in session.calls] == [
        ("GET", "https://disk.example.com/v1/disk/"),
        ("GET", "https://disk.example.com/v1/disk/virtual-disks"),
    ]
    assert session.calls[1][2]["params"] == {"vd_hash": "hash_1"}


@pytest.mark.parametrize("returned_hash", [None, "other", "hash_11"])
def test_shared_info_rejects_missing_or_different_hash(monkeypatch, returned_hash):
    shared = dict(SHARED_INFO, vd_hash=returned_hash)
    client, _, _ = destination(monkeypatch, [
        Response(payload={"user": {"login": "token-owner"}}),
        Response(payload=shared),
    ], config=SHARED_CONFIG)
    with pytest.raises(MigrationError, match="destination_invalid_response"):
        client.info()


@pytest.mark.parametrize("permissions", [[], ["read"], ["write"], None, "readwrite"])
def test_shared_info_requires_read_and_write_permissions(monkeypatch, permissions):
    client, _, _ = destination(monkeypatch, [
        Response(payload={"user": {"login": "token-owner"}}),
        Response(payload=dict(SHARED_INFO, permissions=permissions)),
    ], config=SHARED_CONFIG)
    with pytest.raises(MigrationError, match="destination_shared_access_required"):
        client.info()


def test_shared_info_does_not_reuse_personal_quota(monkeypatch):
    shared = dict(SHARED_INFO)
    shared.pop("total_space")
    shared.pop("used_space")
    client, _, _ = destination(monkeypatch, [
        Response(payload={"total_space": 10000, "used_space": 1}),
        Response(payload=shared),
    ], config=SHARED_CONFIG)
    result = client.info()
    assert "total_space" not in result and "used_space" not in result
    assert result["vd_hash"] == "hash_1"


def test_shared_mkdir_creates_only_missing_child_directories(monkeypatch):
    client, session, _ = destination(monkeypatch, [
        Response(payload={"path": "/Docs", "type": "dir"}),
        Response(404), Response(201),
    ], config=SHARED_CONFIG)
    client.ensure_dir("disk:/Docs/empty")
    assert [(call[0], call[2]["params"]["path"]) for call in session.calls] == [
        ("GET", "vd:hash_1:disk:/Docs"),
        ("GET", "vd:hash_1:disk:/Docs/empty"),
        ("PUT", "vd:hash_1:disk:/Docs/empty"),
    ]
    assert all(call[1].endswith("/virtual-disks/resources") for call in session.calls)
    client.ensure_dir("disk:/")
    assert len(session.calls) == 3


def test_shared_claim_dir_is_exclusive_in_selected_disk(monkeypatch):
    client, session, _ = destination(monkeypatch, [Response(201), Response(409)],
                                     config=SHARED_CONFIG)
    client.claim_dir("disk:/.disk-copy-transfer-one")
    with pytest.raises(MigrationError, match="technical_namespace_exists"):
        client.claim_dir("disk:/.disk-copy-transfer-one")
    assert [(call[0], call[1], call[2]["params"]) for call in session.calls] == [
        ("PUT", "https://disk.example.com/v1/disk/virtual-disks/resources",
         {"path": "vd:hash_1:disk:/.disk-copy-transfer-one"}),
        ("PUT", "https://disk.example.com/v1/disk/virtual-disks/resources",
         {"path": "vd:hash_1:disk:/.disk-copy-transfer-one"}),
    ]


def test_shared_import_and_move_keep_same_disk_and_common_operation_route(monkeypatch):
    client, session, _ = destination(monkeypatch, [
        Response(202, {"href": "/v1/disk/operations?id=import_1"}),
        Response(202, {"href": "/v1/disk/operations?id=move_1"}),
        Response(payload={"status": "success"}),
    ], config=SHARED_CONFIG)
    assert client.import_url("disk:/tmp/a", "https://signed.example/a") == (
        "?id=import_1"
    )
    assert client.move("disk:/tmp/a", "disk:/owner/a") == "?id=move_1"
    assert client.poll("?id=move_1") == "success"
    assert session.calls[0][1].endswith("/virtual-disks/resources/upload")
    assert session.calls[0][2]["params"] == {
        "path": "vd:hash_1:disk:/tmp/a", "url": "https://signed.example/a",
        "overwrite": "false",
    }
    assert session.calls[1][1].endswith("/virtual-disks/resources/move")
    assert session.calls[1][2]["params"] == {
        "from": "vd:hash_1:disk:/tmp/a", "path": "vd:hash_1:disk:/owner/a",
        "overwrite": "false",
    }
    assert session.calls[2][1] == (
        "https://disk.example.com/v1/disk/operations?id=move_1"
    )


@pytest.mark.parametrize("config", [None, SHARED_CONFIG])
def test_before_upload_checkpoints_safe_id_before_body(monkeypatch, tmp_path, config):
    local = tmp_path / "object"
    local.write_bytes(b"data")
    client, session, _ = destination(monkeypatch, [
        Response(payload={"href": "https://upload.example/put?secret=x",
                          "operation_id": "upload_123"}), Response(202),
    ], config=config)
    checkpoints = []

    def before_upload(reference):
        assert len(session.calls) == 1
        assert "body" not in session.calls[0][2]
        checkpoints.append(reference)

    assert client.upload("disk:/tmp/a", local, before_upload=before_upload) == (
        "upload_123"
    )
    assert checkpoints == ["upload_123"]
    expected_path = "vd:hash_1:disk:/tmp/a" if config else "disk:/tmp/a"
    assert session.calls[0][2]["params"] == {
        "path": expected_path, "overwrite": "false",
    }
    if config:
        assert session.calls[0][1].endswith("/virtual-disks/resources/upload")
    assert session.calls[1][2]["headers"] == {"Content-Length": "4"}
    assert session.calls[1][2]["body"] == b"data"


def test_before_upload_callback_failure_prevents_put(monkeypatch, tmp_path):
    local = tmp_path / "object"
    local.write_bytes(b"data")
    response = Response(payload={"href": "https://upload.example/put",
                                 "operation_id": "upload_123"})
    client, session, _ = destination(monkeypatch, [response], config=SHARED_CONFIG)

    def stop_before_bytes(reference):
        assert reference == "upload_123"
        raise RuntimeError("stop before upload")

    with pytest.raises(RuntimeError, match="stop before upload"):
        client.upload("/tmp/a", local, before_upload=stop_before_bytes)
    assert len(session.calls) == 1
    assert response.closed


@pytest.mark.parametrize("payload", [
    {"href": "http://upload.example/put", "operation_id": "upload_123"},
    {"href": "https://upload.example/put", "operation_id": "bad/id"},
])
def test_before_upload_rejects_bad_link_or_id_before_checkpoint(
    monkeypatch, tmp_path, payload,
):
    local = tmp_path / "object"
    local.write_bytes(b"data")
    client, session, _ = destination(monkeypatch, [Response(payload=payload)])
    checkpoints = []
    with pytest.raises(MigrationError):
        client.upload("/tmp/a", local, before_upload=checkpoints.append)
    assert checkpoints == [] and len(session.calls) == 1


def test_before_upload_legacy_missing_id_checkpoints_none(monkeypatch, tmp_path):
    local = tmp_path / "object"
    local.write_bytes(b"data")
    client, session, _ = destination(monkeypatch, [
        Response(payload={"href": "https://upload.example/put"}), Response(201),
    ])
    checkpoints = []
    assert client.upload("/tmp/a", local, before_upload=checkpoints.append) is None
    assert checkpoints == [None]
    assert session.calls[1][2]["body"] == b"data"


@pytest.mark.parametrize("config", [None, SHARED_CONFIG])
@pytest.mark.parametrize("metadata", [
    {"method": "GET"}, {"method": "POST"}, {"method": None},
    {"templated": True}, {"templated": "false"}, {"templated": None},
])
def test_upload_rejects_invalid_link_metadata_before_checkpoint(
    monkeypatch, tmp_path, config, metadata,
):
    local = tmp_path / "object"
    local.write_bytes(b"data")
    response = Response(payload=dict(
        metadata, href="https://upload.example/put", operation_id="upload_123",
    ))
    client, session, _ = destination(
        monkeypatch, [response, Response(201)], config=config,
    )
    checkpoints = []

    with pytest.raises(MigrationError, match="invalid_upload_url") as caught:
        client.upload("/tmp/a", local, before_upload=checkpoints.append)

    assert not caught.value.uncertain
    assert checkpoints == []
    assert [call[0] for call in session.calls] == ["GET"]
    assert response.closed


@pytest.mark.parametrize("config", [None, SHARED_CONFIG])
def test_upload_accepts_explicit_put_non_templated_link(
    monkeypatch, tmp_path, config,
):
    local = tmp_path / "object"
    local.write_bytes(b"data")
    client, session, _ = destination(monkeypatch, [
        Response(payload={
            "href": "https://upload.example/put", "operation_id": "upload_123",
            "method": "PUT", "templated": False,
        }), Response(201),
    ], config=config)
    checkpoints = []

    assert client.upload("/tmp/a", local, before_upload=checkpoints.append) is None
    assert checkpoints == ["upload_123"]
    assert [call[0] for call in session.calls] == ["GET", "PUT"]
    assert session.calls[1][2]["body"] == b"data"


def test_destination_read_only_error_is_safe_and_not_replayed(monkeypatch):
    client, session, _ = destination(monkeypatch, [Response(423)], config=SHARED_CONFIG)
    with pytest.raises(MigrationError, match="destination_read_only") as caught:
        client.move("/tmp/a", "/owner/a")
    assert not caught.value.uncertain
    assert len(session.calls) == 1


@pytest.mark.parametrize("operation", ["info", "stat", "poll", "upload"])
@pytest.mark.parametrize("failure", [
    Response(503), requests.ReadTimeout("private"), requests.ConnectionError("private"),
])
def test_destination_reads_fail_fast_without_starting_upload(
    monkeypatch, tmp_path, operation, failure,
):
    client, session, limiter = destination(monkeypatch, [
        failure, Response(payload={"total_space": 10}),
    ])
    local = tmp_path / "file"
    local.write_bytes(b"keep-cache")
    checkpoints, progress = [], []
    with pytest.raises(MigrationError) as caught:
        if operation == "info":
            client.info()
        elif operation == "stat":
            client.stat("disk:/")
        elif operation == "poll":
            client.poll("operation-1")
        else:
            client.upload("disk:/file", local, before_upload=checkpoints.append,
                          on_progress=progress.append)
    assert not caught.value.uncertain
    assert len(session.calls) == 1 and session.calls[0][0] == "GET"
    assert session.calls[0][2]["timeout"] == (5.0, 15.0)
    assert limiter.delays == []
    assert checkpoints == progress == []
    assert local.read_bytes() == b"keep-cache"
    if isinstance(failure, Response):
        assert failure.closed


def test_single_rate_limited_get_retains_cooldown_without_retry(monkeypatch):
    client, session, limiter = destination(monkeypatch, [
        Response(429, headers={"Retry-After": "11"}), Response(payload={}),
    ])
    with pytest.raises(MigrationError, match="destination_rate_limited"):
        client.info()
    assert len(session.calls) == 1
    assert limiter.delays == [11.0]


@pytest.mark.parametrize("overwrite", [False, True, 1, "true", None])
@pytest.mark.parametrize("method", ["upload", "import_url"])
def test_overwrite_is_only_enabled_by_explicit_true(
    monkeypatch, tmp_path, method, overwrite,
):
    local = tmp_path / "file"
    local.write_bytes(b"data")
    responses = ([Response(payload={"href": "https://upload.example/put"}),
                  Response(201)]
                 if method == "upload" else [Response(202, {"operation_id": "op-1"})])
    client, session, _ = destination(monkeypatch, responses, config=SHARED_CONFIG)
    if method == "upload":
        client.upload("disk:/file", local, overwrite=overwrite)
        assert session.calls[1][2]["timeout"] == (10.0, 120.0)
    else:
        client.import_url("disk:/file", "https://download.example/file",
                          overwrite=overwrite)
        assert session.calls[0][2]["timeout"] == (10.0, 120.0)
    assert session.calls[0][2]["params"]["overwrite"] == (
        "true" if overwrite is True else "false"
    )
    assert not any(call[0] == "DELETE" for call in session.calls)


@pytest.mark.parametrize("shared", [False, True])
def test_check_available_reads_selected_disk_root_and_preserves_info(
    monkeypatch, shared,
):
    owner = {"total_space": 100, "used_space": 10, "user": {"login": "owner"}}
    shared_info = {"vd_hash": "hash_1", "total_space": 200, "used_space": 20,
                   "permissions": ["read", "write"]}
    responses = [Response(payload=owner)]
    if shared:
        responses.append(Response(payload=shared_info))
    responses.append(Response(payload={
        "path": "vd:hash_1:disk:/" if shared else "disk:/", "type": "dir",
    }))
    client, session, _ = destination(monkeypatch, responses,
                                     config=SHARED_CONFIG if shared else None)
    result = client.check_available()
    expected = dict(shared_info, user=owner["user"]) if shared else owner
    assert result == expected
    assert session.calls[-1][2]["params"]["path"] == (
        "vd:hash_1:disk:/" if shared else "disk:/"
    )
    assert all(call[0] == "GET" and call[2]["timeout"] == (5.0, 15.0)
               for call in session.calls)


@pytest.mark.parametrize("root,code", [
    (Response(404), "destination_not_found"),
    (Response(payload={"path": "disk:/", "type": "file"}),
     "destination_invalid_response"),
])
def test_check_available_requires_an_existing_root_directory(monkeypatch, root, code):
    client, session, _ = destination(monkeypatch, [Response(payload={}), root])
    with pytest.raises(MigrationError, match=code) as caught:
        client.check_available()
    assert not caught.value.uncertain
    assert root.closed
    assert [call[0] for call in session.calls] == ["GET", "GET"]
