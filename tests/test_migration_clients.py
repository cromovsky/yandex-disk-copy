"""Migration adapters use mocked HTTP only; no real Disk access."""

import pytest
import requests

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

    def acquire(self, method):
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


def destination(monkeypatch, responses):
    session, limiter = Session(responses), Limiter()
    monkeypatch.setattr("app.migration.destination.requests.Session", lambda: session)
    monkeypatch.setattr(
        "app.migration.destination.get_onprem_limiter", lambda _url: limiter,
    )
    return OnPremDiskDestination(DEST_CONFIG, CREDENTIALS), session, limiter


def token():
    return Response(payload={"access_token": "owner", "expires_in": 3600})


def test_error_never_includes_external_text():
    error = MigrationError("destination_unavailable", uncertain=True)
    assert str(error) == "destination_unavailable"
    assert error.uncertain and not error.auth


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


def test_stream_exception_is_safe_and_stream_closed(monkeypatch):
    class BrokenStream(Response):
        def iter_content(self, chunk_size):
            raise requests.ConnectionError("https://signed/?secret=value")

    stream = BrokenStream()
    client, _, _ = source(monkeypatch, [
        token(), Response(payload={"href": "https://signed.example/a"}), stream,
    ])
    with pytest.raises(MigrationError, match="source_download_failed") as caught:
        list(client.chunks("/a"))
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
    assert str(caught.value) == "destination_unavailable"
    assert session.calls[0][2]["params"]["overwrite"] == "false"


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
    client, _, limiter = destination(monkeypatch, [
        Response(503), Response(payload={"total_space": 10}),
    ])
    client.info()
    assert limiter.delays == [0.5]


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
