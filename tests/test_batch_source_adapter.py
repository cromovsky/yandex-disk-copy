"""Source-list adapters use fake HTTP and never publish shared sources."""

from concurrent.futures import ThreadPoolExecutor
import threading
import time

import pytest

from app.migration.errors import MigrationError
from app.migration.source import API, SaasDiskSource
from app.migration.store import canonical_config
from app.throttle import SplitRateLimiter
from tests.fake_clock import FakeClock
from tests.test_migration_clients import Limiter, Response, Session, token
from tests.test_migration_store import shared_config


def shared_source(monkeypatch, responses):
    session, limiter = Session(responses), Limiter()
    monkeypatch.setattr("app.migration.source.requests.Session", lambda: session)
    monkeypatch.setattr("app.migration.source.get_limiter", lambda: limiter)
    config = {
        "source_client_id": "client", "source_email": "reader@example.com",
        "source_path": "disk:/", "source_type": "shared", "source_vd_hash": "HASH",
    }
    return SaasDiskSource(config, {"source_client_secret": "private"}), session, limiter


@pytest.mark.parametrize("path", ["vd:/HASH/disk/Folder/a.txt", "vd:HASH:disk:/Folder/a.txt"])
def test_shared_stat_normalizes_api_paths_and_selects_virtual_disk(monkeypatch, path):
    source, session, _ = shared_source(monkeypatch, [token(), Response(payload={
        "path": path, "type": "file", "size": 4, "md5": "ABCD",
    })])
    assert source.stat("/Folder/a.txt") == {
        "path": "disk:/Folder/a.txt", "type": "file", "size": 4, "md5": "abcd",
    }
    assert session.calls[1][1] == API + "/virtual-disks/resources"
    assert session.calls[1][2]["params"]["path"] == "vd:HASH:disk:/Folder/a.txt"


def test_shared_rejects_path_from_another_disk(monkeypatch):
    source, _, _ = shared_source(monkeypatch, [token(), Response(payload={
        "path": "vd:/OTHER/disk/a.txt", "type": "file", "size": 4,
    })])
    with pytest.raises(MigrationError, match="source_invalid_response"):
        source.stat("/a.txt")


def test_shared_describe_reads_name_and_shared_download_is_authenticated(monkeypatch):
    source, session, _ = shared_source(monkeypatch, [
        token(), Response(payload={"name": "Archive", "vd_hash": "HASH"}),
        Response(payload={"href": "https://storage.example/file?private=signed"}),
        Response(body=b"data"),
    ])
    assert source.describe() == {"type": "shared", "name": "Archive", "vd_hash": "HASH"}
    assert session.calls[1][1] == API + "/virtual-disks"
    assert session.calls[1][2]["params"] == {"vd_hash": "HASH"}
    assert b"".join(source.chunks("/a.txt")) == b"data"
    assert session.calls[2][1] == API + "/virtual-disks/resources/download"
    assert session.calls[3][2]["headers"] == {"Authorization": "OAuth owner"}
    assert all(call[0] != "PUT" for call in session.calls)


def test_shared_download_does_not_forward_oauth_to_redirect_origin(monkeypatch):
    source, session, _ = shared_source(monkeypatch, [
        token(), Response(302, headers={"Location": "https://other.example/file"}),
        Response(body=b"data"),
    ])
    assert b"".join(source.chunks("/a", download_url="https://storage.example/file")) == b"data"
    assert session.calls[1][2]["headers"] == {"Authorization": "OAuth owner"}
    assert "Authorization" not in session.calls[2][2].get("headers", {})


def test_shared_walk_accepts_slash_api_responses(monkeypatch):
    source, _, _ = shared_source(monkeypatch, [
        token(), Response(payload={"path": "vd:/HASH/disk/", "type": "dir"}),
        Response(payload={"_embedded": {"total": 1, "items": [{
            "path": "vd:/HASH/disk/a.txt", "type": "file", "size": 1,
        }]}}),
    ])
    assert [node["path"] for node in source.walk("/")] == ["disk:/a.txt"]


def test_concurrent_owner_token_refresh_occurs_once(monkeypatch):
    source, session, _ = shared_source(monkeypatch, [token()])
    barrier = threading.Barrier(8)
    original = session.request
    def slow_exchange(*args, **kwargs):
        time.sleep(0.02)
        return original(*args, **kwargs)
    session.request = slow_exchange
    def get():
        barrier.wait(timeout=5)
        return source._owner_token()
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert list(pool.map(lambda _: get(), range(8))) == ["owner"] * 8
    assert len(session.calls) == 1


@pytest.mark.parametrize("method", ["POST", "PUT"])
def test_mutation_429_imposes_shared_cooldown_without_blind_replay(monkeypatch, method):
    source, session, limiter = shared_source(monkeypatch, [Response(429, headers={"Retry-After": "9"})])
    response = source._request(method, API + "/resources/publish")
    assert response.status_code == 429
    assert limiter.delays == [9]
    assert len(session.calls) == 1


def test_aggregate_limiter_spaces_reads_and_writes_in_one_budget():
    clock = FakeClock()
    limiter = SplitRateLimiter(30, 3, total_rps=30, monotonic=clock.monotonic, sleep=clock.sleep)
    sent = []
    for method in ["GET", "PUT", "GET", "GET", "PUT", "GET"]:
        limiter.acquire(method)
        sent.append(clock.monotonic())
    assert all(b - a >= 1 / 30 - 1e-9 for a, b in zip(sent, sent[1:]))
    assert sent[4] - sent[1] >= 1 / 3 - 1e-9


def test_shared_source_config_persists_folder_and_requires_staged_strategy():
    cfg = shared_config(source_type="shared", source_vd_hash="HASH",
                        source_folder="Archive (HASH)", strategy="staged",
                        destination_root="disk:/Archive/Archive (HASH)")
    assert canonical_config(cfg)["source_folder"] == "Archive (HASH)"
    for changes in ({"strategy": "url"}, {"source_public": True},
                    {"source_folder": "../escape"}, {"source_vd_hash": "../hash"}):
        with pytest.raises(ValueError):
            canonical_config(dict(cfg, **changes))


def test_publication_401_refreshes_once_before_confirming(monkeypatch):
    from tests.test_migration_public_source import FILE, metadata, source as personal_source
    denied, second_denied = Response(401), Response(401, {"error": "UnauthorizedError"})
    source, session = personal_source(monkeypatch, [
        token(), Response(payload=metadata(FILE)), denied,
        Response(payload={"access_token": "fresh", "expires_in": 3600}),
        Response(), Response(payload=metadata(FILE, published=True)),
    ])
    assert source.ensure_publication(FILE) == "published"
    assert denied.closed
    assert session.calls[4][2]["headers"] == {"Authorization": "OAuth fresh"}
    assert sum(call[0] == "PUT" for call in session.calls) == 2
    source, session = personal_source(monkeypatch, [
        token(), Response(payload=metadata(FILE)), Response(401), token(), second_denied,
    ])
    with pytest.raises(MigrationError) as caught:
        source.ensure_publication(FILE)
    assert caught.value.code == "source_auth_failed" and caught.value.auth
    assert not caught.value.uncertain
    assert second_denied.closed and len(session.calls) == 5


def test_publication_token_refresh_failure_is_definite_auth_error(monkeypatch):
    from tests.test_migration_public_source import FILE, metadata, source as personal_source
    source, session = personal_source(monkeypatch, [
        token(), Response(payload=metadata(FILE)), Response(401),
        Response(400, {"error": "invalid_grant"}),
    ])
    with pytest.raises(MigrationError) as caught:
        source.ensure_publication(FILE)
    assert caught.value.code == "source_auth_failed" and not caught.value.uncertain
    assert sum(call[0] == "PUT" for call in session.calls) == 1


def test_shared_stream_401_refreshes_link_and_token_once(monkeypatch):
    source, session, _ = shared_source(monkeypatch, [
        token(), Response(401), Response(payload={"access_token": "fresh", "expires_in": 3600}),
        Response(payload={"href": "https://storage.example/fresh"}), Response(body=b"data"),
    ])
    assert b"".join(source.chunks("/a", download_url="https://storage.example/stale")) == b"data"
    assert session.calls[-1][1] == "https://storage.example/fresh"
    assert session.calls[-1][2]["headers"] == {"Authorization": "OAuth fresh"}


def test_shared_source_never_publishes_even_if_raw_config_requests_it(monkeypatch):
    source, session, _ = shared_source(monkeypatch, [])
    source.config["source_public"] = True
    assert source.ensure_publication({"source_path": "disk:/folder"}) == "disabled"
    assert session.calls == []


def test_shared_folder_keeps_id_and_fits_unicode_filesystem_component():
    from app.migration.source import shared_source_folder
    result = shared_source_folder("e\u0301/:" + "Архив" * 100, "HASH")
    assert result.startswith("é__") and result.endswith(" (HASH)")
    assert len(result.encode("utf-8")) <= 255
    assert "/" not in result and ":" not in result


def test_legacy_internal_transport_retry_takes_another_global_slot(monkeypatch):
    from app.disk_copy import _build_session
    from urllib3.exceptions import ConnectTimeoutError
    limiter = Limiter()
    monkeypatch.setattr("app.disk_copy.get_limiter", lambda: limiter)
    session = _build_session()
    try:
        policy = session.get_adapter(API).max_retries
        clone = policy.increment("PUT", API, error=ConnectTimeoutError("private"))
        clone.sleep()
        assert clone.limiter is limiter
        assert limiter.calls == ["PUT"]
    finally:
        session.close()


def test_legacy_metadata_refreshes_rejected_owner_token_once():
    from tests.fake_session import FakeResponse, FakeSession
    from tests.test_disk_copy import make_copier
    session = FakeSession({
        ("GET", "/resources"): [FakeResponse(401), FakeResponse(200, {"_embedded": {"items": [], "total": 0}})],
        ("POST", "oauth.yandex.ru/token"): FakeResponse(200, {"access_token": "fresh", "expires_in": 3600}),
    })
    copier = make_copier(session)
    assert copier._disk_get_meta(copier._source_token(), 1000, 0, "/")["_embedded"]["total"] == 0
    assert copier.source.token == "fresh"
