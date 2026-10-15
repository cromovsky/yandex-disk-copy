"""Public migration uses fake HTTP, never a live user Disk."""

import pytest
import requests

from app.migration.errors import MigrationError
from app.migration.source import API, SaasDiskSource
from tests.test_migration_clients import Limiter, Response, Session, token


FOLDER = {
    "source_path": "disk:/Проекты", "kind": "dir", "resource_id": "folder-id",
    "size": None, "md5": None, "modified": "before", "entry_id": "folder-entry",
}
FILE = {
    "source_path": "disk:/Отчёт.txt", "kind": "file", "resource_id": "file-id",
    "size": 4, "md5": "aaaa", "modified": "before", "entry_id": "file-entry",
}


def metadata(node, *, published=False, **changes):
    result = {
        "path": node["source_path"], "type": node["kind"],
        "resource_id": node["resource_id"], "modified": node["modified"],
    }
    if node["kind"] == "file":
        result.update(size=node["size"], md5=node["md5"])
    if published:
        result.update(public_key="private-public-key", public_url="https://disk.yandex.ru/d/key")
    result.update(changes)
    return result


def source(monkeypatch, responses, *, path="disk:/", public=True):
    session = Session(responses)
    monkeypatch.setattr("app.migration.source.requests.Session", lambda: session)
    monkeypatch.setattr("app.migration.source.get_limiter", Limiter)
    config = {
        "source_client_id": "client", "source_email": "source@example.com",
        "source_path": path,
    }
    if public is not None:
        config["source_public"] = public
    return SaasDiskSource(config, {"source_client_secret": "owner-secret"}), session


def test_new_folder_publication_is_confirmed_after_checkpoint(monkeypatch):
    before = Response(payload=metadata(FOLDER))
    after = Response(payload=metadata(FOLDER, published=True, modified="after"))
    client, session = source(monkeypatch, [token(), before, Response(), after], path="/Проекты")
    seen = []

    def checkpoint():
        seen.append([call[0] for call in session.calls])

    assert client.ensure_publication(FOLDER, checkpoint=checkpoint) == "published"
    # Check cancellation immediately before publication and after its response,
    # then confirm the published metadata with the final GET.
    assert seen == [["POST", "GET"], ["POST", "GET", "PUT"]]
    assert [call[0] for call in session.calls] == ["POST", "GET", "PUT", "GET"]
    assert session.calls[2][1] == API + "/resources/publish"
    assert session.calls[2][2]["params"] == {"path": "disk:/Проекты"}
    assert session.calls[2][2]["headers"] == {"Authorization": "OAuth owner"}
    assert client.has_publication("/Проекты")
    assert all(response.closed for response in (before, after))


def test_existing_publication_preserves_password_and_personal_settings(monkeypatch):
    payload = metadata(FOLDER, published=True, public_settings={
        "password": "existing-password", "accesses": [{"user_ids": ["existing-user"]}],
    })
    client, session = source(monkeypatch, [token(), Response(payload=payload)], path="/Проекты")
    assert client.ensure_publication(FOLDER) == "existing"
    assert client.has_publication("disk:/Проекты")
    assert [call[0] for call in session.calls] == ["POST", "GET"]


def test_existing_public_url_without_key_is_not_republished(monkeypatch):
    client, session = source(monkeypatch, [
        token(), Response(payload=metadata(FILE, public_url="https://disk.yandex.ru/d/key")),
        Response(), Response(payload=metadata(FILE, published=True)),
    ])
    with pytest.raises(MigrationError, match="source_public_invalid_response"):
        client.ensure_publication(FILE)
    assert all(call[0] != "PUT" for call in session.calls)


@pytest.mark.parametrize("change", [
    {"path": "disk:/other"}, {"type": "dir"}, {"resource_id": "replacement"},
    {"resource_id": None}, {"size": 5}, {"md5": "bbbb"}, {"modified": "changed"},
])
def test_changed_file_is_never_published(monkeypatch, change):
    client, session = source(monkeypatch, [token(), Response(payload=metadata(FILE, **change))])
    with pytest.raises(MigrationError) as caught:
        client.ensure_publication(FILE)
    assert caught.value.code in {"source_changed", "source_invalid_response"}
    assert not client.has_publication(FILE["source_path"])
    assert all(call[0] != "PUT" for call in session.calls)


def test_nested_unicode_file_uses_folder_key_and_relative_path(monkeypatch):
    client, session = source(monkeypatch, [
        token(), Response(payload=metadata(FOLDER, published=True)),
        Response(payload={"href": "https://downloader.example/file?secret=signed"}),
        Response(302, headers={"Location": "https://storage.example/file?secret=redirect"}),
        Response(body=b"data"),
    ], path="/Проекты")
    client.ensure_publication(FOLDER)
    assert b"".join(client.chunks("/Проекты/Документы/Отчёт.txt")) == b"data"
    assert session.calls[2][1] == API + "/public/resources/download"
    assert session.calls[2][2]["params"] == {
        "public_key": "private-public-key", "path": "/Документы/Отчёт.txt",
    }
    assert all("Authorization" not in call[2].get("headers", {}) for call in session.calls[2:])


def test_root_file_uses_its_own_key_without_child_path(monkeypatch):
    client, session = source(monkeypatch, [
        token(), Response(payload=metadata(FILE, published=True)),
        Response(payload={"href": "https://downloader.example/file"}), Response(body=b"data"),
    ])
    client.ensure_publication(FILE)
    assert b"".join(client.chunks(FILE["source_path"])) == b"data"
    assert session.calls[2][2]["params"] == {"public_key": "private-public-key"}


def test_cache_miss_resolves_top_level_key_readonly(monkeypatch):
    client, session = source(monkeypatch, [
        token(), Response(payload=metadata(FOLDER, published=True)),
        Response(payload={"href": "https://downloader.example/file"}), Response(body=b"data"),
    ])
    assert not client.has_publication("/Проекты")
    assert b"".join(client.chunks("/Проекты/child.txt")) == b"data"
    assert session.calls[1][2]["params"]["path"] == "disk:/Проекты"
    assert session.calls[2][2]["params"]["path"] == "/child.txt"
    assert all(call[0] != "PUT" for call in session.calls)


def test_private_cache_miss_does_not_publish_from_download(monkeypatch):
    client, session = source(monkeypatch, [token(), Response(payload=metadata(FILE))])
    with pytest.raises(MigrationError, match="source_publication_not_confirmed"):
        list(client.chunks(FILE["source_path"]))
    assert all(call[0] != "PUT" for call in session.calls)


def test_lost_publish_response_is_resolved_by_get_without_replaying_put(monkeypatch):
    client, session = source(monkeypatch, [
        token(), Response(payload=metadata(FILE)), requests.Timeout("owner-secret url=signed"),
        Response(payload=metadata(FILE, published=True)),
    ])
    with pytest.raises(MigrationError) as caught:
        client.ensure_publication(FILE)
    assert caught.value.code == "source_publication_outcome_unknown"
    assert caught.value.uncertain
    assert "secret" not in str(caught.value)
    assert not client.has_publication(FILE["source_path"])
    assert client.ensure_publication(FILE) == "existing"
    assert [call[0] for call in session.calls].count("PUT") == 1


@pytest.mark.parametrize("change", [{"resource_id": "replacement"}, {"md5": "bbbb"}])
def test_changed_post_put_confirmation_preserves_uncertain_mutation(monkeypatch, change):
    client, session = source(monkeypatch, [
        token(), Response(payload=metadata(FILE)), Response(),
        Response(payload=metadata(FILE, published=True, **change)),
        Response(payload=metadata(FILE)),
    ])
    for _ in range(2):
        with pytest.raises(MigrationError) as caught:
            client.ensure_publication(FILE)
        assert caught.value.code == "source_publication_outcome_unknown"
        assert caught.value.uncertain
        assert not client.has_publication(FILE["source_path"])
    assert [call[0] for call in session.calls].count("PUT") == 1


def test_unknown_publication_without_confirmation_never_replays_put(monkeypatch):
    client, session = source(monkeypatch, [
        token(), Response(payload=metadata(FILE)), requests.ConnectionError("signed-secret"),
        Response(payload=metadata(FILE)),
    ])
    for _ in range(2):
        with pytest.raises(MigrationError) as caught:
            client.ensure_publication(FILE)
        assert caught.value.uncertain
        assert caught.value.code == "source_publication_outcome_unknown"
    assert [call[0] for call in session.calls].count("PUT") == 1


@pytest.mark.parametrize("state,code,uncertain", [
    ("intent", "source_publication_outcome_unknown", True),
    ("published", "source_publication_not_confirmed", False),
])
def test_restored_publication_state_never_replays_mutation(
    monkeypatch, state, code, uncertain,
):
    client, session = source(monkeypatch, [
        token(), Response(payload=metadata(FILE)), Response(),
        Response(payload=metadata(FILE, published=True)),
    ])
    with pytest.raises(MigrationError) as caught:
        client.ensure_publication(dict(FILE, state=state))
    assert caught.value.code == code
    assert caught.value.uncertain is uncertain
    assert all(call[0] != "PUT" for call in session.calls)


def test_restored_unknown_publication_is_resolved_by_readonly_metadata(monkeypatch):
    client, session = source(monkeypatch, [token(), Response(payload=metadata(FILE, published=True))])
    assert client.ensure_publication(dict(FILE, state="intent")) == "existing"
    assert client.has_publication(FILE["source_path"])
    assert all(call[0] != "PUT" for call in session.calls)


def test_journaled_public_href_stream_does_not_get_another_href(monkeypatch):
    client, session = source(monkeypatch, [Response(body=b"data")])
    assert b"".join(client.chunks(
        FILE["source_path"], download_url="https://downloader.example/journaled",
    )) == b"data"
    assert [call[1] for call in session.calls] == ["https://downloader.example/journaled"]
    assert "Authorization" not in session.calls[0][2].get("headers", {})


def test_supplied_public_href_is_still_validated_before_transfer(monkeypatch):
    client, session = source(monkeypatch, [])
    with pytest.raises(MigrationError, match="invalid_download_url") as caught:
        list(client.chunks(FILE["source_path"], download_url="http://host/secret"))
    assert "secret" not in str(caught.value)
    assert session.calls == []


@pytest.mark.parametrize("status,code,auth,uncertain", [
    (403, "source_publication_denied", False, False),
    (423, "source_publication_denied", False, False),
    (400, "source_publication_failed", False, False),
    (503, "source_publication_outcome_unknown", False, True),
])
def test_publication_response_classifies_failure_without_retry(
    monkeypatch, status, code, auth, uncertain,
):
    client, session = source(monkeypatch, [
        token(), Response(payload=metadata(FILE)),
        Response(status, {"description": "owner-secret"}, headers={"Retry-After": "17"}),
    ])
    with pytest.raises(MigrationError) as caught:
        client.ensure_publication(FILE)
    assert caught.value.code == code
    assert caught.value.auth is auth
    assert caught.value.uncertain is uncertain
    assert caught.value.http_status == status
    assert caught.value.retry_after == 17.0
    assert "owner-secret" not in str(caught.value)
    assert [call[0] for call in session.calls].count("PUT") == 1


def test_stop_checkpoint_runs_before_any_publication_mutation(monkeypatch):
    client, session = source(monkeypatch, [token(), Response(payload=metadata(FILE))])

    def stopped():
        raise RuntimeError("stopped")

    with pytest.raises(RuntimeError, match="stopped"):
        client.ensure_publication(FILE, checkpoint=stopped)
    assert all(call[0] != "PUT" for call in session.calls)


@pytest.mark.parametrize("action", ["pause", "stop"])
def test_publication_checkpoint_after_limiter_wait_prevents_put(monkeypatch, action):
    requested = []

    class WaitingLimiter(Limiter):
        def acquire(self, method):
            super().acquire(method)
            if method == "PUT":
                requested.append(action)

    client, session = source(monkeypatch, [
        token(), Response(payload=metadata(FILE)), Response(),
        Response(payload=metadata(FILE, published=True)),
    ])
    client.limiter = WaitingLimiter()

    def checkpoint():
        if requested:
            raise RuntimeError(requested[0])

    with pytest.raises(RuntimeError, match=action):
        client.ensure_publication(FILE, checkpoint=checkpoint)
    assert client.limiter.calls.count("PUT") == 1
    assert all(call[0] != "PUT" for call in session.calls)
    assert not client.has_publication(FILE["source_path"])


@pytest.mark.parametrize("status,payload,code", [
    (401, {}, "source_public_access_denied"),
    (403, {}, "source_public_access_denied"),
    (404, {}, "source_public_resource_unavailable"),
    (429, {}, "source_public_download_limit"),
    (403, {"error": "DiskDownloadLimitExceededError"}, "source_public_download_limit"),
    (409, {"error": "DiskDownloadLimitExceededError"}, "source_public_download_limit"),
    (409, {"error": "arbitrary-secret"}, "source_public_download_failed"),
    (503, {}, "source_public_download_failed"),
])
def test_public_download_errors_do_not_request_new_owner_credentials(
    monkeypatch, status, payload, code,
):
    client, session = source(monkeypatch, [
        token(), Response(payload=metadata(FILE, published=True)),
        Response(status, payload, headers={"Retry-After": "23"}),
    ])
    client.ensure_publication(FILE)
    with pytest.raises(MigrationError) as caught:
        list(client.chunks(FILE["source_path"]))
    assert caught.value.code == code
    assert caught.value.auth is False
    assert caught.value.http_status == status
    assert caught.value.retry_after == 23.0
    assert "arbitrary-secret" not in str(caught.value)
    assert len(session.calls) == 3


def test_retry_download_gets_new_public_href(monkeypatch):
    client, session = source(monkeypatch, [
        token(), Response(payload=metadata(FILE, published=True)),
        Response(payload={"href": "https://downloader.example/first"}), Response(503),
        Response(payload={"href": "https://downloader.example/second"}), Response(body=b"data"),
    ])
    client.ensure_publication(FILE)
    with pytest.raises(MigrationError, match="source_public_download_failed"):
        list(client.chunks(FILE["source_path"]))
    assert b"".join(client.chunks(FILE["source_path"])) == b"data"
    assert [call[1] for call in session.calls].count(API + "/public/resources/download") == 2
    assert [call[1] for call in session.calls].count(API + "/resources/publish") == 0


@pytest.mark.parametrize("location", ["http://storage.example/secret", "https://u:secret@storage.example/f"])
def test_public_download_keeps_https_redirect_validation(monkeypatch, location):
    client, session = source(monkeypatch, [
        token(), Response(payload=metadata(FILE, published=True)),
        Response(payload={"href": "https://downloader.example/f"}),
        Response(302, headers={"Location": location}),
    ])
    client.ensure_publication(FILE)
    with pytest.raises(MigrationError, match="invalid_download_url") as caught:
        list(client.chunks(FILE["source_path"]))
    assert "secret" not in str(caught.value)
    assert len(session.calls) == 4


@pytest.mark.parametrize("public", [False, None])
def test_existing_source_mode_does_not_publish_or_use_public_api(monkeypatch, public):
    client, session = source(monkeypatch, [
        token(), Response(payload={"href": "https://downloader.example/f"}), Response(body=b"data"),
    ], public=public)
    assert b"".join(client.chunks("/file")) == b"data"
    assert session.calls[1][1] == API + "/resources/download"
    assert all("publish" not in call[1] for call in session.calls)
