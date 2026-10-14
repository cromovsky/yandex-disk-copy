"""Source access validation reads Directory with fake HTTP; it never changes users."""

import pytest
import requests

from app.migration.errors import MigrationError
from app.migration.source import SaasDiskSource
from tests.test_migration_clients import Limiter, Response, Session, token


DIRECTORY = "https://cloud-api.yandex.net/v1/directory/organizations/12345/users"


def member(*, enabled=True, email="reader@example.com", **changes):
    return {"items": [{"id": "1130000000000001", "email": email,
                       "default_email": email, "aliases": [], "contacts": [],
                       "is_enabled": enabled, **changes}],
            "limit": 100, "offset": 0, "total": 1}


def source(monkeypatch, responses, *, shared=False, admin="admin-private"):
    session, limiter = Session(responses), Limiter()
    monkeypatch.setattr("app.migration.source.requests.Session", lambda: session)
    monkeypatch.setattr("app.migration.source.get_limiter", lambda: limiter)
    config = {"source_org_id": "12345", "source_client_id": "client",
              "source_email": "reader@example.com", "source_path": "disk:/"}
    if shared:
        config.update(source_type="shared", source_vd_hash="HASH")
    credentials = {"source_client_secret": "secret-private"}
    if admin:
        credentials["source_admin_token"] = admin
    return SaasDiskSource(config, credentials), session, limiter


def metadata(shared):
    return Response(payload={"path": "vd:/HASH/disk/" if shared else "disk:/",
                             "type": "dir"})


@pytest.mark.parametrize("shared", [False, True])
def test_admin_validates_organization_before_service_app_exchange(monkeypatch, shared):
    directory = Response(payload=member())
    client, session, limiter = source(monkeypatch, [directory, token(), metadata(shared)],
                                      shared=shared)
    assert client.stat("/")["type"] == "dir"
    assert session.calls[0][:2] == ("GET", DIRECTORY)
    assert session.calls[0][2]["params"] == {"email": "reader@example.com", "limit": 100,
                                            "offset": 0}
    assert session.calls[0][2]["headers"] == {"Authorization": "OAuth admin-private"}
    assert session.calls[1][2]["data"]["subject_token"] == "reader@example.com"
    assert "admin-private" not in str(session.calls[1:])
    assert directory.closed
    assert limiter.calls == ["GET", "POST", "GET"]


@pytest.mark.parametrize("shared", [False, True])
def test_blocked_source_fails_readonly_without_issuing_token(monkeypatch, shared):
    directory = Response(payload=member(enabled=False))
    client, session, _ = source(monkeypatch, [directory, token(), metadata(shared)],
                                shared=shared)
    with pytest.raises(MigrationError) as caught:
        client.stat("/")
    assert caught.value.code == "source_user_blocked"
    assert not caught.value.auth
    assert session.calls == [("GET", DIRECTORY, session.calls[0][2])]
    assert directory.closed


@pytest.mark.parametrize("payload", [
    {"items": [], "limit": 100, "offset": 0, "total": 0},
    member(email="other@example.com"),
])
def test_nonmember_cannot_receive_source_token(monkeypatch, payload):
    client, session, _ = source(monkeypatch, [Response(payload=payload), token()])
    with pytest.raises(MigrationError) as caught:
        client._owner_token()
    assert caught.value.code == "source_user_not_in_organization"
    assert len(session.calls) == 1


@pytest.mark.parametrize("user", [
    member(email="primary@example.com", aliases=["reader"]),
    member(email="primary@example.com", aliases=["reader@example.com"]),
    member(email="primary@primary.example", default_email="primary@example.com",
           aliases=["reader"]),
    member(email="primary@primary.example", contacts=[{
        "type": "email", "value": "reader@example.com", "alias": True,
        "synthetic": False, "main": False,
    }]),
    member(email="primary@primary.example", contacts=[{
        "type": "email", "value": "reader@example.com", "alias": True, "main": False,
    }]),
])
def test_documented_alias_addresses_can_receive_their_owner_token(monkeypatch, user):
    client, session, _ = source(monkeypatch, [Response(payload=user), token(), metadata(False)])
    assert client.stat("/")["type"] == "dir"
    assert session.calls[1][2]["data"]["subject_token"] == "reader@example.com"


def test_manually_entered_contact_is_not_proof_of_source_membership(monkeypatch):
    client, session, _ = source(monkeypatch, [Response(payload=member(
        email="other@example.com", contacts=[{
            "type": "email", "value": "reader@example.com", "alias": False,
            "synthetic": False, "main": True,
        }],
    )), token()])
    with pytest.raises(MigrationError) as caught:
        client._owner_token()
    assert caught.value.code == "source_user_not_in_organization"
    assert len(session.calls) == 1


@pytest.mark.parametrize("status,code,auth", [
    (401, "source_admin_auth_failed", True),
    (403, "source_admin_auth_failed", True),
    (404, "source_user_not_in_organization", False),
    (400, "source_admin_check_failed", False),
])
def test_admin_errors_preserve_safe_http_diagnostics(monkeypatch, status, code, auth):
    response = Response(status, {"error": "ForbiddenError",
                                 "message": "admin-private secret-private"})
    client, session, _ = source(monkeypatch, [response, token()])
    with pytest.raises(MigrationError) as caught:
        client._owner_token()
    error = caught.value
    assert error.code == code and error.auth is auth
    assert error.http_status == status
    assert error.diagnostics == {"error_side": "saas", "api_error": "ForbiddenError",
                                  "request_host": "cloud-api.yandex.net"}
    assert "admin-private" not in str(error) + str(error.diagnostics)
    assert "secret-private" not in str(error) + str(error.diagnostics)
    assert response.closed and len(session.calls) == 1


@pytest.mark.parametrize("payload", [
    None, {}, {"items": "malformed", "total": 1},
    member(enabled="false"), member(id=True), member(id="../unsafe"),
    {"items": member()["items"] * 2, "total": 2},
])
def test_malformed_directory_response_cannot_pass_access_check(monkeypatch, payload):
    response = Response(payload=payload)
    client, session, _ = source(monkeypatch, [response, token()])
    with pytest.raises(MigrationError) as caught:
        client._owner_token()
    assert caught.value.code == "source_admin_check_failed"
    assert response.closed and len(session.calls) == 1


def test_source_revalidates_admin_access_on_rejected_oauth_token(monkeypatch):
    client, session, _ = source(monkeypatch, [
        Response(payload=member()), token(), Response(401),
        Response(payload=member(enabled=False)), token(), metadata(False),
    ])
    with pytest.raises(MigrationError) as caught:
        client.stat("/")
    assert caught.value.code == "source_user_blocked"
    assert [call[1] for call in session.calls] == [DIRECTORY, "https://oauth.yandex.ru/token",
                                                 "https://cloud-api.yandex.net/v1/disk/resources",
                                                 DIRECTORY]


def test_source_revalidates_admin_access_on_expired_token(monkeypatch):
    client, session, _ = source(monkeypatch, [
        Response(payload=member()), token(),
        Response(payload=member(enabled=False)), token(),
    ])
    assert client._owner_token() == "owner"
    client._token_deadline = 0
    with pytest.raises(MigrationError) as caught:
        client._owner_token()
    assert caught.value.code == "source_user_blocked"
    assert sum(call[0] == "POST" for call in session.calls) == 1


def test_admin_transport_error_is_classified_as_directory_check(monkeypatch):
    client, session, _ = source(monkeypatch, [requests.exceptions.SSLError("admin-private")] * 5)
    with pytest.raises(MigrationError) as caught:
        client._owner_token()
    assert caught.value.code == "source_admin_check_failed"
    assert caught.value.diagnostics == {"error_side": "saas", "transport_error": "tls",
                                        "request_host": "cloud-api.yandex.net"}
    assert "admin-private" not in str(caught.value)
    assert all(call[0] == "GET" for call in session.calls)


@pytest.mark.parametrize("shared", [False, True])
def test_service_app_access_without_admin_remains_supported(monkeypatch, shared):
    client, session, _ = source(monkeypatch, [token(), metadata(shared)], shared=shared,
                                admin=None)
    assert client.stat("/")["type"] == "dir"
    assert session.calls[0][1] == "https://oauth.yandex.ru/token"
    assert len(session.calls) == 2
