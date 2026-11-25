from app.migration.source import API, TOKEN_URL
from tests.test_migration_api import api
from tests.test_migration_clients import Limiter, Response, Session, token


BODY = {"source_org_id": "42", "source_client_id": "application",
        "source_email": "tester@example.test", "source_client_secret": "private-example"}
ROUTE = "/api/onprem/source/browse"


def test_browse_reads_one_page_as_the_source_user_without_creating_a_job(api, monkeypatch):
    client, service = api
    membership = {"items": [{"id": "123", "email": BODY["source_email"], "is_enabled": True}], "total": 1}
    rows = [{"type": "dir", "path": "disk:/Архив/Отчёты"},
            {"type": "file", "path": "disk:/Архив/a.txt"}]
    session = Session([Response(payload=membership), token(),
                       Response(payload={"_embedded": {"total": 7, "items": rows}})])
    monkeypatch.setattr("app.migration.source.requests.Session", lambda: session)
    monkeypatch.setattr("app.migration.source.get_limiter", Limiter)
    response = client.post(ROUTE, json={**BODY, "source_admin_token": "directory-example",
                                       "path": "/Архив", "offset": 3})
    assert response.status_code == 200
    assert response.json() == {"path": "disk:/Архив", "next_offset": 5, "items": [
        {"name": row["path"].split("/")[-1], **row} for row in rows
    ]}
    assert response.headers["Cache-Control"] == "no-store"
    directory, oauth, listing = session.calls
    assert directory[:2] == ("GET", "https://cloud-api.yandex.net/v1/directory/organizations/42/users")
    assert oauth[:2] == ("POST", TOKEN_URL)
    assert oauth[2]["data"]["subject_token"] == BODY["source_email"]
    assert listing[:2] == ("GET", API + "/resources")
    assert listing[2]["headers"] == {"Authorization": "OAuth owner"}
    assert listing[2]["params"] == {"path": "disk:/Архив", "offset": 3, "limit": 100,
                                   "sort": "path", "fields": "_embedded.total,_embedded.items.type,_embedded.items.path"}
    assert session.closed and client.get("/api/onprem/jobs").json()["jobs"] == []
    assert b"private-example" not in service.store.path.read_bytes()


def test_browse_refuses_untrusted_request_or_destination_before_using_credentials(api, monkeypatch):
    client, service = api
    def unexpected(*args):
        raise AssertionError("invalid browse started a Disk request")
    monkeypatch.setattr(service.runner, "source_factory", unexpected)
    assert client.post(ROUTE, json=BODY, headers={"X-Onprem-Key": "wrong"}).status_code == 403
    assert client.post(ROUTE, json=BODY, headers={"Origin": "https://other.test"}).status_code == 403
    for fields in [{"destination_url": "https://other.test"}, {"source_email": "invalid"},
                   {"path": "../outside"}, {"offset": -1},
                   {"source_client_secret": "private-example\n"}]:
        result = client.post(ROUTE, json={**BODY, **fields})
        assert result.status_code == 422
        assert "private-example" not in result.text


def test_browse_auth_error_closes_session_and_keeps_response_safe(api, monkeypatch):
    client, _ = api
    session = Session([Response(403, {"message": "private-example"})])
    monkeypatch.setattr("app.migration.source.requests.Session", lambda: session)
    monkeypatch.setattr("app.migration.source.get_limiter", Limiter)
    result = client.post(ROUTE, json=BODY)
    assert result.status_code == 409 and result.json()["detail"] == "source_auth_failed"
    assert session.closed and "private-example" not in result.text
