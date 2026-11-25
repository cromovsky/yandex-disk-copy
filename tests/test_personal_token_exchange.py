from email.message import Message
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests

from app.migration import destination as disk
from app.migration.errors import MigrationError


OAUTH = "https://oauth.test/token"
API = "https://cloud-api.test/v1/disk"
SCOPE = "cloud_api:disk.read cloud_api:disk.write cloud_api:disk.info"


def client(identity="10002"):
    dest = disk.OnPremDiskDestination({
        "destination_auth": "service_application", "destination_url": API,
        "destination_oauth_url": OAUTH, "destination_client_id": "client",
        "destination_identity": identity,
    }, {"destination_client_secret": "secret", "destination_owner_token": "owner"})
    dest.session = Mock()
    dest.limiter = Mock()
    return dest


def response(payload, status=200):
    return Mock(status_code=status, headers={}, json=lambda: payload)


def token(value="temporary", ttl=120):
    return response({"access_token": value, "expires_in": ttl})


@pytest.mark.parametrize("identity,kind", [("10002", "uid"), ("u@example.test", "email")])
def test_exchange_proof_and_upload(identity, kind, tmp_path):
    dest = client(identity)
    href = "https://upload.test/f?signature=private"
    dest.session.request.side_effect = [
        token(), response({"user": {"uid": 10002, "email": "u@example.test"}}),
        response({"href": href}), response({}, 201),
    ]
    file = tmp_path / "payload"
    file.write_bytes(b"a")
    dest.upload("disk:/a", file)
    post, proof, link, put = dest.session.request.call_args_list
    assert post.args == ("POST", OAUTH)
    assert post.kwargs["data"] == {
        "client_id": "client", "client_secret": "secret", "access_token": "owner",
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "scope": SCOPE, "subject_token": identity,
        "subject_token_type": "urn:yandex:params:oauth:token-type:" + kind,
    }
    assert proof.args == ("GET", API + "/")
    assert link.args == ("GET", API + "/resources/upload")
    for call in (proof, link):
        assert call.kwargs["headers"] == {"Authorization": "OAuth temporary"}
    assert put.args == ("PUT", href)
    assert put.kwargs["headers"] == {"Content-Length": "1"}
    for call in (post, proof, link, put):
        assert call.kwargs["verify"]
        assert call.kwargs["allow_redirects"] is False


def test_refresh_rejects_reassigned_email(monkeypatch):
    clock = Mock(return_value=10)
    monkeypatch.setattr(disk.time, "monotonic", clock)
    dest = client("u@example.test")
    dest.session.request.side_effect = [
        token(ttl=99999), response({"user": {"uid": "9007199254740993"}}),
        token("replacement"), response({"user": {"uid": "2"}}),
    ]
    assert dest.info()["user"]["uid"] == "9007199254740993"
    assert dest.expected_uid == "9007199254740993"
    clock.return_value += 3600
    with pytest.raises(MigrationError, match="destination_identity_mismatch"):
        dest.claim_dir("disk:/new")
    assert len(dest.session.request.call_args_list) == 4


@pytest.mark.parametrize("identity,user", [
    ("10002", {"uid": False}), ("10002", {"uid": 10003}),
    ("u@example.test", {"uid": 10002, "email": "broken", "login": "u@example.test"}),
    ("u@example.test", {"uid": 10002, "email": "U@example.test"}),
    ("u@example.test", {"email": "u@example.test"}),
])
def test_invalid_owner_stops_before_mutation(identity, user):
    dest = client(identity)
    dest.session.request.side_effect = [token(), response({"user": user})]
    with pytest.raises(MigrationError, match="destination_identity_"):
        dest.claim_dir("disk:/new")
    assert dest.session.request.call_count == 2


@pytest.mark.parametrize("url", ["http://oauth.test/token", OAUTH + "?", OAUTH + "#",
                                 "https://name@oauth.test/token", OAUTH + "/other"])
def test_invalid_endpoint(url):
    with pytest.raises(MigrationError, match="invalid_destination_oauth_url"):
        disk.normalize_oauth_url(url)


@pytest.mark.parametrize("overrides,error", [
    ({"destination_oauth_url": "https://oauth.other.example/token"}, "invalid_destination_oauth_url"),
    ({"destination_url": "https://disk-api.test/v1/disk"}, "invalid_personal_disk_api_url"),
])
def test_persisted_service_credentials_require_the_same_installation(monkeypatch, overrides, error):
    monkeypatch.setattr(disk.requests, "Session", lambda: pytest.fail("unexpected HTTP client"))
    with pytest.raises(MigrationError, match=error):
        disk.OnPremDiskDestination({
            "destination_auth": "service_application", "destination_url": API,
            "destination_oauth_url": OAUTH, "destination_client_id": "client",
            "destination_identity": "10002", **overrides,
        }, {"destination_client_secret": "secret", "destination_owner_token": "owner"})


@pytest.mark.parametrize("payload", [
    {}, {"access_token": "bad\nheader", "expires_in": 10},
    {"access_token": "valid", "expires_in": True},
    {"access_token": "valid", "expires_in": 0}, ["secret"],
])
def test_malformed_exchange_never_contacts_disk(payload):
    dest = client()
    dest.session.request.return_value = response(payload)
    with pytest.raises(MigrationError, match="destination_auth_failed"):
        dest.claim_dir("disk:/new")
    assert dest.session.request.call_count == 1


def test_rejected_mutation_is_sent_once():
    dest = client()
    dest.session.request.side_effect = [
        token(), response({"user": {"uid": 10002}}), response({}, 401),
    ]
    with pytest.raises(MigrationError, match="destination_auth_failed"):
        dest.claim_dir("disk:/new")
    assert [call.args[0] for call in dest.session.request.call_args_list] == ["POST", "GET", "PUT"]


def test_no_response_cookie_reaches_another_service_or_upload(tmp_path):
    root = "example.test"
    oauth_url = f"https://oauth.{root}/token"
    disk_url = f"https://cloud-api.{root}/v1/disk"
    href = f"https://upload.{root}/file?signature=private"
    dest = disk.OnPremDiskDestination({
        "destination_auth": "service_application", "destination_url": disk_url,
        "destination_oauth_url": oauth_url, "destination_client_id": "client",
        "destination_identity": "10002",
    }, {"destination_client_secret": "secret", "destination_owner_token": "owner"})
    dest.limiter = Mock()
    requests_sent = []
    replies = iter([
        (200, {"access_token": "temporary", "expires_in": 120}),
        (200, {"user": {"uid": 10002}}), (200, {"href": href}), (201, {}),
    ])

    class Transport(requests.adapters.BaseAdapter):
        def send(self, request, **kwargs):
            requests_sent.append(request)
            res = requests.Response()
            res.status_code, payload = next(replies)
            res._content = json.dumps(payload).encode()
            res._content_consumed = True
            headers = Message()
            headers.add_header("Set-Cookie", f"credential=private; Domain={root}; Path=/; Secure")
            res.raw = SimpleNamespace(_original_response=SimpleNamespace(msg=headers))
            return res

        def close(self):
            pass

    dest.session.mount("https://", Transport())
    local = tmp_path / "file"
    local.write_bytes(b"a")
    try:
        dest.upload("disk:/file", local)
        assert [req.url.split("?")[0] for req in requests_sent] == [
            oauth_url, disk_url + "/", disk_url + "/resources/upload", href.split("?")[0],
        ]
        assert all("Cookie" not in req.headers for req in requests_sent)
        assert not dest.session.cookies
        assert "Authorization" not in requests_sent[-1].headers
    finally:
        dest.close()


@pytest.mark.parametrize("optional_email", [{}, {"email": None}, {"email": ""}])
def test_email_token_renews_for_same_recipient_five_minutes_before_one_hour(monkeypatch, optional_email):
    clock = Mock(return_value=0)
    monkeypatch.setattr(disk.time, "monotonic", clock)
    dest = client("u@example.test")
    owner = {"user": {"uid": 10002, **optional_email}}
    dest.session.request.side_effect = [
        response({"access_token": "temporary"}), response(owner),
        response({"access_token": "replacement"}), response(owner), response({}, 404),
    ]
    assert dest.info() == owner
    clock.return_value = 3299
    assert dest.info() == owner
    assert dest.session.request.call_count == 2
    clock.return_value = 3300
    assert dest.stat("disk:/file") is None
    first, _, renewal, proof, read = dest.session.request.call_args_list
    assert renewal.args == first.args == ("POST", OAUTH)
    assert renewal.kwargs["data"] == first.kwargs["data"]
    assert renewal.kwargs["data"]["subject_token"] == "u@example.test"
    assert renewal.kwargs["data"]["subject_token_type"] == "urn:yandex:params:oauth:token-type:email"
    assert proof.args == ("GET", API + "/")
    assert read.args == ("GET", API + "/resources")
    assert proof.kwargs["headers"] == read.kwargs["headers"] == {"Authorization": "OAuth replacement"}


@pytest.mark.parametrize("lifetimes, expected_times", [
    ([120], [10, 11]), ([1, 120], [10, 12, 13]), ([1, 1], [10]),
])
def test_resource_slot_after_proof_and_fresh_token_after_wait(monkeypatch, lifetimes, expected_times):
    clock = Mock(return_value=10)

    def advance(delay):
        clock.return_value += delay

    monkeypatch.setattr(disk.time, "monotonic", clock)
    dest = client()
    dest.limiter = disk.SplitRateLimiter(1, 1, monotonic=clock, sleep=advance)
    ttls = iter(lifetimes)
    sent = []

    def request(method, url, **kwargs):
        if url == OAUTH:
            ttl = next(ttls)
            return token(str(ttl), ttl)
        sent.append((url, clock(), kwargs["headers"]["Authorization"]))
        if url == API + "/":
            return response({"user": {"uid": 10002}})
        return response({}, 404)

    dest.session.request.side_effect = request
    if lifetimes == [1, 1]:
        with pytest.raises(MigrationError, match="destination_auth_failed"):
            dest.stat("disk:/file")
    else:
        assert dest.stat("disk:/file") is None
        assert sent[-1][0] == API + "/resources"
        assert sent[-1][2] == "OAuth 120"
    assert [t for _, t, _ in sent] == expected_times


@pytest.mark.parametrize("forbidden, stage", [
    ("exchange", "check_destination_access"), ("proof", "check_destination_access"),
    ("resource", None),
])
def test_403_marks_only_service_access_failures(forbidden, stage):
    dest = client()
    replies = []
    if forbidden != "exchange":
        replies.append(token())
    if forbidden == "resource":
        replies.append(response({"user": {"uid": "10002"}}))
    dest.session.request.side_effect = replies + [response({}, 403)]
    with pytest.raises(MigrationError) as error:
        dest.claim_dir("disk:/new")
    assert error.value.code == "destination_auth_failed"
    assert error.value.http_status == 403
    assert getattr(error.value, "_migration_stage", None) == stage


def test_two_expirations_while_waiting_to_write_preserve_access_error(monkeypatch):
    clock = Mock(return_value=10)
    monkeypatch.setattr(disk.time, "monotonic", clock)
    dest = client()

    def wait_for_write(method):
        if method == "PUT":
            clock.return_value += 1

    dest.limiter.acquire.side_effect = wait_for_write
    dest.session.request.side_effect = [token(ttl=1), response({"user": {"uid": 10002}})] * 2
    with pytest.raises(MigrationError, match="destination_auth_failed") as caught:
        dest.claim_dir("disk:/new")
    assert getattr(caught.value, "_migration_stage", None) == "check_destination_access"
    assert all(call.args[0] != "PUT" for call in dest.session.request.call_args_list)
