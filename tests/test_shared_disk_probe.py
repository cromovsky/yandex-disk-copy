"""Exercise the email-token probe through the real Disk adapter without network access."""

import hashlib
from types import SimpleNamespace

import pytest
import requests

from app.migration.errors import MigrationError
from tests.test_migration_clients import Limiter, Response
from tools import shared_disk_probe as probe


API = "https://cloud-api.office.test/v1/disk"
EMAIL = "operator@office.test"
HASH = "shared_test"
HREF = "https://upload.office.test/signed"
ARGS = (API, EMAIL, HASH, "app", "test-secret", "test-owner-token")
WEB_LINK = f"https://disk.office.test/client/vd/{HASH}/disk"


@pytest.fixture
def server(monkeypatch):
    state = SimpleNamespace(permissions=["read", "write"], owner_reads=0, shared_uid="10001",
                            put_status=201, written=b"", paths=[], put_count=0, polls=0,
                            file_reads=0, conflict=False, bad_md5=False, bad_size=False)

    def request(method, url, **kwargs):
        if url == "https://oauth.office.test/token":
            assert method == "POST"
            assert kwargs["data"]["subject_token"] == EMAIL
            assert kwargs["data"]["subject_token_type"] == "urn:yandex:params:oauth:token-type:email"
            assert kwargs["data"]["access_token"] == ARGS[-1]
            return Response(payload={"access_token": "delegated", "expires_in": 3600})
        if url == HREF:
            assert method == "PUT" and "Authorization" not in kwargs["headers"]
            state.put_count += 1
            state.written = kwargs["data"].read()
            if state.put_status == "lost":
                raise requests.ReadTimeout("response lost")
            return Response(status=state.put_status)
        assert method == "GET" and kwargs["headers"] == {"Authorization": "OAuth delegated"}
        if url == API + "/":
            state.owner_reads += 1
            return Response(payload={"user": {"uid": "10001" if state.owner_reads == 1 else state.shared_uid}})
        if url == API + "/virtual-disks":
            assert kwargs["params"]["vd_hash"] == HASH
            return Response(payload={"vd_hash": HASH, "permissions": state.permissions})
        if url == API + "/operations/probe-upload":
            state.polls += 1
            return Response(payload={"status": "success"})
        path = kwargs["params"]["path"]
        assert path.startswith(f"vd:{HASH}:disk:/")
        if url == API + "/virtual-disks/resources/upload":
            assert kwargs["params"]["overwrite"] == "false"
            state.paths.append(path)
            return Response(payload={"href": HREF, "operation_id": "probe-upload"})
        assert url == API + "/virtual-disks/resources"
        if path == f"vd:{HASH}:disk:/":
            return Response(payload={"type": "dir", "path": path})
        state.file_reads += 1
        state.paths.append(path)
        if not state.written and not state.conflict:
            return Response(status=404)
        md5 = "0" * 32 if state.bad_md5 else hashlib.md5(state.written).hexdigest()
        size = len(state.written) + int(state.bad_size)
        return Response(payload={"type": "file", "path": path, "size": size, "md5": md5})

    monkeypatch.setattr(requests.Session, "request", lambda _session, *a, **kw: request(*a, **kw))
    monkeypatch.setattr("app.migration.destination.get_onprem_limiter", lambda _url: Limiter())
    monkeypatch.setattr(probe, "POLL_INTERVAL", 0)
    return state


@pytest.mark.parametrize("response", [201, 202, "lost"])
def test_one_upload_verified_on_the_selected_shared_disk(server, response):
    server.put_status = response
    probe.run_probe(*ARGS)
    assert server.written and server.put_count == 1
    assert server.file_reads >= 2 and len(set(server.paths)) == 1
    if response == 202:
        assert server.polls >= 1


@pytest.mark.parametrize("failure", ["read_only", "different_owner", "path_taken"])
def test_checks_before_upload(server, failure):
    if failure == "read_only":
        server.permissions = ["read"]
    elif failure == "different_owner":
        server.shared_uid = "10002"
    else:
        server.conflict = True
    with pytest.raises(MigrationError):
        probe.run_probe(*ARGS)
    assert server.put_count == 0


@pytest.mark.parametrize(("failure", "response"), [
    ("bad_md5", 201), ("bad_md5", "lost"), ("bad_size", 201),
])
def test_mismatched_file_is_not_a_success_or_replayed(server, monkeypatch, failure, response):
    setattr(server, failure, True)
    server.put_status = response
    monkeypatch.setattr(probe, "VERIFY_TIMEOUT", 0)
    with pytest.raises(MigrationError):
        probe.run_probe(*ARGS)
    assert server.file_reads >= 2 and server.put_count == 1


@pytest.mark.parametrize(("address", "label"), [
    (WEB_LINK, None), (WEB_LINK, HASH + "/disk"), (API, HASH),
    (API, HASH + "/disk"), (API, WEB_LINK),
])
def test_cli_uploads_to_the_disk_chosen_by_link_or_label(server, monkeypatch, address, label):
    monkeypatch.setenv("OP_DISK_API", address)
    monkeypatch.setenv("OP_SHARED_OPERATOR_EMAIL", EMAIL)
    if label is None:
        monkeypatch.delenv("OP_VD_HASH", raising=False)
    else:
        monkeypatch.setenv("OP_VD_HASH", label)
    for name, value in zip(("DISK_MIGRATION_CLIENT_ID", "DISK_MIGRATION_CLIENT_SECRET", "OWNER_TOKEN"), ARGS[3:]):
        monkeypatch.setenv(name, value)
    monkeypatch.setattr("builtins.input", lambda prompt: pytest.fail("Unexpected prompt: " + prompt))
    assert probe.main() == 0
    assert server.written and server.put_count == 1


@pytest.mark.parametrize(("address", "label"), [
    (WEB_LINK, "other_disk"), (API, WEB_LINK.replace("office.test", "other.test")),
    (WEB_LINK.replace("https://", "http://"), HASH), ("https://disk.office.test/client/vd/", HASH),
    (API, HASH + "/somewhere"), (API, WEB_LINK.replace("https://", "http://")),
])
def test_cli_rejects_ambiguous_targets_before_secrets_or_network(monkeypatch, address, label):
    monkeypatch.setenv("OP_DISK_API", address)
    monkeypatch.setenv("OP_VD_HASH", label)
    monkeypatch.setenv("OP_SHARED_OPERATOR_EMAIL", EMAIL)
    monkeypatch.setenv("DISK_MIGRATION_CLIENT_ID", ARGS[3])
    monkeypatch.delenv("DISK_MIGRATION_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("OWNER_TOKEN", raising=False)
    monkeypatch.setattr(probe.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(probe, "getpass", lambda prompt: pytest.fail("Unexpected secret prompt: " + prompt))
    monkeypatch.setattr(requests.Session, "request", lambda *a, **kw: pytest.fail("Unexpected HTTP request"))
    assert probe.main() == 1
