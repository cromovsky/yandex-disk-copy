"""Legacy journals expose actual stages without changing transfer requests."""

import errno
import json

import pytest
import requests

from app.disk_copy import CopyError
from tests.fake_clock import FakeClock
from tests.fake_session import FakeResponse, FakeSession
from tests.test_disk_copy import (
    DOWNLOAD_HREF_URL, PUBLISH_URL, SAVE_TO_DISK_URL, TOKEN_URL,
    PUBLIC_DOWNLOAD_URL, RESOURCES_URL, OPERATION_URL, embedded, make_copier,
)


class Journal:
    def __init__(self):
        self.events, self.logs, self.secrets = [], [], []

    def __call__(self, message):
        self.logs.append(message)

    def event(self, payload):
        self.events.append(payload)

    def add_secret(self, value):
        self.secrets.append(value)


def journal_copier(session, **kwargs):
    copier = make_copier(session, **kwargs)
    journal = Journal()
    copier._log = journal
    return copier, journal


def test_mutation_http_500_keeps_safe_api_diagnostics_and_unknown_outcome():
    response = FakeResponse(500, payload={
        "error": "InternalServerError", "message": "OAuth raw-secret",
        "description": "https://signed.example/f?token=private",
    }, headers={"X-Request-Id": "0123456789abcdef"})
    session = FakeSession({("POST", SAVE_TO_DISK_URL): response})
    copier, journal = journal_copier(session)
    with pytest.raises(CopyError) as caught:
        copier._disk_save_public_resource("public-private", "file.txt")
    failure = [event for event in journal.events if event.get("http_status") == 500][-1]
    assert failure["status"] == "outcome_unknown"
    assert failure["error_side"] == "destination"
    assert failure["api_error"] == "InternalServerError"
    assert failure["request_id"] == "0123456789abcdef"
    assert caught.value.uncertain
    assert "raw-secret" not in str(caught.value)
    assert "token=private" not in json.dumps(journal.events)
    assert len(session.calls) == 1


def test_ignored_publish_failure_is_never_reported_as_stage_success():
    session = FakeSession({("PUT", PUBLISH_URL): FakeResponse(503)})
    copier, journal = journal_copier(session)
    copier._disk_publish_resource("source-token", "disk:/file")
    terminal = [event for event in journal.events if event.get("stage") == "publish"
                and event.get("status") != "started"]
    assert terminal and all(event["status"] != "succeeded" for event in terminal)
    assert terminal[-1]["status"] == "outcome_unknown"
    assert len(session.calls) == 1


def test_local_stream_retry_identifies_source_and_preserves_attempts(tmp_path, monkeypatch):
    class Broken(FakeResponse):
        def iter_content(self, chunk_size):
            yield b"partial"
            raise requests.ReadTimeout("https://download.example/f?token=private")
    session = FakeSession({
        ("GET", DOWNLOAD_HREF_URL): FakeResponse(200, payload={
            "href": "https://download.example/f?token=private",
        }),
        ("GET", "download.example"): [Broken(), FakeResponse(200, text="hello")],
    })
    clock = FakeClock()
    copier, journal = journal_copier(session, clock=clock)
    monkeypatch.setattr("app.disk_copy.random.uniform", lambda a, b: 0)
    copier._download_to_path("disk:/file", tmp_path / "file")
    failures = [event for event in journal.events if event.get("transport_error")]
    assert failures[-1]["transport_error"] == "read_timeout"
    assert failures[-1]["error_side"] == "source"
    assert not any(event.get("error") == "local_io_failed" for event in failures)
    retries = [event for event in journal.events if event.get("event_type") == "retry"]
    assert retries[-1]["attempt"] == 1
    assert retries[-1]["max_attempts"] == 6
    assert retries[-1]["retry_delay_ms"] == 2000
    assert "token=private" not in json.dumps(journal.events)
    assert clock.sleeps == [2]
    assert len(session.calls) == 4


def test_local_existing_size_is_skipped_not_copied(tmp_path):
    session = FakeSession({("GET", RESOURCES_URL): FakeResponse(200, payload=embedded([
        {"path": "disk:/file", "type": "file", "size": 4},
    ]))})
    copier, journal = journal_copier(session)
    copier.cfg.local_root = str(tmp_path)
    folder = tmp_path / copier.cfg.source_disk_id
    folder.mkdir()
    (folder / "file").write_bytes(b"same")
    copier.save_local()
    assert copier.links == []
    assert copier.skipped[0]["path"] == "disk:/file"
    file_event = [event for event in journal.events if event.get("stage") == "file"][-1]
    assert file_event["status"] == "skipped"
    assert "контроль" not in file_event.get("message", "").lower()
    assert len(session.calls) == 1


def test_poll_timeout_is_unknown_and_never_logs_operation_url():
    session = FakeSession({("GET", "/operations/"): FakeResponse(
        200, payload={"status": "in-progress"},
    )})
    copier, journal = journal_copier(session)
    with pytest.raises(CopyError) as caught:
        copier._wait_operation(OPERATION_URL + "?token=private", "token", timeout_sec=1)
    assert caught.value.uncertain
    terminal = [event for event in journal.events if event.get("stage") == "poll_operation"][-1]
    assert terminal["status"] == "outcome_unknown"
    assert terminal["operation_id"] == "op-1"
    assert "private" not in json.dumps(journal.events + journal.logs)
    assert "operations/" not in str(caught.value)


def test_token_is_registered_without_raw_payload():
    session = FakeSession({("POST", TOKEN_URL): FakeResponse(200, payload={
        "access_token": "fresh-private-token", "expires_in": 3600,
        "scope": "cloud_api:disk.read Bearer scope-private",
    })})
    copier, journal = journal_copier(session)
    result = copier._get_token(copier.cfg.source, copier.cfg.source_disk_id)
    assert result.token == "fresh-private-token"
    assert "fresh-private-token" in journal.secrets
    assert "scope-private" not in json.dumps(journal.events + journal.logs)


def test_stream_http_error_never_reads_body():
    class StreamError(FakeResponse):
        @property
        def text(self):
            raise AssertionError("streamed errors are not buffered for diagnostics")

        @text.setter
        def text(self, value):
            pass

        @property
        def content(self):
            raise AssertionError("streamed errors are not buffered for diagnostics")

        @content.setter
        def content(self, value):
            pass

        def json(self):
            raise AssertionError("streamed errors are not JSON-inspected")
    error = object.__new__(StreamError)
    error.status_code, error.headers, error.closed = 500, {}, False
    session = FakeSession({
        ("GET", DOWNLOAD_HREF_URL): FakeResponse(200, payload={
            "href": "https://download.example/private",
        }),
        ("GET", "/resources/upload"): FakeResponse(200, payload={
            "href": "https://upload.example/private",
        }),
        ("GET", "download.example"): error,
    })
    copier, journal = journal_copier(session)
    with pytest.raises(CopyError, match="Не удалось скачать"):
        copier._copy_file_bytes("disk:/file", "file")
    assert any(event.get("http_status") == 500 for event in journal.events)


def test_local_write_failure_is_local_and_does_not_download_again(tmp_path, monkeypatch):
    session = FakeSession({
        ("GET", DOWNLOAD_HREF_URL): FakeResponse(200, payload={
            "href": "https://download.example/file?token=private",
        }),
        ("GET", "download.example"): FakeResponse(200, text="hello"),
    })
    copier, journal = journal_copier(session)
    monkeypatch.setattr("app.disk_copy.os.replace", lambda *args: (_ for _ in ()).throw(
        OSError(errno.ENOSPC, "no space: https://private.test?token=private"),
    ))
    with pytest.raises(OSError):
        copier._download_to_path("disk:/file", tmp_path / "file")
    terminal = [event for event in journal.events if event.get("stage") == "local_write"][-1]
    assert terminal["status"] == "failed"
    assert terminal["error_side"] == "local"
    assert terminal["error"] == "local_no_space"
    assert "недостаточно места" in terminal["message"]
    assert len(session.calls) == 2
    assert "token=private" not in json.dumps(journal.events)
    assert not list(tmp_path.glob(".disk-copy-*.part"))


def test_failed_async_operation_retains_api_code_and_http_200():
    session = FakeSession({("GET", "/operations/"): FakeResponse(200, payload={
        "status": "failed", "error": "DiskDownloadLimitExceededError",
        "message": "Bearer raw-private-token",
    })})
    copier, journal = journal_copier(session)
    with pytest.raises(CopyError) as caught:
        copier._wait_operation(OPERATION_URL, "token")
    assert not caught.value.uncertain
    assert caught.value.diagnostics["api_error"] == "DiskDownloadLimitExceededError"
    terminal = [event for event in journal.events if event.get("stage") == "poll_operation"][-1]
    assert terminal["status"] == "failed"
    assert terminal["http_status"] == 200
    assert terminal["error_side"] == "destination"
    assert "raw-private-token" not in json.dumps(journal.events)


def test_direct_upload_202_is_unconfirmed_without_new_poll_requests():
    session = FakeSession({
        ("PUT", RESOURCES_URL): FakeResponse(201),
        ("POST", SAVE_TO_DISK_URL): FakeResponse(404),
        ("GET", DOWNLOAD_HREF_URL): FakeResponse(200, payload={
            "href": "https://download.example/file",
        }),
        ("GET", "/resources/upload"): FakeResponse(200, payload={
            "href": "https://upload.example/file",
        }),
        ("GET", "download.example"): FakeResponse(200, text="hello"),
        ("PUT", "upload.example"): FakeResponse(202),
    })
    copier, journal = journal_copier(session)
    copier.links = [{"path": "disk:/file", "name": "file", "type": "file", "public_key": "key"}]
    copier.save_links()
    assert copier.links == copier.fails == copier.skipped == []
    assert len(copier.unconfirmed) == 1
    terminal = [event for event in journal.events if event.get("stage") == "file"][-1]
    assert terminal["status"] == "waiting"
    assert len(session.calls) == 6
    assert not session.calls_to("/operations/")


def test_server_returned_token_cannot_hide_in_safe_diagnostic_identifier():
    session = FakeSession({("POST", SAVE_TO_DISK_URL): FakeResponse(500, payload={
        "error": "PRIVATEACCESSVALUE",
    }, headers={"X-Request-Id": "0123456789abcdef"})})
    copier, journal = journal_copier(session)
    copier._secret("PRIVATEACCESSVALUE")
    copier._secret("0123456789abcdef")
    with pytest.raises(CopyError):
        copier._disk_save_public_resource("public-private", "file")
    assert "PRIVATEACCESSVALUE" not in json.dumps(journal.events)
    assert "0123456789abcdef" not in json.dumps(journal.events)


@pytest.mark.parametrize("kind,size", [({}, []), (["file"], True), (7, -1)])
def test_malformed_resource_fields_do_not_break_journaling(kind, size):
    copier, journal = journal_copier(FakeSession({}))
    copier._event({"event_type": "stage", "stage": "file", "kind": kind, "size": size})
    assert len(journal.events) == 1
    assert "kind" not in journal.events[0]
    assert "size" not in journal.events[0]


def test_download_links_do_not_accumulate_exact_secrets_but_remain_masked():
    copier, journal = journal_copier(FakeSession({}))
    original_secrets = set(copier._secrets)
    for index in range(1000):
        copier._secret(f"https://download.example/file-{index}?token=private-{index}", link=True)
    copier._secret("HTTPS://download.example/UPPER?token=PRIVATE", link=True)
    copier._secret("opaque-private-public-key", link=True)
    # Auth values remain exact secrets, including unusual URL-shaped values.
    copier._secret("https://auth.example/credential")
    assert copier._secrets == original_secrets | {
        "opaque-private-public-key", "https://auth.example/credential",
    }
    assert journal.secrets == ["opaque-private-public-key", "https://auth.example/credential"]
    copier.log("https://download.example/file-999?token=private-999 "
               "HTTPS://download.example/UPPER?token=PRIVATE opaque-private-public-key")
    assert "private" not in journal.logs[-1]
    assert "PRIVATE" not in journal.logs[-1]
    assert "download.example" not in journal.logs[-1]


def test_local_download_observer_uses_whole_file_attempts(tmp_path, monkeypatch):
    class ObservedDownload(FakeSession):
        def set_observer(self, observer):
            self.observer = observer

        def get(self, url, **kwargs):
            response = super().get(url, **kwargs)
            self.observer({"event_type": "http", "http_status": response.status_code,
                           "status": "waiting", "attempt": 1, "max_attempts": 1})
            return response
    session = FakeSession({("GET", DOWNLOAD_HREF_URL): FakeResponse(200, payload={
        "href": "https://download.example/file?token=private",
    })})
    download = ObservedDownload({("GET", "download.example"): [
        FakeResponse(500), FakeResponse(503), FakeResponse(200, text="hello"),
    ]})
    clock = FakeClock()
    copier, journal = journal_copier(session, clock=clock)
    copier._download_session = download
    monkeypatch.setattr("app.disk_copy.random.uniform", lambda a, b: 0)
    copier._download_to_path("disk:/file", tmp_path / "file")
    http = [event for event in journal.events if event.get("stage") == "download"
            and event.get("event_type") == "http"]
    assert [event["attempt"] for event in http] == [1, 2, 3]
    assert all(event["max_attempts"] == 6 for event in http)
    completed = [event for event in journal.events if event.get("stage") == "download"
                 and event.get("status") == "succeeded"][-1]
    assert completed["attempt"] == 3 and completed["max_attempts"] == 6
    assert clock.sleeps == [2, 4]
    assert len(session.calls) == len(download.calls) == 3


@pytest.mark.parametrize("payload", ["malformed", [], None])
def test_public_download_invalid_json_remains_source_error(payload):
    class Invalid(FakeResponse):
        def json(self):
            if payload == "malformed":
                raise requests.exceptions.JSONDecodeError("invalid", "private", 0)
            return payload
    response = Invalid(200, text="untrusted private server response")
    session = FakeSession({("GET", PUBLIC_DOWNLOAD_URL): response})
    copier, journal = journal_copier(session)
    expected = requests.exceptions.JSONDecodeError if payload == "malformed" else AttributeError
    with pytest.raises(expected) as caught:
        with copier._stage("file", "destination", source="disk:/file", destination="vd:/file"):
            copier._public_download_href("opaque-private-key")
    assert caught.value.http_status == 200
    assert caught.value.code == "invalid_download_response"
    terminal = [event for event in journal.events if event.get("status") == "failed"]
    assert {event["stage"] for event in terminal} == {"get_download_url", "file"}
    assert all(event["error_side"] == "source" and event["http_status"] == 200
               for event in terminal)
    assert not any(event.get("transport_error") for event in terminal)
    assert "private" not in json.dumps(journal.events)
    assert len(session.calls) == 1
