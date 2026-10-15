"""With retries disabled a file failure is final and does not stop its siblings."""

import errno

import pytest
import requests

from app.disk_copy import CopyError
from tests.fake_session import FakeResponse, FakeSession
from tests.test_disk_copy import (
    DOWNLOAD_HREF_URL, PUBLISH_URL, PUBLIC_DOWNLOAD_URL, RESOURCES_URL,
    SAVE_TO_DISK_URL, TOKEN_URL, VD_RESOURCES_URL, _local_session, _shared_routes,
    embedded, file_item,
)
from tests.test_legacy_journal_engine import journal_copier


def disabled_copier(session, **kwargs):
    copier, journal = journal_copier(session, **kwargs)
    copier.retry_policy.set_enabled(False)
    return copier, journal


def skipped(journal, path):
    return [event for event in journal.events
            if event.get("source") == path and event.get("retry_enabled") is False
            and event.get("status") in {"skipped", "outcome_unknown"}][-1]


@pytest.mark.parametrize("status", [403, 404, 500])
def test_personal_publication_error_is_final_despite_old_public_link(status):
    files = [dict(file_item("a.txt"), public_key="old-a"), dict(file_item("b.txt"), public_key="pk-b")]
    session = FakeSession({
        ("PUT", PUBLISH_URL): [FakeResponse(status, payload={"error": "DiskResourceForbiddenError"}), FakeResponse(200)],
        ("GET", RESOURCES_URL): FakeResponse(200, payload=embedded(files)),
        ("PUT", RESOURCES_URL): FakeResponse(201),
        ("POST", SAVE_TO_DISK_URL): FakeResponse(201),
    })
    copier, journal = disabled_copier(session)
    copier.make_links()
    copier.get_links()
    copier.save_links()
    assert [item["name"] for item in copier.links] == ["b.txt"]
    assert [item["name"] for item in copier.fails] == ["a.txt"]
    assert [call.params["public_key"] for call in session.calls_to(SAVE_TO_DISK_URL)] == ["pk-b"]
    failure = skipped(journal, "disk:/a.txt")
    assert failure["stage"] == "publish"
    assert failure["http_status"] == status
    assert failure["api_error"] == "DiskResourceForbiddenError"
    assert failure["error_side"] == "source"
    assert failure["error"] != "retry_disabled"


def test_publication_failure_skips_only_its_folder_branch():
    files = [dict(file_item("folder"), type="dir", public_key="old-folder"),
             dict(file_item("b.txt"), public_key="pk-b")]
    session = FakeSession({("PUT", PUBLISH_URL): [FakeResponse(403), FakeResponse(200)],
                           ("GET", RESOURCES_URL): FakeResponse(200, payload=embedded(files)),
                           ("PUT", RESOURCES_URL): FakeResponse(201),
                           ("POST", SAVE_TO_DISK_URL): FakeResponse(201)})
    copier, journal = disabled_copier(session)
    copier.make_links()
    copier.get_links()
    copier.save_links()
    assert [item["name"] for item in copier.links] == ["b.txt"]
    assert len(copier.fails) == 1


@pytest.mark.parametrize("stage", ["publish", "metadata", "create_directory"])
def test_shared_file_preparation_failure_does_not_stop_other_file(stage):
    files = [file_item("first/a.txt"), file_item("second/b.txt")]
    routes = _shared_routes(files)
    if stage == "publish":
        routes[("PUT", PUBLISH_URL)] = [FakeResponse(403, payload={"error": "DiskResourceForbiddenError"}), FakeResponse(200)]
        metadata = [FakeResponse(200, payload={"public_key": "pk-b"})]
    else:
        metadata = [FakeResponse(403, payload={"error": "DiskResourceForbiddenError"}),
                    FakeResponse(200, payload={"public_key": "pk-b"})] if stage == "metadata" else [FakeResponse(200, payload={"public_key": "pk-b"})]
    routes[("GET", RESOURCES_URL)] = [FakeResponse(200, payload=embedded(files)), *metadata]
    if stage == "create_directory":
        routes[("PUT", VD_RESOURCES_URL)] = [FakeResponse(201),
            FakeResponse(403, payload={"error": "DiskResourceForbiddenError"}), FakeResponse(201)]
    copier, journal = disabled_copier(FakeSession(routes), dest_type="shared", vd_hash="shared")
    copier.save_to_shared()
    assert [item["name"] for item in copier.links] == ["second/b.txt"]
    assert [item["name"] for item in copier.fails] == ["first/a.txt"]
    failure = skipped(journal, "disk:/first/a.txt")
    assert failure["stage"] == stage
    assert failure["http_status"] == 403
    assert failure["api_error"] == "DiskResourceForbiddenError"
    assert failure["error_side"] == ("destination" if stage == "create_directory" else "source")


def test_personal_404_does_not_try_another_download_method():
    session = FakeSession({("PUT", RESOURCES_URL): FakeResponse(201),
                           ("POST", SAVE_TO_DISK_URL): [FakeResponse(404, payload={"error": "DiskNotFoundError"}), FakeResponse(201)]})
    copier, journal = disabled_copier(session)
    copier.links = [dict(file_item("a.txt"), public_key="pk-a"), dict(file_item("b.txt"), public_key="pk-b")]
    copier.save_links()
    assert [item["name"] for item in copier.links] == ["b.txt"]
    assert not session.calls_to(DOWNLOAD_HREF_URL)
    failure = skipped(journal, "disk:/a.txt")
    assert failure["http_status"] == 404
    assert failure["api_error"] == "DiskNotFoundError"
    assert failure["error_side"] == "destination"


def test_local_download_failure_skip_retains_primary_cause_and_next_file(tmp_path):
    files = [dict(file_item("a.txt"), size=1), dict(file_item("b.txt"), size=2)]
    session = _local_session(embedded(files))
    session.routes[("GET", DOWNLOAD_HREF_URL)] = [FakeResponse(403, payload={"error": "DiskResourceForbiddenError"}),
        FakeResponse(200, payload={"href": "https://downloader/b"})]
    session.routes[("GET", "https://downloader/b")] = FakeResponse(200, text="ok")
    copier, journal = disabled_copier(session)
    copier.cfg.local_root = str(tmp_path)
    copier.save_local()
    assert [item["name"] for item in copier.links] == ["b.txt"]
    failure = skipped(journal, "disk:/a.txt")
    assert failure["http_status"] == 403
    assert failure["api_error"] == "DiskResourceForbiddenError"
    assert failure["error_side"] == "source"
    assert failure["error"] != "retry_disabled"


def test_local_cache_failure_skips_file_and_does_not_abort_sibling(tmp_path, monkeypatch):
    files = [dict(file_item("a.txt"), size=1), dict(file_item("b.txt"), size=2)]
    session = _local_session(embedded(files))
    session.routes[("GET", "https://downloader/file")] = FakeResponse(200, text="ok")
    copier, journal = disabled_copier(session)
    copier.cfg.local_root = str(tmp_path)
    import os
    original = os.replace
    def replace(source, target):
        if str(target).endswith("a.txt"):
            raise OSError(errno.ENOSPC, "no space")
        return original(source, target)
    monkeypatch.setattr("app.disk_copy.os.replace", replace)
    copier.save_local()
    assert [item["name"] for item in copier.links] == ["b.txt"]
    failure = skipped(journal, "disk:/a.txt")
    assert failure["error_side"] == "local"
    assert failure["error"] == "local_no_space"


def test_listing_and_initial_token_failures_remain_explicit_job_errors():
    copier, _journal = disabled_copier(FakeSession({("GET", RESOURCES_URL): FakeResponse(503)}))
    with pytest.raises(CopyError):
        copier.make_links()
    assert copier.fails == []


def test_local_directory_permission_failure_does_not_prevent_sibling_download(tmp_path, monkeypatch):
    from pathlib import Path
    root_items = [{"path": "disk:/bad", "name": "bad", "type": "dir"},
                  dict(file_item("good.txt"), size=5)]
    session = _local_session(embedded(root_items))
    session.routes[("GET", RESOURCES_URL)] = [FakeResponse(200, payload=embedded(root_items)),
        FakeResponse(200, payload=embedded([]))]
    copier, journal = disabled_copier(session)
    copier.cfg.local_root = str(tmp_path)
    original = Path.mkdir
    def mkdir(path, *args, **kwargs):
        if path.name == "bad":
            raise PermissionError(errno.EACCES, "directory denied")
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "mkdir", mkdir)
    copier.save_local()
    assert [item["name"] for item in copier.links] == ["good.txt"]
    failure = skipped(journal, "disk:/bad")
    assert failure["error_side"] == "local"
    assert failure["error"] == "local_permission_denied"
    assert "13" in failure["message"]


@pytest.mark.parametrize("poll_error", ["malformed", "unavailable", "missing_status", "unknown_status"])
def test_malformed_accepted_operation_keeps_unknown_outcome_but_continues_sibling(poll_error):
    from tests.test_disk_copy import OPERATION_URL
    session = FakeSession({("PUT", RESOURCES_URL): FakeResponse(201),
        ("POST", SAVE_TO_DISK_URL): [FakeResponse(202, payload={"href": OPERATION_URL}), FakeResponse(201)],
        ("GET", OPERATION_URL): {"malformed": FakeResponse(200, text="invalid-json"),
                                  "unavailable": FakeResponse(503, payload={"error": "InternalServerError"}),
                                  "missing_status": FakeResponse(200, payload={}),
                                  "unknown_status": FakeResponse(200, payload={"status": "unexpected"})}[poll_error]})
    copier, journal = disabled_copier(session)
    copier.links = [dict(file_item("a.txt"), public_key="pk-a"), dict(file_item("b.txt"), public_key="pk-b")]
    copier.save_links()
    assert [item["name"] for item in copier.links] == ["b.txt"]
    assert len(session.calls_to(SAVE_TO_DISK_URL)) == 2
    failure = skipped(journal, "disk:/a.txt")
    assert failure["status"] == "outcome_unknown"
    assert failure["error"] == ("operation_status_unavailable" if poll_error == "unavailable" else "invalid_operation_response")
    assert failure["error_side"] == "destination"
    assert failure["http_status"] == (503 if poll_error == "unavailable" else 200)
    assert len(session.calls_to(OPERATION_URL)) == 1
    if poll_error == "unavailable":
        assert failure["api_error"] == "InternalServerError"


def test_disabled_status_retry_event_keeps_primary_http_reason():
    from app.retry_policy import RetryPolicy
    from tests.test_throttle import URL, make_session
    session, adapter, _, _ = make_session([(503, {})])
    session.set_retry_policy(RetryPolicy(False))
    events = []
    session.set_observer(events.append)
    assert session.get(URL).status_code == 503
    event = events[-1]
    assert event["retry_enabled"] is False
    assert event["error"] == "http_error"
    assert event["http_status"] == 503


def test_disabled_transport_retry_event_keeps_actual_timeout_reason():
    from app.disk_copy import _build_session
    from app.retry_policy import RetryPolicy
    from urllib3.exceptions import MaxRetryError, ReadTimeoutError
    session = _build_session(retry_policy=RetryPolicy(False))
    events = []
    try:
        retry = session.get_adapter("https://cloud-api.yandex.net").max_retries
        retry.observer = events.append
        with pytest.raises(MaxRetryError):
            retry.increment("GET", "https://cloud-api.yandex.net/file", error=ReadTimeoutError(None, "/file", "timeout"))
        assert events[-1]["error"] == "transport_error"
        assert events[-1]["transport_error"] == "read_timeout"
        assert events[-1]["retry_enabled"] is False
    finally:
        session.close()


def test_unsafe_local_child_directory_is_skipped_without_writing_outside_root(tmp_path):
    files = [{"path": "disk:/bad", "name": "bad", "type": "dir"},
             dict(file_item("good.txt"), size=5)]
    session = _local_session(embedded(files))
    copier, journal = disabled_copier(session)
    cache = tmp_path / "cache"
    source = cache / copier.cfg.source_disk_id
    source.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (source / "bad").symlink_to(outside, target_is_directory=True)
    copier.cfg.local_root = str(cache)
    copier.save_local()
    assert [item["name"] for item in copier.links] == ["good.txt"]
    assert [item["name"] for item in copier.fails] == ["bad"]
    assert (source / "good.txt").read_bytes() == b"hello"
    assert list(outside.iterdir()) == []
    failure = skipped(journal, "disk:/bad")
    assert failure["stage"] == "create_directory"
    assert failure["error_side"] == "local"
    assert failure.get("http_status") is None
    assert failure.get("transport_error") is None


@pytest.mark.parametrize("mode", ["personal", "shared", "local"])
@pytest.mark.parametrize("failure", ["credentials", "transport"])
def test_global_token_refresh_failure_is_not_retried_for_each_file(mode, failure, tmp_path):
    class TokenFailureSession(FakeSession):
        def post(self, url, **kwargs):
            if TOKEN_URL in url and failure == "transport":
                self.token_calls = getattr(self, "token_calls", 0) + 1
                raise requests.ConnectTimeout("oauth temporarily unavailable")
            return super().post(url, **kwargs)
    files = [dict(file_item("a.txt"), size=5), dict(file_item("b.txt"), size=5)]
    if mode == "personal":
        routes = {("PUT", RESOURCES_URL): FakeResponse(201),
                  ("POST", SAVE_TO_DISK_URL): FakeResponse(401)}
    elif mode == "shared":
        routes = _shared_routes(files)
        routes[("GET", RESOURCES_URL)] = [FakeResponse(200, payload=embedded(files)),
                                         FakeResponse(200, payload={"public_key": "pk"})]
        routes[("POST", "/v1/disk/virtual-disks/resources/upload")] = FakeResponse(401)
    else:
        routes = {("GET", DOWNLOAD_HREF_URL): FakeResponse(401),
                  ("GET", RESOURCES_URL): FakeResponse(200, payload=embedded(files))}
    routes[("POST", TOKEN_URL)] = FakeResponse(400, payload={"error": "invalid_client"})
    session = TokenFailureSession(routes)
    copier, journal = disabled_copier(session, dest_type="shared" if mode == "shared" else "personal", vd_hash="shared")
    if mode == "personal":
        copier.links = [dict(item, public_key="pk") for item in files]
        action = copier.save_links
    elif mode == "shared":
        action = copier.save_to_shared
    else:
        copier.cfg.local_root = str(tmp_path)
        action = copier.save_local
    with pytest.raises(CopyError if failure == "credentials" else requests.ConnectTimeout) as caught:
        action()
    assert getattr(caught.value, "code", None) == "token_failed" if failure == "credentials" else caught.value.journal_stage == "get_token"
    assert copier.fails == []
    count = len(session.calls_to(TOKEN_URL)) if failure == "credentials" else session.token_calls
    assert count == 1
