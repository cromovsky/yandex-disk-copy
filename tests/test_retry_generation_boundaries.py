"""A cancelled retry cannot be revived between journalling and its wait."""

import pytest
from urllib3.exceptions import ConnectTimeoutError, MaxRetryError

from app.disk_copy import CopyError, _build_session
from app.retry_policy import RetryPolicy
from tests.fake_clock import FakeClock
from tests.fake_session import FakeResponse
from tests.test_disk_copy import _local_session, embedded, make_copier
from tests.test_throttle import URL, make_session


def toggle_after_waiting(policy):
    def observer(event):
        if event.get("event_type") == "retry" and event.get("status") == "waiting":
            policy.set_enabled(False)
            policy.set_enabled(True)
    return observer


def test_status_retry_remains_cancelled_after_toggle_inside_waiting_event():
    clock = FakeClock()
    policy = RetryPolicy(monotonic=clock.monotonic, sleep=clock.sleep)
    session, adapter, _, _ = make_session([(503, {}), (200, {})], clock=clock)
    session.set_retry_policy(policy)
    session.set_observer(toggle_after_waiting(policy))
    assert session.get(URL).status_code == 503
    assert adapter.sends == 1


def test_transport_retry_remains_cancelled_after_toggle_inside_waiting_event():
    clock = FakeClock()
    policy = RetryPolicy(monotonic=clock.monotonic, sleep=clock.sleep)
    session = _build_session(retry_policy=policy)
    try:
        retry = session.get_adapter(URL).max_retries
        retry.observer = toggle_after_waiting(policy)
        retry = retry.increment("GET", URL, error=ConnectTimeoutError("failed"))
        with pytest.raises(MaxRetryError):
            retry.sleep()
    finally:
        session.close()


def test_file_retry_remains_cancelled_after_toggle_inside_waiting_event(tmp_path):
    session = _local_session(embedded([]))
    session.routes[("GET", "https://downloader/file")] = [FakeResponse(503), FakeResponse(200, text="hello")]
    copier = make_copier(session)
    class Journal:
        def __call__(self, message):
            pass
        event = staticmethod(toggle_after_waiting(copier.retry_policy))
    copier._log = Journal()
    with pytest.raises(CopyError):
        copier._download_to_path("disk:/a", tmp_path / "a")
    assert len(session.calls_to("https://downloader/file")) == 1
