import json
from types import SimpleNamespace

import pytest
from urllib3.exceptions import ConnectTimeoutError, ReadTimeoutError
from urllib3.util.retry import Retry

from app.disk_copy import _ObservedRetry, _build_session


URL = "https://cloud-api.yandex.net/private/path?token=never-persist"


def retries():
    return _ObservedRetry(
        total=None, connect=3, read=2, status=0, other=0,
        status_forcelist=[], allowed_methods=None, backoff_factor=0.5,
    )


@pytest.mark.parametrize("error,category", [
    (ConnectTimeoutError("never-persist"), "connect_timeout"),
    (ReadTimeoutError(None, URL, "never-persist"), "read_timeout"),
])
def test_internal_retry_clones_preserve_observer_and_numbers(monkeypatch, error, category):
    events, sleeps, delegated = [], [], []
    original_sleep = Retry.sleep

    def delegate(self, response=None):
        delegated.append(self)
        original_sleep(self, response)
    monkeypatch.setattr(Retry, "sleep", delegate)
    monkeypatch.setattr("urllib3.util.retry.time.sleep", sleeps.append)
    policy = retries()
    policy.observer = events.append

    first = policy.increment("GET", URL, error=error)
    first.sleep()
    second = first.increment("GET", URL, error=error)
    second.sleep()

    assert first.observer is policy.observer
    assert second.observer is policy.observer
    assert delegated == [first, second]
    assert sleeps == [1.0]
    assert [event["attempt"] for event in events] == [1, 2]
    assert [event["retry_delay_ms"] for event in events] == [0, 1000]
    assert all(event["transport_error"] == category for event in events)
    assert all(event["event_type"] == "retry" and event["status"] == "waiting"
               for event in events)
    assert all(event["error"] == "transport_error" for event in events)
    assert all(event["message"] == "Внутренний повтор HTTP-запроса." for event in events)
    assert not any("max_attempts" in event for event in events)
    assert "never-persist" not in json.dumps(events)
    assert "/private/path" not in json.dumps(events)


def test_internal_retry_after_matches_actual_delay(monkeypatch):
    events, sleeps = [], []
    monkeypatch.setattr("urllib3.util.retry.time.sleep", sleeps.append)
    policy = retries()
    policy.observer = events.append
    policy = policy.increment("GET", URL, error=ConnectTimeoutError("private"))

    policy.sleep(SimpleNamespace(headers={"Retry-After": "7"}))

    assert sleeps == [7.0]
    assert events[0]["retry_delay_ms"] == 7000


def test_missing_or_failing_observer_keeps_urllib3_sleep(monkeypatch):
    sleeps = []
    monkeypatch.setattr("urllib3.util.retry.time.sleep", sleeps.append)
    policy = retries().increment("GET", URL, error=ConnectTimeoutError("private"))
    policy = policy.increment("GET", URL, error=ConnectTimeoutError("private"))
    policy.sleep()

    def fail(_payload):
        raise OSError("journal unavailable")
    policy.observer = fail
    policy.sleep()

    assert sleeps == [1.0, 1.0]


def test_build_session_keeps_existing_retry_settings():
    session = _build_session()
    try:
        policy = session.get_adapter("https://cloud-api.yandex.net").max_retries
        assert isinstance(policy, _ObservedRetry)
        assert policy.total is None
        assert (policy.connect, policy.read, policy.status, policy.other) == (3, 2, 0, 0)
        assert policy.allowed_methods is None
        assert not policy.status_forcelist
        assert policy.backoff_factor == 0.5
        assert policy.observer is None
    finally:
        session.close()
