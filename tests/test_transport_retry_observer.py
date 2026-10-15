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



def test_transport_retries_obey_disabled_policy_even_for_existing_clone():
    from app.retry_policy import RetryPolicy
    from urllib3.exceptions import MaxRetryError
    switch = RetryPolicy()
    session = _build_session(retry_policy=switch)
    try:
        retry = session.get_adapter(URL).max_retries
        clone = retry.increment("GET", URL, error=ConnectTimeoutError("connection failed"))
        switch.set_enabled(False)
        with pytest.raises(MaxRetryError):
            clone.increment("GET", URL, error=ConnectTimeoutError("connection failed"))
    finally:
        session.close()


def test_transport_sleep_is_cancelled_when_retries_are_disabled():
    import threading
    from app.retry_policy import RetryPolicy
    from urllib3.exceptions import MaxRetryError
    switch = RetryPolicy()
    session = _build_session(retry_policy=switch)
    try:
        retry = session.get_adapter(URL).max_retries
        retry = retry.increment("GET", URL, error=ConnectTimeoutError("failed"))
        retry = retry.increment("GET", URL, error=ConnectTimeoutError("failed"))
        waiting = threading.Event()
        retry.observer = lambda event: waiting.set()
        outcomes = []
        def run():
            try:
                retry.sleep(SimpleNamespace(headers={"Retry-After": "30"}))
                outcomes.append("resent")
            except MaxRetryError:
                outcomes.append("cancelled")
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        assert waiting.wait(1)
        switch.set_enabled(False)
        thread.join(1)
        assert outcomes == ["cancelled"]
    finally:
        session.close()


@pytest.mark.parametrize("enabled,expected", [(False, 1), (True, 3)])
def test_real_requests_adapter_stops_hidden_transport_retries(enabled, expected):
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from app.retry_policy import RetryPolicy
    from app.throttle import RateLimiter
    import requests
    class Disconnect(BaseHTTPRequestHandler):
        attempts = 0
        def do_GET(self):
            type(self).attempts += 1
            self.close_connection = True
        def log_message(self, *_args):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Disconnect)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    from tests.fake_clock import FakeClock
    clock = FakeClock()
    switch = RetryPolicy(enabled, monotonic=clock.monotonic, sleep=clock.sleep)
    session = _build_session(retry_policy=switch)
    adapter = session.get_adapter("https://")
    session.mount("http://", adapter)
    # The server is local; remove unrelated process RPS sleeps from this regression.
    session._limiter = RateLimiter(1_000_000)
    adapter.max_retries.limiter = session._limiter
    try:
        with pytest.raises(requests.ConnectionError):
            session.get("http://127.0.0.1:%s/file" % server.server_port, timeout=1)
        assert Disconnect.attempts == expected
    finally:
        session.close()
        server.shutdown()
        server.server_close()
        thread.join(1)
