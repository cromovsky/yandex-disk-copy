import json
from types import SimpleNamespace

import pytest
import requests

from app.throttle import ThrottledSession


URL = "https://cloud-api.yandex.net/private/path?token=never-persist"
FIELDS = {
    "event_type", "status", "http_status", "error", "api_error", "api_reason",
    "transport_error", "request_host", "request_id", "duration_ms", "attempt",
    "max_attempts", "retry_delay_ms",
}


class Limiter:
    def __init__(self):
        self.acquires = []
        self.pauses = []
        self.penalties = []

    def acquire(self, method):
        self.acquires.append(method)

    def pause(self, delay):
        self.pauses.append(delay)

    def penalize(self, delay):
        self.penalties.append(delay)
        return min(delay, 120)


def session(monkeypatch, replies, *, max_attempts=5):
    limiter = Limiter()
    client = ThrottledSession(limiter, max_attempts=max_attempts)
    events, calls = [], []
    client.set_observer(events.append)

    def send(_session, request, **kwargs):
        calls.append(kwargs)
        reply = replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply
    monkeypatch.setattr(requests.Session, "send", send)
    return client, limiter, events, calls


def response(status, payload=None, **headers):
    return SimpleNamespace(
        status_code=status, content=json.dumps(payload or {}).encode(),
        url=URL, headers=headers, close=lambda: None,
    )


@pytest.mark.parametrize("method,http,status", [
    ("GET", 200, "succeeded"), ("PUT", 201, "succeeded"),
    ("POST", 202, "waiting"), ("PUT", 409, "skipped"),
    ("GET", 403, "failed"), ("GET", 500, "failed"),
    ("PUT", 500, "outcome_unknown"), ("POST", 503, "outcome_unknown"),
])
def test_observer_records_safe_http_attempt(monkeypatch, method, http, status):
    client, limiter, events, calls = session(monkeypatch, [response(
        http, {"error": "InternalServerError", "message": "never-persist"},
        **{"X-Request-Id": "1d0a036be47b4e1f9d253fbc4cf82a49"},
    )], max_attempts=1)
    request = requests.Request(method, URL).prepare()

    client.send(request)

    assert len(events) == len(calls) == len(limiter.acquires) == 1
    event = events[0]
    assert set(event) <= FIELDS
    assert event["event_type"] == "http"
    assert event["status"] == status
    assert event["http_status"] == http
    assert event["request_host"] == "cloud-api.yandex.net"
    assert event["request_id"] == "1d0a036be47b4e1f9d253fbc4cf82a49"
    assert event["api_error"] == "InternalServerError"
    assert event["attempt"] == event["max_attempts"] == 1
    assert event["duration_ms"] >= 0
    assert "never-persist" not in json.dumps(events)
    assert calls[0]["timeout"] == (10.0, 120.0)


def test_retry_events_keep_attempts_and_pause_logic(monkeypatch):
    client, limiter, events, calls = session(monkeypatch, [
        response(429, **{"Retry-After": "300"}), response(503), response(200),
    ])
    lines = []
    client.set_log(lines.append)

    client.send(requests.Request("GET", URL).prepare())

    assert len(calls) == len(limiter.acquires) == 3
    assert [(e["event_type"], e["attempt"]) for e in events] == [
        ("http", 1), ("retry", 1), ("http", 2), ("retry", 2), ("http", 3),
    ]
    retries = [e for e in events if e["event_type"] == "retry"]
    assert [e["retry_delay_ms"] for e in retries] == [120000, 2000]
    assert all(e["status"] == "waiting" for e in retries)
    assert limiter.penalties == [300]
    assert limiter.pauses == [2]
    assert lines == []


def test_exhausted_attempt_does_not_claim_another_retry(monkeypatch):
    client, limiter, events, calls = session(monkeypatch, [
        response(503), response(503),
    ], max_attempts=2)

    result = client.send(requests.Request("GET", URL).prepare())

    assert result.status_code == 503
    assert [e["event_type"] for e in events] == ["http", "retry", "http"]
    assert events[-1]["status"] == "failed"
    assert len(calls) == 2
    assert limiter.pauses == [1]


def test_stream_observation_never_reads_body(monkeypatch):
    class Stream:
        status_code = 500
        url = URL
        headers = {"Content-Type": "application/json",
                   "X-Request-Id": "1d0a036be47b4e1f9d253fbc4cf82a49"}

        @property
        def content(self):
            raise AssertionError("stream content read")

        def json(self):
            raise AssertionError("stream JSON read")

        def iter_content(self, **kwargs):
            raise AssertionError("stream body read")
    stream = Stream()
    client, _, events, calls = session(monkeypatch, [stream], max_attempts=1)

    assert client.send(requests.Request("GET", URL).prepare(), stream=True) is stream
    assert events[0]["request_id"] == stream.headers["X-Request-Id"]
    assert events[0]["request_host"] == "cloud-api.yandex.net"
    assert "api_error" not in events[0]
    assert calls[0]["stream"] is True


@pytest.mark.parametrize("method,status", [("GET", "failed"), ("PUT", "outcome_unknown")])
def test_transport_error_is_observed_and_reraised_without_retry(monkeypatch, method, status):
    error = requests.ReadTimeout("never-persist")
    client, limiter, events, calls = session(monkeypatch, [error])

    with pytest.raises(requests.ReadTimeout) as caught:
        client.send(requests.Request(method, URL).prepare())

    assert caught.value is error
    assert len(calls) == len(limiter.acquires) == len(events) == 1
    assert events[0]["status"] == status
    assert events[0]["transport_error"] == "read_timeout"
    assert "http_status" not in events[0]
    assert "never-persist" not in json.dumps(events)
    assert not limiter.pauses and not limiter.penalties


def test_failing_observer_cannot_change_http_result(monkeypatch):
    client, _, _, calls = session(monkeypatch, [response(503), response(200)])

    def fail(_payload):
        raise OSError("journal full")
    client.set_observer(fail)
    assert client.send(requests.Request("GET", URL).prepare()).status_code == 200
    assert len(calls) == 2


def test_plain_retry_log_never_contains_path_query_or_credentials(monkeypatch):
    client, _, _, _ = session(monkeypatch, [response(500), response(200)])
    client.set_observer(None)
    lines = []
    client.set_log(lines.append)
    client.send(requests.Request("GET", URL).prepare())
    assert len(lines) == 1
    assert "cloud-api.yandex.net" in lines[0]
    assert "/private/path" not in lines[0] and "never-persist" not in lines[0]
