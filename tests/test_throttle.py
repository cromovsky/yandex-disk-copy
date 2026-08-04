"""Тесты троттлинга: расписание пейсера, барьер 429, разбор Retry-After, сессия.

Расписание проверяется однопоточно — для лимитера N вызовов acquire() подряд
неотличимы от N потоков, а фейковые часы плюс реальные потоки дают флаки.
Корректность локов покрыта одним честным многопоточным тестом на реальных часах.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone

import pytest
import requests
from requests.adapters import HTTPAdapter

from app.throttle import (
    MAX_COOLDOWN,
    RateLimiter,
    ThrottledSession,
    backoff_delay,
    get_limiter,
    retry_after_seconds,
    set_limiter,
)

from .fake_clock import FakeClock


def make_limiter(rps: float = 10.0, clock: FakeClock | None = None):
    clock = clock or FakeClock()
    limiter = RateLimiter(rps, monotonic=clock.monotonic, sleep=clock.sleep)
    return limiter, clock


# ── расписание пейсера ──────────────────────────────────────────────────
def test_first_acquire_does_not_sleep():
    limiter, clock = make_limiter()

    limiter.acquire()

    assert clock.sleeps == []


def test_second_acquire_waits_exactly_one_interval():
    limiter, clock = make_limiter(rps=10.0)

    limiter.acquire()
    limiter.acquire()

    assert clock.sleeps == [pytest.approx(0.1)]


def test_ten_acquires_are_evenly_paced():
    limiter, clock = make_limiter(rps=20.0)

    for _ in range(10):
        limiter.acquire()

    assert clock.sleeps == [pytest.approx(0.05)] * 9


def test_no_wait_after_long_idle():
    limiter, clock = make_limiter(rps=10.0)
    limiter.acquire()

    clock.advance(10.0)
    limiter.acquire()

    assert clock.sleeps == []


def test_idle_does_not_accumulate_burst():
    """Регресс против token-bucket-семантики: простой не даёт права на залп."""
    limiter, clock = make_limiter(rps=10.0)
    limiter.acquire()
    clock.advance(10.0)

    limiter.acquire()  # догнал реальное время, без сна
    limiter.acquire()  # а вот второй подряд всё равно ждёт интервал

    assert clock.sleeps == [pytest.approx(0.1)]


# ── барьер 429 ──────────────────────────────────────────────────────────
def test_penalize_blocks_next_acquire_for_retry_after():
    limiter, clock = make_limiter(rps=10.0)

    assert limiter.penalize(7.0) == pytest.approx(7.0)
    limiter.acquire()

    assert clock.sleeps == [pytest.approx(7.0)]
    assert limiter.cooldowns == 1


def test_penalize_keeps_longest_cooldown():
    limiter, clock = make_limiter(rps=10.0)

    limiter.penalize(30.0)
    limiter.penalize(2.0)  # короче — барьер не должен сдвинуться назад
    limiter.acquire()

    assert clock.sleeps == [pytest.approx(30.0)]


def test_penalize_clamps_to_max_cooldown():
    limiter, clock = make_limiter(rps=10.0)

    actual = limiter.penalize(3600.0)

    assert actual == pytest.approx(MAX_COOLDOWN)
    limiter.acquire()
    assert clock.sleeps == [pytest.approx(MAX_COOLDOWN)]


def test_penalize_zero_and_negative_are_safe():
    limiter, clock = make_limiter(rps=10.0)

    assert limiter.penalize(0.0) == 0.0
    assert limiter.penalize(-5.0) == 0.0
    limiter.acquire()

    assert clock.sleeps == []


def test_threads_spread_by_interval_after_cooldown():
    """После барьера потоки расходятся по слотам, а не стреляют залпом."""
    limiter, clock = make_limiter(rps=10.0)
    limiter.penalize(10.0)

    limiter.acquire()
    limiter.acquire()
    limiter.acquire()

    assert clock.sleeps == [
        pytest.approx(10.0),
        pytest.approx(0.1),
        pytest.approx(0.1),
    ]


def test_acquire_requeues_when_barrier_moves_during_sleep():
    """Слот аннулируется, если другой поток отодвинул барьер за него."""
    clock = FakeClock()
    holder: dict = {}
    calls: list[float] = []

    def sleeping(seconds: float) -> None:
        calls.append(seconds)
        clock.sleep(seconds)
        if len(calls) == 1:
            # имитируем 429 в соседнем потоке, пока мы спали
            holder["limiter"].penalize(5.0)

    limiter = RateLimiter(10.0, monotonic=clock.monotonic, sleep=sleeping)
    holder["limiter"] = limiter

    limiter.acquire()  # занял первый слот, без сна
    limiter.acquire()  # ждёт интервал, барьер уезжает → второй круг

    assert len(calls) == 2
    assert calls[1] == pytest.approx(5.0)


def test_rejects_nonpositive_rps():
    for bad in (0, -1):
        with pytest.raises(ValueError, match="положительным"):
            RateLimiter(bad)


def test_rps_property_roundtrips():
    limiter, _ = make_limiter(rps=25.0)

    assert limiter.rps == pytest.approx(25.0)


# ── синглтон ────────────────────────────────────────────────────────────
def test_get_limiter_returns_same_instance():
    assert get_limiter() is get_limiter()


def test_set_limiter_replaces_singleton():
    custom, _ = make_limiter()

    set_limiter(custom)

    assert get_limiter() is custom


# ── многопоточность: единственный тест на реальных часах ────────────────
def test_concurrent_acquires_are_serialized():
    limiter = RateLimiter(200.0)  # интервал 5 мс — 20 потоков уложатся в ~0.1 с
    seen: list[float] = []
    lock = threading.Lock()

    def worker() -> None:
        limiter.acquire()
        with lock:
            seen.append(time.monotonic())

    started = time.monotonic()
    threads = [threading.Thread(target=worker) for _ in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = time.monotonic() - started

    assert len(seen) == 20
    # 20 слотов по 5 мс: последний не раньше 19 интервалов от старта
    assert elapsed >= 19 * (1 / 200.0)


# ── разбор Retry-After ──────────────────────────────────────────────────
def test_parses_delta_seconds():
    assert retry_after_seconds("120") == pytest.approx(120.0)


def test_parses_delta_seconds_with_whitespace():
    assert retry_after_seconds("  5 ") == pytest.approx(5.0)


def test_negative_delta_seconds_clamped_to_zero():
    assert retry_after_seconds("-10") == 0.0


def test_parses_http_date_in_future():
    now = datetime(2026, 10, 21, 7, 28, 0, tzinfo=timezone.utc)
    later = now + timedelta(seconds=90)
    header = later.strftime("%a, %d %b %Y %H:%M:%S GMT")

    assert retry_after_seconds(header, now=now) == pytest.approx(90.0, abs=1)


def test_http_date_in_past_gives_zero():
    now = datetime(2026, 10, 21, 7, 28, 0, tzinfo=timezone.utc)
    header = (now - timedelta(hours=1)).strftime("%a, %d %b %Y %H:%M:%S GMT")

    assert retry_after_seconds(header, now=now) == 0.0


def test_naive_http_date_treated_as_utc():
    """«-0000» по RFC 5322 = зона неизвестна; не должно падать и не должно врать."""
    now = datetime(2026, 10, 21, 7, 28, 0, tzinfo=timezone.utc)
    header = "Wed, 21 Oct 2026 07:29:00 -0000"

    assert retry_after_seconds(header, now=now) == pytest.approx(60.0, abs=1)


def test_garbage_returns_none():
    for bad in ("скоро", "not-a-date", "12 попугаев"):
        assert retry_after_seconds(bad) is None


def test_none_and_empty_return_none():
    assert retry_after_seconds(None) is None
    assert retry_after_seconds("") is None
    assert retry_after_seconds("   ") is None


def test_backoff_grows_and_caps():
    assert backoff_delay(1) == pytest.approx(1.0)
    assert backoff_delay(2) == pytest.approx(2.0)
    assert backoff_delay(4) == pytest.approx(8.0)
    assert backoff_delay(99) == pytest.approx(32.0)


# ── ThrottledSession ────────────────────────────────────────────────────
class RecordingAdapter(HTTPAdapter):
    """Отдаёт заготовленные ответы, ничего не отправляя в сеть."""

    def __init__(self, responses: list[tuple[int, dict]]):
        super().__init__()
        self.queue = list(responses)
        self.timeouts: list[object] = []
        self.sends = 0

    def send(self, request, **kwargs):
        self.sends += 1
        self.timeouts.append(kwargs.get("timeout"))
        status, headers = (
            self.queue.pop(0) if len(self.queue) > 1 else self.queue[0]
        )
        response = requests.Response()
        response.status_code = status
        response.headers.update(headers)
        response.url = request.url
        response.request = request
        response.raw = None
        response._content = b"{}"
        return response


def make_session(responses, *, clock=None, rps=1000.0):
    clock = clock or FakeClock()
    limiter = RateLimiter(rps, monotonic=clock.monotonic, sleep=clock.sleep)
    session = ThrottledSession(limiter)
    adapter = RecordingAdapter(responses)
    session.mount("https://", adapter)
    return session, adapter, limiter, clock


URL = "https://cloud-api.yandex.net/v1/disk/resources"


def test_default_timeout_injected_into_every_send():
    session, adapter, _, _ = make_session([(200, {})])

    session.get(URL)

    assert adapter.timeouts == [(10.0, 120.0)]


def test_explicit_timeout_is_not_overridden():
    session, adapter, _, _ = make_session([(200, {})])

    session.get(URL, timeout=3.0)

    assert adapter.timeouts == [3.0]


def test_429_penalizes_limiter_with_retry_after():
    session, adapter, limiter, clock = make_session(
        [(429, {"Retry-After": "7"}), (200, {})]
    )

    response = session.get(URL)

    assert response.status_code == 200
    assert adapter.sends == 2
    assert limiter.cooldowns == 1
    assert pytest.approx(7.0) in clock.sleeps


def test_429_without_retry_after_uses_backoff():
    session, adapter, limiter, clock = make_session([(429, {}), (200, {})])

    response = session.get(URL)

    assert response.status_code == 200
    assert limiter.cooldowns == 1
    assert pytest.approx(backoff_delay(1)) in clock.sleeps


def test_429_gives_up_after_max_attempts_and_returns_429():
    session, adapter, limiter, _ = make_session([(429, {"Retry-After": "1"})])

    response = session.get(URL)

    assert response.status_code == 429
    assert adapter.sends == 5
    assert limiter.cooldowns == 5


def test_503_retries_without_global_cooldown():
    session, adapter, limiter, _ = make_session([(503, {}), (200, {})])

    response = session.get(URL)

    assert response.status_code == 200
    assert adapter.sends == 2
    assert limiter.cooldowns == 0, "5xx не должен тормозить остальные потоки"


def test_409_is_not_retried():
    """409 — штатный поток управления «уже существует», ретрай сломал бы логику."""
    session, adapter, _, _ = make_session([(409, {})])

    response = session.get(URL)

    assert response.status_code == 409
    assert adapter.sends == 1


def test_507_and_403_are_not_retried():
    for status in (507, 403, 401, 404):
        session, adapter, _, _ = make_session([(status, {})])

        response = session.get(URL)

        assert response.status_code == status
        assert adapter.sends == 1


def test_limiter_acquired_once_per_attempt():
    session, adapter, limiter, _ = make_session(
        [(503, {}), (503, {}), (200, {})]
    )

    session.get(URL)

    assert adapter.sends == 3
    assert limiter.total_wait >= 0  # гейт участвовал, а не был обойдён


def test_log_receives_429_line():
    lines: list[str] = []
    session, _, _, _ = make_session([(429, {"Retry-After": "2"}), (200, {})])
    session.set_log(lines.append)

    session.get(URL)

    assert any("429" in line and "общая пауза" in line for line in lines)


def test_log_marks_clamped_retry_after():
    lines: list[str] = []
    session, _, _, _ = make_session([(429, {"Retry-After": "3600"}), (200, {})])
    session.set_log(lines.append)

    session.get(URL)

    assert any("урезан" in line for line in lines)
