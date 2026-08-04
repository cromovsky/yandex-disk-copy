"""Процесс-глобальный троттлинг исходящих HTTP-запросов.

Зачем: Условия использования API Диска (п. 2.2) декларируют максимум
40 запросов/с на сервис, причём при превышении «предоставление Сервиса может быть
ограничено или прекращено». Заголовков X-RateLimit-* API Диска не отдаёт, поэтому
упреждающий контур может быть только разомкнутым: темп задаём сами (RateLimiter),
а по факту 429 тормозим все потоки сразу (общий барьер + Retry-After).

Лимитер один на процесс: в пакетном режиме работает до BATCH_CONCURRENCY копиров,
у каждого своя Session, но client_id один — значит и бюджет общий.
"""

from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Callable, Optional

import requests


def _env_float(name: str, default: float) -> float:
    """Положительное число из окружения; мусор не должен ронять импорт приложения."""
    raw = (os.environ.get(name) or "").strip()
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


# Документированный лимит API Диска — 40 запросов/с на сервис. Берём половину:
# окно учёта на стороне сервиса неизвестно, и тем же client_id могут пользоваться
# другие интеграции организации. Видите 429 в логе — снижайте DISK_API_RPS.
TARGET_RPS = _env_float("DISK_API_RPS", 20.0)

# (connect, read). Без таймаута висящий запрос держит поток переноса вечно.
CONNECT_TIMEOUT = 10.0
DEFAULT_TIMEOUT = (CONNECT_TIMEOUT, _env_float("HTTP_TIMEOUT_SEC", 120.0))

MAX_ATTEMPTS = 5
BACKOFF_BASE = 1.0
BACKOFF_MAX = 32.0
MAX_COOLDOWN = 120.0

# 409 — штатный поток управления («уже существует») в четырёх местах disk_copy.py,
# 507 — исчерпана квота. Их ретраить бессмысленно и вредно.
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


def retry_after_seconds(
    value: Optional[str],
    *,
    now: Optional[datetime] = None,
) -> Optional[float]:
    """Retry-After → секунды. Понимает оба формата RFC 7231.

    delta-seconds («120») и HTTP-date («Wed, 21 Oct 2026 07:28:00 GMT»).
    None означает «заголовка нет или он не разобрался» — вызывающий берёт backoff.

    Свой парсер, а не Retry.parse_retry_after: тот прибит к time.time() (фейковые
    часы не подставить) и бросает InvalidHeader вместо None.
    """
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        # «-0000» по RFC 5322 означает «зона неизвестна» — трактуем как UTC
        when = when.replace(tzinfo=timezone.utc)
    reference = now or datetime.now(timezone.utc)
    return max(0.0, (when - reference).total_seconds())


def backoff_delay(attempt: int) -> float:
    """Экспоненциальный откат, когда сервер не подсказал Retry-After."""
    return min(BACKOFF_BASE * 2 ** max(0, attempt - 1), BACKOFF_MAX)


class RateLimiter:
    """Гейт «не чаще одного запроса в interval», общий для всех потоков.

    Алгоритм — минимальный интервал, а не token bucket: окно учёта на стороне
    Яндекса неизвестно, а всплеск запросов — ровно тот риск, от которого уходим.

    Слот резервируется под локом, сон — вне лока: N потоков расходятся по разным
    слотам и не будят друг друга на один и тот же момент времени.
    """

    def __init__(
        self,
        rps: float,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        max_cooldown: float = MAX_COOLDOWN,
    ):
        if rps <= 0:
            raise ValueError("rps должен быть положительным")
        self._interval = 1.0 / rps
        self._monotonic = monotonic
        self._sleep = sleep
        self._max_cooldown = max_cooldown
        self._lock = threading.Lock()
        self._next_slot = 0.0  # моно-время следующего свободного слота
        self._blocked_until = 0.0  # общий барьер после 429
        self.cooldowns = 0  # для лога и тестов
        self.total_wait = 0.0

    @property
    def rps(self) -> float:
        return 1.0 / self._interval

    def acquire(self) -> None:
        """Блокирует поток до момента, когда очередной запрос разрешён."""
        while True:
            with self._lock:
                # барьер 429 учитывается здесь же: слоты после cooldown
                # раскладываются по интервалу, а не выстреливают залпом
                slot = max(self._monotonic(), self._next_slot, self._blocked_until)
                self._next_slot = slot + self._interval
            wait = slot - self._monotonic()
            if wait > 0:
                self._sleep(wait)
                with self._lock:
                    self.total_wait += wait
            with self._lock:
                if self._blocked_until <= slot:
                    return
            # Пока спали, другой поток поймал 429 и отодвинул барьер за наш слот —
            # слот аннулирован, встаём в очередь заново.

    def penalize(self, delay: float) -> float:
        """Общий cooldown: до now + delay запросы не шлёт ни один поток.

        Возвращает фактическую паузу — она может быть урезана max_cooldown.
        """
        capped = max(0.0, min(float(delay), self._max_cooldown))
        with self._lock:
            self._blocked_until = max(
                self._blocked_until, self._monotonic() + capped
            )
            # разносим ожидающие потоки по слотам от барьера
            self._next_slot = max(self._next_slot, self._blocked_until)
            self.cooldowns += 1
        return capped

    def pause(self, delay: float) -> None:
        """Пауза одного потока (откат на 5xx) — общий барьер не двигает."""
        if delay > 0:
            self._sleep(delay)


_LIMITER: Optional[RateLimiter] = None
_LIMITER_LOCK = threading.Lock()


def get_limiter() -> RateLimiter:
    """Один лимитер на процесс — все копиры делят общий бюджет API."""
    global _LIMITER
    with _LIMITER_LOCK:
        if _LIMITER is None:
            _LIMITER = RateLimiter(TARGET_RPS)
        return _LIMITER


def set_limiter(limiter: Optional[RateLimiter]) -> None:
    """Подмена или сброс синглтона. Нужна тестам (фейковые часы) и только им."""
    global _LIMITER
    with _LIMITER_LOCK:
        _LIMITER = limiter


def _short(url: str, limit: int = 80) -> str:
    return url[:limit]


class ThrottledSession(requests.Session):
    """Session, которая пропускает каждый запрос через общий лимитер, сама
    ретраит 429/5xx и подставляет таймаут.

    Всё делается в send(), а не в request(): именно через send() проходят
    редирект-хопы (requests/sessions.py, resolve_redirects), а request() их
    не видит. Один override закрывает гейт, 429 и таймауты на все вызовы.
    """

    def __init__(
        self,
        limiter: RateLimiter,
        *,
        timeout=DEFAULT_TIMEOUT,
        max_attempts: int = MAX_ATTEMPTS,
        log: Optional[Callable[[str], None]] = None,
    ):
        super().__init__()
        self._limiter = limiter
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._log = log or (lambda _msg: None)

    def set_log(self, log: Callable[[str], None]) -> None:
        """Подключить лог копира, чтобы 429 были видны в SSE-логе браузера."""
        self._log = log

    def send(self, request, **kwargs):
        # единственное место, где ставится таймаут
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = self._timeout

        attempt = 0
        while True:
            attempt += 1
            self._limiter.acquire()
            response = super().send(request, **kwargs)
            if response.status_code not in RETRY_STATUSES:
                return response

            last = attempt >= self._max_attempts
            delay = retry_after_seconds(response.headers.get("Retry-After"))
            if delay is None:
                delay = backoff_delay(attempt)

            if response.status_code == 429:
                # тормозим ВСЕ потоки: иначе остальные продолжают долбить и
                # продлевают лимит. Паузу додержит acquire() на следующем витке.
                actual = self._limiter.penalize(delay)
                note = "" if actual >= delay else f" | Retry-After {delay:.0f} с урезан"
                self._log(
                    f"429 | {request.method} {_short(request.url)} | "
                    f"общая пауза {actual:.1f} с | "
                    f"попытка {attempt}/{self._max_attempts}{note}"
                )
                if last:
                    return response
                response.close()
                continue

            # 5xx — беда одной ручки, барьер для всех не ставим
            self._log(
                f"{response.status_code} | {request.method} "
                f"{_short(request.url)} | повтор через {delay:.1f} с | "
                f"попытка {attempt}/{self._max_attempts}"
            )
            if last:
                return response
            response.close()
            self._limiter.pause(delay)
