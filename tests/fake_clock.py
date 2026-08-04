"""Управляемые часы для тестов: код под тестом не спит по-настоящему."""

from __future__ import annotations

import threading


class FakeClock:
    """monotonic() сам не идёт; sleep() перематывает время и пишет в .sleeps.

    Старт не с нуля, а с 1000.0 — чтобы _next_slot = 0.0 у свежего лимитера
    гарантированно оказался в прошлом и не маскировал ошибку знака.
    """

    def __init__(self, start: float = 1000.0):
        self.now = start
        self.sleeps: list[float] = []
        self._lock = threading.Lock()

    def monotonic(self) -> float:
        with self._lock:
            return self.now

    def sleep(self, seconds: float) -> None:
        with self._lock:
            self.sleeps.append(seconds)
            self.now += seconds

    def advance(self, seconds: float) -> None:
        """Время «прошло» само — имитация долгого сетевого RTT."""
        with self._lock:
            self.now += seconds
