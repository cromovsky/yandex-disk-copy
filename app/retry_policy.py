"""A job-local retry switch with interruptible backoff waits."""

from __future__ import annotations

import threading
import time


class RetryPolicy:
    def __init__(self, enabled=True, *, monotonic=time.monotonic, sleep=time.sleep):
        if type(enabled) is not bool:
            raise TypeError("enabled must be a boolean")
        self._enabled = enabled
        self._cancellations = 0
        self._condition = threading.Condition()
        self._monotonic = monotonic
        # An injected clock keeps existing transfer tests deterministic.
        self._sleep = None if sleep is time.sleep else sleep

    @property
    def enabled(self):
        with self._condition:
            return self._enabled

    @property
    def cancellation_generation(self):
        with self._condition:
            return self._cancellations

    def set_enabled(self, enabled):
        if type(enabled) is not bool:
            raise TypeError("enabled must be a boolean")
        with self._condition:
            self._enabled = enabled
            if not enabled:
                self._cancellations += 1
            self._condition.notify_all()

    def wait(self, seconds, checkpoint=None, *, generation=None):
        with self._condition:
            cancellations = self._cancellations if generation is None else generation
        deadline = self._monotonic() + max(0, seconds)
        while True:
            if checkpoint is not None:
                checkpoint()
            with self._condition:
                if not self._enabled or self._cancellations != cancellations:
                    return False
                remaining = deadline - self._monotonic()
                if remaining <= 0:
                    return True
                if self._sleep is None:
                    self._condition.wait(min(0.2, remaining))
                    continue
            self._sleep(remaining)
