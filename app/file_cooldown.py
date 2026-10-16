"""A per-source interval between processed objects, independent of retries."""

import threading
import time

FILE_PAUSE_SECONDS = 20.0


class FileCooldown:
    def __init__(self, *, seconds=None, monotonic=None):
        self.seconds = FILE_PAUSE_SECONDS if seconds is None else seconds
        self._monotonic = monotonic or time.monotonic
        self._deadlines = {}
        self._errors = set()
        self._lock = threading.Lock()

    def mark(self, key, *, after_error=False):
        with self._lock:
            self._deadlines[key] = self._monotonic() + self.seconds
            if after_error:
                self._errors.add(key)
            else:
                self._errors.discard(key)

    def after_error(self, key):
        with self._lock:
            return key in self._errors

    def remaining(self, key):
        with self._lock:
            return max(0.0, self._deadlines.get(key, 0) - self._monotonic())

    def clear(self, key):
        with self._lock:
            self._deadlines.pop(key, None)
            self._errors.discard(key)
