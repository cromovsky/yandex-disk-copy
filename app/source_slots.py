"""Process-wide source lanes, shared by list and standalone transfers."""

from contextlib import contextmanager
import threading


class SourceSlots:
    def __init__(self, limit=4):
        self.limit = limit
        self._condition = threading.Condition()
        self._owners = set()
        self._leases = {}

    @property
    def active_owners(self):
        with self._condition:
            return set(self._owners)

    def try_acquire(self, owner):
        with self._condition:
            if owner in self._owners:
                return True
            if len(self._owners) >= self.limit:
                return False
            self._owners.add(owner)
            return True

    def release(self, owner):
        with self._condition:
            self._owners.discard(owner)
            self._condition.notify_all()

    @contextmanager
    def lease(self, owner, checkpoint=None):
        while True:
            if checkpoint is not None:
                checkpoint()
            with self._condition:
                if self.try_acquire(owner):
                    self._leases[owner] = self._leases.get(owner, 0) + 1
                    break
                self._condition.wait(0.2)
        try:
            if checkpoint is not None:
                checkpoint()
            yield
        finally:
            with self._condition:
                self._leases[owner] -= 1
                if not self._leases[owner]:
                    self._leases.pop(owner)
                    self.release(owner)


SOURCE_SLOTS = SourceSlots()
