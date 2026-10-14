"""One lifecycle-managed worker owns the persistent migration volume."""

from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .credentials import CredentialRegistry
from .runner import ACTIVE, MigrationRunner
from .store import MigrationStore
from app.source_slots import SOURCE_SLOTS


def enabled():
    return os.environ.get("ONPREM_ENABLED", "1").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _positive_int(name, default):
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        value = default
    return max(0, value)


class MigrationService:
    def __init__(self, root, *, start_worker=True, slots=None):
        self.root = Path(root)
        if self.root.is_symlink():
            raise RuntimeError("invalid_migration_volume")
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)
        self._volume_lock = (self.root / "worker.lock").open("a+b")
        try:
            self._lock_volume()
        except OSError:
            self._volume_lock.close()
            raise RuntimeError("migration_volume_in_use") from None
        try:
            self.store = MigrationStore(self.root)
            self.registry = CredentialRegistry()
            self.guard = threading.Lock()
            self.slots = slots or SOURCE_SLOTS
            self.excluded_job_ids = set()
            self._job_locks = {}
            self._job_locks_guard = threading.Lock()
            self.runner = MigrationRunner(
                self.store, self.registry,
                staging_budget=_positive_int("ONPREM_STAGING_MAX_BYTES", 20 * 1024**3),
                reserve_bytes=_positive_int("ONPREM_STAGING_RESERVE_BYTES", 1024**3),
            )
            self.store.recover()
            self.store.release_cancelled_path_conflicts()
            for job in self.store.jobs(limit=1000000):
                if job["state"] == "stopped" and job.get("full_stop_requested"):
                    self.store.cancel_local_entries(job["id"])
                self.runner.cleanup(job["id"], successes_only=True)
            self.stop = threading.Event()
            self.runner.shutdown_event = self.stop
            self.wake = threading.Event()
            self._closed = False
            self._polls = {}
            # Recovered stopped jobs have no local clients to acknowledge.
            self._full_stop_acknowledged = {
                job["id"] for job in self.store.jobs(limit=1000000)
                if job["state"] == "stopped" and job.get("full_stop_requested")
            }
            self.thread = None
            self._executor = ThreadPoolExecutor(max_workers=self.slots.limit, thread_name_prefix="onprem-source")
            self._tasks = {}
            if start_worker:
                self.start()
        except BaseException:
            self._volume_lock.close()
            raise

    def _lock_volume(self):
        if os.name == "nt":
            import msvcrt
            self._volume_lock.seek(0)
            self._volume_lock.write(b"0")
            self._volume_lock.flush()
            self._volume_lock.seek(0)
            msvcrt.locking(self._volume_lock.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(self._volume_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def notify(self):
        self.wake.set()

    def start(self):
        if self.thread is None:
            self.thread = threading.Thread(target=self._run, name="onprem-migration",
                                           daemon=False)
            self.thread.start()

    def job_guard(self, job_id):
        with self._job_locks_guard:
            return self._job_locks.setdefault(job_id, threading.RLock())

    def holding_job(self, jobs=None):
        jobs = self.store.jobs(limit=1000000) if jobs is None else jobs
        return next((job for job in sorted(jobs, key=lambda j: j["created_at"])
                     if not job.get("full_stop_requested") and job["state"] != "stopped"
                     and any(entry["state"] in ACTIVE
                             for entry in self.store.entries(job["id"], limit=1000000))), None)

    def describe_job(self, job, holding):
        blocker = None
        pending = job["state"] == "running" or (
            job["state"] == "awaiting_credentials" and job["resume_state"] == "running"
        )
        owner = "onprem:" + job["id"]
        if (pending and holding and holding["id"] != job["id"]
                and owner not in self.slots.active_owners
                and len(self.slots.active_owners) >= self.slots.limit):
            blocker = {
                "job_id": holding["id"], "state": holding["state"],
                "reason": "operation_in_progress",
            }
        pending_final = (self.runner.final_pending(job["id"])
                         if job["state"] == "stopped" or job.get("full_stop_requested")
                         else [])
        return {**job, "blocked_by": blocker,
                "transfer_rates": self.runner.transfer_rates(job["id"]),
                "stop_blocked": False,
                "remote_pending": bool(pending_final),
                "stop_blocked_attempt_ids": []}

    def _stop_requested_jobs(self, jobs):
        for job in jobs:
            if job["id"] in self.excluded_job_ids:
                continue
            lock = self.job_guard(job["id"])
            if not lock.acquire(blocking=False):
                continue
            try:
                self._stop_one_job(job)
            finally:
                lock.release()

    def _stop_one_job(self, job):
        if job["state"] == "stopped":
            if job.get("full_stop_requested"):
                self._polls.pop(job["id"], None)
                if job["id"] not in self._full_stop_acknowledged:
                    self.store.cancel_local_entries(job["id"])
                    self.runner.release_clients(job["id"])
                    self._full_stop_acknowledged.add(job["id"])
                return
            self._polls.pop(job["id"], None)
            self.runner.release_clients(job["id"])
            return
        if not job.get("stop_requested"):
            return
        current = self.store.get(job["id"])
        if current.get("stop_requested") and current["state"] != "stopped":
            self.runner.stop_job(job["id"])
            self._polls.pop(job["id"], None)
            if current.get("full_stop_requested"):
                self._full_stop_acknowledged.add(job["id"])

    def _pause_idle_jobs(self, jobs):
        for job in jobs:
            if job["id"] in self.excluded_job_ids:
                continue
            if not job["pause_requested"] or job["state"] not in {
                "running", "awaiting_credentials",
            }:
                continue
            lock = self.job_guard(job["id"])
            if not lock.acquire(blocking=False):
                continue
            try:
                current = self.store.get(job["id"])
                if (
                    not current.get("stop_requested")
                    and current["pause_requested"] and (
                        current["state"] == "running" or (
                            current["state"] == "awaiting_credentials"
                            and current["resume_state"] == "running"
                        )
                    )
                    and current["namespace_state"] in {"unclaimed", "owned"}
                    and not any(
                        entry["state"] in ACTIVE
                        for entry in self.store.entries(job["id"], limit=1000000)
                    )
                ):
                    # A local queue pause must not claim a remote namespace.
                    fields = {"state": "paused"} if current["state"] == "running" \
                        else {"resume_state": "paused"}
                    self.store.update_job(job["id"], **fields)
            finally:
                lock.release()

    def _run(self):
        while not self.stop.is_set():
            self.wake.clear()
            jobs = self.store.jobs(limit=1000000)
            jobs.sort(key=lambda j: j["created_at"])
            self._stop_requested_jobs(jobs)
            self._pause_idle_jobs(jobs)
            jobs = self.store.jobs(limit=1000000)
            jobs.sort(key=lambda j: j["created_at"])
            for job_id, task in list(self._tasks.items()):
                if task.done():
                    try:
                        task.result()
                    except Exception:
                        # A damaged source must not terminate the other lane.
                        # Persist only a fixed code; raw exceptions may contain URLs.
                        self.slots.release("onprem:" + job_id)
                        with self.job_guard(job_id):
                            current = self.store.get(job_id)
                            if current["state"] != "stopped":
                                self.store.update_job(job_id, state="needs_attention",
                                                      error="internal_error")
                    self._tasks.pop(job_id)
            for job in jobs:
                if (job["id"] not in self._tasks and job["id"] not in self.excluded_job_ids
                        and (job["state"] not in {"running", "queued"}
                             or job.get("full_stop_requested"))):
                    self.slots.release("onprem:" + job["id"])
            candidates = [j for j in jobs if j["state"] in {"running", "queued"}
                          and not j.get("full_stop_requested")
                          and j["id"] not in self.excluded_job_ids]
            for job in candidates:
                if job["id"] in self._tasks or len(self._tasks) >= self.slots.limit:
                    continue
                previous = self._polls.get(job["id"], (None, 0, 0))
                if previous[2] > time.monotonic():
                    continue
                if not self.slots.try_acquire("onprem:" + job["id"]):
                    continue
                with self.guard:
                    self._tasks[job["id"]] = self._executor.submit(self._advance, job["id"])
            self.wake.wait(0.1)

    def _advance(self, job_id):
        try:
            with self.job_guard(job_id):
                previous = self._polls.get(job_id, (None, 0, 0))
                self.runner.step(job_id)
                current = self.store.get(job_id)
                if current.get("stop_requested") and current["state"] != "stopped":
                    self.runner.stop_job(job_id)
                    if current.get("full_stop_requested"):
                        self._full_stop_acknowledged.add(job_id)
                current = self.store.get(job_id)
                waiting = next((a for a in self.store.attempts(job_id)
                                if a["phase"] in {"waiting_transfer", "waiting_move"}), None)
                if (waiting and current["state"] == "running"
                        and not current.get("full_stop_requested")):
                    marker = (waiting["id"], waiting["phase"])
                    delay = min(10, previous[1] * 2) if marker == previous[0] else 0.5
                    self._polls[job_id] = (marker, delay, time.monotonic() + delay)
                else:
                    self._polls.pop(job_id, None)
        finally:
            current = self.store.get(job_id)
            if (self.stop.is_set() or current["state"] not in {"running", "queued"}
                    or current.get("full_stop_requested")):
                self.slots.release("onprem:" + job_id)
            self.notify()

    def close(self):
        if self._closed:
            return
        self.stop.set()
        self.wake.set()
        if self.thread:
            self.thread.join()
        self._executor.shutdown(wait=True)
        for job in self.store.jobs(limit=1000000):
            self.slots.release("onprem:" + job["id"])
        self.runner.close()
        self.registry.clear()
        self.store.close()
        self._volume_lock.close()
        self._closed = True
