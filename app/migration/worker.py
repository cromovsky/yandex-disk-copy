"""One lifecycle-managed worker owns the persistent migration volume."""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path

from .credentials import CredentialRegistry
from .runner import ACTIVE, FINAL_PENDING, MigrationRunner
from .store import MigrationStore


def enabled():
    return os.environ.get("ONPREM_ENABLED", "0").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _positive_int(name, default):
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        value = default
    return max(0, value)


class MigrationService:
    def __init__(self, root, *, start_worker=True):
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
            self.runner = MigrationRunner(
                self.store, self.registry,
                staging_budget=_positive_int("ONPREM_STAGING_MAX_BYTES", 20 * 1024**3),
                reserve_bytes=_positive_int("ONPREM_STAGING_RESERVE_BYTES", 1024**3),
            )
            self.store.recover()
            for job in self.store.jobs(limit=1000000):
                self.runner.cleanup(job["id"], successes_only=True)
            self.stop = threading.Event()
            self.wake = threading.Event()
            self._closed = False
            self._polls = {}
            self.thread = None
            if start_worker:
                self.thread = threading.Thread(
                    target=self._run, name="onprem-migration", daemon=False,
                )
                self.thread.start()
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

    def holding_job(self, jobs=None):
        jobs = self.store.jobs(limit=1000000) if jobs is None else jobs
        return next((job for job in sorted(jobs, key=lambda j: j["created_at"])
                     if (bool(self.runner.final_pending(job["id"]))
                         if job["state"] == "stopped" else any(
                             entry["state"] in ACTIVE | {"outcome_unknown"}
                             for entry in self.store.entries(
                                 job["id"], limit=1000000,
                             )))), None)

    def describe_job(self, job, holding):
        blocker = None
        pending = job["state"] == "running" or (
            job["state"] == "awaiting_credentials" and job["resume_state"] == "running"
        )
        if pending and holding and holding["id"] != job["id"]:
            unknown = any(
                entry["state"] == "outcome_unknown"
                for entry in self.store.entries(holding["id"], limit=1000000)
            )
            blocker = {
                "job_id": holding["id"], "state": holding["state"],
                "reason": "operation_outcome_unknown" if unknown
                else "operation_in_progress",
            }
        pending_final = (self.runner.final_pending(job["id"])
                         if job["state"] == "stopped" else [])
        return {**job, "blocked_by": blocker,
                "stop_blocked": bool(pending_final),
                "stop_blocked_attempt_ids": [a["id"] for a in pending_final],
                "stop_resolvable_attempt_ids": [
                    a["id"] for a in pending_final
                    if a["phase"] in FINAL_PENDING
                ]}

    def _stop_requested_jobs(self, jobs):
        for job in jobs:
            if job["state"] == "stopped":
                if not self.runner.final_pending(job["id"]):
                    with self.guard:
                        self.runner.release_clients(job["id"])
                continue
            if not job.get("stop_requested"):
                continue
            with self.guard:
                current = self.store.get(job["id"])
                if current.get("stop_requested") and current["state"] != "stopped":
                    self.runner.stop_job(job["id"])
                    self._polls.pop(job["id"], None)

    def _pause_idle_jobs(self, jobs):
        for job in jobs:
            if not job["pause_requested"] or job["state"] not in {
                "running", "awaiting_credentials",
            }:
                continue
            with self.guard:
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
                        entry["state"] in ACTIVE | {"outcome_unknown"}
                        for entry in self.store.entries(job["id"], limit=1000000)
                    )
                ):
                    # A local queue pause must not claim a remote namespace.
                    fields = {"state": "paused"} if current["state"] == "running" \
                        else {"resume_state": "paused"}
                    self.store.update_job(job["id"], **fields)

    def _run(self):
        while not self.stop.is_set():
            self.wake.clear()
            jobs = self.store.jobs(limit=1000000)
            jobs.sort(key=lambda j: j["created_at"])
            self._stop_requested_jobs(jobs)
            self._pause_idle_jobs(jobs)
            jobs = self.store.jobs(limit=1000000)
            jobs.sort(key=lambda j: j["created_at"])
            # Explicitly stopped imports can only finish in their old namespace.
            # Pending final moves still reserve the lane, including after stop.
            holding = self.holding_job(jobs)
            running = holding or next((j for j in jobs if j["state"] == "running"),
                                      None)
            can_work = running and (
                running["state"] == "running" or (
                    running["state"] == "stopped"
                    and self.registry.get(running["id"])
                    and any(a["phase"] == "waiting_move"
                            for a in self.runner.final_pending(running["id"]))
                )
            )
            candidates = ([running] if can_work else []) + [
                j for j in jobs if j["state"] == "queued"
            ]
            for job in candidates:
                previous = self._polls.get(job["id"], (None, 0, 0))
                if previous[2] > time.monotonic():
                    continue
                with self.guard:
                    if job["state"] == "stopped":
                        self.runner.reconcile_stopped_move(job["id"])
                    else:
                        self.runner.step(job["id"])
                    current = self.store.get(job["id"])
                    if (current.get("stop_requested")
                            and current["state"] != "stopped"):
                        self.runner.stop_job(job["id"])
                current = self.store.get(job["id"])
                waiting = next((a for a in self.store.attempts(job["id"])
                                if a["phase"] in {"waiting_transfer", "waiting_move"}),
                               None)
                if waiting and current["state"] in {"running", "stopped"}:
                    marker = (waiting["id"], waiting["phase"])
                    delay = min(10, previous[1] * 2) if marker == previous[0] else 0.5
                    self._polls[job["id"]] = (marker, delay, time.monotonic() + delay)
                else:
                    self._polls.pop(job["id"], None)
                break
            self.wake.wait(0.5)

    def close(self):
        if self._closed:
            return
        self.stop.set()
        self.wake.set()
        if self.thread:
            self.thread.join()
        self.runner.close()
        self.registry.clear()
        self.store.close()
        self._volume_lock.close()
        self._closed = True
