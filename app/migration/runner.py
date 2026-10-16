"""Durable Disk migration with explicit recovery of interrupted uploads."""

from __future__ import annotations

import hashlib
import os
import random
import shutil
import sqlite3
import stat
import threading
import time
import unicodedata
import uuid
from collections import deque
from contextlib import contextmanager
from pathlib import Path

from .destination import OnPremDiskDestination, destination_path
from .diagnostics import diagnostic_fields
from .errors import MigrationError
from .source import SaasDiskSource
from app.retry_policy import RetryPolicy


ACTIVE = {"transferring", "waiting_operation", "verifying", "committing",
          "downloading", "staged"}
UNKNOWN_PHASES = {"transfer_intent", "move_intent", "unknown_transfer",
                  "unknown_move"}
ABANDONED_PHASES = {"abandoned_transfer", "abandoned_move"}
SIDE_EFFECT_PHASES = UNKNOWN_PHASES | {
    "waiting_transfer", "waiting_move", "verifying", "verifying_final",
}
FINAL_PENDING = {"move_intent", "waiting_move", "unknown_move"}
STOP_ISOLATED_PHASES = {
    "prepared", "downloading", "download_retry_wait", "staged",
    "transfer_intent", "waiting_transfer",
    "unknown_transfer", "abandoned_transfer", "abandoned_move", "verifying", "verifying_final",
    "succeeded", "failed",
}
SUCCESS = {"done", "already_verified", "skipped_by_user"}
ACCEPTED_URL_PHASES = {"waiting_transfer", "waiting_move", "verifying",
                       "verifying_final", "succeeded"}
RETRYABLE = {"failed"}
MAX_DOWNLOAD_ATTEMPTS = 5
MAX_POINT_RETRY_ATTEMPTS = 3
POINT_RETRY_ERRORS = {
    "destination_unavailable", "destination_timeout",
    "destination_connection_failed", "destination_stream_interrupted",
    "destination_invalid_response", "destination_rate_limited",
    "destination_not_found", "remote_operation_failed", "destination_url_import_failed",
    "directory_confirmation_pending",
}
DOWNLOAD_ERRORS = {
    "source_unavailable", "source_invalid_response", "invalid_download_url",
    "source_download_failed", "source_timeout", "source_connection_failed",
    "source_stream_interrupted", "source_download_timeout",
    "source_content_changed",
    "source_public_resource_unavailable", "source_public_access_denied",
    "source_public_download_limit", "source_public_download_failed",
    "source_public_invalid_response",
}


def destination_interrupted(exc):
    return bool(isinstance(exc, MigrationError) and (
        exc.code.startswith("destination_")
        and exc.code not in {"destination_conflict", "destination_url_import_failed"}
    ))


def file_access_denied(exc, stage):
    """A resource-level 403 is distinct from rejected application credentials."""
    return bool(isinstance(exc, MigrationError) and exc.http_status == 403
                and exc.code in {"source_auth_failed", "destination_auth_failed",
                                 "source_public_access_denied"}
                and stage not in {"prepare_clients", "planning", "check_destination_access"}
                and exc.diagnostics.get("request_host") != "oauth.yandex.ru"
                and exc.diagnostics.get("api_error") not in {
                    "invalid_client", "invalid_grant", "unauthorized_client", "invalid_scope",
                })


class _StopRequested(Exception):
    pass


def disk_path(value: str) -> str:
    path = value[5:] if value.startswith("disk:") else value
    if not path.startswith("/"):
        path = "/" + path
    path = path.rstrip("/") or "/"
    parts = path.split("/")[1:]
    if path != "/" and any(
        part in ("", ".", "..") or "\\" in part or ":" in part
        or any(ord(c) < 32 or ord(c) == 127 for c in part)
        or len(part.encode("utf-8")) > 255 for part in parts
    ):
        raise ValueError("invalid_disk_path")
    if len(path.encode("utf-8")) > 4096:
        raise ValueError("invalid_disk_path")
    return "disk:" + path


def join_path(parent: str, relative: str) -> str:
    return disk_path(parent.rstrip("/") + "/" + relative.lstrip("/"))


def matching(entry: dict, metadata: dict | None) -> bool:
    return bool(
        metadata and metadata.get("type") == "file"
        and entry.get("md5") and metadata.get("md5")
        and str(entry["md5"]).lower() == str(metadata["md5"]).lower()
        and entry.get("size") == metadata.get("size")
    )


def pending_final_phases(job):
    return FINAL_PENDING if job["remote_root"] else FINAL_PENDING | {
        "transfer_intent", "waiting_transfer", "unknown_transfer",
    }


class MigrationRunner:
    def __init__(
        self, store, registry, *, source_factory=SaasDiskSource,
        destination_factory=OnPremDiskDestination,
        staging_budget=20 * 1024**3, reserve_bytes=1024**3,
    ):
        self.store = store
        self.registry = registry
        self.source_factory = source_factory
        self.destination_factory = destination_factory
        self.staging_budget = staging_budget
        self.reserve_bytes = reserve_bytes
        self.staging = Path(store.root) / "staging"
        self.staging.mkdir(mode=0o700, exist_ok=True)
        os.chmod(self.staging, 0o700)
        self._clients = {}
        self._retry_policies = {}
        self._retry_lock = threading.RLock()
        self._transfers = {}
        self._rate_lock = threading.Lock()
        self._cache_lock = threading.RLock()
        self._cache_reservations = {}
        self._outage_lock = threading.Lock()
        self._destination_outages = {}
        self.shutdown_event = None
        self.default_strategy = None

    @contextmanager
    def _measure_transfer(self, job_id, direction):
        now = time.monotonic()
        transfer = {"direction": direction, "bytes": 0, "last_byte": now,
                    "samples": deque([(now, 0)])}
        with self._rate_lock:
            self._transfers[job_id] = transfer

        def progress(count):
            now = time.monotonic()
            with self._rate_lock:
                transfer["bytes"] += count
                transfer["last_byte"] = now
                samples = transfer["samples"]
                if not transfer["bytes"]:
                    samples.clear()
                    samples.append((now, 0))
                elif now - samples[-1][0] >= 1:
                    samples.append((now, transfer["bytes"]))
                while len(samples) > 1 and samples[1][0] <= now - 5:
                    samples.popleft()

        try:
            yield progress
        finally:
            with self._rate_lock:
                if self._transfers.get(job_id) is transfer:
                    self._transfers.pop(job_id)

    def transfer_rates(self, job_id):
        rates = {"download_bytes_per_second": None, "upload_bytes_per_second": None,
                 "download_active": False, "upload_active": False}
        with self._rate_lock:
            transfer = self._transfers.get(job_id)
            if transfer:
                now = time.monotonic()
                samples = transfer["samples"]
                while len(samples) > 1 and samples[1][0] <= now - 5:
                    samples.popleft()
                started, previous = samples[0]
                speed = (transfer["bytes"] - previous) / max(1, min(5, now - started))
                if now - transfer["last_byte"] >= 5:
                    speed = 0
                direction = transfer["direction"]
                rates[direction + "_bytes_per_second"] = speed
                rates[direction + "_active"] = True
        return rates

    def retry_policy(self, job_id):
        with self._retry_lock:
            policy = self._retry_policies.get(job_id)
            if policy is None:
                policy = RetryPolicy(self.store.get(job_id)["config"].get("retry_enabled", True))
                self._retry_policies[job_id] = policy
            return policy

    def bind_retry_policy(self, job_id, policy):
        with self._retry_lock:
            self._retry_policies[job_id] = policy
            current = self._clients.get(job_id)
            if current:
                for client in current[1:]:
                    if client is not None:
                        client.retry_policy = policy

    def set_retry_enabled(self, job_id, enabled):
        with self._retry_lock:
            policy = self.retry_policy(job_id)
            self.store.set_retry_enabled(job_id, enabled)
            policy.set_enabled(enabled)

    def _download_limit(self, job_id):
        return MAX_DOWNLOAD_ATTEMPTS if self.retry_policy(job_id).enabled else 1

    def _point_limit(self, job_id):
        return MAX_POINT_RETRY_ATTEMPTS if self.retry_policy(job_id).enabled else 1

    def close(self):
        for _, source, destination in self._clients.values():
            if source:
                source.close()
            destination.close()
        self._clients.clear()

    def _logged(self, job, entry, stage, action, *args, attempt=None, **kwargs):
        if self.store.get(job["id"]).get("full_stop_requested"):
            raise _StopRequested()
        payload = {"stage": stage, "status": "started"}
        if entry:
            payload["entry_id"] = entry["id"]
        if attempt:
            payload["attempt_id"] = attempt["id"]
            if attempt.get("operation_id"):
                payload["operation_id"] = attempt["operation_id"]
        self.store.event(job["id"], "transfer_stage", payload)
        started = time.monotonic()
        try:
            if stage in {"create_directory", "prepare_upload_directory",
                         "prepare_destination_directory", "upload_file",
                         "submit_url_upload", "claim_namespace", "move_file"}:
                self._checkpoint(job["id"])
                if not (entry and (entry.get("point_retry_requested")
                                  or not self.retry_policy(job["id"]).enabled)):
                    with self._outage_lock:
                        interrupted = self._destination_outages.get(
                            self._destination_key(job["config"]),
                        )
                    if interrupted:
                        raise MigrationError(interrupted[0], http_status=interrupted[1],
                                             diagnostics=interrupted[2])
            result = action(*args, **kwargs)
        except Exception as exc:
            if isinstance(exc, _StopRequested):
                payload["status"] = "interrupted"
            elif getattr(exc, "uncertain", False) and not destination_interrupted(exc):
                payload["status"] = "outcome_unknown"
            else:
                payload["status"] = "failed"
            if not isinstance(exc, _StopRequested):
                payload["error"] = exc.code if isinstance(exc, MigrationError) \
                    else "internal_error"
            if isinstance(exc, MigrationError):
                exc._migration_stage = getattr(exc, "_migration_stage", stage)
                if (attempt and stage in {"upload_file", "submit_url_upload"}
                        and not exc.uncertain and not exc.auth):
                    # This submission is known to have failed. Read failures
                    # while checking an older unknown write keep its phase.
                    self.store.update_attempt(attempt["id"], phase="failed",
                                              error=exc.code)
                payload.update(exc.diagnostics)
                if exc.http_status is not None:
                    payload["http_status"] = exc.http_status
                if exc.source_path:
                    payload["source_path"] = exc.source_path
                if exc.operation_reference_issue:
                    payload["operation_reference_issue"] = exc.operation_reference_issue
            payload["duration_ms"] = int((time.monotonic() - started) * 1000)
            if entry and stage in {
                "downloading", "get_download_url", "public_download_link",
            }:
                payload.update(
                    download_attempts=self.store.entry(
                        entry["id"],
                    )["download_attempts"],
                    max_download_attempts=self._download_limit(job["id"]),
                )
            self.store.event(job["id"], "transfer_stage", payload)
            raise
        payload["status"] = "succeeded"
        if stage in {"poll_transfer", "poll_move"}:
            if result in {"in-progress", "success", "failed"}:
                payload["remote_status"] = result
            else:
                payload.update(status="outcome_unknown",
                               error="invalid_operation_status")
            if result == "in-progress":
                payload["status"] = "waiting"
            elif result == "failed":
                code = ("destination_url_import_failed"
                        if stage == "poll_transfer" and attempt
                        and attempt["strategy"] == "url" else "remote_operation_failed")
                payload.update(status="failed", error=code)
                remote = getattr(
                    getattr(action, "__self__", None),
                    "last_operation_diagnostics", {},
                )
                payload.update(diagnostic_fields(remote))
                http_status = remote.get("http_status") if isinstance(remote, dict) else None
                if (isinstance(http_status, int) and not isinstance(http_status, bool)
                        and 100 <= http_status <= 599):
                    payload["http_status"] = http_status
        elif result is False and entry:
            payload.update(status="failed", error=(
                self.store.entry(entry["id"]).get("error") or "verification_mismatch"
            ))
        if stage in {"submit_url_upload", "upload_file", "move_file"} and result:
            payload.update(status="waiting", operation_id=result)
        payload["duration_ms"] = int((time.monotonic() - started) * 1000)
        if entry and stage in {
            "downloading", "get_download_url", "public_download_link",
        }:
            payload.update(
                download_attempts=self.store.entry(entry["id"])["download_attempts"],
                max_download_attempts=self._download_limit(job["id"]),
            )
        self.store.event(job["id"], "transfer_stage", payload)
        if result is False and entry and not self.retry_policy(job["id"]).enabled:
            error = self.store.entry(entry["id"]).get("error") or "verification_mismatch"
            self._skip_event(job["id"], entry, MigrationError(error), stage=stage)
        return result

    def final_pending(self, job_id):
        pending = pending_final_phases(self.store.get(job_id))
        return [attempt for attempt in self.store.attempts(job_id)
                if (attempt["phase"] in pending
                    or attempt["phase"] not in STOP_ISOLATED_PHASES)]

    def stop_job(self, job_id):
        self.store.update_job(job_id, state="stopped", resume_state=None,
                              pause_requested=False)
        if self.store.get(job_id).get("full_stop_requested"):
            self.store.cancel_local_entries(job_id)
            self.release_clients(job_id)
        elif not self.final_pending(job_id):
            self.release_clients(job_id)

    def release_clients(self, job_id):
        self.registry.discard(job_id)
        current = self._clients.pop(job_id, None)
        if current:
            if current[1]:
                current[1].close()
            current[2].close()

    def _checkpoint(self, job_id):
        if self.shutdown_event is not None and self.shutdown_event.is_set():
            raise _StopRequested()
        job = self.store.get(job_id)
        if job.get("stop_requested") or job["state"] == "stopped":
            raise _StopRequested()

    def _job_error(self, job_id, **fields):
        try:
            self.store.update_job(job_id, **fields)
        except ValueError as exc:
            if str(exc) != "job_stopped":
                raise

    def destination_client(self, job):
        credentials = self.registry.get(job["id"])
        if not credentials:
            raise MigrationError("credentials_required", auth=True)
        current = self._clients.get(job["id"])
        if current and current[0] != credentials:
            if current[1]:
                current[1].close()
            current[2].close()
            self._clients.pop(job["id"], None)
            current = None
        if not current:
            destination = self.destination_factory(job["config"], credentials)
            destination.retry_policy = self.retry_policy(job["id"])
            current = (credentials, None, destination)
            self._clients[job["id"]] = current
        return current[2]

    def clients(self, job):
        destination = self.destination_client(job)
        current = self._clients[job["id"]]
        if not current[1]:
            source = self.source_factory(job["config"], current[0])
            source.retry_policy = self.retry_policy(job["id"])
            current = (current[0], source, destination)
            self._clients[job["id"]] = current
        return current[1:]

    def preflight(self, config, credentials):
        source = self.source_factory(config, credentials)
        try:
            destination = self.destination_factory(config, credentials)
            try:
                source_meta = source.stat(config["source_path"])
                info = destination.info()
                self._identity(config, info)
                target = destination.stat(config["destination_root"])
                if target and target.get("type") != "dir":
                    raise MigrationError("destination_conflict")
                quota = self._quota(info)
                return {
                    "source": {"path": source_meta.get("path"),
                               "type": source_meta.get("type")},
                    "destination": self._destination_details(config, info),
                    "warnings": ["manual_identity_binding", "stand_not_verified"]
                    + (["quota_unavailable"] if quota is None else []),
                }
            finally:
                destination.close()
        finally:
            source.close()

    def preflight_destination(self, config, credentials):
        destination = self.destination_factory(config, credentials)
        try:
            info = destination.info()
            self._identity(config, info)
            self.clear_destination_outage(config)
            return self._destination_details(config, info)
        finally:
            destination.close()

    @staticmethod
    def _destination_key(config):
        return (config["destination_url"], config.get("destination_vd_hash", ""),
                config.get("destination_type", "personal"))

    def clear_destination_outage(self, config):
        with self._outage_lock:
            self._destination_outages.pop(self._destination_key(config), None)

    def _record_destination_outage(self, job_id, exc):
        if destination_interrupted(exc) and self.retry_policy(job_id).enabled:
            config = self.store.get(job_id)["config"]
            with self._outage_lock:
                self._destination_outages[self._destination_key(config)] = (
                    exc.code, exc.http_status, exc.diagnostics,
                )

    @staticmethod
    def _destination_details(config, info):
        result = {"identity": config["destination_identity"],
                  "free_bytes": MigrationRunner._quota(info)}
        if config.get("destination_type") == "shared":
            result.update(type="shared", vd_hash=config["destination_vd_hash"],
                          path=destination_path(config, config["destination_root"]))
            for key in ("name", "permissions"):
                if key in info:
                    result[key] = info[key]
        return result

    @staticmethod
    def _identity(config, info):
        if not config.get("identity_confirmed"):
            raise MigrationError("identity_confirmation_required")
        user = info.get("user") or {}
        identities = [str(user[k]).casefold() for k in ("login", "email")
                      if user.get(k)]
        if identities and config["destination_identity"].casefold() not in identities:
            raise MigrationError("destination_identity_mismatch")

    @staticmethod
    def _quota(info):
        total, used = info.get("total_space"), info.get("used_space")
        if isinstance(total, int) and isinstance(used, int):
            return max(0, total - used)
        return None

    def plan(self, job, source, destination, *, full_compare=False, checkpoint=None):
        if full_compare:
            return self._full_plan(job, source, destination, checkpoint=checkpoint)
        self._checkpoint(job["id"])
        self.store.update_job(job["id"], state="planning", error=None)
        config = job["config"]
        info = destination.info()
        self._identity(config, info)
        root = config["source_path"]
        root_meta = source.stat(root)
        if root_meta.get("type") not in {"dir", "file"}:
            raise MigrationError("unsupported_resource")
        if job["remote_root"] and destination.stat(job["remote_root"]) is not None:
            raise MigrationError("technical_namespace_exists")
        base = root.rsplit("/", 1)[0] if root != "disk:/" else "disk:"
        entries, seen = [], set()
        for node in source.walk(root):
            self._checkpoint(job["id"])
            path = disk_path(node["path"])
            if path == "disk:/":
                continue
            if root != "disk:/" and path != root and not path.startswith(root + "/"):
                raise MigrationError("invalid_source_tree", source_path=path)
            if not path.startswith(base + "/"):
                raise MigrationError("invalid_source_tree", source_path=path)
            target = join_path(config["destination_root"], path[len(base) + 1:])
            technical = job["remote_root"]
            if technical and (target == technical or target.startswith(technical + "/")
                              or technical.startswith(target + "/")):
                raise MigrationError("technical_path_collision", source_path=path)
            normalized = unicodedata.normalize("NFC", target)
            if normalized in seen:
                raise MigrationError("path_collision", source_path=path)
            seen.add(normalized)
            kind = node.get("type")
            if kind not in {"file", "dir"}:
                raise MigrationError("unsupported_resource", source_path=path)
            size = node.get("size") if kind == "file" else 0
            if kind == "file" and (not isinstance(size, int) or size < 0):
                raise MigrationError("source_size_unavailable", source_path=path)
            entries.append({
                "kind": kind, "source_path": path, "destination_path": target,
                "size": size, "md5": node.get("md5"),
                "modified": node.get("modified"),
                "resource_id": node.get("resource_id"),
            })
        # Parents precede children; file ordering stays stable across restarts.
        entries.sort(key=lambda e: (
            e["kind"] != "dir", e["destination_path"].count("/"),
            e["destination_path"],
        ))
        quota = self._quota(info)
        required = sum(e["size"] for e in entries)
        largest = max((e["size"] for e in entries), default=0)
        extra = largest if job["remote_root"] else 0
        if quota is None:
            self.store.event(job["id"], "warning", {"error": "quota_unavailable"})
        self._checkpoint(job["id"])
        self.store.add_entries(job["id"], entries, mark_ready=True)
        if quota is not None and required + extra > quota:
            self.store.update_job(job["id"], state="needs_attention",
                                  error="destination_space_insufficient")
            raise MigrationError("destination_space_insufficient")

    def _full_plan(self, job, source, destination, *, checkpoint=None):
        """Persist a read-only inventory incrementally, then fence plan completion."""
        def check():
            self._checkpoint(job["id"])
            if checkpoint is not None:
                checkpoint()

        check()
        self.store.update_job(job["id"], state="planning", error=None)
        self.store.reset_incomplete_plan(job["id"])
        config = job["config"]
        info = destination.info()
        self._identity(config, info)
        # Shared info checks permissions; this read confirms the actual resource root.
        if config.get("destination_type") == "shared":
            target_root = destination.stat("disk:/")
            if not target_root or target_root.get("type") != "dir":
                raise MigrationError("destination_not_found")
        existing_root = destination.stat(config["destination_root"])
        if existing_root and existing_root.get("type") != "dir":
            raise MigrationError("destination_conflict")
        root = config["source_path"]
        root_meta = source.stat(root)
        if root_meta.get("type") not in {"dir", "file"}:
            raise MigrationError("unsupported_resource")
        base = root.rsplit("/", 1)[0] if root != "disk:/" else "disk:"
        # Track normalized destination paths in SQLite, so a million-file inventory
        # does not retain a second complete tree in Python memory.
        with self.store._connection(write=True) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS plan_paths("
                               "job_id TEXT NOT NULL,path TEXT NOT NULL,"
                               "PRIMARY KEY(job_id,path))")
            connection.execute("DELETE FROM plan_paths WHERE job_id=?", (job["id"],))
        required, unavailable = 0, False
        try:
            for node in source.walk(root):
                check()
                path = disk_path(node["path"])
                if path == "disk:/":
                    continue
                if (root != "disk:/" and path != root
                        and not path.startswith(root + "/")) or not path.startswith(base + "/"):
                    raise MigrationError("invalid_source_tree", source_path=path)
                target = join_path(config["destination_root"], path[len(base) + 1:])
                normalized = unicodedata.normalize("NFC", target)
                try:
                    with self.store._connection(write=True) as connection:
                        connection.execute("INSERT INTO plan_paths VALUES(?,?)",
                                           (job["id"], normalized))
                except sqlite3.IntegrityError:
                    raise MigrationError("path_collision", source_path=path) from None
                kind = node.get("type")
                size = node.get("size") if kind == "file" else 0
                if kind not in {"file", "dir"}:
                    raise MigrationError("unsupported_resource", source_path=path)
                if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                    raise MigrationError("source_size_unavailable", source_path=path)
                entry = {"kind": kind, "source_path": path,
                         "destination_path": target, "size": size,
                         "md5": node.get("md5"), "modified": node.get("modified"),
                         "resource_id": node.get("resource_id")}
                saved_id = self.store.add_entries(job["id"], [entry], append=True)[0]
                metadata = destination.stat(target)
                state, error = "planned", None
                if metadata and metadata.get("type") != kind:
                    state, error = "conflict", "destination_conflict"
                elif kind == "file":
                    if not entry["md5"] or (metadata and not metadata.get("md5")):
                        state, error = "verification_unavailable", "verification_unavailable"
                    elif matching(entry, metadata):
                        state = "already_verified"
                    elif metadata:
                        state, error = "conflict", "destination_conflict"
                    else:
                        required += size
                if state != "planned":
                    self.store.update_entry(saved_id, state=state, error=error)
                unavailable |= state in {"conflict", "verification_unavailable"}
            check()
            quota = self._quota(info)
            if quota is None:
                self.store.event(job["id"], "warning", {"error": "quota_unavailable"})
            self.store.add_entries(job["id"], [], append=True, mark_ready=True)
            if unavailable:
                self.store.update_job(job["id"], state="needs_attention",
                                      error="plan_conflicts")
            elif quota is not None and required > quota:
                self.store.update_job(job["id"], state="needs_attention",
                                      error="destination_space_insufficient")
                raise MigrationError("destination_space_insufficient")
        finally:
            with self.store._connection(write=True) as connection:
                connection.execute("DELETE FROM plan_paths WHERE job_id=?", (job["id"],))

    def require_staged_job(self, job):
        """Starting old URL work requires a new, reviewed staged Dry Run."""
        if job["config"]["strategy"] != "staged" or any(
            entry["state"] not in SUCCESS and entry.get("attempt_id")
            and self.store.attempt(entry["attempt_id"])["strategy"] != "staged"
            for entry in self.store.entries(job["id"], limit=1000000)
        ):
            raise MigrationError("onprem_staged_required")

    def validate_selection(self, job_id):
        self.store.validate_selection(job_id)
        job = self.store.get(job_id)
        files = [e for e in self.store.entries(job_id, limit=1000000)
                 if e["selected"] and e["kind"] == "file" and e["state"] not in SUCCESS]
        required = sum(e["size"] for e in files)
        largest = max((e["size"] for e in files), default=0)
        if required:
            destination = self.destination_client(job)
            info = destination.info()
            self._identity(job["config"], info)
            quota = self._quota(info)
            extra = largest if job["remote_root"] else 0
            if quota is not None and required + extra > quota:
                raise MigrationError("destination_space_insufficient")
        if largest and (
            largest + self._staging_size() > self.staging_budget
            or largest + self.reserve_bytes > shutil.disk_usage(self.staging).free
        ):
            raise MigrationError("staging_space_insufficient")

    def step(self, job_id):
        job = self.store.get(job_id)
        if (job["state"] not in {"queued", "running"}
                or job.get("stop_requested")):
            return
        entries = [e for e in self.store.entries(job_id, limit=1000000)
                   if e["selected"]]
        # Keep saved configuration and attempt evidence; accepted remote work may finish.
        # A stopped/recovered URL job never begins a fresh transfer or Dry Run.
        try:
            self.require_staged_job(job)
        except MigrationError:
            accepted = next((entry for entry in entries if entry["state"] in ACTIVE
                             and entry.get("attempt_id")
                             and not entry.get("point_retry_requested")
                             and self.store.attempt(entry["attempt_id"])["phase"]
                             in ACCEPTED_URL_PHASES), None)
            if job["state"] == "running" and entries and all(
                entry["state"] in SUCCESS for entry in entries
            ):
                self.store.update_job(job_id, state="completed", error=None)
                return
            if not accepted or job["state"] != "running":
                self._job_error(job_id, state="needs_attention", error="onprem_staged_required")
                return
        if (job["state"] == "running" and job["pause_requested"]
                and not any(e["state"] in ACTIVE for e in entries)):
            self.store.update_job(job_id, state="paused")
            return
        point = next((e for e in entries if e.get("point_retry_requested")), None)
        point_start = False
        if point:
            phase = (self.store.attempt(point["attempt_id"])["phase"]
                     if point.get("attempt_id") else "failed")
            if (phase in {"waiting_transfer", "waiting_move"}
                    and self.store.point_retry_origin(point["id"]) == point["attempt_id"]):
                # Older builds switched an unknown check to unlimited polling.
                phase = "unknown_move" if phase == "waiting_move" else "unknown_transfer"
                self.store.update_attempt(point["attempt_id"], phase=phase)
            if phase in UNKNOWN_PHASES and point["state"] != "outcome_unknown":
                self.store.update_entry(point["id"], state="outcome_unknown",
                                        error=point.get("error")
                                        or "transfer_outcome_unknown")
                point = self.store.entry(point["id"])
            point_start = (not point["point_retry_attempts"]
                           or point["state"] in {"outcome_unknown", "failed"}
                           or phase == "failed")
        if point and job["pause_requested"]:
            self.store.update_job(job_id, state="paused")
            return
        if (point and point_start
                and point["point_retry_attempts"] >= self._point_limit(job_id)):
            self._finish_point_retry(job_id, point, exhausted=True)
            return
        waiting = point or next((e for e in entries if e["state"] in ACTIVE), None)
        if (waiting and not point and not self.retry_policy(job_id).enabled
                and waiting.get("attempt_id")
                and self.store.attempt(waiting["attempt_id"])["phase"] == "download_retry_wait"):
            self._failure(job_id, waiting, MigrationError(waiting.get("error") or "source_download_failed"))
            self.store.event(job_id, "transfer_stage", {
                "stage": "download_retry", "status": "skipped", "entry_id": waiting["id"],
                "error": waiting.get("error") or "source_download_failed",
                "download_attempts": waiting["download_attempts"], "max_download_attempts": 1,
                "retry_enabled": False,
            })
            return
        if waiting and (waiting.get("retry_at_ms") or 0) > int(time.time() * 1000):
            return
        entry = point or waiting
        try:
            if point and point_start:
                self.store.update_entry(
                    point["id"], point_retry_attempts=point["point_retry_attempts"] + 1,
                    retry_at_ms=None,
                )
                point = entry = self.store.entry(point["id"])
                self._point_event(job_id, point, "started")
            source, destination = self._logged(
                job, point, "prepare_clients", self.clients, job,
            )
            if job["state"] == "queued":
                self._logged(job, None, "planning", self.plan, job,
                             source, destination)
                return
            if point:
                self._point_file(job, point, source, destination)
                self._finish_point_retry(job_id, point)
                return
            namespace = job.get("namespace_state", "unclaimed")
            if namespace in {"intent", "unknown"}:
                self.store.update_job(job_id, state="needs_attention",
                                      error="technical_namespace_outcome_unknown")
                return
            if job["config"].get("source_public") and not waiting:
                self._prepare_publications(job, source)
            if job["remote_root"] and namespace == "unclaimed":
                self._checkpoint(job_id)
                self.store.update_job(job_id, namespace_state="intent")
                try:
                    self._logged(job, None, "claim_namespace",
                                 destination.claim_dir, job["remote_root"])
                except MigrationError as exc:
                    self.store.update_job(job_id, namespace_state="unknown"
                                          if exc.uncertain else "unclaimed")
                    raise
                self.store.update_job(job_id, namespace_state="owned")
            self._checkpoint(job_id)
            entries = [e for e in self.store.entries(job_id, limit=1000000)
                       if e["selected"]]
            active = [e for e in entries if e["state"] in ACTIVE]
            if active:
                entry = active[0]
            elif job["pause_requested"]:
                self.store.update_job(job_id, state="paused")
                return
            else:
                entry = next((e for e in entries if e["state"] == "planned"), None)
            if not entry:
                if (self.retry_policy(job_id).enabled
                        and any(e["state"] == "outcome_unknown" for e in entries)):
                    self.store.update_job(job_id, state="needs_attention",
                                          error="transfer_outcome_unknown")
                    return
                issues = any(e["state"] not in SUCCESS for e in entries)
                self.store.update_job(
                    job_id, state="completed_with_issues" if issues else "completed",
                    error=None,
                )
                return
            if entry["kind"] == "dir":
                self._checkpoint(job_id)
                self._logged(job, entry, "create_directory", self._ensure_dir,
                             destination, entry["destination_path"])
                self.store.update_entry(entry["id"], state="done")
                return
            self._file(job, entry, source, destination)
        except _StopRequested:
            return
        except MigrationError as exc:
            if exc.auth and not (entry and not self.retry_policy(job_id).enabled
                                 and file_access_denied(exc, getattr(exc, "_migration_stage", ""))):
                if entry and destination_interrupted(exc):
                    if point:
                        self._point_failure(job_id, entry, exc)
                    else:
                        self._failure(job_id, entry, exc)
                self.registry.discard(job_id)
                self._job_error(job_id, state="awaiting_credentials",
                                resume_state="needs_attention" if destination_interrupted(exc)
                                else job["state"], error=exc.code)
            elif entry:
                if point:
                    self._point_failure(job_id, entry, exc)
                else:
                    self._failure(job_id, entry, exc)
            else:
                self._job_error(job_id, state="needs_attention", error=exc.code)
        except Exception as exc:
            if isinstance(exc, ValueError) and str(exc) == "job_stopped":
                return
            # Do not persist raw exceptions: requests can embed bearer URLs.
            if entry:
                entry = self.store.entry(entry["id"])
                attempt = (self.store.attempt(entry["attempt_id"])
                           if entry.get("attempt_id") else None)
                uncertain = bool(attempt and attempt["phase"] in UNKNOWN_PHASES)
                failure = self._point_failure if point else self._failure
                failure(job_id, entry, MigrationError(
                    "internal_error", uncertain=uncertain,
                ))
            else:
                self._job_error(job_id, state="needs_attention",
                                error="internal_error")

    def _point_event(self, job_id, entry, status, **fields):
        self.store.event(job_id, "transfer_stage", {
            "stage": "point_retry", "entry_id": entry["id"],
            "attempt_id": entry["attempt_id"], "status": status,
            "point_retry_attempts": entry["point_retry_attempts"],
            "max_point_retry_attempts": self._point_limit(job_id),
            "retry_enabled": self.retry_policy(job_id).enabled, **fields,
        })

    def _point_file(self, job, entry, source, destination):
        attempt = (self.store.attempt(entry["attempt_id"])
                   if entry.get("attempt_id") else None)
        self.require_staged_job(job)
        origin = self.store.point_retry_origin(entry["id"])
        retry_old = (not attempt or attempt["phase"] in UNKNOWN_PHASES | ABANDONED_PHASES
                     | {"failed"} or attempt["id"] == origin
                     and attempt["phase"] in {"waiting_transfer", "waiting_move"})
        if retry_old:
            self._logged(job, entry, "check_destination_access", self._destination_available,
                         job, destination, attempt=attempt)
            self._checkpoint(job["id"])
            final = self._logged(job, entry, "check_destination",
                                 destination.stat, entry["destination_path"], attempt=attempt)
            if final and matching(entry, final):
                if attempt:
                    self.store.confirm_point_copy(entry["id"])
                else:
                    self.store.update_entry(entry["id"], state="already_verified",
                                            verification="md5", error=None)
                self._logged(job, entry, "cleanup", self.cleanup, job["id"],
                             successes_only=True, attempt=attempt)
                return
            if final and (final.get("type") != "file"
                          or not self._point_overwrite(job, entry)):
                raise MigrationError("destination_conflict")
            reuse = bool(attempt and attempt.get("local_name") and self._local_matches(
                self._local(attempt["local_name"]), entry,
            ))
            if (attempt and attempt["strategy"] == "staged" and attempt.get("local_name")
                    and not reuse and attempt.get("error") not in DOWNLOAD_ERRORS):
                raise MigrationError("staging_file_unavailable")
            self._checkpoint(job["id"])
            strategy = attempt["strategy"] if attempt else job["config"]["strategy"]
            self.store.new_attempt(entry["id"], strategy, point_retry=True, reuse_local=reuse)
        self._file(job, self.store.entry(entry["id"]), source, destination)

    def _destination_available(self, job, destination):
        info = destination.info()
        self._checkpoint(job["id"])
        root = destination.stat("disk:/")
        if not root:
            raise MigrationError("destination_not_found", http_status=404)
        if root.get("type") != "dir":
            raise MigrationError("destination_invalid_response", http_status=200)
        self._identity(job["config"], info)
        self.clear_destination_outage(job["config"])
        return info

    def _point_overwrite(self, job, entry):
        if job["remote_root"] or not entry.get("point_retry_requested"):
            return False
        origin = self.store.point_retry_origin(entry["id"])
        return any(
            old["entry_id"] == entry["id"]
            and (old["id"] != entry.get("attempt_id") or old["id"] == origin)
            and old["remote_path"] == entry["destination_path"]
            and old["phase"] in {"abandoned_transfer", "unknown_transfer",
                                 "transfer_intent", "waiting_transfer"}
            for old in self.store.attempts(job["id"])
        )

    def _current_or_cache(self, entry, source, attempt):
        if (self.store.entry(entry["id"]).get("point_retry_requested")
                and attempt["strategy"] == "staged" and attempt.get("local_name")
                and self._local_matches(self._local(attempt["local_name"]), entry)):
            return True
        return self._current(entry, source)

    def _point_failure(self, job_id, entry, exc):
        self._record_destination_outage(job_id, exc)
        entry = self.store.entry(entry["id"])
        if destination_interrupted(exc):
            self._failure(job_id, entry, exc)
            self._point_event(job_id, entry, "failed", error=exc.code,
                              http_status=exc.http_status, **exc.diagnostics)
            self._finish_point_retry(job_id, entry, exhausted=True)
            if self.retry_policy(job_id).enabled:
                self._job_error(job_id, state="needs_attention", error=exc.code)
            return
        if not entry.get("attempt_id"):
            self.store.update_entry(entry["id"], state="failed", error=exc.code)
            if not self.retry_policy(job_id).enabled:
                self._skip_event(job_id, entry, exc)
            self._finish_point_retry(job_id, entry, exhausted=True)
            return
        attempt = self.store.attempt(entry["attempt_id"])
        uncertain = exc.uncertain
        phase = attempt["phase"]
        if exc.uncertain:
            phase = "unknown_move" if "move" in phase else "unknown_transfer"
        elif not uncertain:
            phase = ("abandoned_move" if "move" in phase else "abandoned_transfer"
                     if phase in SIDE_EFFECT_PHASES | ABANDONED_PHASES else "failed")
        self.store.update_attempt(attempt["id"], phase=phase, error=exc.code)
        self.store.update_entry(entry["id"], state="outcome_unknown" if uncertain
                                else "conflict" if exc.code == "destination_conflict"
                                else "failed", error=exc.code, retry_at_ms=None)
        self._point_event(job_id, entry, "failed", error=exc.code,
                          http_status=exc.http_status, **exc.diagnostics)
        if self.store.get(job_id).get("full_stop_requested"):
            return
        if (exc.code in POINT_RETRY_ERRORS | DOWNLOAD_ERRORS
                and entry["point_retry_attempts"] < self._point_limit(job_id)):
            delay_ms = (10000, 30000)[entry["point_retry_attempts"] - 1]
            retry_at = int(time.time() * 1000) + delay_ms
            self.store.update_entry(entry["id"], retry_at_ms=retry_at)
            self._point_event(job_id, entry, "waiting", error=exc.code,
                              retry_delay_ms=delay_ms, retry_at_ms=retry_at,
                              **exc.diagnostics)
            return
        if not self.retry_policy(job_id).enabled:
            self._skip_event(job_id, entry, exc)
        self._finish_point_retry(job_id, entry, exhausted=True)

    def _finish_point_retry(self, job_id, entry, *, exhausted=False):
        if self.store.get(job_id).get("full_stop_requested"):
            return
        entry = self.store.entry(entry["id"])
        if not exhausted and entry["state"] in ACTIVE | {"outcome_unknown"}:
            return
        self.store.finish_point_retry(entry["id"])

    def _publication_checkpoint(self, job_id):
        self._checkpoint(job_id)
        if self.store.get(job_id)["pause_requested"]:
            self.store.update_job(job_id, state="paused")
            raise _StopRequested()

    def _prepare_publications(self, job, source, *, path=None):
        pending_roots = set()
        for entry in self.store.entries(job["id"], limit=1000000):
            if entry["selected"] and (
                path is not None or entry["state"] in ACTIVE | {"planned"}
            ):
                parent = entry["source_path"]
                while parent.startswith("disk:/"):
                    pending_roots.add(parent)
                    parent = parent.rsplit("/", 1)[0]
        for root in self.store.publications(job["id"]):
            if root["source_path"] not in pending_roots:
                continue
            if path is not None and not (
                path == root["source_path"]
                or path.startswith(root["source_path"] + "/")
            ):
                continue
            if root["state"] == "published" and (
                path is None or source.has_publication(root["source_path"])
            ):
                continue
            self._publication_checkpoint(job["id"])

            def before_publish():
                self._publication_checkpoint(job["id"])
                self.store.update_publication(
                    job["id"], root["entry_id"], state="intent",
                )
                self._checkpoint(job["id"])

            try:
                self._logged(
                    job, dict(root, id=root["entry_id"]), "publish_source",
                    source.ensure_publication, root, checkpoint=before_publish,
                )
            except MigrationError as exc:
                if not exc.uncertain and root["state"] not in {"intent", "published"}:
                    self.store.update_publication(
                        job["id"], root["entry_id"], state="failed", error=exc.code,
                    )
                if not self.retry_policy(job["id"]).enabled and (
                    not exc.auth or file_access_denied(exc, "publish_source")
                ):
                    # Publication can be uncertain; no destination upload has been
                    # submitted for these planned dependants, so they are safe skips.
                    failure = MigrationError(exc.code, http_status=exc.http_status,
                                             diagnostics=exc.diagnostics)
                    failure._migration_stage = "publish_source"
                    self._failure(job["id"], self.store.entry(root["entry_id"]), failure)
                    if path is None:
                        continue
                raise
            if root["state"] != "published":
                self.store.update_publication(
                    job["id"], root["entry_id"], state="published",
                )
            self._publication_checkpoint(job["id"])

    def _skip_event(self, job_id, entry, exc, *, stage=None):
        details = dict(exc.diagnostics)
        if "error_side" not in details:
            if exc.code.startswith(("staging_", "invalid_staging_", "cache_")):
                details["error_side"] = "local"
            elif exc.code.startswith("source_"):
                details["error_side"] = "saas"
            else:
                details["error_side"] = "onprem"
        self.store.event(job_id, "transfer_stage", {
            "stage": stage or getattr(exc, "_migration_stage", "check_source"),
            "status": "skipped", "entry_id": entry["id"], "error": exc.code,
            "http_status": exc.http_status, "retry_enabled": False, **details,
        })

    def _skip_dependants(self, job_id, entry, exc):
        for child in self.store.entries(job_id, limit=1000000):
            if (child["selected"] and child["state"] == "planned"
                    and child["source_path"].startswith(
                        entry["source_path"].rstrip("/") + "/"
                    )):
                self.store.update_entry(child["id"], state="failed", error=exc.code, retry_at_ms=None)
                self._skip_event(job_id, child, exc)

    def _failure(self, job_id, entry, exc):
        self._record_destination_outage(job_id, exc)
        # Fetch the current attempt, which might have been created in this step.
        entry = self.store.entry(entry["id"])
        state = ("outcome_unknown"
                 if exc.uncertain and not destination_interrupted(exc) else "failed")
        if exc.code == "destination_conflict":
            state = "conflict"
        if entry.get("attempt_id"):
            attempt = self.store.attempt(entry["attempt_id"])
            phase = "failed"
            if (destination_interrupted(exc)
                    and attempt["phase"] in SIDE_EFFECT_PHASES | ABANDONED_PHASES):
                phase = "abandoned_move" if "move" in attempt["phase"] else "abandoned_transfer"
                state = "failed"
                self.store.event(job_id, "transfer_stage", {
                    "stage": "abandon_attempt", "status": "interrupted",
                    "entry_id": entry["id"], "attempt_id": attempt["id"],
                    "operation_id": attempt["operation_id"], "error": exc.code,
                    "http_status": exc.http_status, **exc.diagnostics,
                })
            elif destination_interrupted(exc):
                state = "failed"
            elif exc.uncertain:
                phase = ("unknown_move" if "move" in attempt["phase"]
                         else "unknown_transfer")
            self.store.update_attempt(attempt["id"], phase=phase, error=exc.code)
        self.store.update_entry(entry["id"], state=state, error=exc.code,
                                retry_at_ms=None)
        if self.retry_policy(job_id).enabled:
            if exc.uncertain or destination_interrupted(exc):
                self._job_error(job_id, state="needs_attention", error=exc.code)
        elif not exc.auth or file_access_denied(exc, getattr(exc, "_migration_stage", "")):
            self._skip_event(job_id, entry, exc)
            if entry["kind"] == "dir":
                self._skip_dependants(job_id, entry, exc)

    def _current(self, entry, source):
        current = source.stat(entry["source_path"])
        same = matching(entry, current)
        for key in ("modified", "resource_id"):
            if entry.get(key) is not None and entry[key] != current.get(key):
                same = False
        if not entry.get("md5"):
            self.store.update_entry(entry["id"], state="verification_unavailable",
                                    error="source_hash_unavailable")
            return False
        if not same:
            self.store.update_entry(entry["id"], state="source_changed",
                                    error="source_changed")
        return same

    def _verify(self, entry, metadata):
        if not metadata or not metadata.get("md5"):
            self.store.update_entry(entry["id"], state="verification_unavailable",
                                    error="destination_hash_unavailable")
            return False
        if not matching(entry, metadata):
            raise MigrationError("verification_mismatch")
        return True

    @staticmethod
    def _ensure_dir(destination, path):
        try:
            destination.ensure_dir(path)
        except MigrationError as exc:
            if destination_interrupted(exc) or not exc.uncertain:
                raise
            # mkdir is type-checked and non-overwriting; a later explicit retry
            # may reconcile it via stat. It is not an unknown file transfer.
            raise MigrationError("directory_confirmation_pending",
                                 http_status=exc.http_status,
                                 diagnostics=exc.diagnostics) from None

    def _file(self, job, entry, source, destination):
        self._checkpoint(job["id"])
        direct = not job["remote_root"]
        attempt = (self.store.attempt(entry["attempt_id"])
                   if entry.get("attempt_id") else None)

        if ((not attempt and job["config"]["strategy"] != "staged")
                or attempt and attempt["strategy"] != "staged"
                and attempt["phase"] not in ACCEPTED_URL_PHASES):
            raise MigrationError("onprem_staged_required")

        def check_destination():
            final = destination.stat(entry["destination_path"])
            if final and not matching(entry, final):
                if final.get("type") == "file" and self._point_overwrite(job, entry):
                    return None
                raise MigrationError("destination_conflict")
            return final

        if not attempt:
            if not self._logged(job, entry, "check_source", self._current,
                                entry, source):
                return

            final = self._logged(job, entry, "check_destination", check_destination)
            if final:
                self.store.update_entry(entry["id"], state="already_verified",
                                        verification="md5")
                return
            strategy = self.default_strategy or job["config"]["strategy"]
            if strategy != "staged":
                raise MigrationError("onprem_staged_required")
            self._checkpoint(job["id"])
            attempt = self.store.new_attempt(entry["id"], strategy)
            self.store.update_entry(entry["id"], state="transferring", error=None)
        phase = attempt["phase"]
        if phase == "download_retry_wait":
            self.store.update_entry(entry["id"], retry_at_ms=None)
            self.store.update_attempt(attempt["id"], phase="prepared")
            attempt = self.store.attempt(attempt["id"])
            phase = "prepared"
        if phase in UNKNOWN_PHASES:
            raise MigrationError("transfer_outcome_unknown", uncertain=True)
        if phase == "failed":
            self._failure(job["id"], entry,
                          MigrationError(attempt.get("error") or "transfer_failed"))
            return
        if phase == "succeeded":
            # Crash after the attempt checkpoint but before the entry checkpoint.
            phase = "verifying_final"
        if direct and phase in {"prepared", "downloading", "staged"}:
            final = self._logged(job, entry, "check_destination", check_destination,
                                 attempt=attempt)
            if final:
                phase = "verifying_final"
                self.store.update_attempt(attempt["id"], phase=phase)
        if phase in {"prepared", "downloading", "staged"}:
            if not self._logged(job, entry, "check_source", self._current_or_cache,
                                entry, source, attempt, attempt=attempt):
                self.store.update_attempt(attempt["id"], phase="failed",
                                          error="source_changed")
                return
            self._checkpoint(job["id"])
            self._logged(job, entry, "prepare_upload_directory", self._ensure_dir,
                         destination, attempt["remote_path"].rsplit("/", 1)[0],
                         attempt=attempt)
            if attempt["strategy"] == "staged":
                try:
                    local = self._logged(job, entry, "downloading", self._stage,
                                         entry, attempt, source, attempt=attempt)
                except MigrationError as exc:
                    if self._download_failure(job, entry, attempt, exc):
                        return
                    raise
                if not self._logged(job, entry, "check_source", self._current_or_cache,
                                    entry, source, attempt, attempt=attempt):
                    self.store.update_attempt(attempt["id"], phase="failed",
                                              error="source_changed")
                    return
                self._checkpoint(job["id"])

                def before_upload(reference):
                    self.store.update_attempt(attempt["id"], phase="transfer_intent",
                                              operation_id=reference)
                    self._checkpoint(job["id"])

                with self._measure_transfer(job["id"], "upload") as progress:
                    overwrite = {"overwrite": True} if self._point_overwrite(job, entry) else {}
                    operation = self._logged(
                        job, entry, "upload_file", destination.upload,
                        attempt["remote_path"], local, before_upload=before_upload,
                        on_progress=progress, attempt=attempt, **overwrite,
                    )
            else:
                raise MigrationError("onprem_staged_required")
            phase = "waiting_transfer" if operation else "verifying"
            self.store.update_attempt(attempt["id"], phase=phase,
                                      operation_id=operation)
            self.store.update_entry(entry["id"], state="waiting_operation"
                                    if operation else "verifying")
            attempt = self.store.attempt(attempt["id"])
            self._checkpoint(job["id"])
        if phase in {"waiting_transfer", "waiting_move"}:
            self._checkpoint(job["id"])
            self._logged(job, entry, "check_destination_access", self._destination_available,
                         job, destination, attempt=attempt)
            try:
                status = self._logged(
                    job, entry, "poll_move" if phase == "waiting_move"
                    else "poll_transfer", destination.poll,
                    attempt["operation_id"], attempt=attempt,
                )
            except MigrationError:
                raise
            except Exception:
                raise MigrationError("destination_invalid_response",
                                     diagnostics={"error_side": "onprem"}) from None
            if status == "in-progress":
                return
            if status == "failed":
                remote = getattr(destination, "last_operation_diagnostics", {})
                exc = MigrationError("destination_url_import_failed"
                                     if phase == "waiting_transfer"
                                     and attempt["strategy"] == "url"
                                     else "remote_operation_failed",
                                     http_status=remote.get("http_status"),
                                     diagnostics=remote)
                if self.store.get(job["id"]).get("full_stop_requested"):
                    self._failure(job["id"], entry, exc)
                    self._checkpoint(job["id"])
                if (phase == "waiting_transfer" and attempt["strategy"] == "url"
                        and self._download_failure(job, entry, attempt, exc,
                                                   fresh_attempt=True)):
                    return
                raise exc
            if status != "success":
                raise MigrationError("invalid_operation_status", uncertain=True)
            phase = "verifying" if phase == "waiting_transfer" else "verifying_final"
            self.store.update_attempt(
                attempt["id"], phase=phase,
                operation_id=attempt["operation_id"]
                if self.store.get(job["id"]).get("full_stop_requested") else None,
            )
            self._checkpoint(job["id"])
        if direct and phase == "verifying":
            phase = "verifying_final"
            self.store.update_attempt(attempt["id"], phase=phase)
        if phase == "verifying":
            if not self._logged(
                job, entry, "verify_temporary",
                lambda: self._verify(entry, destination.stat(attempt["remote_path"])),
                attempt=attempt,
            ):
                self.store.update_attempt(attempt["id"], phase="failed",
                                          error="destination_hash_unavailable")
                return
            if not self._logged(job, entry, "check_source", self._current_or_cache,
                                entry, source, attempt, attempt=attempt):
                self.store.update_attempt(attempt["id"], phase="failed",
                                          error="source_changed")
                return
            self._checkpoint(job["id"])
            self._logged(job, entry, "prepare_destination_directory", self._ensure_dir,
                         destination, entry["destination_path"].rsplit("/", 1)[0],
                         attempt=attempt)
            self._checkpoint(job["id"])
            self.store.update_attempt(attempt["id"], phase="move_intent")
            self.store.update_entry(entry["id"], state="committing")
            operation = self._logged(job, entry, "move_file", destination.move,
                                     attempt["remote_path"], entry["destination_path"],
                                     attempt=attempt)
            phase = "waiting_move" if operation else "verifying_final"
            self.store.update_attempt(attempt["id"], phase=phase,
                                      operation_id=operation)
            self._checkpoint(job["id"])
            if operation:
                return
        if phase == "verifying_final":
            if not self._logged(
                job, entry, "verify_destination",
                lambda: self._verify(
                    entry, destination.stat(entry["destination_path"]),
                ),
                attempt=attempt,
            ):
                self.store.update_attempt(attempt["id"], phase="failed",
                                          error="destination_hash_unavailable")
                return
            if not self._logged(job, entry, "check_source", self._current_or_cache,
                                entry, source, attempt, attempt=attempt):
                self.store.update_attempt(attempt["id"], phase="failed",
                                          error="source_changed")
                return
            self.store.update_attempt(attempt["id"], phase="succeeded", error=None)
            self.store.update_entry(entry["id"], state="done", verification="md5",
                                    error=None, retry_at_ms=None)
            self._logged(job, entry, "cleanup", self.cleanup, job["id"],
                         successes_only=True, attempt=attempt)

    def _staging_size(self):
        return sum(p.stat().st_size for p in self.staging.iterdir()
                   if p.is_file() and not p.is_symlink())

    def _local(self, name):
        if not name or "/" in name or "\\" in name or name in {".", ".."}:
            raise MigrationError("invalid_staging_object")
        path = self.staging / name
        if path.is_symlink():
            raise MigrationError("invalid_staging_object")
        return path

    @staticmethod
    def _local_matches(path, entry):
        if not path.is_file():
            return False
        digest = hashlib.md5()
        size = 0
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                size += len(chunk)
                digest.update(chunk)
        return size == entry["size"] and digest.hexdigest() == entry["md5"].lower()

    def _begin_download(self, entry):
        current = self.store.entry(entry["id"])
        if current["download_attempts"] >= self._download_limit(entry["job_id"]):
            raise MigrationError(current.get("error") or "source_stream_interrupted")
        self._checkpoint(entry["job_id"])
        self.store.update_entry(
            entry["id"], download_attempts=current["download_attempts"] + 1,
            retry_at_ms=None,
        )

    def _download_failure(self, job, entry, attempt, exc, *, fresh_attempt=False):
        if attempt["strategy"] != "staged":
            return False
        if self.store.entry(entry["id"]).get("point_retry_requested"):
            return False
        if exc.auth:
            current = self.store.entry(entry["id"])
            self.store.update_entry(entry["id"], download_attempts=max(
                0, current["download_attempts"] - 1,
            ))
            return False
        if exc.uncertain or (
            exc.code not in DOWNLOAD_ERRORS and not fresh_attempt
        ):
            return False
        self._checkpoint(job["id"])
        current = self.store.entry(entry["id"])
        if attempt.get("local_name") or attempt["strategy"] == "staged":
            tracked = self.store.attempt(attempt["id"])
            if tracked.get("local_name"):
                self._local(tracked["local_name"]).unlink(missing_ok=True)
                self.store.update_attempt(attempt["id"], local_name=None)
        payload = {
            "stage": "download_retry", "entry_id": entry["id"],
            "attempt_id": attempt["id"], "error": exc.code,
            "download_attempts": current["download_attempts"],
            "max_download_attempts": self._download_limit(job["id"]),
            "retry_enabled": self.retry_policy(job["id"]).enabled,
            **exc.diagnostics,
        }
        if exc.http_status is not None:
            payload["http_status"] = exc.http_status
        if current["download_attempts"] >= self._download_limit(job["id"]):
            self.store.event(job["id"], "transfer_stage", {
                **payload, "status": "skipped",
            })
            return False
        delay = 2 ** max(1, current["download_attempts"])
        delay += random.uniform(0, delay / 4)
        delay = max(delay, getattr(exc, "retry_after", None) or 0)
        now_ms = int(time.time() * 1000)
        delay_ms = min(int(min(delay, (2 ** 63 - 1) // 1000) * 1000),
                       2 ** 63 - 1 - now_ms)
        retry_at = now_ms + delay_ms
        if fresh_attempt:
            previous = attempt
            if not job["remote_root"]:
                self.store.update_attempt(previous["id"], phase="failed", error=exc.code)
            attempt = self.store.new_attempt(entry["id"], attempt["strategy"])
            if job["remote_root"]:
                self.store.update_attempt(previous["id"], phase="failed", error=exc.code)
        scheduled = self.store.schedule_download_retry(
            entry["id"], attempt["id"], retry_at, {**payload, "retry_delay_ms": delay_ms},
        )
        if not scheduled:
            self.store.event(job["id"], "transfer_stage", {
                **payload, "status": "skipped",
                "retry_enabled": self.retry_policy(job["id"]).enabled,
                "max_download_attempts": self._download_limit(job["id"]),
            })
        return scheduled

    def _stage(self, entry, attempt, source):
        if attempt.get("local_name"):
            existing = self._local(attempt["local_name"])
            if self._local_matches(existing, entry):
                self.store.update_attempt(attempt["id"], phase="staged")
                return existing
            if (attempt["phase"] == "staged" and not attempt.get("operation_id")
                    and not existing.exists()):
                # Recover a purge interrupted after unlink but before the DB update.
                # Only a completed local download starts a fresh download budget;
                # pending remote writes never enter this pre-submission branch.
                self.store.update_entry(entry["id"], download_attempts=0)
            existing.unlink(missing_ok=True)
        name = uuid.uuid4().hex + ".part"
        path = self._local(name)
        with self.reserve_cache(name, entry["size"]):
            return self._download_stage(entry, attempt, source, name, path)

    @contextmanager
    def reserve_cache(self, name, size):
        """Reserve future bytes atomically without double-counting written chunks."""
        with self._cache_lock:
            reserved = self._reserved_cache_bytes()
            available = shutil.disk_usage(self.staging).free - self.reserve_bytes
            if (size + reserved > available
                    or self._staging_size() + reserved + size > self.staging_budget):
                raise MigrationError("staging_space_insufficient")
            self._cache_reservations[name] = size
        try:
            yield
        finally:
            with self._cache_lock:
                self._cache_reservations.pop(name, None)

    def _reserved_cache_bytes(self):
        remaining = 0
        for name, size in self._cache_reservations.items():
            try:
                written = self._local(name).stat().st_size
            except FileNotFoundError:
                written = 0
            remaining += max(0, size - written)
        return remaining

    def _download_stage(self, entry, attempt, source, name, path):
        self._begin_download(entry)
        self.store.update_attempt(attempt["id"], phase="downloading", local_name=name)
        self.store.update_entry(entry["id"], state="downloading")
        digest, size = hashlib.md5(), 0
        job = self.store.get(entry["job_id"])
        if job["config"].get("source_public"):
            self._prepare_publications(job, source, path=entry["source_path"])
            url = self._logged(
                job, entry, "public_download_link",
                source.public_download_url, entry["source_path"], attempt=attempt,
            )
            chunks = source.chunks(entry["source_path"], download_url=url)
        else:
            chunks = source.chunks(entry["source_path"])
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "wb") as stream, self._measure_transfer(
                entry["job_id"], "download",
            ) as progress:
                for chunk in chunks:
                    self._checkpoint(entry["job_id"])
                    size += len(chunk)
                    if size > entry["size"]:
                        raise MigrationError("source_content_changed")
                    if (self._staging_size() + len(chunk) > self.staging_budget
                            or shutil.disk_usage(self.staging).free
                            < self.reserve_bytes + len(chunk)):
                        raise MigrationError("staging_space_insufficient")
                    stream.write(chunk)
                    progress(len(chunk))
                    digest.update(chunk)
                stream.flush()
                os.fsync(stream.fileno())
        except OSError:
            raise MigrationError("staging_io_failed") from None
        finally:
            if hasattr(chunks, "close"):
                chunks.close()
        if size != entry["size"] or digest.hexdigest() != entry["md5"].lower():
            raise MigrationError("source_content_changed")
        self.store.update_attempt(attempt["id"], phase="staged")
        self.store.update_entry(entry["id"], state="staged", error=None)
        return path

    def retry(self, job_id, strategy):
        job = self.store.get(job_id)
        self.require_staged_job(job)
        if strategy != "staged":
            raise MigrationError("onprem_staged_required")
        if job.get("point_retry_active"):
            raise ValueError("point_retry_in_progress")
        allowed = {"completed_with_issues", "needs_attention", "paused"}
        state = (job.get("resume_state") if job["state"] == "awaiting_credentials"
                 else job["state"])
        if state not in allowed:
            raise ValueError("job_not_retryable")
        if job["selection"]["editable"]:
            self.validate_selection(job_id)
            self.store.freeze_selection(job_id)
            self.store.revalidate_verified_files(job_id)
        entries = self.store.entries(job_id, limit=1000000)
        if any(e["state"] in ACTIVE or e["state"] == "outcome_unknown" for e in entries):
            raise ValueError("active_or_unknown_attempt")
        if any(e["state"] in RETRYABLE and e.get("attempt_id")
               and self.store.attempt(e["attempt_id"])["phase"] in ABANDONED_PHASES
               for e in entries):
            raise ValueError("point_retry_required")
        current = self.store.get(job_id)
        with self._outage_lock:
            blocked = self._destination_key(current["config"]) in self._destination_outages
        if blocked:
            self._logged(current, None, "check_destination_access",
                         self._destination_available, current,
                         self.destination_client(current))
        for entry in entries:
            if entry["selected"] and entry["state"] in RETRYABLE:
                if entry["kind"] == "file":
                    old = (self.store.attempt(entry["attempt_id"])
                           if entry.get("attempt_id") else None)
                    new = self.store.new_attempt(entry["id"], strategy)
                    if strategy == "staged" and old and old.get("local_name"):
                        local = self._local(old["local_name"])
                        if self._local_matches(local, entry):
                            self.store.update_attempt(new["id"], phase="staged",
                                                      local_name=old["local_name"])
                            self.store.update_attempt(old["id"], local_name=None)
                self.store.update_entry(entry["id"], state="planned", error=None,
                                        download_attempts=0, retry_at_ms=None)
        self.store.update_job(
            job_id, state="running", resume_state=None,
            pause_requested=False, error=None,
        )

    def _confirm_cached_downloads(self, cache_fd, objects):
        """Recover fsync-before-checkpoint crashes before unlinking valid copies."""
        regular = {name for name, info in objects if stat.S_ISREG(info.st_mode)}
        for attempt in self.store.pending_cache_downloads():
            if attempt["local_name"] not in regular or not attempt["md5"]:
                continue
            try:
                fd = os.open(attempt["local_name"], os.O_RDONLY | os.O_NOFOLLOW,
                             dir_fd=cache_fd)
            except FileNotFoundError:
                continue
            with os.fdopen(fd, "rb") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_size != attempt["size"]:
                    continue
                digest = hashlib.md5()
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest() == attempt["md5"].lower():
                # Persist this proof before unlink. A crash after unlink can then
                # recover in _stage without relaxing retries for partial downloads.
                self.store.update_attempt(attempt["id"], phase="staged")

    def cleanup_cache(self):
        with self._cache_lock:
            if self._cache_reservations:
                raise MigrationError("cache_in_use")
            return self._cleanup_cache()

    def _cleanup_cache(self):
        """Purge the shared staging cache while the service's worker guard is held."""
        if self.store.cache_in_use():
            raise MigrationError("cache_in_use")
        if self.staging != Path(self.store.root) / "staging":
            raise MigrationError("invalid_staging_object")
        root_fd = cache_fd = None
        try:
            flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
            if not stat.S_ISDIR(Path(self.store.root).lstat().st_mode):
                raise MigrationError("invalid_staging_object")
            root_fd = os.open(self.store.root, flags)
            if not stat.S_ISDIR(os.stat(
                "staging", dir_fd=root_fd, follow_symlinks=False,
            ).st_mode):
                raise MigrationError("invalid_staging_object")
            cache_fd = os.open("staging", flags, dir_fd=root_fd)
            with os.scandir(cache_fd) as scan:
                objects = [(item.name, item.stat(follow_symlinks=False)) for item in scan]
            # Validate the entire flat directory first. Never descend into unexpected
            # folders or follow a link; DB-provided local_name is not a deletion path.
            if any(not (stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode))
                   for _, info in objects):
                raise MigrationError("invalid_staging_object")
            self._confirm_cached_downloads(cache_fd, objects)
            cleared = self.store.cache_references() - {name for name, _ in objects}
            removed = removed_bytes = 0
            try:
                for name, info in sorted(objects):
                    try:
                        os.unlink(name, dir_fd=cache_fd)
                    except FileNotFoundError:
                        cleared.add(name)
                        continue
                    cleared.add(name)
                    removed += 1
                    if stat.S_ISREG(info.st_mode):
                        removed_bytes += info.st_size
            except OSError:
                # A partial failure is an error, but already removed copies must not
                # consume the download budget of a later resume.
                self.store.clear_cache_references(cleared, failed=True)
                raise
            self.store.clear_cache_references()
            return {"removed": removed, "removed_bytes": removed_bytes}
        except (OSError, sqlite3.Error):
            raise MigrationError("cache_cleanup_failed") from None
        finally:
            if cache_fd is not None:
                os.close(cache_fd)
            if root_fd is not None:
                os.close(root_fd)

    def cleanup(self, job_id, successes_only=False):
        removed = 0
        attempts = self.store.attempts(job_id)
        for attempt in attempts:
            entry = self.store.entry(attempt["entry_id"])
            terminal = attempt["phase"] in {"succeeded", "failed"} or (
                attempt["phase"] in ABANDONED_PHASES and entry["state"] in SUCCESS
            )
            if (not terminal or entry["state"] in ACTIVE
                    or entry["state"] == "outcome_unknown"
                    or (successes_only and entry["state"] not in SUCCESS)):
                continue
            if attempt.get("local_name"):
                path = self._local(attempt["local_name"])
                if path.is_file():
                    path.unlink()
                    removed += 1
                self.store.update_attempt(attempt["id"], local_name=None)
        return {"removed": removed,
                "remote_root": self.store.get(job_id)["remote_root"],
                "remote_attempts": [
                    {k: a[k] for k in ("id", "remote_path", "phase", "entry_id")}
                    for a in attempts
                ]}
