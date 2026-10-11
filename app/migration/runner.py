"""Plan and execute a durable, non-overwriting personal-disk migration."""

from __future__ import annotations

import hashlib
import os
import shutil
import unicodedata
import uuid
from pathlib import Path

from .destination import OnPremDiskDestination
from .errors import MigrationError
from .source import SaasDiskSource


ACTIVE = {"transferring", "waiting_operation", "verifying", "committing",
          "downloading", "staged"}
UNKNOWN_PHASES = {"transfer_intent", "move_intent", "unknown_transfer",
                  "unknown_move"}
FINAL_PENDING = {"move_intent", "waiting_move", "unknown_move"}
STOP_ISOLATED_PHASES = {
    "prepared", "downloading", "staged", "transfer_intent", "waiting_transfer",
    "unknown_transfer", "verifying", "verifying_final", "succeeded", "failed",
}
SUCCESS = {"done", "already_verified"}
RETRYABLE = {"failed"}


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
        self.default_strategy = None

    def close(self):
        for _, source, destination in self._clients.values():
            if source:
                source.close()
            destination.close()
        self._clients.clear()

    def final_pending(self, job_id):
        return [attempt for attempt in self.store.attempts(job_id)
                if attempt["phase"] not in STOP_ISOLATED_PHASES]

    def stop_job(self, job_id):
        self.store.update_job(job_id, state="stopped", resume_state=None,
                              pause_requested=False)
        if not self.final_pending(job_id):
            self.release_clients(job_id)

    def release_clients(self, job_id):
        self.registry.discard(job_id)
        current = self._clients.pop(job_id, None)
        if current:
            if current[1]:
                current[1].close()
            current[2].close()

    def _checkpoint(self, job_id):
        job = self.store.get(job_id)
        if job.get("stop_requested") or job["state"] == "stopped":
            raise _StopRequested()

    def _job_error(self, job_id, **fields):
        try:
            self.store.update_job(job_id, **fields)
        except ValueError as exc:
            if str(exc) != "job_stopped":
                raise

    def reconcile_stopped_move(self, job_id):
        job = self.store.get(job_id)
        if job["state"] != "stopped":
            return
        waiting = [a for a in self.final_pending(job_id)
                   if a["phase"] == "waiting_move"]
        if not waiting or not self.registry.get(job_id):
            return
        try:
            destination = self.destination_client(job)
            for attempt in waiting:
                status = destination.poll(attempt["operation_id"])
                if status == "in-progress":
                    continue
                if status not in {"success", "failed"}:
                    self.store.update_attempt(attempt["id"], phase="unknown_move",
                                              error="invalid_operation_status")
                    entry = self.store.entry(attempt["entry_id"])
                    if entry["attempt_id"] == attempt["id"]:
                        self.store.update_entry(entry["id"], state="outcome_unknown",
                                                error="invalid_operation_status")
                    continue
                self.store.update_attempt(
                    attempt["id"], phase="verifying_final" if status == "success"
                    else "failed",
                    error=None if status == "success" else "remote_operation_failed",
                )
                entry = self.store.entry(attempt["entry_id"])
                if entry["attempt_id"] == attempt["id"] and status == "failed":
                    self.store.update_entry(entry["id"], state="failed",
                                            error="remote_operation_failed")
        except MigrationError as exc:
            if exc.auth:
                self.registry.discard(job_id)
            self.store.event(job_id, "polling_unavailable", {"error": exc.code})
        except Exception:
            self.store.event(job_id, "polling_unavailable", {
                "error": "polling_unavailable",
            })
        if not self.final_pending(job_id):
            self.release_clients(job_id)

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
            current = (credentials, None, destination)
            self._clients[job["id"]] = current
        return current[2]

    def clients(self, job):
        destination = self.destination_client(job)
        current = self._clients[job["id"]]
        if not current[1]:
            source = self.source_factory(job["config"], current[0])
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
                    "destination": {"identity": config["destination_identity"],
                                    "free_bytes": quota},
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
            return {"identity": config["destination_identity"],
                    "free_bytes": self._quota(info)}
        finally:
            destination.close()

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

    def plan(self, job, source, destination):
        self._checkpoint(job["id"])
        self.store.update_job(job["id"], state="planning", error=None)
        config = job["config"]
        info = destination.info()
        self._identity(config, info)
        root = config["source_path"]
        root_meta = source.stat(root)
        if root_meta.get("type") not in {"dir", "file"}:
            raise MigrationError("unsupported_resource")
        if destination.stat(job["remote_root"]) is not None:
            raise MigrationError("technical_namespace_exists")
        base = root.rsplit("/", 1)[0] if root != "disk:/" else "disk:"
        entries, seen = [], set()
        for node in source.walk(root):
            self._checkpoint(job["id"])
            path = disk_path(node["path"])
            if path == "disk:/":
                continue
            if root != "disk:/" and path != root and not path.startswith(root + "/"):
                raise MigrationError("invalid_source_tree")
            if not path.startswith(base + "/"):
                raise MigrationError("invalid_source_tree")
            target = join_path(config["destination_root"], path[len(base) + 1:])
            technical = job["remote_root"]
            if (target == technical or target.startswith(technical + "/")
                    or technical.startswith(target + "/")):
                raise MigrationError("technical_path_collision")
            normalized = unicodedata.normalize("NFC", target)
            if normalized in seen:
                raise MigrationError("path_collision")
            seen.add(normalized)
            kind = node.get("type")
            if kind not in {"file", "dir"}:
                raise MigrationError("unsupported_resource")
            size = node.get("size") if kind == "file" else 0
            if kind == "file" and (not isinstance(size, int) or size < 0):
                raise MigrationError("source_size_unavailable")
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
        if quota is not None and required + largest > quota:
            raise MigrationError("destination_space_insufficient")
        if quota is None:
            self.store.event(job["id"], "warning", {"error": "quota_unavailable"})
        self._checkpoint(job["id"])
        self.store.add_entries(job["id"], entries, mark_ready=True)

    def step(self, job_id):
        job = self.store.get(job_id)
        if (job["state"] not in {"queued", "running"}
                or job.get("stop_requested")):
            return
        entry = None
        try:
            source, destination = self.clients(job)
            if job["state"] == "queued":
                self.plan(job, source, destination)
                return
            namespace = job.get("namespace_state", "unclaimed")
            if namespace in {"intent", "unknown"}:
                self.store.update_job(job_id, state="needs_attention",
                                      error="technical_namespace_outcome_unknown")
                return
            if namespace == "unclaimed":
                self._checkpoint(job_id)
                self.store.update_job(job_id, namespace_state="intent")
                try:
                    destination.claim_dir(job["remote_root"])
                except MigrationError as exc:
                    self.store.update_job(job_id, namespace_state="unknown"
                                          if exc.uncertain else "unclaimed")
                    raise
                self.store.update_job(job_id, namespace_state="owned")
            self._checkpoint(job_id)
            entries = self.store.entries(job_id, limit=1000000)
            if any(e["state"] == "outcome_unknown" for e in entries):
                self.store.update_job(job_id, state="needs_attention",
                                      error="transfer_outcome_unknown")
                return
            active = [e for e in entries if e["state"] in ACTIVE]
            if active:
                entry = active[0]
            elif job["pause_requested"]:
                self.store.update_job(job_id, state="paused")
                return
            else:
                entry = next((e for e in entries if e["state"] == "planned"), None)
            if not entry:
                issues = any(e["state"] not in SUCCESS for e in entries)
                self.store.update_job(
                    job_id, state="completed_with_issues" if issues else "completed",
                    error=None,
                )
                return
            if entry["kind"] == "dir":
                self._checkpoint(job_id)
                self._ensure_dir(destination, entry["destination_path"])
                self.store.update_entry(entry["id"], state="done")
                return
            self._file(job, entry, source, destination)
        except _StopRequested:
            return
        except MigrationError as exc:
            if exc.auth:
                self.registry.discard(job_id)
                self._job_error(job_id, state="awaiting_credentials",
                                resume_state=job["state"], error=exc.code)
            elif entry:
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
                self._failure(job_id, entry, MigrationError(
                    "internal_error", uncertain=uncertain,
                ))
            else:
                self._job_error(job_id, state="needs_attention",
                                error="internal_error")

    def _failure(self, job_id, entry, exc):
        # Fetch the current attempt, which might have been created in this step.
        entry = self.store.entry(entry["id"])
        state = "outcome_unknown" if exc.uncertain else "failed"
        if exc.code == "destination_conflict":
            state = "conflict"
        if entry.get("attempt_id"):
            attempt = self.store.attempt(entry["attempt_id"])
            phase = "failed"
            if exc.uncertain:
                phase = ("unknown_move" if "move" in attempt["phase"]
                         else "unknown_transfer")
            self.store.update_attempt(attempt["id"], phase=phase, error=exc.code)
        self.store.update_entry(entry["id"], state=state, error=exc.code)
        if exc.uncertain:
            self._job_error(job_id, state="needs_attention", error=exc.code)

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
            if not exc.uncertain:
                raise
            # mkdir is type-checked and non-overwriting; a later explicit retry
            # may reconcile it via stat. It is not an unknown file transfer.
            raise MigrationError("directory_confirmation_pending") from None

    def _file(self, job, entry, source, destination):
        self._checkpoint(job["id"])
        attempt = (self.store.attempt(entry["attempt_id"])
                   if entry.get("attempt_id") else None)
        if not attempt:
            if not self._current(entry, source):
                return
            final = destination.stat(entry["destination_path"])
            if final:
                if matching(entry, final):
                    self.store.update_entry(entry["id"], state="already_verified",
                                            verification="md5")
                else:
                    self.store.update_entry(entry["id"], state="conflict",
                                            error="destination_conflict")
                return
            strategy = self.default_strategy or job["config"]["strategy"]
            self._checkpoint(job["id"])
            attempt = self.store.new_attempt(entry["id"], strategy)
            self.store.update_entry(entry["id"], state="transferring", error=None)
        phase = attempt["phase"]
        if phase in UNKNOWN_PHASES:
            raise MigrationError("transfer_outcome_unknown", uncertain=True)
        if phase == "failed":
            self._failure(job["id"], entry,
                          MigrationError(attempt.get("error") or "transfer_failed"))
            return
        if phase == "succeeded":
            # Crash after the attempt checkpoint but before the entry checkpoint.
            phase = "verifying_final"
        if phase in {"prepared", "downloading", "staged"}:
            if not self._current(entry, source):
                self.store.update_attempt(attempt["id"], phase="failed",
                                          error="source_changed")
                return
            self._checkpoint(job["id"])
            self._ensure_dir(destination, attempt["remote_path"].rsplit("/", 1)[0])
            if attempt["strategy"] == "staged":
                local = self._stage(entry, attempt, source)
                if not self._current(entry, source):
                    self.store.update_attempt(attempt["id"], phase="failed",
                                              error="source_changed")
                    return
                self._checkpoint(job["id"])
                self.store.update_attempt(attempt["id"], phase="transfer_intent")
                operation = destination.upload(attempt["remote_path"], local)
            else:
                url = source.download_url(entry["source_path"])
                self._checkpoint(job["id"])
                self.store.update_attempt(attempt["id"], phase="transfer_intent")
                operation = destination.import_url(attempt["remote_path"], url)
            phase = "waiting_transfer" if operation else "verifying"
            self.store.update_attempt(attempt["id"], phase=phase,
                                      operation_id=operation)
            self.store.update_entry(entry["id"], state="waiting_operation"
                                    if operation else "verifying")
            attempt = self.store.attempt(attempt["id"])
            self._checkpoint(job["id"])
        if phase in {"waiting_transfer", "waiting_move"}:
            self._checkpoint(job["id"])
            try:
                status = destination.poll(attempt["operation_id"])
            except MigrationError as exc:
                if exc.auth:
                    raise
                self.store.update_attempt(attempt["id"], error=exc.code)
                self.store.event(job["id"], "polling_unavailable", {
                    "attempt_id": attempt["id"], "error": exc.code,
                })
                return
            except Exception:
                self.store.update_attempt(attempt["id"], error="polling_unavailable")
                return
            if status == "in-progress":
                return
            if status == "failed":
                raise MigrationError("remote_operation_failed")
            if status != "success":
                raise MigrationError("invalid_operation_status", uncertain=True)
            phase = "verifying" if phase == "waiting_transfer" else "verifying_final"
            self.store.update_attempt(attempt["id"], phase=phase, operation_id=None)
            self._checkpoint(job["id"])
        if phase == "verifying":
            if not self._verify(entry, destination.stat(attempt["remote_path"])):
                self.store.update_attempt(attempt["id"], phase="failed",
                                          error="destination_hash_unavailable")
                return
            if not self._current(entry, source):
                self.store.update_attempt(attempt["id"], phase="failed",
                                          error="source_changed")
                return
            self._checkpoint(job["id"])
            self._ensure_dir(destination, entry["destination_path"].rsplit("/", 1)[0])
            self._checkpoint(job["id"])
            self.store.update_attempt(attempt["id"], phase="move_intent")
            self.store.update_entry(entry["id"], state="committing")
            operation = destination.move(attempt["remote_path"],
                                         entry["destination_path"])
            phase = "waiting_move" if operation else "verifying_final"
            self.store.update_attempt(attempt["id"], phase=phase,
                                      operation_id=operation)
            self._checkpoint(job["id"])
            if operation:
                return
        if phase == "verifying_final":
            if not self._verify(entry, destination.stat(entry["destination_path"])):
                self.store.update_attempt(attempt["id"], phase="failed",
                                          error="destination_hash_unavailable")
                return
            if not self._current(entry, source):
                self.store.update_attempt(attempt["id"], phase="failed",
                                          error="source_changed")
                return
            self.store.update_attempt(attempt["id"], phase="succeeded", error=None)
            self.store.update_entry(entry["id"], state="done", verification="md5",
                                    error=None)
            self.cleanup(job["id"], successes_only=True)

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

    def _stage(self, entry, attempt, source):
        if attempt.get("local_name"):
            existing = self._local(attempt["local_name"])
            if attempt["phase"] == "staged" and self._local_matches(existing, entry):
                return existing
            existing.unlink(missing_ok=True)
        available = shutil.disk_usage(self.staging).free - self.reserve_bytes
        if (entry["size"] > available
                or self._staging_size() + entry["size"] > self.staging_budget):
            raise MigrationError("staging_space_insufficient")
        name = uuid.uuid4().hex + ".part"
        path = self._local(name)
        self.store.update_attempt(attempt["id"], phase="downloading", local_name=name)
        self.store.update_entry(entry["id"], state="downloading")
        digest, size = hashlib.md5(), 0
        chunks = source.chunks(entry["source_path"])
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "wb") as stream:
                for chunk in chunks:
                    self._checkpoint(entry["job_id"])
                    size += len(chunk)
                    if (size > entry["size"]
                            or self._staging_size() + len(chunk) > self.staging_budget
                            or shutil.disk_usage(self.staging).free
                            < self.reserve_bytes + len(chunk)):
                        raise MigrationError("staging_space_insufficient")
                    stream.write(chunk)
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
        self.store.update_entry(entry["id"], state="staged")
        return path

    def retry(self, job_id, strategy):
        job = self.store.get(job_id)
        allowed = {"completed_with_issues", "needs_attention", "paused"}
        if job["state"] not in allowed:
            raise ValueError("job_not_retryable")
        if any(e["state"] in ACTIVE or e["state"] == "outcome_unknown"
               for e in self.store.entries(job_id, limit=1000000)):
            raise ValueError("active_or_unknown_attempt")
        for entry in self.store.entries(job_id, limit=1000000):
            if entry["state"] in RETRYABLE:
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
                self.store.update_entry(entry["id"], state="planned", error=None)
        self.store.update_job(
            job_id, state="running", pause_requested=False, error=None,
        )

    def cleanup(self, job_id, successes_only=False):
        removed = 0
        attempts = self.store.attempts(job_id)
        for attempt in attempts:
            entry = self.store.entry(attempt["entry_id"])
            terminal = attempt["phase"] in {"succeeded", "failed"}
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
