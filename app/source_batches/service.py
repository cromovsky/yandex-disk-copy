"""Shared-lane list coordinator and read-only, durable local Dry Run."""

from __future__ import annotations

import csv
import hashlib
import io
import os
import re
import shutil
import tempfile
import threading
import time
import unicodedata
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from itertools import combinations, groupby
from pathlib import Path

from app.migration.api import personal_available, shared_service_available
from app.migration.credentials import credential_fields
from app.migration.destination import bound_oauth_url
from app.migration.errors import MigrationError
from app.migration.runner import SaasDiskSource, disk_path, join_path, file_access_denied
from app.migration.source import shared_source_folder
from app.run_journal import CSV_FIELDS, JournalError, Redactor
from .store import BatchStore, clean_config, parse_sources
from app.retry_policy import RetryPolicy

TERMINAL = {"completed", "completed_with_issues", "stopped"}
OUTAGES = {"destination_unavailable", "destination_timeout", "destination_connection_failed",
           "destination_stream_interrupted", "destination_rate_limited", "destination_not_found"}


class Interrupted(Exception):
    pass


class _AutomaticRetryDisabled(Exception):
    pass


def _md5(path, checkpoint=None):
    digest = hashlib.md5()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            if checkpoint:
                checkpoint()
            digest.update(chunk)
    return digest.hexdigest()


class BatchService:
    def __init__(self, root, journal, onprem, local_root, slots, *, source_factory=SaasDiskSource,
                 start_worker=True):
        self.store = BatchStore(root)
        self.journal, self.onprem, self.slots = journal, onprem, slots
        self.local_root = Path(local_root).absolute()
        self.source_factory = source_factory
        self.guard = threading.RLock()
        self._credentials = {}
        self._retry_policies = {}
        self._retry_lock = threading.RLock()
        self._redactors = {}
        self._active = {}
        self._planning_checks = set()
        self._rates = {}
        self._rate_lock = threading.Lock()
        self._local_reserved = {}
        self._reserve_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.wake = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=self.slots.limit, thread_name_prefix="disk-source-list")
        self.thread = None
        self.store.recover()
        if self.onprem:
            self.onprem.excluded_job_ids.update(self.store.managed_children() | self.onprem.store.managed_batch_job_ids())
        if start_worker:
            self.start()

    def retry_policy(self, batch_id):
        with self._retry_lock:
            policy = self._retry_policies.get(batch_id)
            if policy is None:
                policy = RetryPolicy(self.store.get(batch_id)["config"].get("retry_enabled", True))
                self._retry_policies[batch_id] = policy
            return policy

    def _bind_child_policy(self, batch_id, child_id):
        with self._retry_lock:
            policy = self.retry_policy(batch_id)
            self.onprem.runner.bind_retry_policy(child_id, policy)
            self.onprem.store.set_retry_enabled(child_id, policy.enabled)

    def set_retry_enabled(self, batch_id, enabled):
        if type(enabled) is not bool:
            raise ValueError("invalid_retry_policy")
        batch = self.store.get(batch_id)
        self._require_staged(batch, check_children=True)
        policy = self.retry_policy(batch_id)
        # No coordinator/job guard: an in-flight HTTP call must not hold this switch.
        with self._retry_lock:
            self.store.set_retry_enabled(batch_id, enabled)
            policy.set_enabled(enabled)
            for source in self.store.sources(batch_id):
                if source["child_job_id"]:
                    self._bind_child_policy(batch_id, source["child_job_id"])
                self._event(batch, source, "retry_settings", "succeeded",
                            retry_enabled=enabled, max_attempts=5 if enabled else 1)
        self.notify()
        if self.onprem:
            self.onprem.notify()
        return {"retry_enabled": enabled}

    def set_child_retry_enabled(self, child_id, enabled):
        batch = self.store.batch_for_child(child_id)
        if batch is None:
            return False
        self.set_retry_enabled(batch["id"], enabled)
        return True

    def start(self):
        if self.thread is None:
            self.thread = threading.Thread(target=self._run, name="disk-source-lists", daemon=False)
            self.thread.start()

    def notify(self):
        self.wake.set()

    def create(self, mode, sources_csv, config, credentials, idempotency_key):
        if mode == "onprem" and self.onprem is None:
            raise ValueError("onprem_disabled")
        config = clean_config(mode, config)
        sources = parse_sources(sources_csv, personal=config.get("destination_type") == "personal")
        credentials = self._valid_credentials(mode, config, credentials)
        batch = self.store.create(mode, config, sources, idempotency_key)
        self.attach_credentials(batch["id"], credentials)
        with self.guard:
            if not self.store.last_sequence(batch["id"]):
                for source in self.store.sources(batch["id"]):
                    self._event(batch, source, "retry_settings", "succeeded",
                                retry_enabled=self.retry_policy(batch["id"]).enabled,
                                max_attempts=5 if self.retry_policy(batch["id"]).enabled else 1)
            return self.detail(batch["id"])

    def _valid_credentials(self, mode, config, credentials):
        if config.get("destination_type") == "shared" and config.get("destination_auth"):
            if not shared_service_available(self.onprem):
                raise ValueError("onprem_shared_grant_unavailable")
        elif config.get("destination_auth") and not personal_available(self.onprem, config["destination_auth"]):
            raise ValueError("onprem_personal_grant_unavailable")
        required = credential_fields(config) if mode == "onprem" else {"source_client_secret"}
        if (not isinstance(credentials, dict) or not required <= set(credentials)
                or set(credentials) - required - {"source_admin_token"}) or any(
            not isinstance(v, str) or not v or len(v) > 8192
            or any(ord(c) < 32 or ord(c) == 127 for c in v)
            for v in credentials.values()
        ):
            raise ValueError("invalid_credentials")
        return dict(credentials)

    def attach_credentials(self, batch_id, credentials):
        with self.guard:
            batch = self.store.get(batch_id)
            self._require_staged(batch, check_children=True)
            if batch["state"] == "stopped":
                raise ValueError("batch_stopped")
            credentials = self._valid_credentials(batch["mode"], batch["config"], credentials)
            previous = self._credentials.get(batch_id)
            if (previous == credentials and batch["state"] != "awaiting_credentials"
                    and not any(issue.get("credential_check") for issue in batch["mapping_issues"].values())):
                return self.detail(batch_id)
            sources = self.store.sources(batch_id)
            if any(s["id"] in self._active for s in sources):
                raise ValueError("source_busy")
            delegated = batch["config"].get("destination_auth") == "service_application"
            finished = {s["id"] for s in sources if not s["selected"] or s["state"] in TERMINAL} if delegated and batch["started"] else set()
            verify = delegated or batch["config"].get("destination_type") == "personal"
            planned = [s for s in sources if verify and s["selected"] and s["plan_complete"] and s["id"] not in finished]
        with self._verified_access(batch, sources, planned, credentials, previous):
            self._credentials[batch_id] = credentials
            redactor = self._redactors.setdefault(batch_id, Redactor())
            for secret in credentials.values():
                redactor.add_secret(secret)
            if self.onprem and batch["mode"] == "onprem":
                for source in sources:
                    if source["child_job_id"]:
                        job = self.onprem.store.get(source["child_job_id"])
                        self.onprem.runner.release_clients(job["id"])
                        if source["id"] in finished:
                            continue
                        self.onprem.registry.put(job["id"], job["config"], credentials)
                        if job["state"] == "awaiting_credentials":
                            state = "paused" if job["resume_state"] in {"running", "paused"} else job["resume_state"] or "ready"
                            error = job["error"] if state == "needs_attention" else None
                            self.onprem.store.update_job(job["id"], state=state, error=error)
            if batch["state"] == "awaiting_credentials":
                resume = batch["resume_state"] or "draft"
                # Reattachment never starts work without the user's action.
                state = "ready" if resume == "ready" else "paused" if resume in {"running", "paused", "planning"} else "draft"
                self.store.update(batch_id, state=state, error=None)
            current = self.store.get(batch_id)
            if current["mode"] == "onprem" and not current["started"] and current["state"] in {"ready", "needs_attention"}:
                sources = self.store.sources(batch_id)
                self._restore_unstarted_children(current, sources)
            self.notify()
            return self.detail(batch_id)

    def _credentials_for(self, batch_id):
        value = self._credentials.get(batch_id)
        if value is None:
            raise MigrationError("credentials_required", auth=True)
        return dict(value)

    def _config(self, batch, source):
        value = {"source_org_id": batch["config"]["source_org_id"],
                 "source_client_id": batch["config"]["source_client_id"],
                 "source_email": source["email"], "source_path": source["path"],
                 "retry_enabled": self.retry_policy(batch["id"]).enabled}
        if source["type"] == "shared":
            value.update(source_type="shared", source_vd_hash=source["vd_hash"], source_folder=source["folder"] or "")
        return value

    def _require_staged(self, batch, *, check_children=False):
        if batch["mode"] == "onprem":
            if batch["config"].get("strategy") != "staged":
                raise ValueError("onprem_staged_required")
            endpoint = None
            if batch["config"].get("destination_auth") == "service_application":
                if not batch["config"].get("destination_oauth_url"):
                    raise MigrationError("invalid_destination_oauth_url")
                endpoint = bound_oauth_url(batch["config"])
            if not check_children:
                return
            for source in self.store.sources(batch["id"]):
                if source["child_job_id"]:
                    try:
                        job = self.onprem.store.get(source["child_job_id"])
                        self.onprem.runner.require_staged_job(job)
                        if endpoint and (not job["config"].get("destination_oauth_url")
                                         or bound_oauth_url(job["config"]) != endpoint):
                            raise MigrationError("invalid_destination_oauth_url")
                    except MigrationError as exc:
                        raise ValueError(exc.code) from None

    def _child_config(self, batch, source):
        self._require_staged(batch)
        value = self._config(batch, source)
        common = batch["config"]
        if common.get("destination_auth") == "service_application":
            value.update({k: common[k] for k in ("destination_auth", "destination_client_id", "destination_oauth_url")})
        if common["destination_type"] == "personal":
            value.update(destination_url=common["destination_url"], destination_type="personal")
            return dict(value, destination_root=source["destination_folder"], destination_identity=source["destination_email"],
                        source_public=True, strategy="staged")
        value.update({k: common[k] for k in ("destination_url", "destination_identity", "destination_type", "destination_vd_hash", "destination_base", "identity_confirmed")})
        value.update(destination_root=join_path("disk:/", source["folder"]),
                     strategy="staged" if source["type"] == "shared" else common["strategy"])
        if source["type"] == "personal":
            value["source_public"] = True
        return value

    def _checkpoint(self, batch_id, *, planning=False, child_id=None):
        if self.stop_event.is_set():
            raise Interrupted()
        batch = self.store.get(batch_id)
        expected = "planning" if planning else "running"
        if batch["state"] != expected:
            raise Interrupted()
        if batch_id not in self._credentials:
            if (child_id and batch["config"].get("destination_auth") == "service_application"
                    and self.onprem.registry.get(child_id)
                    and self.onprem.store.active_point_retry_entry(child_id)):
                return
            raise MigrationError("credentials_required", auth=True)

    def _event(self, batch, source_record, stage, status, **fields):
        redactor = self._redactors.get(batch["id"], Redactor())
        payload = {"stage": stage, "status": status, "source": fields.pop("source", source_record["path"]), **fields}
        payload = {k: redactor.text(v, context=k in {"source", "destination"}) if isinstance(v, str) else v for k, v in payload.items()}
        self.store.event(batch["id"], source_record["id"], payload)
        if batch["mode"] == "local" and source_record.get("journal_id") and self.journal:
            self.journal.append(source_record["journal_id"], payload)

    def _exception(self, batch, source_record, stage, exc, **fields):
        code = exc.code if isinstance(exc, MigrationError) else str(exc) if isinstance(exc, ValueError) and re.fullmatch("[a-z_]{1,64}", str(exc)) else "local_write_failed" if isinstance(exc, OSError) else "source_list_failed"
        details = {"error": code, **fields}
        if isinstance(exc, MigrationError):
            details.update(exc.diagnostics)
            details["http_status"] = exc.http_status
        details["error_side"] = "local" if code.startswith("local_") else "destination" if code.startswith("destination_") else "source"
        self._event(batch, source_record, stage, "failed", **details)
        return code

    def _local_path(self, folder, relative=""):
        if self.local_root.is_symlink() or (self.local_root.exists() and not self.local_root.is_dir()):
            raise MigrationError("local_destination_unsafe")
        path = self.local_root / folder
        for segment in [folder] + relative.split("/") if relative else [folder]:
            if segment in {"", ".", ".."} or "/" in segment or "\\" in segment:
                raise MigrationError("local_destination_unsafe")
        if relative:
            path /= relative
        candidate = self.local_root
        for part in path.relative_to(self.local_root).parts:
            candidate /= part
            if candidate.is_symlink():
                raise MigrationError("local_destination_unsafe")
        return path

    def _local_metadata(self, path, checkpoint):
        if not path.exists():
            return None
        if path.is_symlink():
            raise MigrationError("local_destination_unsafe")
        if path.is_dir():
            return {"type": "dir"}
        if not path.is_file():
            raise MigrationError("local_destination_unsafe")
        return {"type": "file", "size": path.stat().st_size, "md5": _md5(path, checkpoint)}

    def _plan_local(self, batch, source, client):
        checkpoint = lambda: self._checkpoint(batch["id"], planning=True)
        if not self.local_root.is_dir() or not os.access(self.local_root, os.W_OK | os.X_OK):
            raise MigrationError("local_destination_unwritable")
        root = source["path"]
        checkpoint()
        metadata = client.stat(root)
        checkpoint()
        if metadata.get("type") not in {"file", "dir"}:
            raise MigrationError("unsupported_resource")
        self._local_path(source["folder"])
        existing_folder = self._local_metadata(self.local_root / source["folder"], checkpoint)
        if existing_folder and existing_folder["type"] != "dir":
            raise MigrationError("destination_conflict")
        base = root.rsplit("/", 1)[0] if root != "disk:/" else "disk:"
        for node in client.walk(root):
            checkpoint()
            path = disk_path(node["path"])
            if path == "disk:/":
                continue
            if (root != "disk:/" and path != root and not path.startswith(root + "/")) or not path.startswith(base + "/"):
                raise MigrationError("invalid_source_tree")
            kind = node.get("type")
            if kind not in {"file", "dir"}:
                raise MigrationError("unsupported_resource")
            size = node.get("size", 0) if kind == "file" else 0
            md5 = node.get("md5")
            if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                raise MigrationError("source_size_unavailable")
            if md5 is not None and (not isinstance(md5, str) or not re.fullmatch("[a-fA-F0-9]{32}", md5)):
                raise MigrationError("source_invalid_response")
            relative = path[len(base) + 1:]
            target = self._local_path(source["folder"], relative)
            local = self._local_metadata(target, checkpoint)
            state, error = "planned", None
            if kind == "file" and not md5:
                state, error = "verification_unavailable", "source_hash_unavailable"
            elif local and local["type"] != kind:
                state, error = "conflict", "destination_conflict"
            elif local and kind == "file":
                if local.get("size") == size and local.get("md5", "").lower() == md5.lower():
                    state = "already_verified"
                else:
                    state, error = "conflict", "destination_conflict"
            self.store.add_entry(source["id"], {"kind": kind, "source_path": path,
                "destination_path": target.relative_to(self.local_root).as_posix(), "size": size,
                "md5": md5.lower() if md5 else None, "modified": node.get("modified"),
                "resource_id": node.get("resource_id"), "state": state, "error": error})
            self._event(batch, source, "planning", "succeeded", source=path,
                        destination=target.relative_to(self.local_root).as_posix(), kind=kind, size=size, error=error)
        checkpoint()
        return self.store.summary(source["id"])

    def _summary_child(self, child_id):
        job = self.onprem.store.get(child_id)
        summary = {k: job[k] for k in ("total_files", "total_directories", "total_bytes", "verified_bytes")}
        with self.onprem.store._connection() as db:
            row = db.execute(
                "SELECT COALESCE(MAX(CASE WHEN kind='file' THEN size ELSE 0 END),0),"
                "COALESCE(SUM(CASE WHEN kind='file' AND state NOT IN ('done','already_verified') THEN size ELSE 0 END),0),"
                "COALESCE(MAX(CASE WHEN kind='file' AND state NOT IN ('done','already_verified') THEN size ELSE 0 END),0),"
                "COALESCE(SUM(CASE WHEN state IN ('conflict','verification_unavailable','failed','outcome_unknown') THEN 1 ELSE 0 END),0) "
                "FROM entries WHERE job_id=? AND selected=1", (child_id,),
            ).fetchone()
        summary.update(largest_file=row[0], required_bytes=row[1], largest_required_file=row[2],
                       issues=row[3], selection=job["selection"])
        return summary

    def _new_local_journal(self, batch, source):
        if not self.journal:
            return source
        journal_id = uuid.uuid4().hex
        self.journal.create(journal_id, "local", "batch", source["email"] +
                            (" / " + source["vd_hash"] if source["vd_hash"] else ""), source["folder"] or "")
        source = self.store.update_source(source["id"], journal_id=journal_id)
        policy = self.retry_policy(batch["id"])
        self._event(batch, source, "retry_settings", "succeeded",
                    retry_enabled=policy.enabled, max_attempts=5 if policy.enabled else 1)
        return source

    def _finish_local_journal(self, source, status, error=None):
        if (self.journal and source.get("journal_id")
                and (self.journal.get(source["journal_id"]) or {}).get("status") == "running"):
            self.journal.finish(source["journal_id"], status, error=error,
                                summary={"total": self.store.summary(source["id"])["total_files"]})

    def _plan_one(self, source_id):
        source = self.store.source(source_id)
        batch = self.store.get(source["batch_id"])
        source_client = None
        checkpoint = lambda: self._checkpoint(batch["id"], planning=True)
        try:
            with self.slots.lease("batch:" + source_id, checkpoint=checkpoint):
                with self.guard:
                    checkpoint()
                    self.store.begin_plan(source_id)
                    if batch["mode"] == "local":
                        source = self._new_local_journal(batch, source)
                source_client = self.source_factory(self._config(batch, source), self._credentials_for(batch["id"]))
                source_client.retry_policy = self.retry_policy(batch["id"])
                source_client.checkpoint = checkpoint
                source_client.page_limit = batch["config"]["page_limit"]
                checkpoint()
                self._event(batch, source, "prepare_clients", "started")
                description = source_client.describe() if hasattr(source_client, "describe") else {"name": "Общий диск"}
                folder = source["folder"] or (shared_source_folder(description.get("name"), source["vd_hash"]) if source["type"] == "shared" else source["email"])
                with self.guard:
                    checkpoint()
                    source = self.store.update_source(source_id, folder=folder)
                self._event(batch, source, "prepare_clients", "succeeded")
                if batch["mode"] == "local":
                    summary = self._plan_local(batch, source, source_client)
                else:
                    if source["child_job_id"]:
                        with self.onprem.job_guard(source["child_job_id"]):
                            self.onprem.store.request_full_stop(source["child_job_id"])
                            self.onprem.runner.stop_job(source["child_job_id"])
                    self._checkpoint(batch["id"], planning=True)
                    config = self._child_config(batch, source)
                    with self.onprem.guard:
                        self._checkpoint(batch["id"], planning=True)
                        child = self.onprem.store.create(config, "batch-" + source_id + "-" + uuid.uuid4().hex,
                                                         initial_state="planning", batch_child=True)
                        self.onprem.excluded_job_ids.add(child["id"])
                        source = self.store.update_source(source_id, child_job_id=child["id"])
                    self._bind_child_policy(batch["id"], child["id"])
                    with self.onprem.job_guard(child["id"]):
                        self.onprem.registry.put(child["id"], child["config"], self._credentials_for(batch["id"]))
                        delegated = config.get("destination_auth") == "service_application"
                        if delegated:
                            destination = self.onprem.runner.destination_factory(child["config"], self._credentials_for(batch["id"]))
                            destination.retry_policy = self.retry_policy(batch["id"])
                            if config["destination_type"] == "shared":
                                destination.expected_uid = self.store.get(batch["id"])["shared_operator_uid"]
                        else:
                            _, destination = self.onprem.runner.clients(child)
                        destination.checkpoint = checkpoint
                        try:
                            self.onprem.runner.plan(child, source_client, destination, full_compare=True,
                                                    checkpoint=checkpoint)
                        except MigrationError as exc:
                            # The batch checks the selected bytes against each target's quota.
                            if not delegated or exc.code != "destination_space_insufficient":
                                raise
                        finally:
                            destination.checkpoint = None
                            if delegated:
                                destination.close()
                        if delegated and config["destination_type"] == "shared":
                            uid = self.onprem.store.get(child["id"])["destination_verified_uid"]
                            self.store.bind_shared_operator(batch["id"], uid)
                    summary = self._summary_child(child["id"])
                with self.guard:
                    checkpoint()
                    self.store.update_source(source_id, state="error" if summary["issues"] else "ready",
                                             error="plan_conflicts" if summary["issues"] else None,
                                             plan_complete=True, summary=summary)
                if batch["mode"] == "local":
                    self._finish_local_journal(source, "error" if summary["issues"] else "done",
                                               "plan_conflicts" if summary["issues"] else None)
        except Interrupted:
            with self.guard:
                current = self.store.get(batch["id"])
                self.store.update_source(source_id, state="stopped" if current["state"] == "stopped" else "pending")
            if batch["mode"] == "local":
                self._finish_local_journal(source, "interrupted")
            elif source["child_job_id"]:
                # A cancelled read-only scan owns no remote operations. Preserve
                # its history, but release its queue/cache claims before replan.
                with self.onprem.job_guard(source["child_job_id"]):
                    self.onprem.store.request_full_stop(source["child_job_id"])
                    self.onprem.runner.stop_job(source["child_job_id"])
        except Exception as exc:
            code = self._exception(batch, source, "planning", exc)
            with self.guard:
                stopped = self.store.get(batch["id"])["state"] == "stopped"
                complete = bool(source["child_job_id"] and not stopped
                                and self.onprem.store.get(source["child_job_id"])["plan_complete"])
                fields = {"summary": self._summary_child(source["child_job_id"])} if complete else {}
                self.store.update_source(source_id, state="stopped" if stopped else "error",
                                         error=None if stopped else code, plan_complete=complete, **fields)
            if batch["mode"] == "local":
                self._finish_local_journal(source, "error", code)
            if source["child_job_id"] and not stopped:
                with self.onprem.job_guard(source["child_job_id"]):
                    self.onprem.store.update_job(source["child_job_id"], state="needs_attention", error=code)
        finally:
            if self.store.get(batch["id"])["state"] == "stopped":
                latest = self.store.source(source_id)
                self.store.update_source(source_id, state="stopped")
                if latest["child_job_id"]:
                    with self.onprem.job_guard(latest["child_job_id"]):
                        self.onprem.store.request_full_stop(latest["child_job_id"])
                        self.onprem.runner.stop_job(latest["child_job_id"])
            if source_client:
                source_client.close()
            self.notify()

    def plan(self, batch_id, source_id=None):
        with self.guard:
            batch = self.store.get(batch_id)
            self._require_staged(batch, check_children=True)
            if batch["state"] in {"running", "stopped"} or batch["started"]:
                raise ValueError("batch_cannot_replan")
            self._credentials_for(batch_id)
            selected = self.store.sources(batch_id)
            if source_id:
                source = self.store.source(source_id)
                if source["batch_id"] != batch_id:
                    raise KeyError("source_not_found")
                selected = [source]
            for source in selected:
                if source["id"] in self._active:
                    raise ValueError("source_busy")
                if source["selected"] or source_id:
                    self.store.update_source(source["id"], state="pending", plan_complete=False, error=None,
                                             selected=True if source_id else source["selected"])
            self.store.update(batch_id, state="planning", phase="inventory", error=None, resume_state="planning")
            self.notify()
            return self.detail(batch_id)

    def selection(self, batch_id, source_ids):
        with self.guard:
            batch = self.store.get(batch_id)
            self._require_staged(batch, check_children=True)
            if batch["state"] in {"running", "stopped"} or batch["started"]:
                raise ValueError("selection_locked")
            sources = self.store.sources(batch_id)
            if not isinstance(source_ids, list) or not source_ids or set(source_ids) - {s["id"] for s in sources} or len(source_ids) != len(set(source_ids)):
                raise ValueError("invalid_source_selection")
            for source in sources:
                self.store.update_source(source["id"], selected=source["id"] in source_ids)
            selected = [s for s in self.store.sources(batch_id) if s["selected"]]
            if batch["state"] != "planning" and any(not s["plan_complete"] for s in selected):
                issues = self._mapping_conflicts(selected) if batch["config"].get("destination_type") == "personal" else {}
                error = next((i["mapping_error"] for i in issues.values()), "plan_incomplete")
                self.store.update(batch_id, state="needs_attention", error=error, mapping_issues=issues)
            else:
                # Fence any destination check that captured the previous selection.
                self.store.update(batch_id, state="planning", error=None, resume_state="planning")
        self._finish_planning(batch_id)
        return self.detail(batch_id)

    def _entry_selection_editable(self, batch, source):
        resumable = batch["state"] == "awaiting_credentials" and batch["resume_state"] in {"ready", "needs_attention"}
        return bool(not batch["started"] and (batch["state"] in {"ready", "needs_attention"} or resumable)
                    and source["plan_complete"] and source["state"] in {"ready", "error"}
                    and source.get("error") in {None, "plan_conflicts", "destination_space_insufficient"}
                    and source["id"] not in self._active and batch["id"] not in self._planning_checks)

    def entry_selection(self, batch_id, source_id, entry_id, selected):
        with self.guard:
            batch, source = self.store.get(batch_id), self.store.source(source_id)
            self._require_staged(batch, check_children=True)
            if source["batch_id"] != batch_id:
                raise KeyError("source_not_found")
            if not self._entry_selection_editable(batch, source):
                raise ValueError("selection_not_allowed")
            if source["child_job_id"]:
                with self.onprem.job_guard(source["child_job_id"]):
                    child = self.onprem.store.get(source["child_job_id"])
                    self.onprem.store.set_entry_selection(child["id"], entry_id, selected, child["revision"])
                    summary = self._summary_child(child["id"])
            else:
                self.store.set_entry_selection(source_id, entry_id, selected)
                summary = self.store.summary(source_id)
            self.store.update_source(source_id, summary=summary,
                                     state="error" if summary["issues"] else "ready",
                                     error="plan_conflicts" if summary["issues"] else None)
            # Invalidate destination checks that captured the previous selection.
            awaiting_credentials = batch["state"] == "awaiting_credentials" or (
                batch["mode"] == "onprem" and batch_id not in self._credentials
            )
            if awaiting_credentials:
                self.store.update(batch_id, state="awaiting_credentials", resume_state="ready", error="credentials_required")
            else:
                self.store.update(batch_id, state="planning", error=None, resume_state="planning")
        if not awaiting_credentials:
            self._finish_planning(batch_id)
        return self.detail(batch_id)

    def _aggregate(self, sources):
        selected = [s for s in sources if s["selected"]]
        summary = {k: sum(s["summary"].get(k, 0) for s in selected) for k in ("total_files", "total_directories", "total_bytes", "required_bytes", "verified_bytes")}
        summary.update(largest_file=max((s["summary"].get("largest_file", 0) for s in selected), default=0),
                       ready_sources=sum(s["state"] == "ready" and s["plan_complete"] for s in selected),
                       failed_sources=sum(s["state"] == "error" for s in selected), selected_sources=len(selected))
        return summary

    @staticmethod
    def _row_issue(issues, code, rows, path=None):
        numbers = {r["csv_row"] for r in rows}
        for row in rows:
            issue = issues.setdefault(row["id"], {"mapping_error": code, "conflict_rows": set()})
            if issue["mapping_error"] == code:
                issue["conflict_rows"].update(numbers)
                if path:
                    issue.setdefault("conflict_path", path)

    @staticmethod
    def _overlap(left, right):
        return left == right or right.startswith(left.rstrip("/") + "/") or left.startswith(right.rstrip("/") + "/")

    def _mapping_conflicts(self, sources):
        issues, by_source, by_email, by_uid = {}, {}, {}, {}
        for row in sources:
            if not row["selected"]:
                continue
            by_source.setdefault(row["email"].strip().casefold(), []).append(row)
            by_email.setdefault(row["destination_email"].strip().casefold(), []).append(row)
            uid = row.get("destination_verified_uid")
            if uid is not None:
                by_uid.setdefault(uid, []).append(row)
        for rows in by_source.values():
            if (len({r["destination_email"].strip().casefold() for r in rows}) > 1
                    or len({r["destination_verified_uid"] for r in rows if r.get("destination_verified_uid")}) > 1):
                self._row_issue(issues, "personal_mapping_conflict", rows)
        for rows in (*by_email.values(), *by_uid.values()):
            if len({r["email"].strip().casefold() for r in rows}) > 1:
                self._row_issue(issues, "personal_mapping_conflict", rows)
        for rows in by_source.values():
            paths = [(r, unicodedata.normalize("NFC", r["path"])) for r in rows]
            for (left, p), (right, q) in combinations(paths, 2):
                if self._overlap(p, q):
                    self._row_issue(issues, "overlapping_source_paths", [left, right], min(p, q, key=len))
        for issue in issues.values():
            issue["conflict_rows"] = sorted(issue["conflict_rows"])
        return issues

    def _target_conflicts(self, rows, issues, checkpoint):
        roots = [(r, unicodedata.normalize("NFC", join_path(r["destination_folder"], r["path"][6:]))) for r in rows]
        by_job = {}
        for (left, p), (right, q) in combinations(roots, 2):
            if self._overlap(p, q):
                by_job[left["child_job_id"]] = left
                by_job[right["child_job_id"]] = right
        if not by_job:
            return
        # Sort only potentially overlapping jobs on this UID. A file and all its descendants
        # are adjacent with the trailing slash, so only the ancestor files stay in memory.
        with self.onprem.store._connection() as db:
            db.create_function("nfc", 1, lambda path: unicodedata.normalize("NFC", path))
            entries = db.execute("SELECT nfc(destination_path) AS path,kind,job_id FROM entries "
                                 "WHERE selected=1 AND job_id IN (" + ",".join("?" for _ in by_job)
                                 + ") ORDER BY nfc(destination_path)||'/'", tuple(by_job))
            parents = []
            for path, matches in groupby(entries, key=lambda e: e["path"]):
                if checkpoint:
                    checkpoint()
                matches = list(matches)
                paths = [by_job[e["job_id"]] for e in matches]
                files = [by_job[e["job_id"]] for e in matches if e["kind"] == "file"]
                while parents and not path.startswith(parents[-1][0] + "/"):
                    parents.pop()
                for ancestor, owners in parents:
                    self._row_issue(issues, "destination_path_collision", owners + paths, ancestor)
                if files:
                    if len(paths) > 1:
                        self._row_issue(issues, "destination_path_collision", paths, path)
                    parents.append((path, files))

    def _check_disk(self, batch, source, credentials, checkpoint=None):
        job = self.onprem.store.get(source["child_job_id"])
        runner = self.onprem.runner
        config = job["config"]
        client = runner.destination_factory(config, credentials)
        try:
            client.expected_uid = runner.plan_uid(job)
            client.checkpoint = checkpoint
            client.retry_policy = self.retry_policy(batch["id"])
            info = client.check_available()
            runner._identity(config, info, client)
            permissions = info.get("permissions")
            if isinstance(permissions, dict) and permissions.get("write") is False:
                raise MigrationError("destination_read_only")
            runner.clear_destination_outage(config)
            return runner._quota(info)
        finally:
            client.close()

    def _require_current_check(self, batch, previous):
        if (self._credentials.get(batch["id"]) is not previous
                or self.store.get(batch["id"])["updated_at"] != batch["updated_at"]):
            raise ValueError("source_busy")

    @contextmanager
    def _verified_access(self, batch, sources, planned, credentials, previous):
        # Remote checks run unlocked. Apply their results only to the same plan and credentials.
        issues = dict(batch.get("mapping_issues", {}))
        error = None

        def checkpoint():
            self._require_current_check(batch, previous)

        if batch["config"].get("destination_type") == "personal":
            for source in planned:
                checkpoint()
                try:
                    self._check_disk(batch, source, credentials, checkpoint)
                except MigrationError as exc:
                    issue = issues.get(source["id"], {})
                    if not issue or issue.get("credential_check"):
                        issues[source["id"]] = {"mapping_error": exc.code, "conflict_rows": [source["csv_row"]],
                                                "credential_check": True}
                    error = exc
                    break
                if issues.get(source["id"], {}).get("credential_check"):
                    issues.pop(source["id"])
        elif planned:
            self._check_shared_disk(batch, planned, credentials, checkpoint)
        with self.guard, self._child_guards(sources):
            checkpoint()
            if (self.store.sources(batch["id"]) != sources or
                    any((future := self._active.get(s["id"])) is not None and not future.done() for s in sources)):
                raise ValueError("source_busy")
            if issues != batch["mapping_issues"]:
                self.store.update(batch["id"], mapping_issues=issues)
            if error:
                raise error
            yield

    def _check_shared_disk(self, batch, sources, credentials, checkpoint=None):
        if batch["config"].get("destination_auth") == "service_application":
            uid = batch["shared_operator_uid"]
            if not uid:
                raise MigrationError("destination_identity_unverified", auth=True)
            for source in sources:
                child = self.onprem.store.get(source["child_job_id"])
                if self.onprem.runner.plan_uid(child) != uid:
                    raise MigrationError("destination_identity_mismatch", auth=True)
        # All rows name the same shared Disk and operator. One fresh access check covers them.
        return self._check_disk(batch, sources[0], credentials, checkpoint)

    def _check_personal_disks(self, batch, rows, checkpoint):
        issues = self._mapping_conflicts(rows)
        groups = {}
        plan_error = None
        credentials = self._credentials_for(batch["id"])
        for row in rows:
            if checkpoint:
                checkpoint()
            if row["id"] in issues:
                continue
            if not row["plan_complete"] or not row["child_job_id"]:
                plan_error = "plan_incomplete"
                continue
            if row["summary"]["issues"]:
                plan_error = plan_error or "plan_conflicts"
                continue
            try:
                quota = self._check_disk(batch, row, credentials, checkpoint)
            except MigrationError as exc:
                self._row_issue(issues, exc.code, [row])
                continue
            uid = row["destination_verified_uid"]
            groups.setdefault(uid, []).append((row, quota))
        for members in groups.values():
            group = [r for r, _ in members if r["summary"]["selection"]["selected"]]
            self._target_conflicts(group, issues, checkpoint)
            available = [q for _, q in members if q is not None]
            if available and sum(r["summary"]["required_bytes"] for r in group) > min(available):
                self._row_issue(issues, "destination_space_insufficient", group)
        for issue in issues.values():
            issue["conflict_rows"] = sorted(issue["conflict_rows"])
        return (next(iter(issues.values()))["mapping_error"] if issues else plan_error), issues

    def _group_check(self, batch, sources, *, checkpoint=None):
        self._require_staged(batch, check_children=True)
        if checkpoint:
            checkpoint()
        included = [s for s in sources if s["selected"]]
        selected = [s for s in included if not s["plan_complete"] or s["summary"]["selection"]["selected"]]
        if batch["config"].get("destination_type") == "personal":
            error, issues = self._check_personal_disks(batch, included, checkpoint)
            if error:
                return error, issues
        else:
            folders = set()
            for source in selected:
                if not source["folder"]:
                    return "plan_incomplete", {}
                normalized = unicodedata.normalize("NFC", source["folder"])
                if batch["mode"] == "local":
                    normalized = normalized.casefold()
                if normalized in folders:
                    return "target_folder_collision", {}
                folders.add(normalized)
            if any(not s["plan_complete"] or s["state"] not in {"ready", "completed"} for s in included):
                return "plan_incomplete", {}
            if any(s["summary"]["issues"] for s in selected):
                return "plan_conflicts", {}
            if selected:
                required = self._aggregate(selected)["required_bytes"]
                if batch["mode"] == "local":
                    if shutil.disk_usage(self.local_root).free < required:
                        return "local_space_insufficient", {}
                else:
                    quota = self._check_shared_disk(batch, selected, self._credentials_for(batch["id"]), checkpoint)
                    if quota is not None and required > quota:
                        return "destination_space_insufficient", {}
        if batch["mode"] == "onprem" and selected:
            largest = sorted((s["summary"].get("largest_required_file", s["summary"].get("largest_file", 0))
                              for s in selected), reverse=True)[:self.slots.limit]
            needed = sum(largest)
            runner = self.onprem.runner
            if needed + runner._staging_size() > runner.staging_budget or needed + runner.reserve_bytes > shutil.disk_usage(runner.staging).free:
                return "local_cache_space_insufficient", {}
        return None, {}

    def _finish_planning(self, batch_id):
        with self.guard:
            batch = self.store.get(batch_id)
            if batch["state"] not in {"planning", "draft", "ready", "needs_attention"} or batch_id in self._planning_checks:
                return
            sources = self.detail(batch_id)["sources"]
            selected = [s for s in sources if s["selected"]]
            if not selected or any(s["state"] in {"pending", "planning"} for s in selected):
                return
            previous = self._credentials.get(batch_id)
            self._planning_checks.add(batch_id)

        def checkpoint():
            current = self.store.get(batch_id)
            if (self.stop_event.is_set() or current["updated_at"] != batch["updated_at"]
                    or self._credentials.get(batch_id) is not previous):
                raise Interrupted()

        try:
            try:
                error, issues = self._group_check(batch, sources, checkpoint=checkpoint)
            except Interrupted:
                return
            except Exception as exc:
                error, issues = exc.code if isinstance(exc, MigrationError) else "destination_check_failed", {}
            with self.guard:
                checkpoint()
                self.store.update(batch_id, state="ready" if error is None else "needs_attention", error=error, mapping_issues=issues)
        except Interrupted:
            return
        finally:
            with self.guard:
                self._planning_checks.discard(batch_id)
            self.notify()

    def _restore_unstarted_children(self, batch, sources):
        if batch["mode"] != "onprem" or batch["started"]:
            return
        repairs = []
        for source in sources:
            if not source["child_job_id"]:
                continue
            job = self.onprem.store.get(source["child_job_id"])
            if (source["state"] not in {"running", "paused", "completed"}
                    and job["state"] not in {"running", "paused", "completed"}):
                continue
            # Managed children cannot dispatch before the parent commits started.
            # Require stored evidence too, so actual work is never rewound.
            if (not source["plan_complete"] or not job["plan_complete"]
                    or job["stop_requested"] or job["full_stop_requested"]
                    or job["state"] not in {"ready", "needs_attention", "running", "paused", "completed"}):
                raise ValueError("selection_not_allowed")
            with self.onprem.store._connection() as db:
                attempts = db.execute("SELECT 1 FROM attempts a JOIN entries e ON e.id=a.entry_id "
                                      "WHERE e.job_id=? LIMIT 1", (job["id"],)).fetchone()
                executed = db.execute("SELECT 1 FROM entries WHERE job_id=? AND state NOT IN "
                                      "('planned','already_verified','conflict','verification_unavailable','skipped_by_user') "
                                      "LIMIT 1", (job["id"],)).fetchone()
                publications = db.execute("SELECT 1 FROM publications WHERE job_id=? AND state!='planned' LIMIT 1",
                                          (job["id"],)).fetchone()
            if attempts or executed or publications:
                raise ValueError("selection_not_allowed")
            summary = self._summary_child(job["id"])
            error = "plan_conflicts" if summary["issues"] else (
                "destination_space_insufficient" if job["error"] == "destination_space_insufficient" else None
            )
            repairs.append((source, job, summary, error))
        for source, job, summary, error in repairs:
            self.onprem.store.update_job(job["id"], state="needs_attention" if error else "ready",
                                         pause_requested=False, resume_state=None, error=error)
            self.store.update_source(source["id"], state="error" if summary["issues"] else "ready",
                                     error="plan_conflicts" if summary["issues"] else None, summary=summary)

    @contextmanager
    def _child_guards(self, sources):
        with ExitStack() as locks:
            for source in sources:
                if source["child_job_id"]:
                    lock = self.onprem.job_guard(source["child_job_id"])
                    if not lock.acquire(blocking=False):
                        raise ValueError("source_busy")
                    locks.callback(lock.release)
            yield

    def start_transfer(self, batch_id):
        with self.guard:
            batch = self.store.get(batch_id)
            self._require_staged(batch, check_children=True)
            sources = self.detail(batch_id)["sources"]
            if (all(s["plan_complete"] for s in sources if s["selected"])
                    and not any(s["selected"] and s["summary"]["selection"]["selected"] for s in sources)):
                raise ValueError("empty_selection")
            if batch["state"] != "ready":
                raise ValueError("dry_run_required")
            self._credentials_for(batch_id)
            previous = self._credentials[batch_id]
            with self._child_guards(sources):
                self._restore_unstarted_children(batch, sources)
                sources = self.detail(batch_id)["sources"]
                rows = self.store.sources(batch_id)
                for source in sources:
                    if source["selected"] and source["child_job_id"]:
                        self.onprem.store.validate_selection(source["child_job_id"], allow_empty=True)

        def checkpoint():
            self._require_current_check(batch, previous)

        error, issues = self._group_check(batch, sources, checkpoint=checkpoint)
        with self.guard, self._child_guards(sources):
            checkpoint()
            if self.store.sources(batch_id) != rows:
                raise ValueError("source_busy")
            if error:
                self.store.update(batch_id, state="needs_attention", error=error, mapping_issues=issues)
                raise ValueError(error)
            if batch["mode"] == "local":
                self.store.start_local_transfer(batch_id)
                for source in sources:
                    if source["selected"] and source["summary"]["selection"]["selected"]:
                        self._new_local_journal(batch, source)
            else:
                try:
                    for source in sources:
                        if source["selected"]:
                            if not source["summary"]["selection"]["selected"]:
                                self.onprem.store.freeze_selection(source["child_job_id"], allow_empty=True)
                                self.onprem.store.update_job(source["child_job_id"], state="completed", error=None)
                                self.store.update_source(source["id"], state="completed", error=None)
                                continue
                            self.store.update_source(source["id"], state="running")
                            self.onprem.store.freeze_selection(source["child_job_id"])
                            self.onprem.store.revalidate_verified_files(source["child_job_id"])
                            self.onprem.store.update_job(source["child_job_id"], state="running", pause_requested=False, error=None)
                    self.store.update(batch_id, state="running", phase="transfer", started=True, error=None, resume_state="running")
                except Exception:
                    try:
                        self._restore_unstarted_children(self.store.get(batch_id), self.store.sources(batch_id))
                    except Exception:
                        pass  # Preserve the admission error if storage is still unavailable.
                    raise
            self.notify()
            return self.detail(batch_id)

    def pause(self, batch_id):
        with self.guard:
            batch = self.store.get(batch_id)
            if batch["state"] not in {"running", "planning"}:
                raise ValueError("batch_not_active")
            self.store.update(batch_id, state="paused", resume_state=batch["state"])
            for source in self.store.sources(batch_id):
                if source["child_job_id"]:
                    self.onprem.store.update_job(source["child_job_id"], pause_requested=True)
            self.notify()
            return self.detail(batch_id)

    def resume(self, batch_id):
        with self.guard:
            batch = self.store.get(batch_id)
            self._require_staged(batch, check_children=True)
            if batch["state"] != "paused":
                raise ValueError("batch_not_paused")
            credentials = self._credentials_for(batch_id)
            previous = self._credentials[batch_id]
            state = batch["resume_state"] or ("running" if batch["started"] else "planning")
            sources = self.store.sources(batch_id)
            planned = []
            if batch["mode"] == "onprem" and state == "running":
                planned = [s for s in sources if s["selected"] and s["state"] not in TERMINAL]
        with self._verified_access(batch, sources, planned, credentials, previous):
            self.store.update(batch_id, state=state, error=None)
            if state == "running":
                for source in sources:
                    if not source["selected"] or source["state"] in TERMINAL:
                        continue
                    self.store.update_source(source["id"], state="running", error=None)
                    if batch["mode"] == "local":
                        self._new_local_journal(batch, source)
                    if source["child_job_id"]:
                        self.onprem.store.update_job(source["child_job_id"], state="running", pause_requested=False,
                                                     resume_state=None, error=None)
            self.notify()
            return self.detail(batch_id)

    def stop(self, batch_id):
        with self.guard:
            self.store.update(batch_id, state="stopped", error=None)
            for source in self.store.sources(batch_id):
                self.store.update_source(source["id"], state="stopped")
                future = self._active.get(source["id"])
                if future:
                    future.cancel()
                if not source["child_job_id"] and source["id"] not in self._active:
                    self._finish_local_journal(source, "interrupted")
                if source["child_job_id"]:
                    self.onprem.store.request_full_stop(source["child_job_id"])
                    # Cancellation flags are visible immediately to in-flight work.
                    if source["id"] not in self._active:
                        with self.onprem.job_guard(source["child_job_id"]):
                            self.onprem.runner.stop_job(source["child_job_id"])
            self._credentials.pop(batch_id, None)
            self.notify()
            return self.detail(batch_id)

    def require_child_active(self, child_id, *, recovering_point=False):
        for batch in self.store.batches(limit=1000000)["batches"]:
            for source in self.store.sources(batch["id"]):
                if source["child_job_id"] == child_id:
                    if not batch["started"] or batch["state"] == "stopped" or not source["selected"]:
                        raise ValueError("managed_batch_not_active")
                    if batch["state"] == "awaiting_credentials":
                        if (recovering_point and batch["config"].get("destination_auth") == "service_application"
                                and batch["resume_state"] == "running"):
                            child = self.onprem.store.get(child_id)
                            if (child["point_retry_active"] and not child["pause_requested"]
                                    and not child["stop_requested"]):
                                return True
                        raise ValueError("managed_batch_credentials_required")
                    if batch["state"] == "paused" and batch.get("error") not in OUTAGES:
                        raise ValueError("managed_batch_paused")
                    return True
        if self.onprem and child_id in self.onprem.store.managed_batch_job_ids():
            raise ValueError("managed_batch_not_active")
        return False

    def prepare_child_retry(self, child_id):
        if not self.require_child_active(child_id, recovering_point=True):
            return False
        batch = self.store.batch_for_child(child_id)
        source = next(s for s in self.store.sources(batch["id"]) if s["child_job_id"] == child_id)
        credentials = self.onprem.registry.get(child_id)
        delegated = batch["config"].get("destination_auth") == "service_application"
        restore = delegated and credentials is None
        shared_service = delegated and batch["config"].get("destination_type") == "shared"
        if restore or shared_service or batch.get("error") in OUTAGES or source.get("error") in OUTAGES:
            credentials = credentials or self._credentials_for(batch["id"])
            if shared_service:
                self._check_shared_disk(batch, [source], credentials)
            else:
                self._check_disk(batch, source, credentials)
            if restore:
                self.onprem.registry.put(child_id, self.onprem.store.get(child_id)["config"], credentials)
        return True

    def activate_child_retry(self, child_id):
        with self.guard:
            for batch in self.store.batches(limit=1000000)["batches"]:
                for source in self.store.sources(batch["id"]):
                    if source["child_job_id"] != child_id:
                        continue
                    self.require_child_active(child_id, recovering_point=True)
                    self.store.update(batch["id"], state="running", phase="transfer", error=None)
                    self.store.update_source(source["id"], state="running", error=None)
                    if (batch.get("error") in OUTAGES and batch["config"].get("destination_type") == "shared"
                            and batch["id"] in self._credentials):
                        for sibling in self.store.sources(batch["id"]):
                            if sibling["selected"] and sibling["state"] == "paused":
                                self.store.update_source(sibling["id"], state="running", error=None)
                                self.onprem.store.update_job(sibling["child_job_id"], state="running", pause_requested=False, error=None)
                    self.notify()
                    return True
        return False

    def _run(self):
        while not self.stop_event.is_set():
            self.wake.clear()
            planning = []
            with self.guard:
                for source_id, future in list(self._active.items()):
                    if future.done():
                        if not future.cancelled():
                            future.result()
                        self._active.pop(source_id, None)
                for batch in reversed(self.store.batches(limit=1000000)["batches"]):
                    for source in self.store.sources(batch["id"]):
                        if source["id"] in self._active or not source["selected"] or len(self._active) >= self.slots.limit:
                            continue
                        action = None
                        if batch["state"] == "planning" and source["state"] == "pending":
                            self.store.update_source(source["id"], state="planning")
                            action = self._plan_one
                        elif batch["state"] == "running" and source["state"] == "running":
                            action = self._transfer_one
                        elif (batch["started"] and batch["state"] in {"completed", "completed_with_issues"}
                              and source["state"] in {"completed", "completed_with_issues"}
                              and source["child_job_id"] and self.onprem.store.get(source["child_job_id"])["state"] == "running"):
                            self.store.update(batch["id"], state="running", phase="transfer")
                            self.store.update_source(source["id"], state="running")
                            action = self._transfer_one
                        if action:
                            self._active[source["id"]] = self.executor.submit(action, source["id"])
                    if batch["state"] == "planning":
                        planning.append(batch["id"])
                    elif batch["state"] == "running":
                        selected = [s for s in self.store.sources(batch["id"]) if s["selected"]]
                        if selected and all(s["state"] in TERMINAL | {"error"} for s in selected):
                            issues = any(s["state"] != "completed" for s in selected)
                            self.store.update(batch["id"], state="completed_with_issues" if issues else "completed")
                        elif (batch["config"].get("destination_type") == "personal" and selected
                              and all(s["state"] in TERMINAL | {"error", "paused", "awaiting_credentials"} for s in selected)):
                            waiting = [s for s in selected if s["state"] in {"paused", "awaiting_credentials"}]
                            blocked = next((s for s in waiting if s["state"] == "awaiting_credentials"), waiting[0])
                            self.store.update(batch["id"], state=blocked["state"], error=blocked["error"], resume_state="running")
            for batch_id in planning:
                self._finish_planning(batch_id)
            self.wake.wait(0.2)

    def _transfer_one(self, source_id):
        source = self.store.source(source_id)
        batch = self.store.get(source["batch_id"])
        client = None
        owner = "onprem:" + source["child_job_id"] if source["child_job_id"] else "batch:" + source_id
        checkpoint = lambda: self._checkpoint(batch["id"], child_id=source["child_job_id"])
        try:
            with self.slots.lease(owner, checkpoint=checkpoint):
                checkpoint()
                if source["child_job_id"]:
                    self._transfer_child(batch, source)
                else:
                    client = self.source_factory(self._config(batch, source), self._credentials_for(batch["id"]))
                    client.page_limit = batch["config"]["page_limit"]
                    client.retry_policy = self.retry_policy(batch["id"])
                    client.checkpoint = checkpoint
                    self._transfer_local(batch, source, client)
        except Interrupted:
            current = self.store.get(batch["id"])
            self.store.update_source(source_id, state="stopped" if current["state"] == "stopped" else "paused")
            if batch["mode"] == "local":
                self._finish_local_journal(source, "interrupted")
        except Exception as exc:
            code = self._exception(batch, source, "transfer", exc)
            self.store.update_source(source_id, state="error", error=code)
            if batch["mode"] == "local":
                self._finish_local_journal(source, "error", code)
            if isinstance(exc, MigrationError) and exc.auth:
                self.store.update(batch["id"], state="awaiting_credentials", resume_state="running", error=code)
        finally:
            if source["child_job_id"] and self.store.get(batch["id"])["state"] == "stopped":
                with self.onprem.job_guard(source["child_job_id"]):
                    self.onprem.runner.stop_job(source["child_job_id"])
            if client:
                client.close()
            with self._rate_lock:
                self._rates.pop(source_id, None)
            self.notify()

    def _transfer_child(self, batch, source):
        child_id = source["child_job_id"]
        self._bind_child_policy(batch["id"], child_id)
        while True:
            self._checkpoint(batch["id"], child_id=child_id)
            with self.onprem.job_guard(child_id):
                old_failures = {(e["id"], e["updated_at"]) for e in self.onprem.store.entries(child_id, limit=1000000, state="failed")}
                self.onprem.runner.step(child_id)
                job = self.onprem.store.get(child_id)
                if (job["state"] == "running" and batch["id"] not in self._credentials
                        and not job["point_retry_active"] and batch["config"].get("destination_auth") == "service_application"):
                    # The named file finished. The remaining queue requires the batch's credentials.
                    job = self.onprem.store.update_job(child_id, state="awaiting_credentials",
                                                       resume_state="running", error="credentials_required")
                summary = self._summary_child(child_id)
                self.store.update_source(source["id"], summary=summary)
                entries = self.onprem.store.entries(child_id, limit=1000000, state="failed")
                outage = next((e["error"] for e in entries if e.get("error") in OUTAGES
                               and (e["id"], e["updated_at"]) not in old_failures), None)
                if outage and self.retry_policy(batch["id"]).enabled:
                    if batch["config"].get("destination_type") == "personal":
                        self.onprem.store.update_job(child_id, state="paused", resume_state="running", error=outage)
                    else:
                        self._pause_destination(batch, outage)
                    self.store.update_source(source["id"], state="paused", error=outage)
                    return
                if job["state"] != "running":
                    state = job["state"]
                    if state == "needs_attention":
                        state = "paused" if job["resume_state"] == "running" else "completed_with_issues"
                    self.store.update_source(source["id"], state=state, error=job["error"])
                    if batch["config"].get("destination_type") != "personal" and state in {"paused", "awaiting_credentials"}:
                        fields = {"error": job["error"]} if job["error"] else {}
                        self.store.update(batch["id"], state=state, resume_state="running", **fields)
                    return
            if self.stop_event.wait(0.2):
                raise Interrupted()

    def _pause_destination(self, batch, code):
        for other in self.store.batches(mode="onprem", limit=1000000)["batches"]:
            if (other["state"] == "running" and self.retry_policy(other["id"]).enabled
                    and other["config"].get("destination_type") == "shared"
                    and all(other["config"][key] == batch["config"][key]
                            for key in ("destination_url", "destination_vd_hash"))):
                self.store.update(other["id"], state="paused", resume_state="running", error=code)
                for source in self.store.sources(other["id"]):
                    if source["child_job_id"]:
                        self.onprem.store.update_job(source["child_job_id"], pause_requested=True)

    def _current_local(self, entry, source):
        current = source.stat(entry["source_path"])
        if current.get("type") != entry["kind"] or any(
            entry.get(k) is not None and current.get(k) != entry.get(k)
            for k in ("size", "md5", "modified", "resource_id") if entry["kind"] == "file"
        ):
            raise MigrationError("source_changed")

    def _transfer_local(self, batch, source, client):
        checkpoint = lambda: self._checkpoint(batch["id"])
        self._local_path(source["folder"]).mkdir(parents=True, exist_ok=True)
        while entry := self.store.next_entry(source["id"]):
            checkpoint()
            relative = entry["destination_path"][len(source["folder"]) + 1:]
            stage = "check_source"
            try:
                target = self._local_path(source["folder"], relative)
                self._event(batch, source, "check_source", "started", source=entry["source_path"])
                self._current_local(entry, client)
                self._event(batch, source, "check_source", "succeeded", source=entry["source_path"])
                stage = "check_destination"
                self._event(batch, source, stage, "started", source=entry["source_path"], destination=entry["destination_path"])
                current = self._local_metadata(target, checkpoint)
                self._event(batch, source, stage, "succeeded", source=entry["source_path"], destination=entry["destination_path"])
                if current and current.get("type") != entry["kind"]:
                    raise MigrationError("destination_conflict")
                if entry["kind"] == "dir":
                    stage = "create_directory"
                    self._event(batch, source, "create_directory", "started", destination=entry["destination_path"])
                    target.mkdir(parents=True, exist_ok=True)
                    self._event(batch, source, "create_directory", "succeeded", destination=entry["destination_path"])
                    self.store.update_entry(entry["id"], state="done", error=None)
                elif current:
                    if current["size"] == entry["size"] and current["md5"] == entry["md5"]:
                        self.store.update_entry(entry["id"], state="already_verified", error=None)
                    else:
                        raise MigrationError("destination_conflict")
                else:
                    stage = "downloading"
                    self._download_local(batch, source, entry, client, target, checkpoint)
            except Interrupted:
                self.store.update_entry(entry["id"], state="planned")
                raise
            except Exception as exc:
                code = getattr(exc, "_source_batch_logged", None) or self._exception(batch, source, stage, exc, source=entry["source_path"], destination=entry["destination_path"])
                global_auth = isinstance(exc, MigrationError) and exc.auth and not (
                    not self.retry_policy(batch["id"]).enabled and file_access_denied(exc, stage)
                )
                self.store.update_entry(entry["id"], state="planned" if global_auth else "failed", error=code)
                if global_auth:
                    raise
                if not self.retry_policy(batch["id"]).enabled:
                    details = dict(exc.diagnostics) if isinstance(exc, MigrationError) else {}
                    details["error_side"] = "source" if code.startswith("source_") else "local"
                    self._event(batch, source, stage, "skipped", source=entry["source_path"],
                                destination=entry["destination_path"], error=code,
                                http_status=getattr(exc, "http_status", None), retry_enabled=False,
                                **details)
            self.store.update_source(source["id"], summary=self.store.summary(source["id"]))
        summary = self.store.summary(source["id"])
        state = "completed_with_issues" if summary["issues"] else "completed"
        self.store.update_source(source["id"], state=state, summary=summary)
        if self.journal and source["journal_id"]:
            selected_files = summary["selection"]["selected_files"]
            self.journal.finish(source["journal_id"], "done" if not summary["issues"] else "error",
                                summary={"saved": selected_files - summary["issues"], "failed": summary["issues"],
                                         "skipped": summary["total_files"] - selected_files, "total": summary["total_files"]})

    def _download_local(self, batch, source, entry, client, target, checkpoint):
        target.parent.mkdir(parents=True, exist_ok=True)
        policy = self.retry_policy(batch["id"])
        for attempt in range(1, 6):
            checkpoint()
            temporary = None
            self.store.update_entry(entry["id"], state="downloading", attempts=attempt)
            started, count = time.monotonic(), 0
            with self._reserve_lock:
                if entry["size"] + sum(self._local_reserved.values()) > shutil.disk_usage(self.local_root).free:
                    raise MigrationError("local_space_insufficient")
                self._local_reserved[entry["id"]] = entry["size"]
            try:
                self._current_local(entry, client)
                self._event(batch, source, "downloading", "started", source=entry["source_path"], attempt=attempt, max_attempts=5 if policy.enabled else 1)
                fd, filename = tempfile.mkstemp(prefix=".disk-copy-", suffix=".part", dir=target.parent)
                temporary = Path(filename)
                digest = hashlib.md5()
                with os.fdopen(fd, "wb") as stream:
                    for chunk in client.chunks(entry["source_path"]):
                        checkpoint()
                        count += len(chunk)
                        if count > entry["size"]:
                            raise MigrationError("source_content_changed")
                        stream.write(chunk)
                        with self._reserve_lock:
                            self._local_reserved[entry["id"]] = max(0, entry["size"] - count)
                        digest.update(chunk)
                        with self._rate_lock:
                            self._rates[source["id"]] = count / max(1, time.monotonic() - started)
                    stream.flush()
                    os.fsync(stream.fileno())
                if count != entry["size"] or digest.hexdigest() != entry["md5"]:
                    raise MigrationError("source_content_changed")
                checkpoint()
                self._local_path(source["folder"], entry["destination_path"][len(source["folder"]) + 1:])
                self._event(batch, source, "verify_destination", "started", source=entry["source_path"], destination=entry["destination_path"])
                try:
                    os.link(temporary, target)
                except FileExistsError:
                    current = self._local_metadata(target, checkpoint)
                    if not current or current.get("size") != count or current.get("md5") != digest.hexdigest():
                        raise MigrationError("destination_conflict") from None
                self.store.update_entry(entry["id"], state="done", error=None)
                self._event(batch, source, "verify_destination", "succeeded", source=entry["source_path"], destination=entry["destination_path"], size=count, attempt=attempt)
                return
            except Interrupted:
                raise
            except Exception as exc:
                code = self._exception(batch, source, "downloading", exc, source=entry["source_path"], attempt=attempt, max_attempts=5 if policy.enabled else 1)
                exc._source_batch_logged = code
                retryable = isinstance(exc, MigrationError) and not exc.auth and code.startswith("source_") and code != "source_changed"
                if attempt == 5 or not retryable or not policy.enabled:
                    self._event(batch, source, "downloading", "skipped", source=entry["source_path"], error=code, attempt=attempt)
                    raise
                delay = max((2, 5, 10, 20)[attempt - 1], min(120, exc.retry_after or 0))
                generation = policy.cancellation_generation
                self._event(batch, source, "download_retry", "waiting", source=entry["source_path"], error=code, attempt=attempt, max_attempts=5 if policy.enabled else 1, retry_delay_ms=int(delay * 1000))
                def retry_checkpoint():
                    checkpoint()
                    if not policy.enabled or policy.cancellation_generation != generation:
                        raise _AutomaticRetryDisabled()
                retry_checkpoint.retry_policy = policy
                retry_checkpoint.retry_generation = generation
                try:
                    if self._delay(delay, retry_checkpoint) is False:
                        raise _AutomaticRetryDisabled()
                    retry_checkpoint()
                except _AutomaticRetryDisabled:
                    self._event(batch, source, "downloading", "skipped", source=entry["source_path"],
                                error=code, attempt=attempt, max_attempts=1, retry_enabled=False)
                    raise exc
            finally:
                if temporary:
                    temporary.unlink(missing_ok=True)
                with self._reserve_lock:
                    self._local_reserved.pop(entry["id"], None)

    def _delay(self, delay, checkpoint):
        policy = getattr(checkpoint, "retry_policy", None)
        if policy is not None:
            return policy.wait(delay, checkpoint=checkpoint,
                               generation=checkpoint.retry_generation)
        end = time.monotonic() + delay
        while time.monotonic() < end:
            checkpoint()
            self.stop_event.wait(min(0.2, end - time.monotonic()))

    def detail(self, batch_id):
        batch, sources = self.store.get(batch_id), self.store.sources(batch_id)
        personal = batch["config"].get("destination_type") == "personal"
        for source in sources:
            if source["child_job_id"] and self.onprem:
                child = self.onprem.store.get(source["child_job_id"])
                source["child_strategy"] = child["config"]["strategy"]
                if batch["config"].get("destination_auth") == "service_application":
                    source["destination_verified_uid"] = child["destination_verified_uid"]
                if personal and source["plan_complete"]:
                    config = child["config"]
                    source["planned_destination_path"] = join_path(config["destination_root"], config["source_path"][6:])
                source["summary"] = self._summary_child(source["child_job_id"])
                source["transfer_rates"] = self.onprem.runner.transfer_rates(source["child_job_id"])
            else:
                source["summary"] = self.store.summary(source["id"])
                with self._rate_lock:
                    source["transfer_rates"] = {"download_bytes_per_second": self._rates.get(source["id"]), "upload_bytes_per_second": None}
            selection = source["summary"]["selection"]
            selection["editable"] = self._entry_selection_editable(batch, source) and (
                selection["editable"] if source["child_job_id"] else True
            )
        if personal:
            issues = self._mapping_conflicts(sources) if batch["state"] == "draft" else batch["mapping_issues"]
            for source in sources:
                source.update(issues.get(source["id"], {}))
        batch["summary"] = self._aggregate(sources)
        batch["selection"] = {"total": sum(s["summary"]["selection"]["total"] for s in sources),
                              "editable": any(s["summary"]["selection"]["editable"] for s in sources)}
        for key in ("selected", "selected_files", "selected_directories", "selected_bytes"):
            batch["selection"][key] = sum(s["summary"]["selection"][key] for s in sources if s["selected"])
        batch["selection"]["excluded"] = batch["selection"]["total"] - batch["selection"]["selected"]
        batch["credentials_available"] = batch_id in self._credentials
        batch["stop_pending"] = batch["state"] == "stopped" and (
            batch_id in self._planning_checks or any(
                (future := self._active.get(source["id"])) is not None and not future.done()
                for source in sources
            )
        )
        return {"batch": batch, "sources": sources}

    def entries(self, source_id, limit=100, offset=0):
        source = self.store.source(source_id)
        batch = self.store.get(source["batch_id"])
        editable = self._entry_selection_editable(batch, source)
        if source["child_job_id"]:
            entries = self.onprem.store.entries(source["child_job_id"], limit=limit, offset=offset)
            summary = self._summary_child(source["child_job_id"])
            result = {"entries": entries, "total": summary["total_files"] + summary["total_directories"]}
        else:
            result = self.store.entries(source_id, limit, offset)
        for entry in result["entries"]:
            entry["can_select"] = editable and entry.get("can_select", True)
        return result

    @staticmethod
    def _csv_value(value):
        return "'" + value if isinstance(value, str) and value.lstrip().startswith(("=", "+", "-", "@")) else value

    def export(self, batch_id, *, journal=False):
        detail = self.detail(batch_id)
        sources = detail["sources"]
        batch_cutoff = self.store.last_sequence(batch_id)
        spool = tempfile.SpooledTemporaryFile(max_size=1024 * 1024, mode="w+b")
        output = io.TextIOWrapper(spool, encoding="utf-8-sig", newline="")
        extra_journal = ["job_id", "stage_label", "status_label", "strategy", "attempt_id", "remote_path",
                         "remote_status", "error_message", "diagnostic_message", "download_attempts",
                         "max_download_attempts", "point_retry_attempts", "max_point_retry_attempts", "retry_at_ms"]
        if detail["batch"]["mode"] == "onprem":
            extra_journal.extend(["publication_attempts", "max_publication_attempts"])
        report_fields = ["source_state", "plan_complete", "total_files", "total_directories", "total_bytes",
                         "required_bytes", "verified_bytes", "state", "error", "source_path", "destination_path", "kind", "size", "md5", "selected"]
        mapping_fields = []
        if detail["batch"]["config"].get("destination_type") == "personal":
            mapping_fields = ["csv_row", "destination_email", "destination_folder", "destination_verified_uid",
                              "mapping_error", "conflict_rows", "conflict_path", "source_error"]
        fields = ["source_id", "type", "email", "vd_hash"] + mapping_fields + (
            CSV_FIELDS + extra_journal if journal else report_fields
        )
        writer = csv.DictWriter(output, fields, extrasaction="ignore")
        writer.writeheader()
        try:
            for source in sources:
                context = {k: source[k] for k in ("type", "email", "vd_hash")}
                context["source_id"] = source["id"]
                if mapping_fields:
                    context.update({key: source.get(key) for key in mapping_fields})
                    context["source_error"] = source["error"]
                    context["conflict_rows"] = ", ".join(map(str, source.get("conflict_rows", [])))
                if not journal:
                    context.update(source_state=source["state"], plan_complete=source["plan_complete"], **source["summary"])
                offset = 0
                emitted = False
                cutoff = self.onprem.store.last_sequence(source["child_job_id"]) if journal and source["child_job_id"] else batch_cutoff
                if journal and source["child_job_id"]:
                    # Access/planning failures can precede child creation and must
                    # remain visible alongside the child's detailed file journal.
                    event_cursor = 0
                    while batch_events := self.store.events(batch_id, after=event_cursor, limit=500, through=batch_cutoff):
                        for event in batch_events:
                            if event.get("source_id") == source["id"]:
                                writer.writerow({k: self._csv_value(v) for k, v in {**event, **context}.items()})
                        event_cursor = batch_events[-1]["sequence"]
                while True:
                    if journal and source["child_job_id"]:
                        rows = self.onprem.store.events(source["child_job_id"], after=offset, limit=500, through=cutoff, journal=True)
                    elif journal:
                        rows = self.store.events(batch_id, after=offset, limit=500, through=cutoff)
                    else:
                        rows = self.entries(source["id"], 500, offset)["entries"]
                    if not rows:
                        if not journal and not emitted:
                            if mapping_fields:
                                context["source_path"] = source["path"]
                            writer.writerow({k: self._csv_value(v) for k, v in context.items()})
                        break
                    for row in rows:
                        if journal and not source["child_job_id"] and row.get("source_id") != source["id"]:
                            continue
                        if journal and source["child_job_id"]:
                            row = {**row, **row.get("payload", {}), **row.get("context", {})}
                            row["event_type"] = row.get("type", "stage")
                        writer.writerow({k: self._csv_value(v) for k, v in {**row, **context}.items()})
                        emitted = True
                    offset = rows[-1]["sequence"] if journal else offset + len(rows)
            output.flush()
            output.detach()
            spool.seek(0)
            return spool
        except BaseException:
            output.close()
            raise

    def check_local_journal_cleanup(self):
        # The API holds guard through legacy journal cleanup and this purge, so
        # neither planner nor transfer dispatch can start between the checks.
        for batch in self.store.batches(mode="local", limit=1000000)["batches"]:
            if batch["state"] in {"planning", "running"} or any(
                s["id"] in self._active for s in self.store.sources(batch["id"])
            ):
                raise JournalError("journal_in_use")

    def clear_local_journal_cache(self):
        self.check_local_journal_cleanup()
        with self.store.connection() as db:
            count = db.execute("SELECT COUNT(*) FROM events WHERE batch_id IN (SELECT id FROM batches WHERE mode='local')").fetchone()[0]
            db.execute("DELETE FROM events WHERE batch_id IN (SELECT id FROM batches WHERE mode='local')")
            db.execute("UPDATE sources SET journal_id=NULL WHERE batch_id IN (SELECT id FROM batches WHERE mode='local')")
        return count

    def close(self):
        self.stop_event.set()
        self.wake.set()
        if self.thread:
            self.thread.join()
        self.executor.shutdown(wait=True, cancel_futures=True)
        self._credentials.clear()
        for redactor in self._redactors.values():
            redactor.clear()
        self.store.close()
