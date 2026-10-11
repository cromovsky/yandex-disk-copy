"""SQLite intents and replayable events, with no persisted credentials."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

from .operations import valid_operation_reference

CONFIG_KEYS = frozenset({
    "source_org_id", "source_client_id", "source_email", "source_path",
    "destination_url", "destination_identity", "destination_root", "strategy",
    "identity_confirmed",
})
_CODE = re.compile(r"[a-z][a-z0-9_]{0,79}\Z")
_OPERATION_ID = re.compile(r"[A-Za-z0-9_.:-]{1,256}\Z")
_EVENT_KEYS = frozenset({
    "state", "phase", "error", "entry_id", "attempt_id", "strategy", "count",
    "total_bytes", "verified_bytes", "total_files", "total_directories",
    "revision", "resume_state", "pause_requested", "verification", "status",
    "operation_id", "terminal_state", "evidence_kind", "retry_count",
    "remaining", "required_bytes", "available_bytes",
    "namespace_state", "terminal", "evidence_digest",
    "stop_requested", "restart_of", "restart_job_id",
})


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _string(value, *, limit=4096):
    if not isinstance(value, str) or not value or len(value) > limit:
        raise ValueError("invalid_value")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("invalid_value")
    return value


def _code(value):
    if value is not None and (
        not isinstance(value, str) or not _CODE.fullmatch(value)
    ):
        raise ValueError("invalid_safe_code")
    return value


def _json(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def canonical_config(config):
    """Explicit whitelist; secret-bearing or signed API addresses are rejected."""
    if not isinstance(config, dict) or set(config) - CONFIG_KEYS:
        raise ValueError("invalid_config_fields")
    value = dict(config)
    value.setdefault("strategy", "url")
    value.setdefault("identity_confirmed", False)
    if set(value) != CONFIG_KEYS:
        raise ValueError("missing_config_fields")
    for key in CONFIG_KEYS - {"strategy", "identity_confirmed"}:
        _string(value[key])
    parsed = urlsplit(value["destination_url"])
    if (
        parsed.scheme != "https" or not parsed.hostname or parsed.username
        or parsed.password or parsed.query or parsed.fragment
        or parsed.path.rstrip("/") != "/v1/disk"
    ):
        raise ValueError("invalid_destination_url")
    for key in ("source_path", "destination_root"):
        path = value[key]
        if not path.startswith("disk:/") or ".." in PurePosixPath(path[5:]).parts:
            raise ValueError("invalid_disk_path")
    if value["strategy"] not in {"url", "staged"}:
        raise ValueError("invalid_strategy")
    if not isinstance(value["identity_confirmed"], bool):
        raise ValueError("invalid_identity_confirmation")
    return value


class MigrationStore:
    """One private database; mutations and their events share a transaction."""

    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.root, 0o700)
        self.path = self.root / "jobs.db"
        self._lock = threading.RLock()
        self._closed = False
        with self._connection() as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version not in {0, 1, 2, 3}:
                raise ValueError("unsupported_store_version")
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE,
                    config TEXT NOT NULL, state TEXT NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 0, error TEXT,
                    pause_requested INTEGER NOT NULL DEFAULT 0,
                    stop_requested INTEGER NOT NULL DEFAULT 0,
                    restart_of TEXT REFERENCES jobs(id),
                    resume_state TEXT, remote_root TEXT NOT NULL,
                    namespace_state TEXT NOT NULL DEFAULT 'unclaimed',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS entries (
                    id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id),
                    kind TEXT NOT NULL CHECK(kind IN ('file','dir')),
                    source_path TEXT NOT NULL, destination_path TEXT NOT NULL,
                    size INTEGER NOT NULL CHECK(size >= 0), md5 TEXT,
                    modified TEXT, resource_id TEXT, state TEXT NOT NULL,
                    error TEXT, attempt_id TEXT REFERENCES attempts(id),
                    verification TEXT, created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(job_id, destination_path)
                );
                CREATE TABLE IF NOT EXISTS attempts (
                    id TEXT PRIMARY KEY,
                    entry_id TEXT NOT NULL REFERENCES entries(id),
                    strategy TEXT NOT NULL CHECK(strategy IN ('url','staged')),
                    phase TEXT NOT NULL, remote_path TEXT NOT NULL UNIQUE,
                    operation_id TEXT, local_name TEXT, error TEXT,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL REFERENCES jobs(id),
                    created_at TEXT NOT NULL, type TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS entries_job ON entries(job_id);
                CREATE INDEX IF NOT EXISTS events_job ON events(job_id, sequence);
            """)
            columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(jobs)")
            }
            if "namespace_state" not in columns:
                connection.execute(
                    "ALTER TABLE jobs ADD COLUMN namespace_state TEXT "
                    "NOT NULL DEFAULT 'unclaimed'",
                )
            if "stop_requested" not in columns:
                connection.execute(
                    "ALTER TABLE jobs ADD COLUMN stop_requested INTEGER "
                    "NOT NULL DEFAULT 0",
                )
            if "restart_of" not in columns:
                connection.execute(
                    "ALTER TABLE jobs ADD COLUMN restart_of TEXT REFERENCES jobs(id)",
                )
            connection.execute("PRAGMA user_version=3")
        os.chmod(self.path, 0o600)

    @contextmanager
    def _connection(self, *, write=False):
        with self._lock:
            if self._closed:
                raise RuntimeError("store_closed")
            connection = sqlite3.connect(self.path, timeout=30)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys=ON")
            try:
                if write:
                    connection.execute("BEGIN IMMEDIATE")
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            finally:
                connection.close()

    def close(self):
        with self._lock:
            self._closed = True

    @staticmethod
    def _required(connection, table, item_id):
        row = connection.execute(
            f"SELECT * FROM {table} WHERE id = ?", (item_id,),
        ).fetchone()
        if row is None:
            raise KeyError(item_id)
        return dict(row)

    @staticmethod
    def _job(connection, row):
        value = dict(row)
        value.pop("idempotency_key", None)
        value["config"] = json.loads(value["config"])
        value["pause_requested"] = bool(value["pause_requested"])
        value["stop_requested"] = bool(value["stop_requested"])
        totals = connection.execute(
            "SELECT kind,state,COUNT(*),SUM(size) FROM entries "
            "WHERE job_id=? GROUP BY kind,state", (value["id"],),
        ).fetchall()
        counts, directories = {}, {}
        total_bytes = verified_bytes = total_files = total_directories = 0
        for kind, state, count, size in totals:
            if kind == "dir":
                directories[state] = count
                total_directories += count
            else:
                counts[state] = count
                total_files += count
                total_bytes += size
                if state in {"done", "already_verified"}:
                    verified_bytes += size
        value.update(
            counts=counts, directory_counts=directories,
            total_bytes=total_bytes, verified_bytes=verified_bytes,
            total_files=total_files, total_directories=total_directories,
        )
        return value

    @staticmethod
    def _event(connection, job_id, event_type, payload, *, increment=True):
        _code(event_type)
        if set(payload) - _EVENT_KEYS:
            raise ValueError("invalid_event_fields")
        for key, value in payload.items():
            if value is None or isinstance(value, (bool, int)):
                continue
            if key == "operation_id":
                if not valid_operation_reference(value):
                    raise ValueError("invalid_event_value")
            elif key in {"entry_id", "attempt_id", "restart_of", "restart_job_id"}:
                if not isinstance(value, str) or not _OPERATION_ID.fullmatch(value):
                    raise ValueError("invalid_event_value")
            elif key == "evidence_digest":
                if (
                    not isinstance(value, str)
                    or not re.fullmatch("[a-f0-9]{64}", value)
                ):
                    raise ValueError("invalid_evidence_digest")
            elif key == "status" and value == "in-progress":
                continue
            else:
                _code(value)
        now = _now()
        if increment:
            connection.execute(
                "UPDATE jobs SET revision=revision+1,updated_at=? WHERE id=?",
                (now, job_id),
            )
        cursor = connection.execute(
            "INSERT INTO events(job_id,created_at,type,payload) VALUES(?,?,?,?)",
            (job_id, now, event_type, _json(payload)),
        )
        return {
            "sequence": cursor.lastrowid, "job_id": job_id,
            "created_at": now, "type": event_type, "payload": dict(payload),
        }

    def create(self, config, idempotency_key):
        config = canonical_config(config)
        if not isinstance(idempotency_key, str) or not _OPERATION_ID.fullmatch(
            idempotency_key,
        ):
            raise ValueError("invalid_idempotency_key")
        serialized = _json(config)
        with self._connection(write=True) as connection:
            existing = connection.execute(
                "SELECT * FROM jobs WHERE idempotency_key=?", (idempotency_key,),
            ).fetchone()
            if existing:
                if existing["config"] != serialized:
                    raise ValueError("idempotency_config_mismatch")
                if existing["restart_of"] is not None:
                    raise ValueError("idempotency_parent_mismatch")
                return self._job(connection, existing)
            return self._create_job(connection, serialized, idempotency_key)

    def _create_job(self, connection, config, idempotency_key, *, restart_of=None):
        job_id, now = uuid.uuid4().hex, _now()
        connection.execute(
            "INSERT INTO jobs(id,idempotency_key,config,state,remote_root,"
            "restart_of,created_at,updated_at) VALUES(?,?,?,'queued',?,?,?,?)",
            (job_id, idempotency_key, config,
             f"disk:/.disk-copy-transfer-{job_id}", restart_of, now, now),
        )
        payload = {"state": "queued"}
        if restart_of is not None:
            payload["restart_of"] = restart_of
        self._event(connection, job_id, "job_created", payload, increment=False)
        return self._job(connection, self._required(connection, "jobs", job_id))

    def request_stop(self, job_id):
        """Persist a local stop independently of the worker's remote-request guard."""
        with self._connection(write=True) as connection:
            job = self._required(connection, "jobs", job_id)
            if job["stop_requested"] or job["state"] == "stopped":
                return self._job(connection, job)
            if job["state"] not in {
                "queued", "planning", "ready", "running", "paused",
                "awaiting_credentials", "needs_attention", "completed_with_issues",
            }:
                raise ValueError("job_not_stoppable")
            connection.execute(
                "UPDATE jobs SET stop_requested=1 WHERE id=?", (job_id,),
            )
            self._event(connection, job_id, "stop_requested", {"stop_requested": True})
            return self._job(connection, self._required(connection, "jobs", job_id))

    def restart(self, job_id, config, idempotency_key):
        config = canonical_config(config)
        if not isinstance(idempotency_key, str) or not _OPERATION_ID.fullmatch(
            idempotency_key,
        ):
            raise ValueError("invalid_idempotency_key")
        serialized = _json(config)
        with self._connection(write=True) as connection:
            parent = self._required(connection, "jobs", job_id)
            if parent["state"] not in {
                "stopped", "completed", "completed_with_issues",
            } or (parent["stop_requested"] and parent["state"] != "stopped"):
                raise ValueError("job_not_restartable")
            if parent["config"] != serialized:
                raise ValueError("restart_config_mismatch")
            existing = connection.execute(
                "SELECT * FROM jobs WHERE idempotency_key=?", (idempotency_key,),
            ).fetchone()
            if existing:
                if existing["restart_of"] != job_id:
                    raise ValueError("idempotency_parent_mismatch")
                if existing["config"] != serialized:
                    raise ValueError("idempotency_config_mismatch")
                return self._job(connection, existing)
            child = self._create_job(
                connection, serialized, idempotency_key, restart_of=job_id,
            )
            self._event(connection, job_id, "job_restarted", {
                "restart_job_id": child["id"],
            })
            return child

    def get(self, job_id):
        with self._connection() as connection:
            return self._job(connection, self._required(connection, "jobs", job_id))

    def jobs(self, limit=50, offset=0):
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC,id LIMIT ? OFFSET ?",
                (max(0, limit), max(0, offset)),
            ).fetchall()
            return [self._job(connection, row) for row in rows]

    def update_job(self, job_id, *, expected_revision=None, **fields):
        if not fields or set(fields) - {
            "state", "error", "pause_requested", "resume_state", "namespace_state",
            "stop_requested",
        }:
            raise ValueError("invalid_job_fields")
        for key, value in fields.items():
            if key in {"pause_requested", "stop_requested"}:
                if not isinstance(value, bool):
                    raise ValueError(f"invalid_{key}")
            elif key == "namespace_state":
                if value not in {"unclaimed", "intent", "owned", "unknown"}:
                    raise ValueError("invalid_namespace_state")
            else:
                _code(value)
        with self._connection(write=True) as connection:
            current = self._required(connection, "jobs", job_id)
            if current["stop_requested"] or current["state"] == "stopped":
                if fields.get("stop_requested") is False or (
                    "state" in fields and fields["state"] != "stopped"
                    and fields["state"] != current["state"]
                ):
                    raise ValueError("job_stopped")
            if (
                expected_revision is not None
                and current["revision"] != expected_revision
            ):
                raise ValueError("stale_revision")
            connection.execute(
                "UPDATE jobs SET " + ",".join(f"{key}=?" for key in fields)
                + " WHERE id=?", (*fields.values(), job_id),
            )
            self._event(connection, job_id, "job_updated", fields)
            return self._job(connection, self._required(connection, "jobs", job_id))

    def add_entries(self, job_id, entries, *, mark_ready=False):
        if not isinstance(mark_ready, bool):
            raise ValueError("invalid_mark_ready")
        values = []
        for item in entries:
            if set(item) - {
                "kind", "source_path", "destination_path", "size", "md5",
                "modified", "resource_id",
            }:
                raise ValueError("invalid_entry_fields")
            kind = item["kind"]
            if kind not in {"file", "dir"}:
                raise ValueError("invalid_entry_kind")
            size = item.get("size", 0)
            if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                raise ValueError("invalid_entry_size")
            source = _string(item["source_path"])
            destination = _string(item["destination_path"])
            if not source.startswith("disk:/") or not destination.startswith("disk:/"):
                raise ValueError("invalid_disk_path")
            md5 = item.get("md5")
            if md5 is not None and (
                not isinstance(md5, str) or not re.fullmatch("[a-fA-F0-9]{32}", md5)
            ):
                raise ValueError("invalid_entry_md5")
            modified, resource_id = item.get("modified"), item.get("resource_id")
            for value in (modified, resource_id):
                if value is not None:
                    _string(value)
            entry_id, now = uuid.uuid4().hex, _now()
            values.append((
                entry_id, job_id, kind, source, destination, size, md5,
                modified, resource_id, now, now,
            ))
        try:
            with self._connection(write=True) as connection:
                job = self._required(connection, "jobs", job_id)
                if job["stop_requested"] or job["state"] == "stopped":
                    raise ValueError("job_stopped")
                if connection.execute(
                    "SELECT 1 FROM entries WHERE job_id=? LIMIT 1", (job_id,),
                ).fetchone():
                    raise ValueError("plan_already_exists")
                connection.executemany(
                    "INSERT INTO entries(id,job_id,kind,source_path,destination_path,"
                    "size,md5,modified,resource_id,state,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,'planned',?,?)", values,
                )
                payload = {"count": len(values)}
                if mark_ready:
                    connection.execute(
                        "UPDATE jobs SET state='ready',error=NULL WHERE id=?",
                        (job_id,),
                    )
                    payload["state"] = "ready"
                self._event(connection, job_id, "plan_created", payload)
        except sqlite3.IntegrityError as error:
            raise ValueError("invalid_plan") from error

    def entries(self, job_id, limit=200, offset=0, state=None):
        with self._connection() as connection:
            self._required(connection, "jobs", job_id)
            condition, parameters = "job_id=?", [job_id]
            if state is not None:
                condition += " AND state=?"
                parameters.append(state)
            rows = connection.execute(
                "SELECT * FROM entries WHERE " + condition
                + " ORDER BY rowid LIMIT ? OFFSET ?",
                (*parameters, max(0, limit), max(0, offset)),
            ).fetchall()
            return [dict(row) for row in rows]

    def entry(self, entry_id):
        with self._connection() as connection:
            return self._required(connection, "entries", entry_id)

    def update_entry(self, entry_id, **fields):
        if not fields or set(fields) - {"state", "error", "attempt_id", "verification"}:
            raise ValueError("invalid_entry_fields")
        for key, value in fields.items():
            if key != "attempt_id":
                _code(value)
        with self._connection(write=True) as connection:
            current = self._required(connection, "entries", entry_id)
            if fields.get("attempt_id") is not None:
                attempt = self._required(connection, "attempts", fields["attempt_id"])
                if attempt["entry_id"] != entry_id:
                    raise ValueError("attempt_entry_mismatch")
            connection.execute(
                "UPDATE entries SET " + ",".join(f"{key}=?" for key in fields)
                + ",updated_at=? WHERE id=?", (*fields.values(), _now(), entry_id),
            )
            self._event(
                connection, current["job_id"], "entry_updated",
                {"entry_id": entry_id, **fields},
            )
            return self._required(connection, "entries", entry_id)

    def new_attempt(self, entry_id, strategy):
        if strategy not in {"url", "staged"}:
            raise ValueError("invalid_strategy")
        with self._connection(write=True) as connection:
            entry = self._required(connection, "entries", entry_id)
            job = self._required(connection, "jobs", entry["job_id"])
            if job["stop_requested"] or job["state"] == "stopped":
                raise ValueError("job_stopped")
            name = PurePosixPath(entry["destination_path"]).name
            if not name or name in {".", ".."}:
                raise ValueError("invalid_attempt_name")
            attempt_id, now = uuid.uuid4().hex, _now()
            remote = f"{job['remote_root']}/{entry_id}/{attempt_id}/{name}"
            connection.execute(
                "INSERT INTO attempts(id,entry_id,strategy,phase,remote_path,"
                "created_at,updated_at) VALUES(?,?,?,'prepared',?,?,?)",
                (attempt_id, entry_id, strategy, remote, now, now),
            )
            connection.execute(
                "UPDATE entries SET attempt_id=?,updated_at=? WHERE id=?",
                (attempt_id, now, entry_id),
            )
            self._event(connection, entry["job_id"], "attempt_created", {
                "entry_id": entry_id, "attempt_id": attempt_id,
                "strategy": strategy, "phase": "prepared",
            })
            return self._required(connection, "attempts", attempt_id)

    def attempt(self, attempt_id):
        with self._connection() as connection:
            return self._required(connection, "attempts", attempt_id)

    def attempts(self, job_id):
        with self._connection() as connection:
            self._required(connection, "jobs", job_id)
            rows = connection.execute(
                "SELECT attempts.* FROM attempts JOIN entries "
                "ON entries.id=attempts.entry_id WHERE entries.job_id=? "
                "ORDER BY attempts.rowid", (job_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def update_attempt(self, attempt_id, **fields):
        if not fields or set(fields) - {"phase", "operation_id", "local_name", "error"}:
            raise ValueError("invalid_attempt_fields")
        for key, value in fields.items():
            if value is None:
                continue
            if key == "operation_id":
                if not valid_operation_reference(value):
                    raise ValueError("invalid_operation_id")
            elif key == "local_name":
                _string(value, limit=256)
                path = PurePosixPath(value)
                if (
                    path.is_absolute() or ".." in path.parts
                    or any(not re.fullmatch(r"[A-Za-z0-9_.-]+", part)
                           for part in value.split("/"))
                ):
                    raise ValueError("invalid_local_name")
            else:
                _code(value)
        with self._connection(write=True) as connection:
            current = self._required(connection, "attempts", attempt_id)
            entry = self._required(connection, "entries", current["entry_id"])
            connection.execute(
                "UPDATE attempts SET " + ",".join(f"{key}=?" for key in fields)
                + ",updated_at=? WHERE id=?", (*fields.values(), _now(), attempt_id),
            )
            payload = {"entry_id": entry["id"], "attempt_id": attempt_id}
            payload.update({
                key: value for key, value in fields.items() if key != "local_name"
            })
            self._event(connection, entry["job_id"], "attempt_updated", payload)
            return self._required(connection, "attempts", attempt_id)

    def event(self, job_id, event_type, payload):
        with self._connection(write=True) as connection:
            self._required(connection, "jobs", job_id)
            return self._event(connection, job_id, event_type, payload)

    def resolve_attempt(
        self, job_id, attempt_id, expected_revision, *, phase, operation_id,
        entry_state, error, status, evidence_kind, evidence_digest=None,
    ):
        """CAS an operator-established outcome and its audit record atomically."""
        if (
            not isinstance(expected_revision, int)
            or isinstance(expected_revision, bool)
        ):
            raise ValueError("invalid_revision")
        for value in (phase, entry_state, error, evidence_kind):
            _code(value)
        if not isinstance(status, str) or status not in {
            "success", "failed", "in-progress",
        }:
            raise ValueError("invalid_resolution_status")
        if operation_id is not None and not valid_operation_reference(operation_id):
            raise ValueError("invalid_operation_id")
        if evidence_kind == "operation_reference":
            if not operation_id or evidence_digest is not None:
                raise ValueError("invalid_resolution_evidence")
        elif evidence_kind == "administrator_terminal_server_evidence":
            if (
                operation_id is not None or status == "in-progress"
                or not isinstance(evidence_digest, str)
                or not re.fullmatch("[a-f0-9]{64}", evidence_digest)
            ):
                raise ValueError("invalid_resolution_evidence")
        else:
            raise ValueError("invalid_resolution_evidence")
        with self._connection(write=True) as connection:
            job = self._required(connection, "jobs", job_id)
            if job["revision"] != expected_revision:
                raise ValueError("stale_revision")
            attempt = self._required(connection, "attempts", attempt_id)
            entry = self._required(connection, "entries", attempt["entry_id"])
            stopped = job["state"] == "stopped"
            stopped_final = stopped and attempt["phase"] in {
                "move_intent", "waiting_move", "unknown_move",
            }
            if (job["stop_requested"] and not stopped) or (
                stopped and not stopped_final
            ):
                raise ValueError("job_stopped")
            if (
                entry["job_id"] != job_id or entry["attempt_id"] != attempt_id
                or (not stopped_final and attempt["phase"] not in {
                    "transfer_intent", "move_intent", "unknown_transfer",
                    "unknown_move",
                })
            ):
                raise ValueError("attempt_not_unknown")
            if (stopped_final and attempt["operation_id"] and operation_id
                    and attempt["operation_id"] != operation_id):
                raise ValueError("operation_reference_mismatch")
            move = "move" in attempt["phase"]
            waiting = "waiting_move" if move else "waiting_transfer"
            verifying = "verifying_final" if move else "verifying"
            states = {"committing", "waiting_operation"} if move else {
                "verifying", "waiting_operation",
            }
            if status == "failed":
                valid = phase == entry_state == "failed" and bool(error)
            else:
                valid = (
                    error is None and entry_state in states
                    and (
                        (phase == waiting and operation_id is not None)
                        or (status == "success" and phase == verifying)
                    )
                )
            if not valid:
                raise ValueError("invalid_resolution_transition")
            now = _now()
            connection.execute(
                "UPDATE attempts SET phase=?,operation_id=?,error=?,updated_at=? "
                "WHERE id=?", (phase, operation_id, error, now, attempt_id),
            )
            connection.execute(
                "UPDATE entries SET state=?,error=?,updated_at=? WHERE id=?",
                (entry_state, error, now, entry["id"]),
            )
            changed = connection.execute(
                "UPDATE jobs SET state=?,error=NULL WHERE id=? AND revision=?",
                ("stopped" if stopped else "needs_attention", job_id,
                 expected_revision),
            )
            if changed.rowcount != 1:
                raise ValueError("stale_revision")
            self._event(connection, job_id, "administrative_resolution", {
                "attempt_id": attempt_id, "entry_id": entry["id"],
                "status": status, "phase": phase, "state": entry_state,
                "operation_id": operation_id, "error": error,
                "evidence_kind": evidence_kind, "evidence_digest": evidence_digest,
            })
            return self._job(connection, self._required(connection, "jobs", job_id))

    def events(self, job_id, after=0, limit=200):
        with self._connection() as connection:
            self._required(connection, "jobs", job_id)
            rows = connection.execute(
                "SELECT * FROM events WHERE job_id=? AND sequence>? "
                "ORDER BY sequence LIMIT ?", (job_id, max(0, after), max(0, limit)),
            ).fetchall()
            return [
                {**dict(row), "payload": json.loads(row["payload"])} for row in rows
            ]

    def recover(self):
        with self._connection(write=True) as connection:
            rows = connection.execute(
                "SELECT id,state FROM jobs WHERE state IN "
                "('queued','planning','ready','running','paused','needs_attention') "
                "AND stop_requested=0",
            ).fetchall()
            for row in rows:
                resume = "queued" if row["state"] == "planning" else row["state"]
                connection.execute(
                    "UPDATE jobs SET state='awaiting_credentials',resume_state=? "
                    "WHERE id=?", (resume, row["id"]),
                )
                self._event(connection, row["id"], "credentials_required", {
                    "state": "awaiting_credentials", "resume_state": resume,
                })
