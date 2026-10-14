"""SQLite intents and replayable events, with no persisted credentials."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import uuid
import unicodedata
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

from .diagnostics import DIAGNOSTIC_KEYS, diagnostic_fields
from .errors import MigrationError, OPERATION_REFERENCE_ISSUES, disk_path
from .journal import STAGE_LABELS, STAGE_STATUSES, enrich_event
from .operations import valid_operation_reference

POINT_RETRY_PHASES = frozenset({
    "failed", "transfer_intent", "unknown_transfer", "move_intent", "unknown_move",
    "abandoned_transfer", "abandoned_move", "waiting_transfer", "waiting_move",
})

CONFIG_KEYS = frozenset({
    "source_org_id", "source_client_id", "source_email", "source_path",
    "destination_url", "destination_identity", "destination_root", "strategy",
    "identity_confirmed",
})
SHARED_CONFIG_KEYS = frozenset({
    "destination_type", "destination_vd_hash", "destination_base",
})
SOURCE_CONFIG_KEYS = frozenset({"source_type", "source_vd_hash", "source_folder"})
_CODE = re.compile(r"[a-z][a-z0-9_]{0,79}\Z")
_OPERATION_ID = re.compile(r"[A-Za-z0-9_.:-]{1,256}\Z")
_EVENT_KEYS = DIAGNOSTIC_KEYS | frozenset({
    "state", "phase", "error", "entry_id", "attempt_id", "strategy", "count",
    "total_bytes", "verified_bytes", "total_files", "total_directories",
    "revision", "resume_state", "pause_requested", "verification", "status",
    "operation_id", "retry_count",
    "remaining", "required_bytes", "available_bytes",
    "namespace_state",
    "stop_requested", "full_stop_requested", "restart_of", "restart_job_id",
    "stage", "http_status", "duration_ms", "remote_status", "source_path",
    "download_attempts", "max_download_attempts", "retry_delay_ms", "retry_at_ms",
    "point_retry_requested", "point_retry_attempts", "max_point_retry_attempts",
    "operation_reference_issue",
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
    if not isinstance(config, dict):
        raise ValueError("invalid_config_fields")
    kind = config.get("destination_type", "personal")
    if kind not in {"personal", "shared"}:
        raise ValueError("invalid_destination_shared_disk")
    value = dict(config)
    source_kind = value.get("source_type", "personal")
    if source_kind not in {"personal", "shared"}:
        raise ValueError("invalid_source_shared_disk")
    source_public = value.pop("source_public", False)
    if not isinstance(source_public, bool) or (source_public and kind != "shared"):
        raise ValueError("invalid_source_public")
    keys = CONFIG_KEYS | SHARED_CONFIG_KEYS if kind == "shared" else CONFIG_KEYS
    if source_kind == "shared":
        keys |= SOURCE_CONFIG_KEYS
        if (kind != "shared" or source_public
                or value.get("strategy") != "staged"):
            raise ValueError("invalid_source_shared_disk")
        vd_hash = value.get("source_vd_hash")
        folder = value.get("source_folder")
        if (not isinstance(vd_hash, str)
                or not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", vd_hash)
                or not isinstance(folder, str) or not folder or folder in {".", ".."}
                or any(c in folder for c in "/\\:")
                or any(ord(c) < 32 or ord(c) == 127 for c in folder)
                or len(folder.encode("utf-8")) > 255
                or unicodedata.normalize("NFC", folder) != folder):
            raise ValueError("invalid_source_shared_disk")
    elif value.get("source_type") == "personal":
        # Legacy personal jobs remain byte-for-byte canonical without this field.
        value.pop("source_type")
    if set(value) - keys:
        raise ValueError("invalid_config_fields")
    value.setdefault("strategy", "url")
    value.setdefault("identity_confirmed", False)
    if set(value) != keys:
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
    if kind == "shared":
        vd_hash, base = value["destination_vd_hash"], value["destination_base"]
        if (not isinstance(vd_hash, str)
                or not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", vd_hash)):
            raise ValueError("invalid_destination_shared_disk")
        if not isinstance(base, str) or not base.startswith("disk:/"):
            raise ValueError("invalid_destination_shared_disk")
        suffix = base[5:]
        if (len(base.encode("utf-8")) > 4096
                or (suffix != "/" and any(
                    p in {"", ".", ".."} or "\\" in p or ":" in p
                    or any(ord(c) < 32 or ord(c) == 127 for c in p)
                    or len(p.encode("utf-8")) > 255
                    for p in suffix.split("/")[1:]
                ))):
            raise ValueError("invalid_destination_shared_disk")
        email = value["source_email"]
        folder = value["source_folder"] if source_kind == "shared" else email
        if ("@" not in email or any(c.isspace() for c in email)
                or any(c in email for c in "/\\:")
                or len(email.encode("utf-8")) > 255
                or value["destination_root"] != base.rstrip("/") + "/" + folder):
            raise ValueError("invalid_destination_shared_disk")
        if len(value["destination_root"].encode("utf-8")) > 4096:
            raise ValueError("invalid_destination_shared_disk")
    if value["strategy"] not in {"url", "staged"}:
        raise ValueError("invalid_strategy")
    if not isinstance(value["identity_confirmed"], bool):
        raise ValueError("invalid_identity_confirmation")
    if source_public:
        value["source_public"] = True
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
            if version not in {0, 1, 2, 3, 4, 5, 6, 7, 8}:
                raise ValueError("unsupported_store_version")
            connection.execute("PRAGMA journal_mode=WAL")
            # Rebuild attempts while keeping entries.attempt_id references unchanged.
            connection.execute("PRAGMA foreign_keys=OFF")
            connection.execute("BEGIN IMMEDIATE")
            schema = """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE,
                    config TEXT NOT NULL, state TEXT NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 0, error TEXT,
                    pause_requested INTEGER NOT NULL DEFAULT 0,
                    stop_requested INTEGER NOT NULL DEFAULT 0,
                    full_stop_requested INTEGER NOT NULL DEFAULT 0
                        CHECK(full_stop_requested IN (0,1)),
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
                    download_attempts INTEGER NOT NULL DEFAULT 0,
                    retry_at_ms INTEGER,
                    point_retry_requested INTEGER NOT NULL DEFAULT 0
                        CHECK(point_retry_requested IN (0,1)),
                    point_retry_attempts INTEGER NOT NULL DEFAULT 0
                        CHECK(point_retry_attempts BETWEEN 0 AND 3),
                    UNIQUE(job_id, destination_path)
                );
                CREATE TABLE IF NOT EXISTS attempts (
                    id TEXT PRIMARY KEY,
                    entry_id TEXT NOT NULL REFERENCES entries(id),
                    strategy TEXT NOT NULL CHECK(strategy IN ('url','staged')),
                    phase TEXT NOT NULL, remote_path TEXT NOT NULL,
                    operation_id TEXT, local_name TEXT, error TEXT,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS publications (
                    entry_id TEXT PRIMARY KEY REFERENCES entries(id),
                    job_id TEXT NOT NULL REFERENCES jobs(id),
                    state TEXT NOT NULL CHECK(state IN (
                        'planned','intent','published','failed'
                    )), error TEXT, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL REFERENCES jobs(id),
                    created_at TEXT NOT NULL, type TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS entries_job ON entries(job_id);
                CREATE INDEX IF NOT EXISTS publications_job ON publications(job_id);
                CREATE INDEX IF NOT EXISTS events_job ON events(job_id, sequence);
            """
            for statement in schema.split(";"):
                if statement.strip():
                    connection.execute(statement)
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
            if "full_stop_requested" not in columns:
                connection.execute(
                    "ALTER TABLE jobs ADD COLUMN full_stop_requested INTEGER "
                    "NOT NULL DEFAULT 0 CHECK(full_stop_requested IN (0,1))",
                )
            if "restart_of" not in columns:
                connection.execute(
                    "ALTER TABLE jobs ADD COLUMN restart_of TEXT REFERENCES jobs(id)",
                )
            entry_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(entries)")
            }
            if "download_attempts" not in entry_columns:
                connection.execute(
                    "ALTER TABLE entries ADD COLUMN download_attempts INTEGER "
                    "NOT NULL DEFAULT 0",
                )
            if "retry_at_ms" not in entry_columns:
                connection.execute("ALTER TABLE entries ADD COLUMN retry_at_ms INTEGER")
            if "point_retry_requested" not in entry_columns:
                connection.execute(
                    "ALTER TABLE entries ADD COLUMN point_retry_requested INTEGER "
                    "NOT NULL DEFAULT 0 CHECK(point_retry_requested IN (0,1))",
                )
            if "point_retry_attempts" not in entry_columns:
                connection.execute(
                    "ALTER TABLE entries ADD COLUMN point_retry_attempts INTEGER "
                    "NOT NULL DEFAULT 0 CHECK(point_retry_attempts BETWEEN 0 AND 3)",
                )
            if version < 7:
                connection.execute("""
                    CREATE TABLE attempts_v7 (
                        id TEXT PRIMARY KEY,
                        entry_id TEXT NOT NULL REFERENCES entries(id),
                        strategy TEXT NOT NULL CHECK(strategy IN ('url','staged')),
                        phase TEXT NOT NULL, remote_path TEXT NOT NULL,
                        operation_id TEXT, local_name TEXT, error TEXT,
                        created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                    )
                """)
                connection.execute(
                    "INSERT INTO attempts_v7(rowid,id,entry_id,strategy,phase,"
                    "remote_path,operation_id,local_name,error,created_at,updated_at) "
                    "SELECT rowid,id,entry_id,strategy,phase,remote_path,operation_id,"
                    "local_name,error,created_at,updated_at FROM attempts",
                )
                connection.execute("DROP TABLE attempts")
                connection.execute("ALTER TABLE attempts_v7 RENAME TO attempts")
                if connection.execute("PRAGMA foreign_key_check").fetchone():
                    raise sqlite3.IntegrityError("migration_foreign_key_check_failed")
            if version < 8:
                connection.execute("PRAGMA user_version=8")
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
        value = dict(row)
        if table == "entries":
            value["point_retry_requested"] = bool(value["point_retry_requested"])
        return value

    @staticmethod
    def _job(connection, row):
        value = dict(row)
        value.pop("idempotency_key", None)
        value["config"] = json.loads(value["config"])
        value["pause_requested"] = bool(value["pause_requested"])
        value["stop_requested"] = bool(value["stop_requested"])
        value["full_stop_requested"] = bool(value["full_stop_requested"])
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
            point_retry_active=bool(connection.execute(
                "SELECT 1 FROM entries WHERE job_id=? "
                "AND point_retry_requested=1 LIMIT 1", (value["id"],),
            ).fetchone()),
        )
        return value

    @staticmethod
    def _event(connection, job_id, event_type, payload, *, increment=True):
        _code(event_type)
        if set(payload) - _EVENT_KEYS:
            raise ValueError("invalid_event_fields")
        if event_type == "transfer_stage":
            if (payload.get("stage") not in STAGE_LABELS
                    or payload.get("status") not in STAGE_STATUSES):
                raise ValueError("invalid_transfer_stage")
            _code(payload.get("error"))
        for key, value in payload.items():
            if key in DIAGNOSTIC_KEYS:
                if diagnostic_fields({key: value}) != {key: value}:
                    raise ValueError("invalid_event_value")
                continue
            if key == "operation_reference_issue":
                if (payload.get("error") != "invalid_operation_reference"
                        or not isinstance(value, str)
                        or value not in OPERATION_REFERENCE_ISSUES):
                    raise ValueError("invalid_event_value")
                continue
            if key in {
                "download_attempts", "max_download_attempts", "retry_delay_ms",
                "retry_at_ms", "point_retry_attempts", "max_point_retry_attempts",
            }:
                nullable = key in {"retry_delay_ms", "retry_at_ms"}
                minimum = 1 if key in {
                    "max_download_attempts", "max_point_retry_attempts",
                } else 0
                maximum = (3 if key in {
                    "point_retry_attempts", "max_point_retry_attempts",
                } else 5 if key in {
                    "download_attempts", "max_download_attempts",
                } else 2 ** 63 - 1)
                if value is None and nullable:
                    continue
                if (not isinstance(value, int) or isinstance(value, bool)
                        or not minimum <= value <= maximum):
                    raise ValueError("invalid_event_value")
                continue
            if key == "point_retry_requested":
                if not isinstance(value, bool):
                    raise ValueError("invalid_event_value")
                continue
            if key in {"http_status", "duration_ms"}:
                if value is not None and (
                    not isinstance(value, int) or isinstance(value, bool)
                    or (not 100 <= value <= 599 if key == "http_status" else value < 0)
                ):
                    raise ValueError("invalid_event_value")
                continue
            if key == "source_path":
                if value is None:
                    continue
                _string(value)
                try:
                    if (not value.startswith("disk:/") or disk_path(value) != value
                            or "//" in value):
                        raise ValueError("invalid_event_value")
                except MigrationError:
                    raise ValueError("invalid_event_value") from None
                continue
            if key == "remote_status":
                if value not in {None, "in-progress", "success", "failed"}:
                    raise ValueError("invalid_event_value")
                continue
            if value is None:
                continue
            if key == "operation_id":
                if not valid_operation_reference(value):
                    raise ValueError("invalid_event_value")
            elif key in {"entry_id", "attempt_id", "restart_of", "restart_job_id"}:
                if not isinstance(value, str) or not _OPERATION_ID.fullmatch(value):
                    raise ValueError("invalid_event_value")
            elif isinstance(value, (bool, int)):
                continue
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

    def create(self, config, idempotency_key, *, initial_state="queued"):
        if initial_state not in {"queued", "planning"}:
            raise ValueError("invalid_initial_state")
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
            return self._create_job(connection, serialized, idempotency_key,
                                    initial_state=initial_state)

    def _create_job(self, connection, config, idempotency_key, *, restart_of=None,
                    initial_state="queued"):
        job_id, now = uuid.uuid4().hex, _now()
        direct = json.loads(config).get("destination_type") == "shared"
        connection.execute(
            "INSERT INTO jobs(id,idempotency_key,config,state,remote_root,"
            "namespace_state,restart_of,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (job_id, idempotency_key, config, initial_state,
             "" if direct else f"disk:/.disk-copy-transfer-{job_id}",
             "owned" if direct else "unclaimed", restart_of, now, now),
        )
        payload = {"state": initial_state}
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

    def request_full_stop(self, job_id):
        """Stop local work without declaring a pending server write cancelled."""
        with self._connection(write=True) as connection:
            job = self._required(connection, "jobs", job_id)
            if job["full_stop_requested"]:
                return self._job(connection, job)
            if job["state"] not in {
                "queued", "planning", "ready", "running", "paused", "stopped",
                "awaiting_credentials", "needs_attention", "completed_with_issues",
            }:
                raise ValueError("job_not_stoppable")
            connection.execute(
                "UPDATE jobs SET stop_requested=1,full_stop_requested=1 WHERE id=?",
                (job_id,),
            )
            connection.execute(
                "UPDATE entries SET point_retry_requested=0,retry_at_ms=NULL "
                "WHERE job_id=?", (job_id,),
            )
            self._event(connection, job_id, "full_stop_requested", {
                "stop_requested": True, "full_stop_requested": True,
            })
            return self._job(connection, self._required(connection, "jobs", job_id))

    def cancel_local_entries(self, job_id):
        """Cancel queued local work after its last request has unwound under the guard."""
        with self._connection(write=True) as connection:
            job = self._required(connection, "jobs", job_id)
            if job["state"] != "stopped" or not job["full_stop_requested"]:
                raise ValueError("job_not_fully_stopped")
            changed = connection.execute(
                "UPDATE entries SET state='cancelled',error='local_operation_cancelled',"
                "point_retry_requested=0,retry_at_ms=NULL,updated_at=? WHERE job_id=? "
                "AND state IN ('planned','transferring','waiting_operation','verifying',"
                "'committing','downloading','staged','outcome_unknown')", (_now(), job_id),
            ).rowcount
            if changed:
                self._event(connection, job_id, "local_operations_cancelled", {"count": changed})
            return changed

    def release_cancelled_path_conflicts(self):
        """Release only artificial conflicts left by the removed path reservation."""
        with self._connection(write=True) as connection:
            rows = connection.execute(
                "SELECT id,job_id FROM entries WHERE state='conflict' "
                "AND error='destination_path_reserved'",
            ).fetchall()
            for row in rows:
                connection.execute(
                    "UPDATE entries SET state='failed',error='stopped_job_path_released',"
                    "point_retry_requested=0,retry_at_ms=NULL,updated_at=? WHERE id=?",
                    (_now(), row["id"]),
                )
                self._event(connection, row["job_id"], "entry_updated", {
                    "entry_id": row["id"], "state": "failed", "error": "stopped_job_path_released",
                })
            return len(rows)

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
            if config.get("destination_type") == "shared":
                config.update(
                    destination_base="disk:/",
                    destination_root="disk:/" + config["source_email"],
                    source_public=True,
                )
                serialized = _json(canonical_config(config))
            existing = connection.execute(
                "SELECT * FROM jobs WHERE idempotency_key=?", (idempotency_key,),
            ).fetchone()
            if existing:
                if existing["restart_of"] != job_id:
                    raise ValueError("idempotency_parent_mismatch")
                if existing["config"] not in {serialized, parent["config"]}:
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

    def add_entries(self, job_id, entries, *, mark_ready=False, append=False):
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
                if not append and connection.execute(
                    "SELECT 1 FROM entries WHERE job_id=? LIMIT 1", (job_id,),
                ).fetchone():
                    raise ValueError("plan_already_exists")
                connection.executemany(
                    "INSERT INTO entries(id,job_id,kind,source_path,destination_path,"
                    "size,md5,modified,resource_id,state,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,'planned',?,?)", values,
                )
                config = json.loads(job["config"])
                if config.get("source_public"):
                    root = config["source_path"]
                    publications = [
                        (value[0], job_id, value[-1]) for value in values
                        if (value[3] == root if root != "disk:/" else (
                            value[3] != "disk:/" and "/" not in value[3][6:]
                        ))
                    ]
                    connection.executemany(
                        "INSERT INTO publications(entry_id,job_id,state,updated_at) "
                        "VALUES(?,?,'planned',?)", publications,
                    )
                payload = {"count": len(values)}
                if mark_ready:
                    connection.execute(
                        "UPDATE jobs SET state='ready',error=NULL WHERE id=?",
                        (job_id,),
                    )
                    payload["state"] = "ready"
                self._event(connection, job_id, "plan_created", payload)
                return [value[0] for value in values]
        except sqlite3.IntegrityError as error:
            raise ValueError("invalid_plan") from error

    def reset_incomplete_plan(self, job_id):
        """Discard only an unfinished, read-only scan; never erase transfer evidence."""
        with self._connection(write=True) as connection:
            job = self._required(connection, "jobs", job_id)
            if (job["state"] not in {"planning", "queued", "awaiting_credentials"}
                    or connection.execute(
                        "SELECT 1 FROM attempts a JOIN entries e ON e.id=a.entry_id "
                        "WHERE e.job_id=? LIMIT 1", (job_id,),
                    ).fetchone()):
                raise ValueError("plan_already_exists")
            connection.execute("DELETE FROM publications WHERE job_id=?", (job_id,))
            connection.execute("DELETE FROM entries WHERE job_id=?", (job_id,))
            self._event(connection, job_id, "plan_reset", {"state": "planning"})

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
            return [dict(row, point_retry_requested=bool(row["point_retry_requested"]))
                    for row in rows]

    def managed_batch_job_ids(self):
        """Keep all batch child generations isolated, including crash-window orphans."""
        with self._connection() as connection:
            return {row[0] for row in connection.execute(
                "SELECT id FROM jobs WHERE idempotency_key LIKE 'batch-%'",
            )}

    def revalidate_verified_files(self, job_id):
        """Recheck snapshot matches during execution without widening membership."""
        with self._connection(write=True) as connection:
            job = self._required(connection, "jobs", job_id)
            if job["state"] != "ready" or job["stop_requested"]:
                raise ValueError("plan_not_ready")
            count = connection.execute(
                "UPDATE entries SET state='planned',verification=NULL "
                "WHERE job_id=? AND kind='file' AND state='already_verified' "
                "AND attempt_id IS NULL", (job_id,),
            ).rowcount
            self._event(connection, job_id, "verification_recheck_requested", {"count": count})

    def entry(self, entry_id):
        with self._connection() as connection:
            return self._required(connection, "entries", entry_id)

    def publications(self, job_id):
        with self._connection() as connection:
            self._required(connection, "jobs", job_id)
            rows = connection.execute(
                "SELECT p.entry_id,e.source_path,e.kind,e.resource_id,e.size,e.md5,"
                "e.modified,p.state,p.error,p.updated_at FROM publications p "
                "JOIN entries e ON e.id=p.entry_id AND e.job_id=p.job_id "
                "WHERE p.job_id=? ORDER BY e.rowid", (job_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def update_publication(self, job_id, entry_id, *, state, error=None):
        transitions = {
            "planned": {"planned", "intent", "published", "failed"},
            "intent": {"intent", "published", "failed"},
            "failed": {"failed", "intent", "published"},
            "published": {"published"},
        }
        if not isinstance(state, str) or state not in transitions:
            raise ValueError("invalid_publication_state")
        _code(error)
        with self._connection(write=True) as connection:
            job = self._required(connection, "jobs", job_id)
            row = connection.execute(
                "SELECT * FROM publications WHERE entry_id=?", (entry_id,),
            ).fetchone()
            if row is None:
                raise KeyError(entry_id)
            if row["job_id"] != job_id:
                raise ValueError("publication_job_mismatch")
            if job["stop_requested"] or job["state"] == "stopped":
                raise ValueError("job_stopped")
            if state not in transitions[row["state"]]:
                raise ValueError("invalid_publication_transition")
            now = _now()
            connection.execute(
                "UPDATE publications SET state=?,error=?,updated_at=? "
                "WHERE entry_id=? AND job_id=?", (state, error, now, entry_id, job_id),
            )
            self._event(connection, job_id, "publication_checkpoint", {
                "entry_id": entry_id, "phase": state, "error": error,
            })
            return dict(row, state=state, error=error, updated_at=now)

    def update_entry(self, entry_id, **fields):
        if not fields or set(fields) - {
            "state", "error", "attempt_id", "verification", "download_attempts",
            "retry_at_ms", "point_retry_requested", "point_retry_attempts",
        }:
            raise ValueError("invalid_entry_fields")
        for key, value in fields.items():
            if key == "point_retry_requested":
                if not isinstance(value, bool):
                    raise ValueError("invalid_entry_value")
            elif key in {"download_attempts", "retry_at_ms", "point_retry_attempts"}:
                if key == "retry_at_ms" and value is None:
                    continue
                maximum = (3 if key == "point_retry_attempts"
                           else 5 if key == "download_attempts" else 2 ** 63 - 1)
                if (not isinstance(value, int) or isinstance(value, bool)
                        or not 0 <= value <= maximum):
                    raise ValueError("invalid_entry_value")
            elif key != "attempt_id":
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

    def active_point_retry_entry(self, job_id):
        with self._connection() as connection:
            self._required(connection, "jobs", job_id)
            rows = connection.execute(
                "SELECT id FROM entries WHERE job_id=? AND point_retry_requested=1 LIMIT 2",
                (job_id,),
            ).fetchall()
            if len(rows) > 1:
                raise ValueError("point_retry_in_progress")
            return self._required(connection, "entries", rows[0][0]) if rows else None

    def point_retry_origin(self, entry_id):
        with self._connection() as connection:
            entry = self._required(connection, "entries", entry_id)
            row = connection.execute(
                "SELECT payload FROM events WHERE job_id=? "
                "AND type='point_retry_requested' ORDER BY sequence DESC LIMIT 1",
                (entry["job_id"],),
            ).fetchone()
            if row:
                payload = json.loads(row["payload"])
                if payload.get("entry_id") == entry_id:
                    return payload.get("attempt_id")
            return None

    @staticmethod
    def _point_retry_job_error(job):
        if job["stop_requested"] or job["state"] == "stopped":
            return "job_stopped"
        state = (job["resume_state"] if job["state"] == "awaiting_credentials"
                 else job["state"])
        if state not in {"needs_attention", "paused", "completed_with_issues"}:
            return "job_not_point_retryable"
        if job["namespace_state"] != "owned":
            return "technical_namespace_outcome_unknown"
        return None

    @staticmethod
    def _point_retry_entry_valid(job_id, entry, attempt):
        if (entry["job_id"] != job_id or entry["kind"] != "file"
                or entry["state"] not in {"failed", "outcome_unknown"}):
            return False
        if attempt is None:
            return entry["attempt_id"] is None and entry["state"] == "failed"
        return (entry["attempt_id"] == attempt["id"]
                and attempt["entry_id"] == entry["id"]
                and attempt["phase"] in POINT_RETRY_PHASES)

    @staticmethod
    def _point_retry_busy(connection, job_id):
        return bool(connection.execute(
            "SELECT 1 FROM entries WHERE job_id=? AND ("
            "point_retry_requested=1 OR state IN ('transferring',"
            "'waiting_operation','verifying','committing','downloading','staged')) "
            "LIMIT 1", (job_id,),
        ).fetchone())

    def describe_entries(self, job_id, entries):
        """Return current rows with the same retry guards used by the mutation."""
        with self._connection() as connection:
            job = self._required(connection, "jobs", job_id)
            ready = (not self._point_retry_job_error(job)
                     and not self._point_retry_busy(connection, job_id))
            result = []
            for item in entries:
                entry = self._required(connection, "entries", item["id"])
                if entry["job_id"] != job_id:
                    raise KeyError(item["id"])
                attempt = (self._required(connection, "attempts", entry["attempt_id"])
                           if entry["attempt_id"] else None)
                result.append({
                    **entry, "can_retry": bool(ready and self._point_retry_entry_valid(
                        job_id, entry, attempt,
                    )),
                })
            return result

    def queue_point_retry(self, job_id, entry_id, attempt_id, expected_revision):
        if (not isinstance(expected_revision, int)
                or isinstance(expected_revision, bool) or expected_revision < 0):
            raise ValueError("invalid_revision")
        with self._connection(write=True) as connection:
            job = self._required(connection, "jobs", job_id)
            if job["revision"] != expected_revision:
                raise ValueError("stale_revision")
            error = self._point_retry_job_error(job)
            if error:
                raise ValueError(error)
            entry = self._required(connection, "entries", entry_id)
            attempt = (self._required(connection, "attempts", attempt_id)
                       if attempt_id is not None else None)
            if not self._point_retry_entry_valid(job_id, entry, attempt):
                raise ValueError("attempt_not_retryable")
            if self._point_retry_busy(connection, job_id):
                raise ValueError("active_or_unknown_attempt")
            now = _now()
            connection.execute(
                "UPDATE entries SET point_retry_requested=1,point_retry_attempts=0,"
                "retry_at_ms=NULL,updated_at=? WHERE id=?", (now, entry_id),
            )
            connection.execute(
                "UPDATE jobs SET state='running',resume_state=NULL,"
                "pause_requested=0,error=NULL WHERE id=?", (job_id,),
            )
            self._event(connection, job_id, "point_retry_requested", {
                "entry_id": entry_id, "attempt_id": attempt_id,
                "stage": "point_retry", "status": "started",
                "max_point_retry_attempts": 3,
            })
            return self._job(connection, self._required(connection, "jobs", job_id))

    def confirm_point_copy(self, entry_id):
        with self._connection(write=True) as connection:
            entry = self._required(connection, "entries", entry_id)
            job = self._required(connection, "jobs", entry["job_id"])
            if job["stop_requested"] or job["state"] == "stopped":
                raise ValueError("job_stopped")
            attempt = (self._required(connection, "attempts", entry["attempt_id"])
                       if entry["attempt_id"] else None)
            if (not entry["point_retry_requested"] or entry["kind"] != "file"
                    or not attempt or attempt["phase"] not in POINT_RETRY_PHASES):
                raise ValueError("attempt_not_retryable")
            now = _now()
            phase = ("succeeded" if not job["remote_root"] else
                     "abandoned_move" if "move" in attempt["phase"] else "abandoned_transfer")
            connection.execute(
                "UPDATE entries SET state='already_verified',verification='md5',"
                "error=NULL,updated_at=? WHERE id=?", (now, entry_id),
            )
            connection.execute(
                "UPDATE attempts SET phase=?,updated_at=? "
                "WHERE id=?",
                (phase, now, attempt["id"]),
            )
            self._event(connection, job["id"], "entry_updated", {
                "entry_id": entry_id, "state": "already_verified",
                "verification": "md5", "error": None,
            })
            self._event(connection, job["id"], "attempt_updated", {
                "entry_id": entry_id, "attempt_id": attempt["id"],
                "phase": phase,
            })
            return self._required(connection, "entries", entry_id)

    def finish_point_retry(self, entry_id):
        with self._connection(write=True) as connection:
            entry = self._required(connection, "entries", entry_id)
            job = self._required(connection, "jobs", entry["job_id"])
            if not entry["point_retry_requested"]:
                return self._job(connection, job)
            connection.execute(
                "UPDATE entries SET point_retry_requested=0,retry_at_ms=NULL,"
                "updated_at=? WHERE id=?", (_now(), entry_id),
            )
            states = {row[0] for row in connection.execute(
                "SELECT DISTINCT state FROM entries WHERE job_id=?", (job["id"],),
            )}
            if job["stop_requested"] or job["state"] == "stopped":
                state, error = job["state"], job["error"]
            elif job["pause_requested"]:
                state, error = "paused", None
            elif states & {
                "planned", "transferring", "waiting_operation", "verifying",
                "committing", "downloading", "staged",
            }:
                state, error = "running", None
            elif "outcome_unknown" in states:
                state, error = "needs_attention", (entry["error"]
                                                   or "transfer_outcome_unknown")
            else:
                state = ("completed" if states <= {"done", "already_verified"}
                         else "completed_with_issues")
                error = None
            resume_state = job["resume_state"]
            if (job["state"] == "awaiting_credentials"
                    and not job["stop_requested"]):
                resume_state = state
                state, error = "awaiting_credentials", job["error"]
            connection.execute(
                "UPDATE jobs SET state=?,error=?,resume_state=? WHERE id=?",
                (state, error, resume_state, job["id"]),
            )
            self._event(connection, job["id"], "transfer_stage", {
                "stage": "point_retry", "entry_id": entry_id,
                "attempt_id": entry["attempt_id"],
                "status": "succeeded" if entry["state"] in {"done", "already_verified"}
                else "skipped", "error": entry["error"],
                "point_retry_attempts": entry["point_retry_attempts"],
                "max_point_retry_attempts": 3,
            })
            return self._job(connection, self._required(connection, "jobs", job["id"]))

    def new_attempt(self, entry_id, strategy, *, point_retry=False, reuse_local=False):
        if strategy not in {"url", "staged"}:
            raise ValueError("invalid_strategy")
        if (not isinstance(point_retry, bool) or not isinstance(reuse_local, bool)
                or (reuse_local and not point_retry)):
            raise ValueError("invalid_point_retry")
        with self._connection(write=True) as connection:
            entry = self._required(connection, "entries", entry_id)
            job = self._required(connection, "jobs", entry["job_id"])
            if job["stop_requested"] or job["state"] == "stopped":
                raise ValueError("job_stopped")
            old = (self._required(connection, "attempts", entry["attempt_id"])
                   if entry["attempt_id"] else None)
            if (not point_retry and not job["remote_root"] and old
                    and old["phase"] != "failed"):
                raise ValueError("attempt_not_retryable")
            local_name = None
            if point_retry:
                if not entry["point_retry_requested"]:
                    raise ValueError("attempt_not_retryable")
                if old is None:
                    if entry["state"] != "failed" or reuse_local:
                        raise ValueError("attempt_not_retryable")
                elif (old["phase"] not in POINT_RETRY_PHASES or old["strategy"] != strategy
                      or reuse_local and not old["local_name"]):
                    raise ValueError("attempt_not_retryable")
                local_name = old["local_name"] if reuse_local else None
            name = PurePosixPath(entry["destination_path"]).name
            if not name or name in {".", ".."}:
                raise ValueError("invalid_attempt_name")
            attempt_id, now = uuid.uuid4().hex, _now()
            remote = (f"{job['remote_root']}/{entry_id}/{attempt_id}/{name}"
                      if job["remote_root"] else entry["destination_path"])
            connection.execute(
                "INSERT INTO attempts(id,entry_id,strategy,phase,remote_path,"
                "local_name,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                (attempt_id, entry_id, strategy, "staged" if local_name else "prepared",
                 remote, local_name, now, now),
            )
            if point_retry:
                if old:
                    abandoned = ("failed" if old["phase"] == "failed" else
                                 "abandoned_move" if "move" in old["phase"] else "abandoned_transfer")
                    connection.execute(
                        "UPDATE attempts SET phase=?,local_name=?,updated_at=? WHERE id=?",
                        (abandoned, None if reuse_local else old["local_name"], now, old["id"]),
                    )
                connection.execute(
                    "UPDATE entries SET attempt_id=?,state='transferring',error=NULL,"
                    "download_attempts=0,retry_at_ms=NULL,updated_at=? WHERE id=?",
                    (attempt_id, now, entry_id),
                )
            else:
                connection.execute(
                    "UPDATE entries SET attempt_id=?,updated_at=? WHERE id=?",
                    (attempt_id, now, entry_id),
                )
            self._event(connection, entry["job_id"], "attempt_created", {
                "entry_id": entry_id, "attempt_id": attempt_id,
                "strategy": strategy, "phase": "staged" if local_name else "prepared",
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

    def cache_in_use(self):
        with self._connection() as connection:
            return bool(connection.execute(
                "SELECT 1 FROM jobs WHERE state IN ('queued','planning','running') LIMIT 1",
            ).fetchone())

    def cache_references(self):
        with self._connection() as connection:
            return {row[0] for row in connection.execute(
                "SELECT DISTINCT local_name FROM attempts WHERE local_name IS NOT NULL",
            )}

    def pending_cache_downloads(self):
        with self._connection() as connection:
            return [dict(row) for row in connection.execute(
                "SELECT attempts.id,attempts.local_name,entries.size,entries.md5 "
                "FROM entries JOIN attempts ON attempts.id=entries.attempt_id "
                "WHERE attempts.strategy='staged' AND attempts.local_name IS NOT NULL "
                "AND attempts.operation_id IS NULL "
                "AND attempts.phase IN ('prepared','downloading','download_retry_wait') "
                "AND entries.state IN ('planned','transferring','downloading','staged')",
            )]

    def clear_cache_references(self, names=None, *, failed=False):
        """Forget purged local copies without changing remote operation checkpoints."""
        with self._connection(write=True) as connection:
            rows = connection.execute(
                "SELECT attempts.*,entries.job_id,entries.state AS entry_state,"
                "entries.attempt_id AS current_attempt "
                "FROM attempts JOIN entries ON entries.id=attempts.entry_id "
                "WHERE attempts.local_name IS NOT NULL",
            ).fetchall()
            counts, now = {}, _now()
            for row in rows:
                if names is not None and row["local_name"] not in names:
                    continue
                connection.execute(
                    "UPDATE attempts SET local_name=NULL,updated_at=? WHERE id=?",
                    (now, row["id"]),
                )
                # A verified local download has completed its retry cycle. An explicit
                # purge must allow that copy to be fetched again, including after try 5.
                if (row["id"] == row["current_attempt"] and row["phase"] == "staged"
                        and row["strategy"] == "staged" and not row["operation_id"]
                        and row["entry_state"] in {
                            "planned", "transferring", "downloading", "staged",
                        }):
                    connection.execute(
                        "UPDATE entries SET download_attempts=0,updated_at=? WHERE id=?",
                        (now, row["entry_id"]),
                    )
                counts[row["job_id"]] = counts.get(row["job_id"], 0) + 1
            for job_id, count in counts.items():
                self._event(connection, job_id, "local_cleanup", {
                    "stage": "cleanup", "status": "failed" if failed else "succeeded",
                    "count": count,
                    **({"error": "cache_cleanup_failed"} if failed else {}),
                })
            return counts

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

    def event(self, job_id, event_type, payload, *, increment=True):
        with self._connection(write=True) as connection:
            self._required(connection, "jobs", job_id)
            return self._event(connection, job_id, event_type, payload,
                               increment=increment)

    def last_sequence(self, job_id):
        with self._connection() as connection:
            self._required(connection, "jobs", job_id)
            return connection.execute(
                "SELECT COALESCE(MAX(sequence),0) FROM events WHERE job_id=?",
                (job_id,),
            ).fetchone()[0]

    def tail_cursor(self, job_id, limit, through):
        with self._connection() as connection:
            self._required(connection, "jobs", job_id)
            row = connection.execute(
                "SELECT sequence FROM events WHERE job_id=? AND sequence<=? "
                "ORDER BY sequence DESC LIMIT 1 OFFSET ?",
                (job_id, max(0, through), max(0, limit - 1)),
            ).fetchone()
            return row[0] - 1 if row else 0

    def events(self, job_id, after=0, limit=200, *, through=None, journal=False):
        with self._connection() as connection:
            job = self._required(connection, "jobs", job_id)
            cutoff, parameters = "", [job_id, max(0, after)]
            if through is not None:
                cutoff = "AND sequence<=? "
                parameters.append(max(0, through))
            parameters.append(max(0, limit))
            rows = connection.execute(
                "SELECT * FROM events WHERE job_id=? AND sequence>? "
                + cutoff + "ORDER BY sequence LIMIT ?", parameters,
            ).fetchall()
            result = [
                {**dict(row), "payload": json.loads(row["payload"])} for row in rows
            ]
            detailed = [event for event in result
                        if journal or event["type"] == "transfer_stage"]
            if not detailed:
                return result
            entries, attempts = {}, {}
            entry_ids = {event["payload"]["entry_id"] for event in detailed
                         if event["payload"].get("entry_id")}
            attempt_ids = {event["payload"]["attempt_id"] for event in detailed
                           if event["payload"].get("attempt_id")}
            if attempt_ids:
                placeholders = ",".join("?" for _ in attempt_ids)
                attempts = {row["id"]: dict(row) for row in connection.execute(
                    "SELECT attempts.* FROM attempts JOIN entries ON "
                    "entries.id=attempts.entry_id WHERE entries.job_id=? AND "
                    f"attempts.id IN ({placeholders})", (job_id, *attempt_ids),
                )}
            entry_ids.update(attempt["entry_id"] for attempt in attempts.values())
            if entry_ids:
                placeholders = ",".join("?" for _ in entry_ids)
                entries = {row["id"]: dict(row) for row in connection.execute(
                    f"SELECT * FROM entries WHERE job_id=? AND id IN ({placeholders})",
                    (job_id, *entry_ids),
                )}
            config = json.loads(job["config"])
            for event in detailed:
                payload = event["payload"]
                entry = entries.get(payload.get("entry_id"))
                attempt = attempts.get(payload.get("attempt_id"))
                if attempt and not payload.get("entry_id"):
                    entry = entries.get(attempt["entry_id"])
                if attempt and (not entry or attempt["entry_id"] != entry["id"]):
                    attempt = None
                enrich_event(event, config, job["remote_root"], entry, attempt)
            return result

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
