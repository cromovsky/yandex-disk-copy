"""Durable source lists and incremental local manifests, without credentials."""

from __future__ import annotations

import csv
import io
import json
import os
import re
import sqlite3
import threading
import unicodedata
import uuid
from contextlib import contextmanager
from pathlib import Path

from app.migration.runner import disk_path
from app.run_journal import normalize_event, now

COMMON_KEYS = {"source_org_id", "source_client_id", "page_limit", "retry_enabled"}
ONPREM_KEYS = {"destination_url", "destination_identity", "destination_vd_hash",
               "destination_type", "destination_base", "identity_confirmed", "strategy"}


def parse_sources(content):
    if not isinstance(content, str) or len(content.encode("utf-8")) > 2 * 1024**2:
        raise ValueError("invalid_sources_csv")
    reader = csv.DictReader(io.StringIO(content.lstrip("\ufeff")))
    if reader.fieldnames != ["type", "email", "vd_hash", "path"]:
        raise ValueError("invalid_sources_columns")
    result, seen = [], []
    for index, row in enumerate(reader, 2):
        if index > 1001 or None in row or any(v is None for v in row.values()):
            raise ValueError("invalid_sources_row")
        row = {k: v.strip() for k, v in row.items()}
        kind, email, vd = row["type"], row["email"], row["vd_hash"]
        if (kind not in {"personal", "shared"} or "@" not in email
                or len(email.encode("utf-8")) > 254
                or any(c.isspace() or ord(c) < 32 or c in "/\\:" for c in email)
                or (kind == "personal" and vd)
                or (kind == "shared" and not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", vd))):
            raise ValueError("invalid_sources_row")
        path = disk_path(row["path"] or "/")
        identity = (kind, vd if kind == "shared" else email.casefold())
        canonical = unicodedata.normalize("NFC", path)
        for previous_identity, previous_path in seen:
            if identity == previous_identity and (
                canonical == previous_path or canonical.startswith(previous_path.rstrip("/") + "/")
                or previous_path.startswith(canonical.rstrip("/") + "/")
            ):
                raise ValueError("duplicate_source")
        seen.append((identity, canonical))
        result.append({"type": kind, "email": email, "vd_hash": vd, "path": path})
    if not result:
        raise ValueError("empty_source_list")
    return result


def clean_config(mode, config):
    keys = COMMON_KEYS | (ONPREM_KEYS if mode == "onprem" else set())
    if mode not in {"local", "onprem"} or not isinstance(config, dict) or set(config) - keys:
        raise ValueError("invalid_batch_config")
    value = dict(config)
    if "retry_enabled" in value and type(value["retry_enabled"]) is not bool:
        raise ValueError("invalid_retry_policy")
    for key in ("source_org_id", "source_client_id"):
        item = value.get(key)
        if not isinstance(item, str) or not item or len(item) > 256 or any(ord(c) < 32 for c in item):
            raise ValueError("invalid_batch_config")
    limit = value.setdefault("page_limit", 1000)
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 1000:
        raise ValueError("invalid_page_limit")
    if mode == "onprem":
        if value.get("strategy", "staged") != "staged":
            raise ValueError("onprem_staged_required")
        from app.migration.destination import normalize_destination_url
        value["destination_url"] = normalize_destination_url(value.get("destination_url", ""))
        identity = value.get("destination_identity", "")
        vd = value.get("destination_vd_hash", "")
        if (not isinstance(identity, str) or not identity or len(identity) > 256
                or any(ord(c) < 32 for c in identity)
                or not isinstance(vd, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", vd)
                or value.get("identity_confirmed") is not True
                or value.get("destination_type", "shared") != "shared"
                or value.get("destination_base", "disk:/") not in {"/", "disk:/"}):
            raise ValueError("invalid_batch_destination")
        value.update(destination_type="shared", destination_base="disk:/")
        value.setdefault("strategy", "staged")
    return value


class BatchStore:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = self.root / "source-batches.db"
        self.lock = threading.RLock()
        if self.root.is_symlink() or self.path.is_symlink():
            raise ValueError("invalid_batch_storage")
        self.db = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
        self.db.row_factory = sqlite3.Row
        os.chmod(self.path, 0o600)
        self.db.executescript("""
            PRAGMA foreign_keys=ON;
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS batches (
                id TEXT PRIMARY KEY, idempotency_key TEXT UNIQUE NOT NULL,
                mode TEXT NOT NULL, config TEXT NOT NULL, state TEXT NOT NULL,
                phase TEXT NOT NULL DEFAULT 'access', resume_state TEXT,
                error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                started INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS sources (
                id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES batches(id),
                position INTEGER NOT NULL, type TEXT NOT NULL, email TEXT NOT NULL,
                vd_hash TEXT NOT NULL, path TEXT NOT NULL, folder TEXT,
                selected INTEGER NOT NULL DEFAULT 1, state TEXT NOT NULL DEFAULT 'pending',
                plan_complete INTEGER NOT NULL DEFAULT 0, child_job_id TEXT,
                journal_id TEXT, error TEXT, summary TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS sources_batch ON sources(batch_id,position);
            CREATE TABLE IF NOT EXISTS entries (
                id TEXT PRIMARY KEY, source_id TEXT NOT NULL REFERENCES sources(id),
                kind TEXT NOT NULL, source_path TEXT NOT NULL, destination_path TEXT NOT NULL,
                size INTEGER NOT NULL, md5 TEXT, modified TEXT, resource_id TEXT,
                state TEXT NOT NULL, error TEXT, attempts INTEGER NOT NULL DEFAULT 0,
                selected INTEGER NOT NULL DEFAULT 1 CHECK(selected IN (0,1)),
                normalized_path TEXT NOT NULL, UNIQUE(source_id,normalized_path)
            );
            CREATE INDEX IF NOT EXISTS entries_source ON entries(source_id,state);
            CREATE TABLE IF NOT EXISTS events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL,
                source_id TEXT, created_at TEXT NOT NULL, payload TEXT NOT NULL
            );
        """)
        columns = {row["name"] for row in self.db.execute("PRAGMA table_info(entries)")}
        if "selected" not in columns:
            self.db.execute("ALTER TABLE entries ADD COLUMN selected INTEGER "
                            "NOT NULL DEFAULT 1 CHECK(selected IN (0,1))")
        self.db.commit()

    @contextmanager
    def connection(self):
        with self.lock, self.db:
            yield self.db

    @staticmethod
    def _batch(row):
        if row is None:
            raise KeyError("batch_not_found")
        result = dict(row)
        result.pop("idempotency_key", None)
        result["config"] = json.loads(result["config"])
        result["started"] = bool(result["started"])
        return result

    @staticmethod
    def _source(row):
        if row is None:
            raise KeyError("source_not_found")
        result = dict(row)
        result["summary"] = json.loads(result["summary"])
        for key in ("selected", "plan_complete"):
            result[key] = bool(result[key])
        return result

    def create(self, mode, config, sources, key):
        if not isinstance(key, str) or not 8 <= len(key) <= 128 or any(ord(c) < 32 for c in key):
            raise ValueError("invalid_idempotency_key")
        config = clean_config(mode, config)
        with self.connection() as db:
            previous = db.execute("SELECT * FROM batches WHERE idempotency_key=?", (key,)).fetchone()
            if previous:
                old = self._batch(previous)
                previous_sources = self.sources(old["id"])
                if old["mode"] != mode or {k: v for k, v in old["config"].items() if k != "retry_enabled"} != {k: v for k, v in config.items() if k != "retry_enabled"} or [
                    {k: s[k] for k in ("type", "email", "vd_hash", "path")} for s in previous_sources
                ] != sources:
                    raise ValueError("idempotency_conflict")
                return old
            batch_id, timestamp = uuid.uuid4().hex, now()
            db.execute("INSERT INTO batches(id,idempotency_key,mode,config,state,created_at,updated_at) "
                       "VALUES(?,?,?,?,'draft',?,?)",
                       (batch_id, key, mode, json.dumps(config), timestamp, timestamp))
            db.executemany("INSERT INTO sources(id,batch_id,position,type,email,vd_hash,path) "
                           "VALUES(?,?,?,?,?,?,?)", [
                               (uuid.uuid4().hex, batch_id, i, s["type"], s["email"], s["vd_hash"], s["path"])
                               for i, s in enumerate(sources)
                           ])
            return self.get(batch_id)

    def set_retry_enabled(self, batch_id, enabled):
        if type(enabled) is not bool:
            raise ValueError("invalid_retry_policy")
        with self.connection() as db:
            row = db.execute("SELECT config FROM batches WHERE id=?", (batch_id,)).fetchone()
            if row is None:
                raise KeyError("batch_not_found")
            config = json.loads(row["config"])
            if config.get("retry_enabled", True) != enabled or "retry_enabled" not in config:
                config["retry_enabled"] = enabled
                # Changing an execution preference does not invalidate a running inventory.
                db.execute("UPDATE batches SET config=? WHERE id=?", (json.dumps(config), batch_id))

    def get(self, batch_id):
        with self.lock:
            return self._batch(self.db.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone())

    def batches(self, mode=None, limit=50, offset=0):
        with self.lock:
            condition, args = (" WHERE mode=?", [mode]) if mode else ("", [])
            rows = self.db.execute("SELECT * FROM batches" + condition +
                                   " ORDER BY created_at DESC,id LIMIT ? OFFSET ?",
                                   (*args, limit + 1, offset)).fetchall()
            return {"batches": [self._batch(row) for row in rows[:limit]], "has_more": len(rows) > limit}

    def batch_for_child(self, child_job_id):
        with self.lock:
            row = self.db.execute(
                "SELECT batches.* FROM batches JOIN sources ON sources.batch_id=batches.id "
                "WHERE sources.child_job_id=? LIMIT 1", (child_job_id,),
            ).fetchone()
            return self._batch(row) if row is not None else None

    def sources(self, batch_id):
        self.get(batch_id)
        with self.lock:
            return [self._source(row) for row in self.db.execute(
                "SELECT * FROM sources WHERE batch_id=? ORDER BY position", (batch_id,))]

    def source(self, source_id):
        with self.lock:
            return self._source(self.db.execute("SELECT * FROM sources WHERE id=?", (source_id,)).fetchone())

    def update(self, batch_id, **fields):
        if not fields or set(fields) - {"state", "phase", "resume_state", "error", "started"}:
            raise ValueError("invalid_batch_update")
        with self.connection() as db:
            db.execute("UPDATE batches SET " + ",".join(k + "=?" for k in fields) +
                       ",updated_at=? WHERE id=?", (*fields.values(), now(), batch_id))
        return self.get(batch_id)

    def update_source(self, source_id, **fields):
        if not fields or set(fields) - {"state", "selected", "folder", "plan_complete", "child_job_id", "journal_id", "error", "summary"}:
            raise ValueError("invalid_source_update")
        if "summary" in fields:
            fields["summary"] = json.dumps(fields["summary"])
        with self.connection() as db:
            db.execute("UPDATE sources SET " + ",".join(k + "=?" for k in fields) + " WHERE id=?",
                       (*fields.values(), source_id))
        return self.source(source_id)

    def begin_plan(self, source_id):
        with self.connection() as db:
            db.execute("DELETE FROM entries WHERE source_id=?", (source_id,))
            db.execute("UPDATE sources SET state='planning',plan_complete=0,error=NULL,summary='{}' WHERE id=?", (source_id,))

    def add_entry(self, source_id, entry):
        entry = dict(entry, id=uuid.uuid4().hex, source_id=source_id,
                     normalized_path=unicodedata.normalize("NFC", entry["destination_path"]))
        keys = ["id", "source_id", "kind", "source_path", "destination_path", "size", "md5", "modified", "resource_id", "state", "error", "normalized_path"]
        try:
            with self.connection() as db:
                db.execute("INSERT INTO entries(" + ",".join(keys) + ") VALUES(" + ",".join("?" for _ in keys) + ")",
                           [entry.get(k) for k in keys])
        except sqlite3.IntegrityError:
            raise ValueError("path_collision") from None
        return entry

    def entries(self, source_id, limit=100, offset=0):
        self.source(source_id)
        with self.lock:
            total = self.db.execute("SELECT COUNT(*) FROM entries WHERE source_id=?", (source_id,)).fetchone()[0]
            rows = self.db.execute("SELECT e.*, CASE WHEN kind='dir' THEN EXISTS("
                                   "SELECT 1 FROM entries child WHERE child.source_id=e.source_id "
                                   "AND substr(child.normalized_path,1,length(e.normalized_path)+1)=e.normalized_path||'/' "
                                   "AND child.selected!=e.selected) ELSE 0 END AS selection_partial "
                                   "FROM entries e WHERE source_id=? ORDER BY kind, length(destination_path),destination_path LIMIT ? OFFSET ?",
                                   (source_id, limit, offset)).fetchall()
            return {"entries": [dict(row, selected=bool(row["selected"]),
                                     selection_partial=bool(row["selection_partial"]))
                                for row in rows], "total": total}

    def set_entry_selection(self, source_id, entry_id, selected):
        if type(selected) is not bool:
            raise ValueError("invalid_entry_selection")
        with self.connection() as db:
            entry = db.execute("SELECT * FROM entries WHERE source_id=? AND id=?",
                               (source_id, entry_id)).fetchone()
            if entry is None:
                raise KeyError("entry_not_found")
            target = entry["normalized_path"]
            db.execute("UPDATE entries SET selected=? WHERE source_id=? AND (id=? "
                       "OR (?='dir' AND substr(normalized_path,1,length(?)+1)=?||'/'))",
                       (selected, source_id, entry_id, entry["kind"], target, target))
            if selected:
                db.execute("UPDATE entries SET selected=1 WHERE source_id=? AND kind='dir' "
                           "AND substr(?,1,length(normalized_path)+1)=normalized_path||'/'",
                           (source_id, target))

    def start_local_transfer(self, batch_id):
        with self.connection() as db:
            batch = self._batch(db.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone())
            if batch["mode"] != "local" or batch["state"] != "ready" or batch["started"]:
                raise ValueError("dry_run_required")
            selected_sources = "SELECT id FROM sources WHERE batch_id=? AND selected=1"
            db.execute("UPDATE entries SET state='skipped_by_user',error=NULL "
                       "WHERE selected=0 AND source_id IN (" + selected_sources + ")", (batch_id,))
            db.execute("UPDATE entries SET state='planned',error=NULL "
                       "WHERE selected=1 AND kind='file' AND state='already_verified' "
                       "AND source_id IN (" + selected_sources + ")", (batch_id,))
            db.execute("UPDATE sources SET state=CASE WHEN EXISTS("
                       "SELECT 1 FROM entries WHERE entries.source_id=sources.id AND entries.selected=1) "
                       "THEN 'running' ELSE 'completed' END,error=NULL WHERE batch_id=? AND selected=1", (batch_id,))
            db.execute("UPDATE batches SET state='running',phase='transfer',started=1,error=NULL,"
                       "resume_state='running',updated_at=? WHERE id=?", (now(), batch_id))

    def next_entry(self, source_id):
        with self.lock:
            row = self.db.execute("SELECT * FROM entries WHERE source_id=? AND selected=1 AND state IN ('planned','downloading') ORDER BY kind,length(destination_path),destination_path LIMIT 1", (source_id,)).fetchone()
            return dict(row) if row else None

    def update_entry(self, entry_id, **fields):
        if set(fields) - {"state", "error", "attempts"}:
            raise ValueError("invalid_entry_update")
        with self.connection() as db:
            db.execute("UPDATE entries SET " + ",".join(k + "=?" for k in fields) + " WHERE id=?", (*fields.values(), entry_id))

    def summary(self, source_id):
        with self.lock:
            rows = self.db.execute("SELECT kind,state,selected,COUNT(*) count,COALESCE(SUM(size),0) bytes,COALESCE(MAX(size),0) largest FROM entries WHERE source_id=? GROUP BY kind,state,selected", (source_id,)).fetchall()
        value = {"total_files": 0, "total_directories": 0, "total_bytes": 0,
                 "required_bytes": 0, "verified_bytes": 0, "largest_file": 0, "largest_required_file": 0, "issues": 0}
        selection = {"total": 0, "selected": 0, "excluded": 0, "selected_files": 0,
                     "selected_directories": 0, "selected_bytes": 0, "editable": False}
        for row in rows:
            value["total_files" if row["kind"] == "file" else "total_directories"] += row["count"]
            selection["total"] += row["count"]
            selection["selected" if row["selected"] else "excluded"] += row["count"]
            if row["kind"] == "file":
                value["total_bytes"] += row["bytes"]
                value["largest_file"] = max(value["largest_file"], row["largest"])
            if not row["selected"]:
                continue
            selection["selected_files" if row["kind"] == "file" else "selected_directories"] += row["count"]
            if row["kind"] == "file":
                selection["selected_bytes"] += row["bytes"]
                value["verified_bytes" if row["state"] in {"done", "already_verified"} else "required_bytes"] += row["bytes"]
                if row["state"] not in {"done", "already_verified"}:
                    value["largest_required_file"] = max(value["largest_required_file"], row["largest"])
            if row["state"] in {"conflict", "verification_unavailable", "failed"}:
                value["issues"] += row["count"]
        value["selection"] = selection
        return value

    def event(self, batch_id, source_id, payload):
        with self.connection() as db:
            db.execute("INSERT INTO events(batch_id,source_id,created_at,payload) VALUES(?,?,?,?)",
                       (batch_id, source_id, now(), json.dumps(normalize_event(payload))))

    def events(self, batch_id, after=0, limit=1000, through=None):
        with self.lock:
            return [{"sequence": row["sequence"], "source_id": row["source_id"], "created_at": row["created_at"], **json.loads(row["payload"])}
                    for row in self.db.execute("SELECT * FROM events WHERE batch_id=? AND sequence>? AND sequence<=? ORDER BY sequence LIMIT ?", (batch_id, after, through if through is not None else 2**63-1, limit))]

    def last_sequence(self, batch_id):
        with self.lock:
            return self.db.execute("SELECT COALESCE(MAX(sequence),0) FROM events WHERE batch_id=?", (batch_id,)).fetchone()[0]

    def managed_children(self):
        with self.lock:
            return {row[0] for row in self.db.execute("SELECT child_job_id FROM sources WHERE child_job_id IS NOT NULL")}

    def recover(self):
        with self.connection() as db:
            db.execute("UPDATE batches SET resume_state=CASE WHEN state='paused' THEN resume_state ELSE state END,state='awaiting_credentials',error='credentials_required' WHERE state IN ('planning','running','ready','paused')")
            db.execute("UPDATE sources SET state='pending' WHERE state='planning' AND plan_complete=0")
            db.execute("UPDATE sources SET state='paused' WHERE state='running'")
            db.execute("UPDATE entries SET state='planned' WHERE state='downloading'")

    def close(self):
        with self.lock:
            self.db.close()
