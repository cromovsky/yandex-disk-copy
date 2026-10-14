"""Private, replayable journals for the three legacy transfer modes."""

from __future__ import annotations

import csv
import io
import json
import os
import re
import sqlite3
import threading
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

MODES = frozenset({"single", "cross", "local"})
EVENT_TYPES = frozenset({"log", "stage", "http", "retry", "job"})
TEXT_FIELDS = {
    "message", "stage", "status", "source", "destination", "kind", "error",
    "api_error", "api_reason", "transport_error", "error_side", "request_host",
    "request_id", "operation_id",
}
NUMBER_FIELDS = {
    "size", "pair_index", "http_status", "duration_ms", "attempt", "max_attempts",
    "retry_delay_ms",
}
CSV_FIELDS = [
    "sequence", "created_at", "event_type", "stage", "status", "message", "source",
    "destination", "kind", "size", "pair_index", "http_status", "error", "api_error",
    "api_reason", "transport_error", "error_side", "request_host", "request_id",
    "duration_ms", "attempt", "max_attempts", "retry_delay_ms", "operation_id",
]


def now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


class JournalError(Exception):
    """A stable public code, never a raw SQLite or filesystem error."""


def safe_text(value, limit=4096, *, context=False):
    if not isinstance(value, str):
        return ""
    value = re.sub(r"https?://[^\s<>\"']+", "[ссылка скрыта]", value, flags=re.I)
    value = re.sub(r"\b(?:OAuth|Bearer)\s+\S+", "[токен скрыт]", value, flags=re.I)
    value = re.sub(
        r"(?i)\b(?:access_token|refresh_token|client_secret|admin_token|authorization|"
        r"public_key|password|token)\s*[=:]\s*[^\s,;]+", "[секрет скрыт]", value,
    )
    # Old callbacks may still include a response dictionary/body. Structured HTTP
    # diagnostics are recorded separately; these untrusted bodies are never stored.
    if not context:
        value = re.sub(r"\{[^\n]*\}", "[данные ответа скрыты]", value)
        value = re.sub(r"<[^>]*>", "", value)
    return "".join(c for c in value if ord(c) >= 32 or c in "\n\t")[:limit]


class Redactor:
    def __init__(self):
        self._secrets = set()
        self._lock = threading.RLock()

    def add_secret(self, value):
        if isinstance(value, str) and value:
            with self._lock:
                self._secrets.add(value)

    def clear(self):
        with self._lock:
            self._secrets.clear()

    def text(self, value, limit=4096, *, context=False):
        if not isinstance(value, str):
            return ""
        with self._lock:
            for secret in sorted(self._secrets, key=len, reverse=True):
                value = value.replace(secret, "[скрыто]")
        return safe_text(value, limit, context=context)


def normalize_event(payload, redactor=None):
    clean = (redactor or Redactor()).text
    event_type = payload.get("event_type", "stage" if "stage" in payload else "log")
    event = {"event_type": event_type if event_type in EVENT_TYPES else "log"}
    for key in TEXT_FIELDS:
        value = payload.get(key)
        if isinstance(value, str):
            if key in {"source", "destination", "operation_id", "request_id"} and re.search(
                r"https?://|[?&](?:token|signature|key)=", value, re.I,
            ):
                continue
            value = clean(value, 4096 if key == "message" else 1024,
                          context=key in {"source", "destination"})
            if key == "error_side" and value not in {"source", "destination", "local"}:
                continue
            if key in {"api_error", "api_reason", "transport_error", "request_host",
                       "request_id", "operation_id"} and not re.fullmatch(
                r"[A-Za-z0-9_.:-]{1,256}", value,
            ):
                continue
            event[key] = value
    for key in NUMBER_FIELDS:
        value = payload.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and 0 <= value < 2**63:
            if key != "http_status" or 100 <= value <= 599:
                event[key] = value
    return event


def clean_summary(summary):
    return {key: value for key, value in (summary or {}).items()
            if key in {"saved", "failed", "skipped", "unconfirmed", "total", "done", "pairs_failed"}
            and isinstance(value, int) and not isinstance(value, bool) and value >= 0}


class RunJournal:
    def __init__(self, root):
        self.root = Path(root)
        self.directory = self.root / "journals"
        self.path = self.directory / "history.db"
        self.lock = threading.RLock()
        self.db = None
        try:
            if self.root.is_symlink() or self.directory.is_symlink():
                raise JournalError("invalid_journal_storage")
            self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            if self.directory.resolve().parent != self.root.resolve():
                raise JournalError("invalid_journal_storage")
            os.chmod(self.directory, 0o700)
            self._check_files()
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                pass
            else:
                os.close(fd)
            os.chmod(self.path, 0o600)
            self.db = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
            self.db.row_factory = sqlite3.Row
            self.db.execute("PRAGMA foreign_keys=ON")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.execute("PRAGMA secure_delete=ON")
            self.db.execute("PRAGMA auto_vacuum=FULL")
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY, mode TEXT NOT NULL, kind TEXT NOT NULL,
                    status TEXT NOT NULL, created_at TEXT NOT NULL, finished_at TEXT,
                    source TEXT NOT NULL, destination TEXT NOT NULL, error TEXT,
                    summary TEXT NOT NULL DEFAULT '{}', journal_error TEXT
                );
                CREATE INDEX IF NOT EXISTS runs_mode_date ON runs(mode,created_at DESC,id);
                CREATE TABLE IF NOT EXISTS events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                    created_at TEXT NOT NULL, payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS events_job_sequence ON events(job_id,sequence);
            """)
            self.db.commit()
        except (OSError, sqlite3.Error):
            if self.db:
                self.db.close()
            raise JournalError("journal_unavailable") from None

    def _check_files(self):
        if self.root.is_symlink() or self.directory.is_symlink():
            raise JournalError("invalid_journal_storage")
        for suffix in ("", "-journal", "-wal", "-shm"):
            path = Path(str(self.path) + suffix)
            if path.is_symlink() or (path.exists() and not path.is_file()):
                raise JournalError("invalid_journal_storage")

    @contextmanager
    def connection(self):
        with self.lock:
            try:
                self._check_files()
                if self.db is None:
                    raise JournalError("journal_unavailable")
                with self.db:
                    yield self.db
            except (OSError, sqlite3.Error):
                raise JournalError("journal_unavailable") from None

    @staticmethod
    def _job(row):
        if row is None:
            return None
        value = dict(row)
        value["summary"] = json.loads(value["summary"])
        return value

    @staticmethod
    def _append(db, job_id, payload):
        created = now()
        event = normalize_event(payload)
        cursor = db.execute(
            "INSERT INTO events(job_id,created_at,payload) VALUES(?,?,?)",
            (job_id, created, json.dumps(event, ensure_ascii=False)),
        )
        return {"sequence": cursor.lastrowid, "created_at": created, **event}

    def create(self, job_id, mode, kind, source, destination):
        if mode not in MODES or kind not in {"single", "batch"}:
            raise JournalError("invalid_journal_mode")
        with self.connection() as db:
            db.execute(
                "INSERT INTO runs(id,mode,kind,status,created_at,source,destination) "
                "VALUES(?,?,?,'running',?,?,?)",
                (job_id, mode, kind, now(), safe_text(source, context=True),
                 safe_text(destination, context=True)),
            )
            self._append(db, job_id, {"event_type": "job", "status": "running",
                                      "message": "Задание запущено"})
            saved = self._job(db.execute("SELECT * FROM runs WHERE id=?", (job_id,)).fetchone())
        return saved

    def append(self, job_id, payload):
        with self.connection() as db:
            return self._append(db, job_id, payload)

    def finish(self, job_id, status, *, error=None, summary=None, journal_error=None):
        if status not in {"done", "error", "interrupted"}:
            raise JournalError("invalid_journal_status")
        with self.connection() as db:
            db.execute(
                "UPDATE runs SET status=?,finished_at=?,error=?,summary=?,journal_error=? "
                "WHERE id=?",
                (status, now(), safe_text(error) if error else None,
                 json.dumps(clean_summary(summary)), journal_error, job_id),
            )
            self._append(db, job_id, {"event_type": "job", "status": status,
                                      "message": error or "Задание завершено"})

    def recover(self):
        with self.connection() as db:
            for row in db.execute("SELECT id FROM runs WHERE status='running'").fetchall():
                message = ("Задание прервано перезапуском утилиты; "
                           "перенос автоматически не продолжен.")
                db.execute("UPDATE runs SET status='interrupted',finished_at=?,error=? WHERE id=?",
                           (now(), message, row["id"]))
                self._append(db, row["id"], {"event_type": "job", "status": "interrupted",
                                             "message": message})

    def get(self, job_id):
        with self.connection() as db:
            return self._job(db.execute("SELECT * FROM runs WHERE id=?", (job_id,)).fetchone())

    def history(self, mode, limit=50, offset=0):
        if mode not in MODES:
            raise JournalError("invalid_journal_mode")
        with self.connection() as db:
            rows = db.execute(
                "SELECT * FROM runs WHERE mode=? ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?",
                (mode, limit + 1, offset),
            ).fetchall()
            active = [self._job(row) for row in db.execute(
                "SELECT * FROM runs WHERE mode=? AND status='running' ORDER BY created_at,id",
                (mode,),
            )]
            return {"jobs": [self._job(row) for row in rows[:limit]],
                    "has_more": len(rows) > limit, "running_count": len(active),
                    "active_jobs": active}

    def events(self, job_id, after=0, limit=200, tail=0, through=None):
        with self.connection() as db:
            if not db.execute("SELECT 1 FROM runs WHERE id=?", (job_id,)).fetchone():
                raise JournalError("journal_not_found")
            cutoff = db.execute("SELECT COALESCE(MAX(sequence),0) FROM events WHERE job_id=?",
                                (job_id,)).fetchone()[0]
            if through is not None:
                cutoff = min(cutoff, through)
            descending = bool(tail and not after)
            rows = db.execute(
                "SELECT sequence,created_at,payload FROM events WHERE job_id=? "
                "AND sequence>? AND sequence<=? ORDER BY sequence "
                + ("DESC" if descending else "ASC") + " LIMIT ?",
                (job_id, after, cutoff, tail if descending else limit),
            ).fetchall()
            if descending:
                rows = reversed(rows)
            return {"events": [{"sequence": r["sequence"], "created_at": r["created_at"],
                                **json.loads(r["payload"])} for r in rows],
                    "last_sequence": cutoff}

    def csv(self, job_id):
        """Finish a bounded-memory export before releasing the DB to cleanup/writers."""
        spool = tempfile.SpooledTemporaryFile(max_size=1024 * 1024, mode="w+b")
        text = io.TextIOWrapper(spool, encoding="utf-8-sig", newline="")
        try:
            with self.lock:
                cutoff = self.events(job_id, limit=1)["last_sequence"]
                writer = csv.DictWriter(text, CSV_FIELDS, extrasaction="ignore")
                writer.writeheader()
                after = 0
                while after < cutoff:
                    page = self.events(job_id, after=after, limit=1000,
                                       through=cutoff)["events"]
                    if not page:
                        raise JournalError("journal_export_failed")
                    for event in page:
                        safe = {}
                        for key, value in event.items():
                            if isinstance(value, str) and value.lstrip().startswith(
                                ("=", "+", "-", "@"),
                            ):
                                value = "'" + value
                            safe[key] = value
                        writer.writerow(safe)
                    after = page[-1]["sequence"]
                text.flush()
                text.detach()
                spool.seek(0)
                return spool
        except (JournalError, OSError):
            try:
                text.close()
            except ValueError:
                pass
            spool.close()
            raise JournalError("journal_export_failed") from None

    def cleanup(self, mode, *, completed_ids=()):
        if mode not in MODES:
            raise JournalError("invalid_journal_mode")
        with self.connection() as db:
            active = {r[0] for r in db.execute(
                "SELECT id FROM runs WHERE mode=? AND status='running'", (mode,),
            )}
            if active - set(completed_ids):
                raise JournalError("journal_in_use")
            before = self.path.stat().st_size
            jobs = db.execute("SELECT COUNT(*) FROM runs WHERE mode=?", (mode,)).fetchone()[0]
            events = db.execute(
                "SELECT COUNT(*) FROM events JOIN runs ON runs.id=events.job_id WHERE runs.mode=?",
                (mode,),
            ).fetchone()[0]
            db.execute("DELETE FROM runs WHERE mode=?", (mode,))
        # Vacuum requires its own transaction; the same lock excludes concurrent writes.
        with self.connection() as db:
            db.execute("VACUUM")
            after = self.path.stat().st_size
        return {"removed_jobs": jobs, "removed_events": events,
                "removed_bytes": max(0, before - after)}

    def close(self):
        with self.lock:
            if self.db:
                self.db.close()
                self.db = None
