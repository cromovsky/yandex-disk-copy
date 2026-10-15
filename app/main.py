"""FastAPI-обёртка вокруг DiskCopier: форма параметров + живой лог по SSE."""

from __future__ import annotations

import asyncio
import os
import queue
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from contextlib import asynccontextmanager, nullcontext
from pathlib import Path
from typing import Literal, Optional

import markdown
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .disk_copy import (
    CopyConfig, CopyError, DiskCopier, OrgCreds, validate_source_email,
)
from .throttle import READ_RPS, WRITE_RPS, TOTAL_RPS
from .run_journal import JournalError, Redactor, RunJournal, normalize_event, now
from .migration.api import router as onprem_router
from .migration.worker import MigrationService, enabled as onprem_enabled
from .retry_policy import RetryPolicy
from .source_slots import SOURCE_SLOTS
from .source_batches.api import router as source_batches_router
from .source_batches.service import BatchService

STATIC_DIR = Path(__file__).parent / "static"
INSTRUCTION_MD = Path(__file__).parent.parent / "docs" / "INSTRUCTION.md"
ONPREM_INSTRUCTION_MD = INSTRUCTION_MD.with_name("ONPREM.md")

BATCH_MAX_PAIRS = 25
BATCH_CONCURRENCY = 5
# Куда писать локальное сохранение внутри процесса. На хост каталог попадает
# томом ./downloads:/downloads (см. docker-compose). LOCAL_SAVE_HOST_DIR —
# путь, который показываем в логе: его видит человек на машине запуска.
LOCAL_SAVE_DIR = os.environ.get("LOCAL_SAVE_DIR", "./downloads")
LOCAL_SAVE_HOST_DIR = os.environ.get("LOCAL_SAVE_HOST_DIR", "./downloads")


@asynccontextmanager
async def lifespan(application):
    application.state.onprem = None
    application.state.source_batches = None
    root = os.environ.get("ONPREM_DATA_DIR", "./migration-data")
    journal_owned = getattr(application.state, "run_journal", None) is None
    if journal_owned:
        try:
            application.state.run_journal = RunJournal(
                os.environ.get("ONPREM_DATA_DIR", "./migration-data"),
            )
            application.state.run_journal.recover()
            application.state.journal_error = None
        except JournalError:
            application.state.run_journal = None
            application.state.journal_error = "journal_unavailable"
    if onprem_enabled():
        application.state.onprem = MigrationService(root, start_worker=False, slots=SOURCE_SLOTS)
    application.state.source_batches = BatchService(
        root, application.state.run_journal, application.state.onprem,
        LOCAL_SAVE_DIR, SOURCE_SLOTS,
    )
    if application.state.onprem:
        application.state.onprem.start()
    try:
        yield
    finally:
        if application.state.source_batches:
            await asyncio.to_thread(application.state.source_batches.close)
            application.state.source_batches = None
        if application.state.onprem:
            await asyncio.to_thread(application.state.onprem.close)
            application.state.onprem = None
        if journal_owned and application.state.run_journal:
            application.state.run_journal.close()
            application.state.run_journal = None


app = FastAPI(title="Yandex Disk Copy", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.include_router(onprem_router)
app.include_router(source_batches_router)

# Справка включает тот же runbook, который доступен отдельно в docs/ONPREM.md.
_instruction_html: Optional[str] = None


def _render_instruction() -> str:
    global _instruction_html
    if _instruction_html is None:
        if not INSTRUCTION_MD.exists():
            _instruction_html = "<p>Инструкция недоступна.</p>"
        else:
            text = INSTRUCTION_MD.read_text(encoding="utf-8")
            marker = "<!-- onprem-instruction -->"
            if marker in text:
                if ONPREM_INSTRUCTION_MD.exists():
                    runbook = ONPREM_INSTRUCTION_MD.read_text(encoding="utf-8")
                else:
                    runbook = "Инструкция по переносу в on-premises недоступна."
                text = text.replace(marker, runbook)
                text = text.replace(
                    "[onprem-guide]: ONPREM.md", "[onprem-guide]: #onprem-guide"
                )
            _instruction_html = markdown.markdown(
                text, extensions=["tables", "fenced_code", "sane_lists"]
            )
    return _instruction_html


class RunRequest(BaseModel):
    """Режим 1 — перенос в пределах одной организации (один сервис-аккаунт)."""

    client_id: str = Field(..., min_length=1)
    client_secret: str = Field(..., min_length=1)
    orgid: str = Field(..., min_length=1)
    admin_token: str = Field(..., min_length=1)
    source_disk_id: str = Field(..., min_length=1)
    destination_disk_id: str = Field(..., min_length=1)
    path: str = "/"
    page_limit: int = Field(1000, ge=1)
    retry_enabled: bool = Field(True, strict=True)
    # personal — личный Диск; shared — общий диск (нужен destination_vd_hash;
    # destination_disk_id тогда — email сотрудника с правом записи).
    destination_type: str = "personal"
    destination_vd_hash: str = ""


class RunCrossRequest(BaseModel):
    """Режим 2 — перенос между организациями (свой сервис-аккаунт у каждой).

    admin_token только у источника: разблокируем источника при необходимости,
    получатель считается активным.
    """

    src_client_id: str = Field(..., min_length=1)
    src_client_secret: str = Field(..., min_length=1)
    src_orgid: str = Field(..., min_length=1)
    src_admin_token: str = Field(..., min_length=1)
    source_disk_id: str = Field(..., min_length=1)
    dst_client_id: str = Field(..., min_length=1)
    dst_client_secret: str = Field(..., min_length=1)
    destination_disk_id: str = Field(..., min_length=1)
    path: str = "/"
    page_limit: int = Field(1000, ge=1)
    retry_enabled: bool = Field(True, strict=True)
    destination_type: str = "personal"
    destination_vd_hash: str = ""


class TransferPair(BaseModel):
    source_disk_id: str = Field(..., min_length=1)
    # Для personal — обязателен; для shared в пакете может быть пустым
    # (тогда берётся общий destination_disk_id запроса).
    destination_disk_id: str = ""


class RunBatchRequest(BaseModel):
    """Пакетный перенос в одной организации (до BATCH_MAX_PAIRS пар)."""

    client_id: str = Field(..., min_length=1)
    client_secret: str = Field(..., min_length=1)
    orgid: str = Field(..., min_length=1)
    admin_token: str = Field(..., min_length=1)
    pairs: list[TransferPair] = Field(..., min_length=1)
    path: str = "/"
    page_limit: int = Field(1000, ge=1)
    retry_enabled: bool = Field(True, strict=True)
    destination_type: str = "personal"
    destination_vd_hash: str = ""
    # Общий email с правом записи — для пакета на общий диск
    # (все источники из списка переносятся туда).
    destination_disk_id: str = ""


class RunBatchCrossRequest(BaseModel):
    """Пакетный перенос между организациями."""

    src_client_id: str = Field(..., min_length=1)
    src_client_secret: str = Field(..., min_length=1)
    src_orgid: str = Field(..., min_length=1)
    src_admin_token: str = Field(..., min_length=1)
    dst_client_id: str = Field(..., min_length=1)
    dst_client_secret: str = Field(..., min_length=1)
    pairs: list[TransferPair] = Field(..., min_length=1)
    path: str = "/"
    page_limit: int = Field(1000, ge=1)
    retry_enabled: bool = Field(True, strict=True)
    destination_type: str = "personal"
    destination_vd_hash: str = ""
    destination_disk_id: str = ""


_DONE = object()  # сентинел конца лога в очереди


def _rate_limit_note() -> str:
    """Строка в лог: с каким темпом работали — видно в присланном логе."""
    return (
        f"RATE LIMIT | чтение: {READ_RPS:g} запросов/с, "
        f"изменение: {WRITE_RPS:g} запросов/с, всего: {TOTAL_RPS:g} запросов/с "
        f"(общий бюджет утилиты на процесс)"
    )


@dataclass
class PairState:
    index: int
    source_disk_id: str
    destination_disk_id: str
    status: str = "pending"  # pending | running | done | error
    error: Optional[str] = None
    result: Optional[dict] = None


@dataclass
class Job:
    id: str
    queue: "queue.Queue" = field(default_factory=queue.Queue)
    status: str = "running"  # running | done | error
    result: Optional[dict] = None
    error: Optional[str] = None
    kind: str = "single"  # single | batch
    pairs: list[PairState] = field(default_factory=list)
    mode: str = "single"
    created_at: str = field(default_factory=now)
    finished_at: Optional[str] = None
    journal: Optional[RunJournal] = field(default=None, repr=False)
    journal_error: Optional[str] = None
    summary: dict = field(default_factory=dict)
    redactor: Redactor = field(default_factory=Redactor, repr=False)
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy, repr=False)


JOBS: dict[str, Job] = {}
JOBS_LOCK = threading.RLock()


@app.exception_handler(JournalError)
async def journal_error_handler(request, exc):
    status = 409 if str(exc) == "journal_in_use" else (
        404 if str(exc) == "journal_not_found" else 503
    )
    return JSONResponse(status_code=status, content={"detail": str(exc)})


def _journal():
    with JOBS_LOCK:
        store = getattr(app.state, "run_journal", None)
        if store is None:
            if getattr(app.state, "journal_error", None):
                raise JournalError("journal_unavailable")
            store = RunJournal(os.environ.get("ONPREM_DATA_DIR", "./migration-data"))
            store.recover()
            app.state.run_journal = store
        return store


def _result_summary(result):
    result = result if isinstance(result, dict) else {}
    summary = {"saved": len(result.get("saved") or []),
               "failed": len(result.get("fails") or [])}
    for key in ("skipped", "unconfirmed"):
        if result.get(key):
            summary[key] = len(result[key])
    return summary


def _render_event(event):
    parts = ([event["message"]] if event.get("message") else
             [str(event[key]) for key in ("stage", "status", "source", "destination")
              if event.get(key)])
    if event.get("http_status"):
        label = f"HTTP {event['http_status']}"
        if label not in " | ".join(parts):
            parts.append(label)
    for key in ("error", "api_error", "api_reason", "transport_error"):
        if event.get(key) and str(event[key]) not in " | ".join(parts):
            parts.append(str(event[key]))
    return " | ".join(parts)


class JobLog:
    def __init__(self, job, *, context=None, prefix=""):
        self.job = job
        self.context = context or {}
        self.prefix = prefix

    def add_secret(self, value):
        self.job.redactor.add_secret(value)

    def __call__(self, message):
        self.event({"event_type": "log", "message": str(message)})

    def event(self, payload):
        event = normalize_event({**self.context, **payload}, self.job.redactor)
        if self.prefix:
            event["message"] = self.job.redactor.text(
                self.prefix + " " + _render_event(event),
            )
        if not self.job.journal:
            self.job.queue.put(_render_event(event))
        if self.job.journal:
            try:
                self.job.journal.append(self.job.id, event)
            except JournalError:
                self.job.journal_error = "journal_write_failed"


def _register_config(job, cfg):
    for credentials in (cfg.source, cfg.destination):
        for key in ("client_secret", "admin_token"):
            job.redactor.add_secret(getattr(credentials, key, ""))


def _safe_failure(job, exc):
    if isinstance(exc, CopyError):
        return job.redactor.text(str(exc))
    return "Ошибка соединения или выполнения переноса; подробности указаны в журнале."


def _failure_event(log, exc):
    details = getattr(exc, "diagnostics", {}) or {}
    log.event({
        "event_type": "job", "stage": "transfer", "status": "outcome_unknown"
        if getattr(exc, "uncertain", False) else "failed",
        "message": _safe_failure(log.job, exc),
        "error": getattr(exc, "code", "transfer_failed"),
        "http_status": getattr(exc, "http_status", None), **details,
    })


def _complete_job(job, status, error, summary):
    with JOBS_LOCK:
        job.error = error
        job.summary = summary
        job.finished_at = now()
        if job.journal:
            try:
                job.journal.finish(job.id, status, error=error, summary=summary,
                                   journal_error=job.journal_error)
            except JournalError:
                job.journal_error = "journal_write_failed"
        job.status = status
        if job.journal:
            # Keep only a small safe compatibility snapshot after the worker exits.
            job.result = None
            for pair in job.pairs:
                pair.result = None
                pair.source_disk_id = job.redactor.text(pair.source_disk_id, context=True)
                pair.destination_disk_id = job.redactor.text(pair.destination_disk_id, context=True)
            job.redactor.clear()
        else:
            job.queue.put(_DONE)


def _run_job(job: Job, cfg: CopyConfig) -> None:
    _register_config(job, cfg)
    log = JobLog(job, context={"source": cfg.source_disk_id,
                              "destination": cfg.destination_disk_id})
    log(_rate_limit_note())
    state, error, summary = "done", None, {}
    try:
        lane = (SOURCE_SLOTS.lease("local:" + job.id) if cfg.local_save else nullcontext())
        with lane, DiskCopier(cfg, log=log) as copier:
            job.result = copier.run()
        summary = _result_summary(job.result)
        if summary["failed"]:
            error = f"Частично: перенесено {summary['saved']}, ошибок {summary['failed']}"
        if summary.get("unconfirmed"):
            warning = f"Результат {summary['unconfirmed']} файлов не подтверждён"
            error = f"{error}; {warning}" if error else warning
        if error:
            log(f"SUMMARY | {error}")
    except Exception as exc:
        state, error = "error", _safe_failure(job, exc)
        _failure_event(log, exc)
    finally:
        _complete_job(job, state, error, summary)


def _run_batch(job: Job, base_cfgs: list[CopyConfig]) -> None:
    """Keep pair contexts separate while sharing one durable run and redactor."""
    lock = threading.Lock()
    total = len(job.pairs)
    for cfg in base_cfgs:
        _register_config(job, cfg)
    log = JobLog(job)

    def run_one(pair, cfg):
        pair_log = JobLog(job, context={"pair_index": pair.index,
                         "source": pair.source_disk_id, "destination": pair.destination_disk_id},
                         prefix=(f"[{pair.index}/{total} {pair.source_disk_id} → "
                                 f"{pair.destination_disk_id}]"))
        with lock:
            pair.status = "running"
        pair_log.event({"event_type": "job", "status": "running", "message": "START"})
        try:
            with DiskCopier(cfg, log=pair_log) as copier:
                pair.result = copier.run()
            summary = _result_summary(pair.result)
            with lock:
                pair.status = "done"
                if summary["failed"]:
                    pair.error = (f"Частично: перенесено {summary['saved']}, "
                                  f"ошибок {summary['failed']}")
            if summary.get("unconfirmed"):
                warning = f"Результат {summary['unconfirmed']} файлов не подтверждён"
                pair.error = f"{pair.error}; {warning}" if pair.error else warning
            pair_log.event({"event_type": "job", "status": "done",
                            "message": pair.error or "DONE"})
        except Exception as exc:
            with lock:
                pair.status = "error"
                pair.error = _safe_failure(job, exc)
            _failure_event(pair_log, exc)

    log(_rate_limit_note())
    log(f"BATCH START | pairs: {total}, concurrency: {BATCH_CONCURRENCY}, max: {BATCH_MAX_PAIRS}")
    unexpected = None
    try:
        with ThreadPoolExecutor(max_workers=BATCH_CONCURRENCY) as pool:
            futures = [pool.submit(run_one, pair, cfg) for pair, cfg in zip(job.pairs, base_cfgs)]
            for future in futures:
                future.result()
    except Exception as exc:
        unexpected = _safe_failure(job, exc)
        _failure_event(log, exc)
    finally:
        done_ok = sum(p.status == "done" for p in job.pairs)
        done_err = sum(p.status == "error" for p in job.pairs)
        file_summaries = [_result_summary(p.result) for p in job.pairs]
        summary = {key: sum(s.get(key, 0) for s in file_summaries)
                   for key in ("saved", "failed", "skipped", "unconfirmed")}
        summary.update(total=total, done=done_ok, pairs_failed=done_err)
        job.result = {"total": total, "done": done_ok, "failed": done_err,
                      "pairs": [{"index": p.index, "status": p.status, "error": p.error,
                                 "source_disk_id": p.source_disk_id,
                                 "destination_disk_id": p.destination_disk_id,
                                 "result": p.result} for p in job.pairs]}
        state = "error" if unexpected or (done_err and not done_ok) else "done"
        error = unexpected
        if not error and (done_err or summary["failed"]):
            error = (f"Частично: перенесено {summary['saved']}, ошибок файлов {summary['failed']}; "
                     f"ошибок пар {done_err} из {total}")
        if summary["unconfirmed"]:
            warning = f"Результат {summary['unconfirmed']} файлов не подтверждён"
            error = f"{error}; {warning}" if error else warning
        log(f"BATCH COMPLETED | {error or f'ok: {done_ok}/{total}'}")
        _complete_job(job, state, error, summary)


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/instruction", response_class=HTMLResponse)
def instruction() -> str:
    return _render_instruction()


@app.get("/api/config")
def config() -> dict:
    """Флаги сборки для фронта. CROSS_ORG_ENABLED=0 скрывает режим 2 (образ v1)."""
    val = os.environ.get("CROSS_ORG_ENABLED", "1").strip().lower()
    return {
        "cross_org": val not in ("0", "false", "no", "off"),
        "onprem_enabled": onprem_enabled(),
        "onprem_auth_required": bool(os.environ.get("ONPREM_ADMIN_TOKEN")),
        "batch_max_pairs": BATCH_MAX_PAIRS,
        "batch_concurrency": BATCH_CONCURRENCY,
        "target_rps": READ_RPS,
        "read_rps": READ_RPS,
        "write_rps": WRITE_RPS,
        "total_rps": TOTAL_RPS,
        "source_concurrency": SOURCE_SLOTS.limit,
        "local_save_host_dir": LOCAL_SAVE_HOST_DIR,
    }


def _create_job(job, cfgs):
    with JOBS_LOCK:
        job.retry_policy.set_enabled(cfgs[0].retry_enabled)
        for cfg in cfgs:
            _register_config(job, cfg)
            cfg.retry_policy = job.retry_policy
        source = (cfgs[0].source_disk_id if len(cfgs) == 1
                  else f"Источников: {len(cfgs)}")
        destination = ("Локальное сохранение" if job.mode == "local" else
                       cfgs[0].destination_disk_id if len(cfgs) == 1
                       else f"Получателей: {len(cfgs)}")
        job.journal = _journal()
        saved = job.journal.create(job.id, job.mode, job.kind,
                                   job.redactor.text(source, context=True),
                                   job.redactor.text(destination, context=True))
        job.created_at = saved["created_at"]
        JOBS[job.id] = job
        _log_retry_settings(job)


def _start_job(cfg: CopyConfig, mode="single") -> dict:
    job = Job(id=uuid.uuid4().hex, kind="single", mode=mode)
    with JOBS_LOCK:
        _create_job(job, [cfg])
        try:
            threading.Thread(target=_run_job, args=(job, cfg), daemon=True).start()
        except Exception:
            _complete_job(job, "error", "Не удалось запустить перенос.", {})
            raise HTTPException(503, "transfer_start_failed") from None
    return {"job_id": job.id}


def _start_batch(cfgs: list[CopyConfig], pairs: list[TransferPair], mode="single") -> dict:
    job = Job(id=uuid.uuid4().hex, kind="batch", mode=mode,
              pairs=[PairState(i, p.source_disk_id.strip(), p.destination_disk_id.strip())
                     for i, p in enumerate(pairs, 1)])
    with JOBS_LOCK:
        _create_job(job, cfgs)
        try:
            threading.Thread(target=_run_batch, args=(job, cfgs), daemon=True).start()
        except Exception:
            _complete_job(job, "error", "Не удалось запустить перенос.", {})
            raise HTTPException(503, "transfer_start_failed") from None
    return {"job_id": job.id, "pairs": len(pairs)}


def _normalize_dest_type(value: str) -> str:
    v = (value or "personal").strip().lower()
    if v not in ("personal", "shared"):
        raise HTTPException(
            status_code=400,
            detail="destination_type должен быть 'personal' или 'shared'",
        )
    return v


def _normalize_vd_hash(value: str) -> str:
    """Принимает сырой hash или URL/путь вида .../vd/<hash>/..."""
    raw = (value or "").strip()
    if not raw:
        return ""
    # вырезаем метку после vd/ если вставили ссылку из браузера
    marker = "/vd/"
    if marker in raw:
        raw = raw.split(marker, 1)[1]
    raw = raw.strip("/").split("/", 1)[0]
    if raw.startswith("vd:"):
        raw = raw[3:].split(":", 1)[0]
    return raw.strip()


def _validate_batch_pairs(
    pairs: list[TransferPair],
    *,
    dest_type: str = "personal",
    common_destination: str = "",
) -> list[TransferPair]:
    if len(pairs) > BATCH_MAX_PAIRS:
        raise HTTPException(
            status_code=400,
            detail=f"Слишком много пар: {len(pairs)}. Максимум {BATCH_MAX_PAIRS}.",
        )

    if dest_type == "shared":
        common = (common_destination or "").strip()
        if not common:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Для пакетного переноса на общий диск укажите "
                    "destination_disk_id — email сотрудника с правом записи"
                ),
            )
        cleaned: list[TransferPair] = []
        seen_src: set[str] = set()
        for i, pair in enumerate(pairs, 1):
            src = pair.source_disk_id.strip()
            if not src:
                raise HTTPException(
                    status_code=400,
                    detail=f"Строка #{i}: source не должен быть пустым",
                )
            key = src.lower()
            if key in seen_src:
                raise HTTPException(
                    status_code=400,
                    detail=f"Строка #{i}: дубликат источника {src}",
                )
            seen_src.add(key)
            cleaned.append(
                TransferPair(source_disk_id=src, destination_disk_id=common)
            )
        return cleaned

    cleaned = []
    seen: set[tuple[str, str]] = set()
    for i, pair in enumerate(pairs, 1):
        src = pair.source_disk_id.strip()
        dst = pair.destination_disk_id.strip()
        if not src or not dst:
            raise HTTPException(
                status_code=400,
                detail=f"Пара #{i}: source и destination не должны быть пустыми",
            )
        key = (src.lower(), dst.lower())
        if key in seen:
            raise HTTPException(
                status_code=400,
                detail=f"Пара #{i}: дубликат {src} → {dst}",
            )
        seen.add(key)
        cleaned.append(TransferPair(source_disk_id=src, destination_disk_id=dst))
    return cleaned


@app.post("/api/run")
def run(req: RunRequest) -> dict:
    # Режим 1: один и тот же сервис-аккаунт для источника и получателя.
    dest_type = _normalize_dest_type(req.destination_type)
    vd_hash = _normalize_vd_hash(req.destination_vd_hash)
    if dest_type == "shared" and not vd_hash:
        raise HTTPException(
            status_code=400,
            detail="Для общего диска укажите destination_vd_hash",
        )
    creds = OrgCreds(
        client_id=req.client_id,
        client_secret=req.client_secret,
        orgid=req.orgid,
        admin_token=req.admin_token,
    )
    cfg = CopyConfig(
        source=creds,
        destination=creds,
        source_disk_id=req.source_disk_id,
        destination_disk_id=req.destination_disk_id,
        path=req.path or "/",
        page_limit=req.page_limit,
        retry_enabled=req.retry_enabled,
        destination_type=dest_type,
        destination_vd_hash=vd_hash,
    )
    return _start_job(cfg)


@app.post("/api/run-cross")
def run_cross(req: RunCrossRequest) -> dict:
    # Режим 2: раздельные сервис-аккаунты; admin/orgid только у источника.
    dest_type = _normalize_dest_type(req.destination_type)
    vd_hash = _normalize_vd_hash(req.destination_vd_hash)
    if dest_type == "shared" and not vd_hash:
        raise HTTPException(
            status_code=400,
            detail="Для общего диска укажите destination_vd_hash",
        )
    cfg = CopyConfig(
        source=OrgCreds(
            client_id=req.src_client_id,
            client_secret=req.src_client_secret,
            orgid=req.src_orgid,
            admin_token=req.src_admin_token,
        ),
        destination=OrgCreds(
            client_id=req.dst_client_id,
            client_secret=req.dst_client_secret,
        ),
        source_disk_id=req.source_disk_id,
        destination_disk_id=req.destination_disk_id,
        path=req.path or "/",
        page_limit=req.page_limit,
        retry_enabled=req.retry_enabled,
        destination_type=dest_type,
        destination_vd_hash=vd_hash,
    )
    return _start_job(cfg, mode="cross")


class LocalSaveRequest(BaseModel):
    """Скачать Диск источника на машину, с которой запущена утилита."""

    client_id: str = Field(..., min_length=1)
    client_secret: str = Field(..., min_length=1)
    orgid: str = Field(..., min_length=1)
    admin_token: str = Field(..., min_length=1)
    source_disk_id: str = ""
    source_type: str = "personal"
    source_vd_hash: str = ""
    path: str = "/"
    page_limit: int = Field(1000, ge=1)
    retry_enabled: bool = Field(True, strict=True)


class LocalSaveCrossRequest(BaseModel):
    """То же между организациями: нужны только реквизиты источника."""

    src_client_id: str = Field(..., min_length=1)
    src_client_secret: str = Field(..., min_length=1)
    src_orgid: str = Field(..., min_length=1)
    src_admin_token: str = Field(..., min_length=1)
    source_disk_id: str = ""
    source_type: str = "personal"
    source_vd_hash: str = ""
    path: str = "/"
    page_limit: int = Field(1000, ge=1)
    retry_enabled: bool = Field(True, strict=True)


def _local_cfg(
    source: OrgCreds, source_disk_id: str, path: str, page_limit: int,
    source_type: str = "personal", source_vd_hash: str = "", retry_enabled: bool = True,
) -> CopyConfig:
    source_type = (source_type or "personal").strip().lower()
    if source_type not in ("personal", "shared"):
        raise HTTPException(
            status_code=400, detail="source_type должен быть 'personal' или 'shared'"
        )
    try:
        source_disk_id = validate_source_email(
            source_disk_id, shared=source_type == "shared"
        )
    except CopyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    vd_hash = _normalize_vd_hash(source_vd_hash) if source_type == "shared" else ""
    if source_type == "shared" and not re.fullmatch(r"[A-Za-z0-9_-]+", vd_hash):
        raise HTTPException(
            status_code=400,
            detail="Для общего источника укажите корректный source_vd_hash",
        )
    return CopyConfig(
        source=source,
        destination=source,
        source_disk_id=source_disk_id,
        destination_disk_id="",
        path=path or "/",
        page_limit=page_limit,
        local_save=True,
        local_root=LOCAL_SAVE_DIR,
        source_type=source_type,
        source_vd_hash=vd_hash,
        retry_enabled=retry_enabled,
    )


@app.post("/api/save-local")
def save_local(req: LocalSaveRequest) -> dict:
    creds = OrgCreds(
        client_id=req.client_id,
        client_secret=req.client_secret,
        orgid=req.orgid,
        admin_token=req.admin_token,
    )
    return _start_job(
        _local_cfg(
            creds, req.source_disk_id, req.path, req.page_limit,
            req.source_type, req.source_vd_hash, req.retry_enabled,
        ), mode="local",
    )


@app.post("/api/save-local-cross")
def save_local_cross(req: LocalSaveCrossRequest) -> dict:
    creds = OrgCreds(
        client_id=req.src_client_id,
        client_secret=req.src_client_secret,
        orgid=req.src_orgid,
        admin_token=req.src_admin_token,
    )
    return _start_job(
        _local_cfg(
            creds, req.source_disk_id, req.path, req.page_limit,
            req.source_type, req.source_vd_hash, req.retry_enabled,
        ), mode="local",
    )


@app.post("/api/run-batch")
def run_batch(req: RunBatchRequest) -> dict:
    dest_type = _normalize_dest_type(req.destination_type)
    vd_hash = _normalize_vd_hash(req.destination_vd_hash)
    if dest_type == "shared" and not vd_hash:
        raise HTTPException(
            status_code=400,
            detail="Для общего диска укажите destination_vd_hash",
        )
    pairs = _validate_batch_pairs(
        req.pairs,
        dest_type=dest_type,
        common_destination=req.destination_disk_id,
    )
    creds = OrgCreds(
        client_id=req.client_id,
        client_secret=req.client_secret,
        orgid=req.orgid,
        admin_token=req.admin_token,
    )
    cfgs = [
        CopyConfig(
            source=creds,
            destination=creds,
            source_disk_id=p.source_disk_id,
            destination_disk_id=p.destination_disk_id,
            path=req.path or "/",
            page_limit=req.page_limit,
            retry_enabled=req.retry_enabled,
            destination_type=dest_type,
            destination_vd_hash=vd_hash,
        )
        for p in pairs
    ]
    return _start_batch(cfgs, pairs)


@app.post("/api/run-batch-cross")
def run_batch_cross(req: RunBatchCrossRequest) -> dict:
    dest_type = _normalize_dest_type(req.destination_type)
    vd_hash = _normalize_vd_hash(req.destination_vd_hash)
    if dest_type == "shared" and not vd_hash:
        raise HTTPException(
            status_code=400,
            detail="Для общего диска укажите destination_vd_hash",
        )
    pairs = _validate_batch_pairs(
        req.pairs,
        dest_type=dest_type,
        common_destination=req.destination_disk_id,
    )
    src = OrgCreds(
        client_id=req.src_client_id,
        client_secret=req.src_client_secret,
        orgid=req.src_orgid,
        admin_token=req.src_admin_token,
    )
    dst = OrgCreds(
        client_id=req.dst_client_id,
        client_secret=req.dst_client_secret,
    )
    cfgs = [
        CopyConfig(
            source=src,
            destination=dst,
            source_disk_id=p.source_disk_id,
            destination_disk_id=p.destination_disk_id,
            path=req.path or "/",
            page_limit=req.page_limit,
            retry_enabled=req.retry_enabled,
            destination_type=dest_type,
            destination_vd_hash=vd_hash,
        )
        for p in pairs
    ]
    return _start_batch(cfgs, pairs, mode="cross")


def _snapshot(job_id):
    store = _journal()
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        try:
            saved = store.get(job_id)
        except JournalError:
            if job is None:
                raise
            saved = None
            job.journal_error = "journal_write_failed"
        if saved is None and job is None:
            raise HTTPException(404, "job not found")
        if job:
            return {**(saved or {}), "id": job.id, "mode": job.mode, "kind": job.kind,
                    "status": job.status, "retry_enabled": job.retry_policy.enabled,
                    "created_at": job.created_at,
                    "finished_at": job.finished_at, "error": job.error,
                    "summary": job.summary, "journal_error": job.journal_error,
                    "result": job.summary or None,
                    "pairs": [{"index": p.index,
                               "source_disk_id": job.redactor.text(p.source_disk_id),
                               "destination_disk_id": job.redactor.text(p.destination_disk_id),
                               "status": p.status, "error": p.error} for p in job.pairs]}
    return {**saved, "result": saved["summary"] or None, "pairs": []}


@app.get("/api/run/{job_id}/stream")
def stream(job_id: str, request: Request, after: int = Query(0, ge=0)) -> StreamingResponse:
    _snapshot(job_id)
    try:
        after = max(after, int(request.headers.get("Last-Event-ID", "0")))
    except ValueError:
        raise HTTPException(422, "invalid_event_cursor") from None
    store = _journal()

    def event_stream():
        cursor = after
        while True:
            try:
                page = store.events(job_id, after=cursor)
                for event in page["events"]:
                    lines = _render_event(event).splitlines() or [""]
                    yield (f"id: {event['sequence']}\n"
                           + "".join(f"data: {line}\n" for line in lines) + "\n")
                    cursor = event["sequence"]
                job = _snapshot(job_id)
                if job["journal_error"]:
                    yield "event: journal_error\ndata: journal_write_failed\n\n"
                    return
                cutoff = page["last_sequence"]
                if job["status"] != "running":
                    cutoff = store.events(job_id, after=cursor, limit=1)["last_sequence"]
                if job["status"] != "running" and cursor >= cutoff:
                    state = "error" if job["status"] == "interrupted" else job["status"]
                    message = (job["error"] or "ok").replace("\n", " ")
                    yield f"event: {state}\ndata: {message}\n\n"
                    return
                if not page["events"]:
                    yield ": keepalive\n\n"
                    time.sleep(0.5)
            except (JournalError, HTTPException):
                yield "event: journal_error\ndata: journal_unavailable\n\n"
                return
    return StreamingResponse(event_stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache"})


class RetrySettingsRequest(BaseModel):
    enabled: bool = Field(..., strict=True)


def _log_retry_settings(job):
    enabled = job.retry_policy.enabled
    JobLog(job).event({"event_type": "job", "stage": "retry_settings",
                       "status": "enabled" if enabled else "disabled",
                       "retry_enabled": enabled,
                       "message": "Автоматические повторы включены." if enabled else
                       "Автоматические повторы отключены: после ошибки выполняется одна попытка."})


@app.post("/api/run/{job_id}/retries")
def set_run_retries(job_id: str, payload: RetrySettingsRequest, request: Request):
    origin = request.headers.get("Origin")
    if (origin and origin.rstrip("/") != str(request.base_url).rstrip("/")) or (
        request.headers.get("Sec-Fetch-Site", "").lower() not in {"", "same-origin", "none"}
    ):
        raise HTTPException(403, "journal_origin_rejected")
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if job is None:
            raise HTTPException(404, "job_not_found")
        if job.status != "running":
            raise HTTPException(409, "job_not_running")
        job.retry_policy.set_enabled(payload.enabled)
        _log_retry_settings(job)
        return {"retry_enabled": job.retry_policy.enabled}


@app.get("/api/run/{job_id}")
def status(job_id: str) -> dict:
    return _snapshot(job_id)


@app.get("/api/history")
def history(mode: Literal["single", "cross", "local"],
            limit: int = Query(50, ge=1, le=1000), offset: int = Query(0, ge=0)):
    with JOBS_LOCK:
        result = _journal().history(mode, limit, offset)
        result["jobs"] = [_snapshot(job["id"]) for job in result["jobs"]]
        result["active_jobs"] = [_snapshot(job["id"]) for job in result["active_jobs"]]
        result["active_jobs"] = [j for j in result["active_jobs"] if j["status"] == "running"]
        result["running_count"] = len(result["active_jobs"])
    return result


@app.get("/api/run/{job_id}/events")
def run_events(job_id: str, after: int = Query(0, ge=0),
               limit: int = Query(200, ge=1, le=1000), tail: int = Query(0, ge=0, le=500)):
    return _journal().events(job_id, after, limit, tail)


@app.get("/api/run/{job_id}/journal")
def run_journal(job_id: str):
    store = _journal()
    if store.get(job_id) is None:
        raise HTTPException(404, "job not found")
    exported = store.csv(job_id)
    length = exported.seek(0, os.SEEK_END)
    exported.seek(0)

    def chunks():
        try:
            while data := exported.read(64 * 1024):
                yield data
        finally:
            exported.close()
    return StreamingResponse(chunks(), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition":
                                      f'attachment; filename="journal-{job_id}.csv"',
                                      "Content-Length": str(length)})


class JournalCleanupRequest(BaseModel):
    mode: Literal["single", "cross", "local"]


@app.post("/api/history/cleanup")
def cleanup_history(payload: JournalCleanupRequest, request: Request):
    origin = request.headers.get("Origin")
    if (origin and origin.rstrip("/") != str(request.base_url).rstrip("/")) or (
        request.headers.get("Sec-Fetch-Site", "").lower() not in {"", "same-origin", "none"}
    ):
        raise HTTPException(403, "journal_origin_rejected")
    batches = getattr(request.app.state, "source_batches", None)
    batch_guard = batches.guard if batches and payload.mode == "local" else nullcontext()
    with JOBS_LOCK, batch_guard:
        if batches and payload.mode == "local":
            batches.check_local_journal_cleanup()
        if any(j.mode == payload.mode and j.status == "running" for j in JOBS.values()):
            raise JournalError("journal_in_use")
        completed = [j.id for j in JOBS.values() if j.mode == payload.mode]
        result = _journal().cleanup(payload.mode, completed_ids=completed)
        if batches and payload.mode == "local":
            result["removed_events"] += batches.clear_local_journal_cache()
        for job_id in completed:
            JOBS.pop(job_id, None)
        return result
