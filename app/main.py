"""FastAPI-обёртка вокруг DiskCopier: форма параметров + живой лог по SSE."""

from __future__ import annotations

import os
import queue
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import markdown
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .disk_copy import CopyConfig, CopyError, DiskCopier, OrgCreds
from .throttle import TARGET_RPS

STATIC_DIR = Path(__file__).parent / "static"
INSTRUCTION_MD = Path(__file__).parent.parent / "docs" / "INSTRUCTION.md"

BATCH_MAX_PAIRS = 25
BATCH_CONCURRENCY = 5

app = FastAPI(title="Yandex Disk Copy")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# рендер инструкции из markdown один раз при старте (единый источник — .md-файл)
_instruction_html: Optional[str] = None


def _render_instruction() -> str:
    global _instruction_html
    if _instruction_html is None:
        if not INSTRUCTION_MD.exists():
            _instruction_html = "<p>Инструкция недоступна.</p>"
        else:
            text = INSTRUCTION_MD.read_text(encoding="utf-8")
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
    destination_type: str = "personal"
    destination_vd_hash: str = ""
    destination_disk_id: str = ""


_DONE = object()  # сентинел конца лога в очереди


def _rate_limit_note() -> str:
    """Строка в лог: с каким темпом работали — видно в присланном логе."""
    return (
        f"RATE LIMIT | целевой темп: {TARGET_RPS:g} запросов/с "
        f"(общий на процесс, документированный потолок API Диска — 40)"
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


JOBS: dict[str, Job] = {}


def _run_job(job: Job, cfg: CopyConfig) -> None:
    def log(message: str) -> None:
        job.queue.put(message)

    log(_rate_limit_note())
    try:
        with DiskCopier(cfg, log=log) as copier:
            job.result = copier.run()
        job.status = "done"
        # Частичный успех: файлы, которые не доехали, не должны прятаться за
        # «Готово ✓» — отдаём сводку тем же способом, что и пакетный режим.
        failed = len((job.result or {}).get("fails") or [])
        if failed:
            saved = len((job.result or {}).get("saved") or [])
            job.error = f"Частично: перенесено {saved}, ошибок {failed}"
            log(f"SUMMARY | {job.error}")
    except CopyError as exc:
        job.error = str(exc)
        job.status = "error"
        log(f"ERROR | {exc}")
    except Exception as exc:  # noqa: BLE001 — любую ошибку показываем в логе
        job.error = f"{type(exc).__name__}: {exc}"
        job.status = "error"
        log(f"ERROR | {job.error}")
    finally:
        job.queue.put(_DONE)


def _run_batch(job: Job, base_cfgs: list[CopyConfig]) -> None:
    """Очередь пар с ограниченной параллельностью."""
    lock = threading.Lock()
    total = len(job.pairs)

    def log(message: str) -> None:
        job.queue.put(message)

    def run_one(pair: PairState, cfg: CopyConfig) -> None:
        prefix = (
            f"[{pair.index}/{total} {pair.source_disk_id} → "
            f"{pair.destination_disk_id}]"
        )
        with lock:
            pair.status = "running"
        log(f"{prefix} START")

        def pair_log(message: str) -> None:
            job.queue.put(f"{prefix} {message}")

        try:
            with DiskCopier(cfg, log=pair_log) as copier:
                pair.result = copier.run()
            with lock:
                pair.status = "done"
            log(f"{prefix} DONE")
        except CopyError as exc:
            with lock:
                pair.status = "error"
                pair.error = str(exc)
            log(f"{prefix} ERROR | {exc}")
        except Exception as exc:  # noqa: BLE001
            err = f"{type(exc).__name__}: {exc}"
            with lock:
                pair.status = "error"
                pair.error = err
            log(f"{prefix} ERROR | {err}")

    log(_rate_limit_note())
    log(
        f"BATCH START | pairs: {total}, concurrency: {BATCH_CONCURRENCY}, "
        f"max: {BATCH_MAX_PAIRS}"
    )
    try:
        with ThreadPoolExecutor(max_workers=BATCH_CONCURRENCY) as pool:
            futures = [
                pool.submit(run_one, pair, cfg)
                for pair, cfg in zip(job.pairs, base_cfgs)
            ]
            for fut in futures:
                fut.result()
    finally:
        done_ok = sum(1 for p in job.pairs if p.status == "done")
        done_err = sum(1 for p in job.pairs if p.status == "error")
        job.result = {
            "total": total,
            "done": done_ok,
            "failed": done_err,
            "pairs": [
                {
                    "index": p.index,
                    "source_disk_id": p.source_disk_id,
                    "destination_disk_id": p.destination_disk_id,
                    "status": p.status,
                    "error": p.error,
                    "result": p.result,
                }
                for p in job.pairs
            ],
        }
        if done_err == 0:
            job.status = "done"
            log(f"BATCH COMPLETED | ok: {done_ok}/{total}")
        elif done_ok == 0:
            job.status = "error"
            job.error = f"Все переносы завершились ошибкой ({done_err}/{total})"
            log(f"BATCH FAILED | errors: {done_err}/{total}")
        else:
            # частичный успех — считаем done, ошибка в summary
            job.status = "done"
            job.error = f"Частично: ok {done_ok}, ошибок {done_err} из {total}"
            log(f"BATCH COMPLETED WITH ERRORS | ok: {done_ok}, fail: {done_err}")
        job.queue.put(_DONE)


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
        "batch_max_pairs": BATCH_MAX_PAIRS,
        "batch_concurrency": BATCH_CONCURRENCY,
        "target_rps": TARGET_RPS,
    }


def _start_job(cfg: CopyConfig) -> dict:
    job = Job(id=uuid.uuid4().hex, kind="single")
    JOBS[job.id] = job
    threading.Thread(target=_run_job, args=(job, cfg), daemon=True).start()
    return {"job_id": job.id}


def _start_batch(cfgs: list[CopyConfig], pairs: list[TransferPair]) -> dict:
    job = Job(
        id=uuid.uuid4().hex,
        kind="batch",
        pairs=[
            PairState(
                index=i,
                source_disk_id=p.source_disk_id.strip(),
                destination_disk_id=p.destination_disk_id.strip(),
            )
            for i, p in enumerate(pairs, 1)
        ],
    )
    JOBS[job.id] = job
    threading.Thread(target=_run_batch, args=(job, cfgs), daemon=True).start()
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
        destination_type=dest_type,
        destination_vd_hash=vd_hash,
    )
    return _start_job(cfg)


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
            destination_type=dest_type,
            destination_vd_hash=vd_hash,
        )
        for p in pairs
    ]
    return _start_batch(cfgs, pairs)


@app.get("/api/run/{job_id}/stream")
def stream(job_id: str) -> StreamingResponse:
    job = JOBS.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")

    def event_stream():
        while True:
            item = job.queue.get()
            if item is _DONE:
                payload = job.error if job.status == "error" else (
                    job.error or "ok"
                )
                # частичный успех батча: event=done, data=текст summary
                yield f"event: {job.status}\ndata: {payload}\n\n"
                return
            # одна строка лога может содержать переводы строк — экранируем
            for line in str(item).splitlines() or [""]:
                yield f"data: {line}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/api/run/{job_id}")
def status(job_id: str) -> dict:
    job = JOBS.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return {
        "id": job.id,
        "kind": job.kind,
        "status": job.status,
        "result": job.result,
        "error": job.error,
        "pairs": [
            {
                "index": p.index,
                "source_disk_id": p.source_disk_id,
                "destination_disk_id": p.destination_disk_id,
                "status": p.status,
                "error": p.error,
            }
            for p in job.pairs
        ],
    }
