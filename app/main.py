"""FastAPI-обёртка вокруг DiskCopier: форма параметров + живой лог по SSE."""

import os
import queue
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import markdown
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .disk_copy import CopyConfig, CopyError, DiskCopier, OrgCreds

STATIC_DIR = Path(__file__).parent / "static"
INSTRUCTION_MD = Path(__file__).parent.parent / "docs" / "INSTRUCTION.md"

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
    page_limit: int = 1000
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
    page_limit: int = 1000
    destination_type: str = "personal"
    destination_vd_hash: str = ""


_DONE = object()  # сентинел конца лога в очереди


@dataclass
class Job:
    id: str
    queue: "queue.Queue" = field(default_factory=queue.Queue)
    status: str = "running"  # running | done | error
    result: Optional[dict] = None
    error: Optional[str] = None


JOBS: dict[str, Job] = {}


def _run_job(job: Job, cfg: CopyConfig) -> None:
    def log(message: str) -> None:
        job.queue.put(message)

    try:
        copier = DiskCopier(cfg, log=log)
        job.result = copier.run()
        job.status = "done"
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
    return {"cross_org": val not in ("0", "false", "no", "off")}


def _start_job(cfg: CopyConfig) -> dict:
    job = Job(id=uuid.uuid4().hex)
    JOBS[job.id] = job
    threading.Thread(target=_run_job, args=(job, cfg), daemon=True).start()
    return {"job_id": job.id}


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


@app.get("/api/run/{job_id}/stream")
def stream(job_id: str) -> StreamingResponse:
    job = JOBS.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")

    def event_stream():
        while True:
            item = job.queue.get()
            if item is _DONE:
                payload = job.error if job.status == "error" else "ok"
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
        "status": job.status,
        "result": job.result,
        "error": job.error,
    }
