"""Protected on-premises job API; never serialize credentials or HTTP errors."""

from __future__ import annotations

import asyncio
import csv
import hashlib
import hmac
import io
import ipaddress
import json
import os
import re
from contextlib import contextmanager
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError

from .destination import normalize_destination_url
from .errors import MigrationError
from .operations import valid_operation_reference
from .runner import FINAL_PENDING, UNKNOWN_PHASES, disk_path
from .worker import enabled


def require_access(request: Request):
    if not enabled():
        raise HTTPException(404, "onprem_disabled")
    host = request.client.host if request.client else ""
    try:
        local = ipaddress.ip_address(host).is_loopback
    except ValueError:
        local = False
    allow_http = os.environ.get("ONPREM_ALLOW_INSECURE_HTTP", "0").strip().lower() in {
        "1", "true", "yes", "on",
    }
    expected = os.environ.get("ONPREM_ADMIN_TOKEN", "")
    if expected:
        supplied = request.headers.get("X-Onprem-Key", "")
        if not hmac.compare_digest(supplied.encode(), expected.encode()):
            raise HTTPException(403, "onprem_access_required")
        if not allow_http and not local and request.url.scheme != "https":
            raise HTTPException(403, "onprem_https_required")
    else:
        hostname = request.url.hostname or ""
        try:
            local_host = ipaddress.ip_address(hostname).is_loopback
        except ValueError:
            local_host = hostname == "localhost"
        if not local_host or (not local and not allow_http):
            raise HTTPException(403, "onprem_access_required")
    # Loopback does not protect from another website targeting a local service.
    origin = request.headers.get("origin")
    if origin and origin.rstrip("/") != str(request.base_url).rstrip("/"):
        raise HTTPException(403, "onprem_origin_rejected")


router = APIRouter(prefix="/api/onprem", dependencies=[Depends(require_access)])


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Configuration(StrictModel):
    source_org_id: str = Field(min_length=1, max_length=256)
    source_client_id: str = Field(min_length=1, max_length=256)
    source_email: str = Field(min_length=3, max_length=254)
    source_path: str = "/"
    destination_url: str = Field(min_length=1, max_length=2048)
    destination_identity: str = Field(min_length=1, max_length=256)
    destination_root: str = ""
    strategy: Literal["url", "staged"] = "url"
    identity_confirmed: Literal[True]


class Credentials(StrictModel):
    source_client_secret: SecretStr
    destination_token: SecretStr


class AccessRequest(StrictModel):
    config: Configuration
    credentials: Credentials


class CreateRequest(AccessRequest):
    idempotency_key: str = Field(min_length=8, max_length=128)


class RetryRequest(StrictModel):
    strategy: Literal["url", "staged"] = "url"


class ResolveRequest(StrictModel):
    attempt_id: str = Field(min_length=1, max_length=64)
    revision: int = Field(ge=0)
    operation_id: str | None = None
    terminal_status: Literal["success", "failed"] | None = None
    evidence: str = Field(default="", max_length=2000)
    confirmed: Literal[True]


async def body(request, model):
    try:
        return model.model_validate(await request.json())
    except (ValueError, ValidationError):
        # Default Pydantic diagnostics can echo the secret-bearing request.
        raise HTTPException(422, "invalid_request") from None


def access_data(model):
    config = model.config.model_dump()
    if ("@" not in config["source_email"] or "/" in config["source_email"]
            or any(c.isspace() for c in config["source_email"])):
        raise HTTPException(422, "invalid_source_email")
    for key in ("source_org_id", "source_client_id", "destination_identity"):
        if any(ord(c) < 32 for c in config[key]):
            raise HTTPException(422, "invalid_config")
    try:
        config["source_path"] = disk_path(config["source_path"])
        config["destination_root"] = disk_path(
            config["destination_root"] or "disk:/" + config["source_email"],
        )
        config["destination_url"] = normalize_destination_url(config["destination_url"])
    except (MigrationError, ValueError):
        raise HTTPException(422, "invalid_paths_or_destination") from None
    credentials = {k: v.get_secret_value()
                   for k, v in model.credentials.__dict__.items()}
    if any(not value or len(value) > 8192 or any(ord(c) < 32 for c in value)
           for value in credentials.values()):
        raise HTTPException(422, "invalid_credentials")
    return config, credentials


def service(request):
    value = getattr(request.app.state, "onprem", None)
    if not value:
        raise HTTPException(503, "onprem_unavailable")
    return value


def job_of(value, job_id):
    try:
        return value.store.get(job_id)
    except KeyError:
        raise HTTPException(404, "job_not_found") from None


def require_live_job(job):
    if job["state"] == "stopped" or job.get("stop_requested"):
        raise HTTPException(409, "job_stopped")


def has_pending_final_move(value, job_id):
    return any(attempt["phase"] in FINAL_PENDING
               for attempt in value.store.attempts(job_id))


@contextmanager
def guarded(value):
    if not value.guard.acquire(blocking=False):
        raise HTTPException(409, "job_busy")
    try:
        yield
    except MigrationError as exc:
        raise HTTPException(409, exc.code) from None
    except ValueError as exc:
        code = str(exc)
        if not re.fullmatch(r"[a-z_]{1,80}", code):
            code = "invalid_action"
        raise HTTPException(409, code) from None
    finally:
        value.guard.release()


@router.post("/preflight")
async def preflight(request: Request):
    config, credentials = access_data(await body(request, AccessRequest))
    value = service(request)
    with guarded(value):
        return await asyncio.to_thread(value.runner.preflight, config, credentials)


@router.post("/jobs")
async def create_job(request: Request):
    model = await body(request, CreateRequest)
    config, credentials = access_data(model)
    value = service(request)
    with guarded(value):
        job = value.store.create(config, model.idempotency_key)
        if (job["state"] == "queued" and job["revision"] == 0
                and not value.registry.get(job["id"])):
            value.registry.put(job["id"], config, credentials)
    value.notify()
    return job


@router.post("/jobs/{job_id}/restart")
async def restart(job_id: str, request: Request):
    model = await body(request, CreateRequest)
    config, credentials = access_data(model)
    value = service(request)
    with guarded(value):
        job_of(value, job_id)
        job = value.store.restart(job_id, config, model.idempotency_key)
        if (job["state"] == "queued" and job["revision"] == 0
                and not job["stop_requested"] and not value.registry.get(job["id"])):
            value.registry.put(job["id"], config, credentials)
    value.notify()
    return job


@router.post("/jobs/{job_id}/stop")
async def stop(job_id: str, request: Request):
    if await request.body():
        await body(request, StrictModel)
    value = service(request)
    job_of(value, job_id)
    try:
        job = value.store.request_stop(job_id)
    except ValueError:
        raise HTTPException(409, "job_not_stoppable") from None
    value.notify()
    return job


@router.get("/jobs")
def jobs(request: Request, limit: int = 50, offset: int = 0):
    value = service(request)
    holding = value.holding_job()
    return {"jobs": [value.describe_job(job, holding) for job in value.store.jobs(
        limit=max(1, min(limit, 200)), offset=max(0, offset),
    )]}


@router.get("/jobs/{job_id}")
def get_job(job_id: str, request: Request):
    value = service(request)
    return value.describe_job(job_of(value, job_id), value.holding_job())


@router.get("/jobs/{job_id}/entries")
def entries(job_id: str, request: Request, limit: int = 200, offset: int = 0,
            state: str | None = None):
    value = service(request)
    job_of(value, job_id)
    return {"entries": value.store.entries(
        job_id, limit=max(1, min(limit, 1000)), offset=max(0, offset), state=state,
    )}


@router.post("/jobs/{job_id}/start")
def start(job_id: str, request: Request):
    value = service(request)
    with guarded(value):
        job = job_of(value, job_id)
        require_live_job(job)
        if job["state"] != "ready":
            raise HTTPException(409, "plan_not_ready")
        if not value.registry.get(job_id):
            raise HTTPException(409, "credentials_required")
        value.store.update_job(job_id, state="running", pause_requested=False)
    value.notify()
    return value.store.get(job_id)


@router.post("/jobs/{job_id}/pause")
def pause(job_id: str, request: Request):
    value = service(request)
    job = job_of(value, job_id)
    require_live_job(job)
    if job["state"] != "running":
        raise HTTPException(409, "job_not_running")
    value.store.update_job(job_id, pause_requested=True)
    value.notify()
    return value.store.get(job_id)


@router.put("/jobs/{job_id}/credentials")
async def credentials(job_id: str, request: Request):
    config, secret = access_data(await body(request, AccessRequest))
    value = service(request)
    with guarded(value):
        job = job_of(value, job_id)
        stopped_final_move = (
            job["state"] == "stopped" and has_pending_final_move(value, job_id)
        )
        if not stopped_final_move:
            require_live_job(job)
        if job["config"] != config:
            raise HTTPException(409, "credential_binding_mismatch")
        if not stopped_final_move and job["state"] not in {
            "awaiting_credentials", "ready", "paused", "needs_attention",
            "completed_with_issues",
        }:
            raise HTTPException(409, "job_not_awaiting_credentials")
        # Read-only validation before trusting a freshly provided token.
        validate = (value.runner.preflight_destination if stopped_final_move
                    else value.runner.preflight)
        await asyncio.to_thread(validate, config, secret)
        if not stopped_final_move:
            require_live_job(job_of(value, job_id))
        value.registry.put(job_id, config, secret)
    if stopped_final_move:
        value.notify()
    return value.store.get(job_id)


@router.post("/jobs/{job_id}/resume")
def resume(job_id: str, request: Request):
    value = service(request)
    with guarded(value):
        job = job_of(value, job_id)
        require_live_job(job)
        if not value.registry.get(job_id):
            raise HTTPException(409, "credentials_required")
        if job["state"] not in {"paused", "awaiting_credentials", "needs_attention"}:
            raise HTTPException(409, "job_not_resumable")
        if any(e["state"] == "outcome_unknown"
               for e in value.store.entries(job_id, limit=1000000)):
            raise HTTPException(409, "active_or_unknown_attempt")
        if job.get("namespace_state") in {"intent", "unknown"}:
            raise HTTPException(409, "technical_namespace_outcome_unknown")
        target = job.get("resume_state")
        if target in {"queued", "planning"}:
            target = "queued"
        elif target == "ready":
            target = "ready"
        elif not value.store.entries(job_id, limit=1):
            target = "queued"
        else:
            target = "running"
        value.store.update_job(job_id, state=target, resume_state=None,
                               pause_requested=False, error=None)
    value.notify()
    return value.store.get(job_id)


@router.post("/jobs/{job_id}/retry")
async def retry(job_id: str, request: Request):
    model = await body(request, RetryRequest)
    value = service(request)
    with guarded(value):
        require_live_job(job_of(value, job_id))
        if not value.registry.get(job_id):
            raise HTTPException(409, "credentials_required")
        value.runner.retry(job_id, model.strategy)
    value.notify()
    return value.store.get(job_id)


@router.post("/jobs/{job_id}/resolve")
async def resolve(job_id: str, request: Request):
    model = await body(request, ResolveRequest)
    value = service(request)
    with guarded(value):
        job = job_of(value, job_id)
        if job.get("stop_requested") and job["state"] != "stopped":
            raise HTTPException(409, "job_stopped")
        try:
            attempt = value.store.attempt(model.attempt_id)
            entry = value.store.entry(attempt["entry_id"])
        except KeyError:
            raise HTTPException(404, "attempt_not_found") from None
        stopped_final_move = (
            job["state"] == "stopped" and attempt["phase"] in FINAL_PENDING
        )
        if job["state"] == "stopped" and not stopped_final_move:
            raise HTTPException(409, "job_stopped")
        if (entry["job_id"] != job_id or entry["attempt_id"] != attempt["id"]
                or (not stopped_final_move and attempt["phase"] not in UNKNOWN_PHASES)):
            raise HTTPException(409, "attempt_not_unknown")
        if job["revision"] != model.revision:
            raise HTTPException(409, "stale_revision")
        is_move = "move" in attempt["phase"]
        if model.operation_id:
            if (
                model.terminal_status
                or not valid_operation_reference(model.operation_id)
            ):
                raise HTTPException(422, "invalid_operation_id")
            if (stopped_final_move and attempt["operation_id"]
                    and attempt["operation_id"] != model.operation_id):
                raise HTTPException(409, "operation_reference_mismatch")
            if stopped_final_move:
                destination = value.runner.destination_client(job)
            else:
                _, destination = value.runner.clients(job)
            status = await asyncio.to_thread(destination.poll, model.operation_id)
            phase = "waiting_move" if is_move else "waiting_transfer"
            if stopped_final_move and status == "success":
                phase = "verifying_final"
            operation_id = model.operation_id
        else:
            if (not model.terminal_status or len(model.evidence.strip()) < 20
                    or re.search(r"https?://|oauth|bearer|token=", model.evidence, re.I)
                    or re.fullmatch(r".*(?:404|timeout|таймер|таймаут).*",
                                    model.evidence, re.I)):
                raise HTTPException(422, "terminal_server_evidence_required")
            status, operation_id = model.terminal_status, None
            phase = "verifying_final" if is_move else "verifying"
        value.store.resolve_attempt(
            job_id, attempt["id"], model.revision,
            phase="failed" if status == "failed" else phase,
            operation_id=operation_id,
            entry_state="failed" if status == "failed" else (
                "committing" if is_move else "verifying"
            ),
            error="remote_operation_failed" if status == "failed" else None,
            status=status,
            evidence_kind="operation_reference" if operation_id
            else "administrator_terminal_server_evidence",
            evidence_digest=hashlib.sha256(model.evidence.encode("utf-8")).hexdigest()
            if not operation_id else None,
        )
    return value.store.get(job_id)


@router.post("/jobs/{job_id}/cleanup")
def cleanup(job_id: str, request: Request):
    value = service(request)
    with guarded(value):
        job_of(value, job_id)
        return value.runner.cleanup(job_id)


@router.get("/jobs/{job_id}/events")
async def events(job_id: str, request: Request, after: int = 0, format: str = "sse"):
    value = service(request)
    job_of(value, job_id)
    if format == "json":
        return {"events": value.store.events(job_id, after=max(0, after))}
    try:
        cursor = max(0, after, int(request.headers.get("Last-Event-ID", "0")))
    except ValueError:
        raise HTTPException(422, "invalid_event_cursor") from None

    async def stream():
        nonlocal cursor
        while not await request.is_disconnected():
            batch = value.store.events(job_id, after=cursor)
            for event in batch:
                cursor = event["sequence"]
                yield f"id: {cursor}\ndata: {json.dumps(event, ensure_ascii=True)}\n\n"
            if not batch:
                yield ": keepalive\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache"})


def csv_cell(value):
    text = "" if value is None else str(value)
    return "'" + text if text.startswith(("=", "+", "-", "@", "\t", "\r")) else text


@router.get("/jobs/{job_id}/report")
def report(job_id: str, request: Request):
    value = service(request)
    job = job_of(value, job_id)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["source", "destination", "kind", "size", "md5", "modified",
                     "strategy", "state", "verification", "error", "remote_path",
                     "remote_phase"])
    for entry in value.store.entries(job_id, limit=1000000):
        attempt = (value.store.attempt(entry["attempt_id"])
                   if entry.get("attempt_id") else {})
        writer.writerow([csv_cell(v) for v in (
            entry["source_path"], entry["destination_path"], entry["kind"],
            entry["size"], entry.get("md5"), entry.get("modified"),
            attempt.get("strategy", job["config"]["strategy"]), entry["state"],
            entry.get("verification"), entry.get("error"),
            attempt.get("remote_path"), attempt.get("phase"),
        )])
    return Response("\ufeff" + output.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition":
                             f'attachment; filename="migration-{job_id}.csv"'})
