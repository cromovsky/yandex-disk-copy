"""Protected on-premises job API; never serialize credentials or HTTP errors."""

from __future__ import annotations

import asyncio
import csv
import hmac
import io
import ipaddress
import json
import os
import re
from contextlib import contextmanager
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError

from .destination import destination_path, normalize_destination_url
from .diagnostics import describe_diagnostics
from .errors import MigrationError
from .runner import ACTIVE, disk_path, join_path
from .store import canonical_config, config_identity
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


class MigrationRoute(APIRoute):
    def get_route_handler(self):
        handler = super().get_route_handler()

        async def migration_handler(request):
            try:
                return await handler(request)
            except MigrationError as exc:
                content = {"detail": exc.code}
                message = describe_diagnostics({
                    "error": exc.code, "http_status": exc.http_status,
                    **exc.diagnostics,
                })
                if message:
                    content["diagnostic_message"] = message
                return JSONResponse(status_code=409, content=content)

        return migration_handler


router = APIRouter(prefix="/api/onprem", dependencies=[Depends(require_access)],
                   route_class=MigrationRoute)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Configuration(StrictModel):
    source_org_id: str = Field(min_length=1, max_length=256)
    source_client_id: str = Field(min_length=1, max_length=256)
    source_email: str = Field(min_length=3, max_length=254)
    source_path: str = "/"
    source_type: Literal["personal", "shared"] = "personal"
    source_vd_hash: str = ""
    source_folder: str = ""
    destination_url: str = Field(min_length=1, max_length=2048)
    destination_identity: str = Field(min_length=1, max_length=256)
    destination_root: str = ""
    destination_type: Literal["personal", "shared"] = "personal"
    destination_vd_hash: str = ""
    destination_base: str = "disk:/"
    strategy: Literal["url", "staged"] = "staged"
    source_public: bool = Field(default=False, strict=True)
    retry_enabled: bool = Field(default=True, strict=True)
    identity_confirmed: Literal[True]


class Credentials(StrictModel):
    source_client_secret: SecretStr
    destination_token: SecretStr
    source_admin_token: SecretStr | None = None


class AccessRequest(StrictModel):
    config: Configuration
    credentials: Credentials


class CredentialsRequest(AccessRequest):
    point_retry_entry_id: str | None = Field(default=None, min_length=1, max_length=64)


class CreateRequest(AccessRequest):
    idempotency_key: str = Field(min_length=8, max_length=128)


class RetryRequest(StrictModel):
    strategy: Literal["url", "staged"] = "staged"


class RetryPolicyRequest(StrictModel):
    enabled: bool = Field(strict=True)


class PointRetryRequest(StrictModel):
    attempt_id: str | None = Field(default=None, min_length=1, max_length=64)
    revision: int = Field(ge=0, strict=True)


async def body(request, model):
    try:
        return model.model_validate(await request.json())
    except (ValueError, ValidationError):
        # Default Pydantic diagnostics can echo the secret-bearing request.
        raise HTTPException(422, "invalid_request") from None


def access_data(model, *, new_job=False, staged_only=False):
    config = model.config.model_dump()
    if (new_job or staged_only) and config["strategy"] != "staged":
        raise HTTPException(422, "onprem_staged_required")
    source_shared = config["source_type"] == "shared"
    if not source_shared:
        if config["source_vd_hash"] or config["source_folder"]:
            raise HTTPException(422, "invalid_source_shared_disk")
        for key in ("source_type", "source_vd_hash", "source_folder"):
            config.pop(key)
    if config["source_public"]:
        if config["destination_type"] != "shared":
            raise HTTPException(422, "invalid_source_public")
    else:
        config.pop("source_public")
    if ("@" not in config["source_email"] or "/" in config["source_email"]
            or any(c.isspace() for c in config["source_email"])):
        raise HTTPException(422, "invalid_source_email")
    for key in ("source_org_id", "source_client_id", "destination_identity"):
        if any(ord(c) < 32 for c in config[key]):
            raise HTTPException(422, "invalid_config")
    try:
        config["source_path"] = disk_path(config["source_path"])
        config["destination_url"] = normalize_destination_url(config["destination_url"])
    except (MigrationError, ValueError):
        raise HTTPException(422, "invalid_paths_or_destination") from None
    if config["destination_type"] == "shared":
        try:
            config["destination_base"] = disk_path(config["destination_base"])
            folder = config.get("source_folder") or config["source_email"]
            root = join_path(config["destination_base"], folder)
            if (config["destination_root"]
                    and disk_path(config["destination_root"]) != root):
                raise ValueError("invalid_destination_shared_disk")
            config["destination_root"] = root
            config = canonical_config(config)
            if new_job:
                config.update(
                    destination_base="disk:/",
                    destination_root=join_path("disk:/", folder),
                    source_public=not source_shared,
                )
                if source_shared or "strategy" not in model.config.model_fields_set:
                    config["strategy"] = "staged"
        except (MigrationError, ValueError):
            raise HTTPException(422, "invalid_destination_shared_disk") from None
    else:
        if config["destination_vd_hash"]:
            raise HTTPException(422, "invalid_destination_shared_disk")
        for key in ("destination_type", "destination_vd_hash", "destination_base"):
            config.pop(key)
        try:
            config["destination_root"] = disk_path(
                config["destination_root"] or "disk:/" + config["source_email"],
            )
        except ValueError:
            raise HTTPException(422, "invalid_paths_or_destination") from None
    credentials = {k: v.get_secret_value()
                   for k, v in model.credentials.__dict__.items() if v is not None}
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


def require_batch_active(request, job_id, *, starting=False):
    batches = getattr(request.app.state, "source_batches", None)
    if batches is None:
        return
    try:
        managed = batches.require_child_active(job_id)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from None
    if starting and managed:
        raise HTTPException(409, "managed_batch_start_required")


@contextmanager
def guarded(value, job_id=None):
    lock = value.job_guard(job_id) if job_id else value.guard
    if not lock.acquire(blocking=False):
        raise HTTPException(409, "job_busy")
    try:
        yield
    except ValueError as exc:
        code = str(exc)
        if not re.fullmatch(r"[a-z_]{1,80}", code):
            code = "invalid_action"
        raise HTTPException(409, code) from None
    finally:
        lock.release()


@router.post("/preflight")
async def preflight(request: Request):
    config, credentials = access_data(await body(request, AccessRequest), new_job=True)
    value = service(request)
    with guarded(value):
        return await asyncio.to_thread(value.runner.preflight, config, credentials)


@router.post("/jobs")
async def create_job(request: Request):
    model = await body(request, CreateRequest)
    if model.idempotency_key.startswith("batch-"):
        raise HTTPException(422, "reserved_idempotency_key")
    config, credentials = access_data(model, new_job=True)
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
    config, credentials = access_data(model, staged_only=True)
    value = service(request)
    with guarded(value, job_id):
        value.runner.require_staged_job(job_of(value, job_id))
        job = value.store.restart(job_id, config, model.idempotency_key)
        if (job["state"] == "queued" and job["revision"] == 0
                and not job["stop_requested"] and not value.registry.get(job["id"])):
            value.registry.put(job["id"], job["config"], credentials)
    value.notify()
    return job


@router.post("/jobs/{job_id}/stop")
async def stop(job_id: str, request: Request):
    if await request.body():
        await body(request, StrictModel)
    value = service(request)
    job_of(value, job_id)
    try:
        job = value.store.request_full_stop(job_id)
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
    return {"entries": value.store.describe_entries(job_id, value.store.entries(
        job_id, limit=max(1, min(limit, 1000)), offset=max(0, offset), state=state,
    ))}


@router.get("/jobs/{job_id}/entries/{entry_id}")
def get_entry(job_id: str, entry_id: str, request: Request):
    value = service(request)
    job_of(value, job_id)
    try:
        return value.store.describe_entries(job_id, [{"id": entry_id}])[0]
    except KeyError:
        raise HTTPException(404, "entry_not_found") from None


@router.post("/jobs/{job_id}/start")
def start(job_id: str, request: Request):
    require_batch_active(request, job_id, starting=True)
    value = service(request)
    with guarded(value, job_id):
        job = job_of(value, job_id)
        require_live_job(job)
        value.runner.require_staged_job(job)
        if job["state"] != "ready":
            raise HTTPException(409, "plan_not_ready")
        if not value.registry.get(job_id):
            raise HTTPException(409, "credentials_required")
        value.store.update_job(job_id, state="running", pause_requested=False)
    value.notify()
    return value.store.get(job_id)


@router.post("/jobs/{job_id}/retries")
async def retries(job_id: str, request: Request):
    model = await body(request, RetryPolicyRequest)
    value = service(request)
    job = job_of(value, job_id)
    value.runner.require_staged_job(job)
    batches = getattr(request.app.state, "source_batches", None)
    if batches and batches.set_child_retry_enabled(job_id, model.enabled):
        return {"retry_enabled": model.enabled}
    # This switch must remain responsive while a remote request owns job_guard.
    value.runner.set_retry_enabled(job_id, model.enabled)
    value.notify()
    return {"retry_enabled": model.enabled}


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
    require_batch_active(request, job_id)
    model = await body(request, CredentialsRequest)
    config, secret = access_data(model, staged_only=True)
    value = service(request)
    with guarded(value, job_id):
        job = job_of(value, job_id)
        require_live_job(job)
        value.runner.require_staged_job(job)
        if config_identity(job["config"]) != config_identity(config):
            raise HTTPException(409, "credential_binding_mismatch")
        if job["state"] not in {
            "awaiting_credentials", "ready", "paused", "needs_attention",
            "completed_with_issues",
        }:
            raise HTTPException(409, "job_not_awaiting_credentials")
        # A recovered point retry also reattaches without requiring SaaS.
        active_entry = value.store.active_point_retry_entry(job_id)
        retry_entry_id = model.point_retry_entry_id or (
            active_entry["id"] if active_entry else None
        )

        def check_retry_entry():
            if retry_entry_id is None:
                return None
            try:
                entry = value.store.describe_entries(job_id, [{"id": retry_entry_id}])[0]
            except KeyError:
                raise HTTPException(404, "entry_not_found") from None
            pending = value.store.active_point_retry_entry(job_id)
            if not entry["can_retry"] and not (
                pending and pending["id"] == entry["id"] and entry["kind"] == "file"
            ):
                raise HTTPException(409, "attempt_not_retryable")
            return entry

        retry_entry = check_retry_entry()
        try:
            if retry_entry:
                attempt = (value.store.attempt(retry_entry["attempt_id"])
                           if retry_entry["attempt_id"] else None)
                await asyncio.to_thread(
                    value.runner._logged, job, retry_entry, "check_destination_access",
                    value.runner.preflight_destination, config, secret, attempt=attempt,
                )
            else:
                await asyncio.to_thread(value.runner.preflight, config, secret)
        except Exception:
            require_live_job(job_of(value, job_id))
            raise
        require_live_job(job_of(value, job_id))
        check_retry_entry()
        value.registry.put(job_id, config, secret)
    return value.store.get(job_id)


@router.post("/jobs/{job_id}/resume")
def resume(job_id: str, request: Request):
    require_batch_active(request, job_id)
    value = service(request)
    value.runner.require_staged_job(job_of(value, job_id))
    batches = getattr(request.app.state, "source_batches", None)
    if batches is not None:
        batches.prepare_child_retry(job_id)
    with guarded(value, job_id):
        job = job_of(value, job_id)
        require_live_job(job)
        if not value.registry.get(job_id):
            raise HTTPException(409, "credentials_required")
        if job["state"] not in {"paused", "awaiting_credentials", "needs_attention"}:
            raise HTTPException(409, "job_not_resumable")
        entries = value.store.entries(job_id, limit=1000000)
        if (not job["point_retry_active"]
                and not any(e["state"] == "planned" or e["state"] in ACTIVE
                            for e in entries)
                and any(e["state"] == "outcome_unknown" for e in entries)):
            raise HTTPException(409, "active_or_unknown_attempt")
        if job.get("namespace_state") in {"intent", "unknown"}:
            raise HTTPException(409, "technical_namespace_outcome_unknown")
        target = job.get("resume_state")
        if job["point_retry_active"]:
            target = "running"
        elif target in {"queued", "planning"}:
            target = "queued"
        elif target == "ready":
            target = "ready"
        elif not entries:
            target = "queued"
        else:
            target = "running"
        value.store.update_job(job_id, state=target, resume_state=None,
                               pause_requested=False, error=None)
    batches = getattr(request.app.state, "source_batches", None)
    if batches is not None:
        batches.activate_child_retry(job_id)
    value.notify()
    return value.store.get(job_id)


@router.post("/jobs/{job_id}/retry")
async def retry(job_id: str, request: Request):
    require_batch_active(request, job_id)
    model = await body(request, RetryRequest)
    if model.strategy != "staged":
        raise HTTPException(422, "onprem_staged_required")
    value = service(request)
    value.runner.require_staged_job(job_of(value, job_id))
    batches = getattr(request.app.state, "source_batches", None)
    if batches is not None:
        batches.prepare_child_retry(job_id)
    with guarded(value, job_id):
        require_live_job(job_of(value, job_id))
        if not value.registry.get(job_id):
            raise HTTPException(409, "credentials_required")
        value.runner.retry(job_id, model.strategy)
    batches = getattr(request.app.state, "source_batches", None)
    if batches is not None:
        batches.activate_child_retry(job_id)
    value.notify()
    return value.store.get(job_id)


@router.post("/jobs/{job_id}/entries/{entry_id}/retry")
async def point_retry(job_id: str, entry_id: str, request: Request):
    require_batch_active(request, job_id)
    model = await body(request, PointRetryRequest)
    value = service(request)
    value.runner.require_staged_job(job_of(value, job_id))
    batches = getattr(request.app.state, "source_batches", None)
    if batches is not None:
        batches.prepare_child_retry(job_id)
    with guarded(value, job_id):
        require_live_job(job_of(value, job_id))
        if not value.registry.get(job_id):
            raise HTTPException(409, "credentials_required")
        try:
            value.store.entry(entry_id)
        except KeyError:
            raise HTTPException(404, "entry_not_found") from None
        if model.attempt_id is not None:
            try:
                value.store.attempt(model.attempt_id)
            except KeyError:
                raise HTTPException(404, "attempt_not_found") from None
        value.store.queue_point_retry(
            job_id, entry_id, model.attempt_id, model.revision,
        )
    batches = getattr(request.app.state, "source_batches", None)
    if batches is not None:
        batches.activate_child_retry(job_id)
    value.notify()
    return value.store.get(job_id)


@router.post("/jobs/{job_id}/cleanup")
def cleanup(job_id: str, request: Request):
    value = service(request)
    with guarded(value, job_id):
        job_of(value, job_id)
        return value.runner.cleanup(job_id)


@router.post("/cache/cleanup")
def cleanup_cache(request: Request):
    value = service(request)
    if not value.guard.acquire(blocking=False):
        raise HTTPException(409, "cache_in_use")
    try:
        if any(owner.startswith("onprem:") for owner in value.slots.active_owners):
            raise HTTPException(409, "cache_in_use")
        return value.runner.cleanup_cache()
    finally:
        value.guard.release()


@router.get("/jobs/{job_id}/events")
async def events(job_id: str, request: Request, after: int = 0, format: str = "sse",
                 tail: int = Query(0, ge=0, le=500)):
    value = service(request)
    job_of(value, job_id)
    if format == "json":
        if after <= 0 and tail:
            cutoff = value.store.last_sequence(job_id)
            cursor = value.store.tail_cursor(job_id, tail, cutoff)
            return {"events": value.store.events(
                job_id, after=cursor, limit=tail, through=cutoff, journal=True,
            )}
        return {"events": value.store.events(job_id, after=max(0, after), journal=True)}
    try:
        cursor = max(0, after, int(request.headers.get("Last-Event-ID", "0")))
    except ValueError:
        raise HTTPException(422, "invalid_event_cursor") from None

    async def stream():
        nonlocal cursor
        while not await request.is_disconnected():
            batch = value.store.events(job_id, after=cursor, journal=True)
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


@router.get("/jobs/{job_id}/journal")
def journal(job_id: str, request: Request):
    value = service(request)
    job_of(value, job_id)
    cutoff = value.store.last_sequence(job_id)

    def stream():
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "timestamp_utc", "sequence", "job_id", "event_type", "stage",
            "stage_label", "status", "status_label", "source", "destination",
            "kind", "size", "strategy", "attempt_id", "operation_id",
            "remote_path", "http_status", "error", "error_message", "duration_ms",
            "remote_status", "download_attempts", "max_download_attempts",
            "retry_delay_ms", "retry_at_ms", "point_retry_attempts",
            "max_point_retry_attempts", "retry_enabled",
            "error_side", "api_error", "api_reason", "transport_error",
            "request_host", "request_id", "diagnostic_message",
        ])
        yield "\ufeff" + output.getvalue()
        cursor = 0
        while cursor < cutoff:
            batch = value.store.events(job_id, after=cursor, limit=200,
                                       through=cutoff, journal=True)
            if not batch:
                break
            output.seek(0)
            output.truncate(0)
            for event in batch:
                payload, context = event["payload"], event["context"]
                writer.writerow([csv_cell(item) for item in (
                    event["created_at"], event["sequence"], job_id, event["type"],
                    payload.get("stage", payload.get("phase")), event["stage_label"],
                    payload.get("status", payload.get("state")), event["status_label"],
                    context["source"], context["destination"], context["kind"],
                    context["size"], context["strategy"], payload.get("attempt_id"),
                    payload.get("operation_id"), context["remote_path"],
                    payload.get("http_status"), payload.get("error"),
                    event["error_message"], payload.get("duration_ms"),
                    payload.get("remote_status"),
                    payload.get("download_attempts"),
                    payload.get("max_download_attempts"),
                    payload.get("retry_delay_ms"), payload.get("retry_at_ms"),
                    payload.get("point_retry_attempts"),
                    payload.get("max_point_retry_attempts"), payload.get("retry_enabled"),
                    payload.get("error_side"), payload.get("api_error"),
                    payload.get("api_reason"), payload.get("transport_error"),
                    payload.get("request_host"), payload.get("request_id"),
                    event["diagnostic_message"],
                )])
            cursor = batch[-1]["sequence"]
            yield output.getvalue()

    return StreamingResponse(stream(), media_type="text/csv; charset=utf-8", headers={
        "Content-Disposition": f'attachment; filename="migration-journal-{job_id}.csv"',
        "Cache-Control": "no-store",
    })


@router.get("/jobs/{job_id}/report")
def report(job_id: str, request: Request):
    value = service(request)
    job = job_of(value, job_id)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["source", "destination", "kind", "size", "md5", "modified",
                     "strategy", "state", "verification", "error", "remote_path",
                     "remote_phase", "download_attempts", "retry_at_ms",
                     "point_retry_attempts", "max_point_retry_attempts"])
    for entry in value.store.entries(job_id, limit=1000000):
        attempt = (value.store.attempt(entry["attempt_id"])
                   if entry.get("attempt_id") else {})
        writer.writerow([csv_cell(v) for v in (
            entry["source_path"],
            destination_path(job["config"], entry["destination_path"]),
            entry["kind"],
            entry["size"], entry.get("md5"), entry.get("modified"),
            attempt.get("strategy", job["config"]["strategy"]), entry["state"],
            entry.get("verification"), entry.get("error"),
            (destination_path(job["config"], attempt["remote_path"])
             if attempt.get("remote_path") else None), attempt.get("phase"),
            entry.get("download_attempts", 0), entry.get("retry_at_ms"),
            entry.get("point_retry_attempts", 0),
            3 if job["config"].get("retry_enabled", True) else 1,
        )])
    return Response("\ufeff" + output.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition":
                             f'attachment; filename="migration-{job_id}.csv"'})
