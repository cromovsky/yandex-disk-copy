"""Source lists use the same protected Onprem boundary as individual jobs."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError

from app.migration.api import MigrationRoute, require_access

router = APIRouter(prefix="/api/source-batches", route_class=MigrationRoute)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Credentials(StrictModel):
    source_client_secret: SecretStr
    destination_token: SecretStr | None = None
    source_admin_token: SecretStr | None = None

    def plain(self):
        return {k: v.get_secret_value() for k, v in self.__dict__.items() if v is not None}


class CreateRequest(StrictModel):
    mode: Literal["local", "onprem"]
    sources_csv: str = Field(min_length=1, max_length=2 * 1024**2)
    config: dict
    credentials: Credentials
    idempotency_key: str = Field(min_length=8, max_length=128)


class CredentialsRequest(StrictModel):
    credentials: Credentials


class SelectionRequest(StrictModel):
    source_ids: list[str] = Field(min_length=1, max_length=1000)


async def body(request, model):
    try:
        return model.model_validate(await request.json())
    except (ValueError, ValidationError):
        raise HTTPException(422, "invalid_request") from None


def service(request):
    value = getattr(request.app.state, "source_batches", None)
    if value is None:
        raise HTTPException(503, "source_batches_unavailable")
    return value


def access(request, mode):
    if mode == "onprem":
        require_access(request)
    else:
        origin = request.headers.get("origin")
        if origin and origin.rstrip("/") != str(request.base_url).rstrip("/"):
            raise HTTPException(403, "origin_rejected")


def existing(request, batch_id):
    value = service(request)
    try:
        batch = value.store.get(batch_id)
    except KeyError:
        raise HTTPException(404, "batch_not_found") from None
    access(request, batch["mode"])
    return value


def invoke(action, *args):
    try:
        return action(*args)
    except KeyError:
        raise HTTPException(404, "source_not_found") from None
    except ValueError as exc:
        code = str(exc)
        if not code.replace("_", "").isalnum() or len(code) > 80:
            code = "invalid_request"
        raise HTTPException(409, code) from None


@router.post("")
async def create(request: Request):
    model = await body(request, CreateRequest)
    access(request, model.mode)
    return invoke(service(request).create, model.mode, model.sources_csv, model.config,
                  model.credentials.plain(), model.idempotency_key)


@router.get("")
def listing(request: Request, mode: Literal["local", "onprem"] = "local",
            limit: int = Query(default=50, ge=1, le=100), offset: int = Query(default=0, ge=0)):
    access(request, mode)
    value = service(request)
    result = value.store.batches(mode=mode, limit=limit, offset=offset)
    result["batches"] = [value.detail(b["id"])["batch"] for b in result["batches"]]
    return result


@router.get("/{batch_id}")
def detail(request: Request, batch_id: str):
    return existing(request, batch_id).detail(batch_id)


@router.post("/{batch_id}/plan")
def plan(request: Request, batch_id: str):
    return invoke(existing(request, batch_id).plan, batch_id)


@router.post("/{batch_id}/start")
def start(request: Request, batch_id: str):
    return invoke(existing(request, batch_id).start_transfer, batch_id)


@router.post("/{batch_id}/pause")
def pause(request: Request, batch_id: str):
    return invoke(existing(request, batch_id).pause, batch_id)


@router.post("/{batch_id}/resume")
def resume(request: Request, batch_id: str):
    return invoke(existing(request, batch_id).resume, batch_id)


@router.post("/{batch_id}/stop")
def stop(request: Request, batch_id: str):
    return invoke(existing(request, batch_id).stop, batch_id)


@router.post("/{batch_id}/credentials")
@router.put("/{batch_id}/credentials", include_in_schema=False)
async def credentials(request: Request, batch_id: str):
    value = existing(request, batch_id)
    model = await body(request, CredentialsRequest)
    return invoke(value.attach_credentials, batch_id, model.credentials.plain())


@router.post("/{batch_id}/selection")
@router.put("/{batch_id}/selection", include_in_schema=False)
async def selection(request: Request, batch_id: str):
    value = existing(request, batch_id)
    model = await body(request, SelectionRequest)
    return invoke(value.selection, batch_id, model.source_ids)


def source_access(request, batch_id, source_id):
    value = existing(request, batch_id)
    try:
        source = value.store.source(source_id)
    except KeyError:
        raise HTTPException(404, "source_not_found") from None
    if source["batch_id"] != batch_id:
        raise HTTPException(404, "source_not_found")
    return value


@router.post("/{batch_id}/sources/{source_id}/plan")
def plan_source(request: Request, batch_id: str, source_id: str):
    value = source_access(request, batch_id, source_id)
    return invoke(value.plan, batch_id, source_id)


@router.get("/{batch_id}/sources/{source_id}/entries")
def entries(request: Request, batch_id: str, source_id: str,
            limit: int = Query(default=100, ge=1, le=500), offset: int = Query(default=0, ge=0)):
    return source_access(request, batch_id, source_id).entries(source_id, limit, offset)


def csv_response(value, batch_id, journal):
    spool = value.export(batch_id, journal=journal)

    def chunks():
        try:
            while chunk := spool.read(64 * 1024):
                yield chunk
        finally:
            spool.close()

    name = ("journal" if journal else "dry-run") + "-" + batch_id + ".csv"
    return StreamingResponse(chunks(), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": 'attachment; filename="' + name + '"'})


@router.get("/{batch_id}/report.csv")
def report(request: Request, batch_id: str):
    return csv_response(existing(request, batch_id), batch_id, False)


@router.get("/{batch_id}/journal.csv")
def journal(request: Request, batch_id: str):
    return csv_response(existing(request, batch_id), batch_id, True)
