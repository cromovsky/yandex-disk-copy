"""Тесты web-слоя: валидация запросов и сводка результата джоба."""

from __future__ import annotations

import os
import queue

import pytest
from fastapi.testclient import TestClient

from app.disk_copy import CopyConfig, CopyError, OrgCreds
from app.main import (
    Job,
    TransferPair,
    _rate_limit_note,
    _run_job,
    _validate_batch_pairs,
    app,
)
from app.throttle import _env_float


client = TestClient(app)


def make_cfg() -> CopyConfig:
    creds = OrgCreds(
        client_id="ci", client_secret="cs", orgid="org-1", admin_token="tok"
    )
    return CopyConfig(
        source=creds,
        destination=creds,
        source_disk_id="src@company.ru",
        destination_disk_id="dst@company.ru",
    )


def drain(job: Job) -> list[str]:
    lines: list[str] = []
    while True:
        item = job.queue.get_nowait()
        if not isinstance(item, str):
            return lines
        lines.append(item)


class StubCopier:
    """Подменяет DiskCopier в _run_job; фиксирует, закрыли ли его."""

    instances: list = []

    def __init__(self, result=None, exc=None):
        self.result = result
        self.exc = exc
        self.closed = False
        StubCopier.instances.append(self)

    def __call__(self, cfg, log=None):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True

    def run(self):
        if self.exc:
            raise self.exc
        return self.result


@pytest.fixture(autouse=True)
def _reset_stubs():
    StubCopier.instances = []


def test_run_job_reports_partial_failures(monkeypatch):
    stub = StubCopier(
        result={"saved": [{"name": "a.txt"}], "fails": [{"name": "b.txt"}]}
    )
    monkeypatch.setattr("app.main.DiskCopier", stub)
    job = Job(id="j1")

    _run_job(job, make_cfg())

    assert job.status == "done"
    assert job.error == "Частично: перенесено 1, ошибок 1"
    assert any("SUMMARY" in line for line in drain(job))
    assert stub.closed, "сессия копира должна закрываться"


def test_run_job_stays_clean_when_nothing_failed(monkeypatch):
    stub = StubCopier(result={"saved": [{"name": "a.txt"}], "fails": []})
    monkeypatch.setattr("app.main.DiskCopier", stub)
    job = Job(id="j2")

    _run_job(job, make_cfg())

    assert job.status == "done"
    assert job.error is None


def test_run_job_marks_error_on_copy_error(monkeypatch):
    stub = StubCopier(exc=CopyError("нет места"))
    monkeypatch.setattr("app.main.DiskCopier", stub)
    job = Job(id="j3")

    _run_job(job, make_cfg())

    assert job.status == "error"
    assert job.error == "нет места"
    assert stub.closed


def test_run_job_puts_done_sentinel(monkeypatch):
    monkeypatch.setattr(
        "app.main.DiskCopier", StubCopier(result={"saved": [], "fails": []})
    )
    job = Job(id="j4")

    _run_job(job, make_cfg())

    items = []
    while True:
        try:
            items.append(job.queue.get_nowait())
        except queue.Empty:
            break
    assert not isinstance(items[-1], str), "последним в очереди должен быть сентинел"


# ── валидация входа ─────────────────────────────────────────────────────
def test_page_limit_zero_rejected():
    response = client.post(
        "/api/run",
        json={
            "client_id": "a",
            "client_secret": "b",
            "orgid": "o",
            "admin_token": "t",
            "source_disk_id": "s@x.ru",
            "destination_disk_id": "d@x.ru",
            "page_limit": 0,
        },
    )
    assert response.status_code == 422


def test_shared_without_vd_hash_rejected():
    response = client.post(
        "/api/run",
        json={
            "client_id": "a",
            "client_secret": "b",
            "orgid": "o",
            "admin_token": "t",
            "source_disk_id": "s@x.ru",
            "destination_disk_id": "d@x.ru",
            "destination_type": "shared",
        },
    )
    assert response.status_code == 400
    assert "destination_vd_hash" in response.json()["detail"]


def test_batch_pairs_reject_duplicates():
    pairs = [
        TransferPair(source_disk_id="a@x.ru", destination_disk_id="b@x.ru"),
        TransferPair(source_disk_id="A@x.ru", destination_disk_id="B@x.ru"),
    ]
    with pytest.raises(Exception, match="дубликат"):
        _validate_batch_pairs(pairs)


def test_config_exposes_target_rps():
    cfg = client.get("/api/config").json()

    assert cfg["target_rps"] > 0
    assert cfg["target_rps"] <= 40, "не должны по умолчанию упираться в потолок API"


def test_rate_limit_note_mentions_pace_and_ceiling():
    note = _rate_limit_note()

    assert "RATE LIMIT" in note
    assert "40" in note, "потолок должен быть виден в логе"


def test_run_job_logs_rate_limit_first(monkeypatch):
    monkeypatch.setattr(
        "app.main.DiskCopier", StubCopier(result={"saved": [], "fails": []})
    )
    job = Job(id="j5")

    _run_job(job, make_cfg())

    assert "RATE LIMIT" in drain(job)[0]


def test_env_float_falls_back_on_garbage():
    """DISK_API_RPS=abc не должен ронять импорт приложения."""
    os.environ["DISK_API_RPS_TEST"] = "abc"
    try:
        assert _env_float("DISK_API_RPS_TEST", 20.0) == 20.0
    finally:
        del os.environ["DISK_API_RPS_TEST"]


def test_env_float_rejects_nonpositive():
    for bad in ("0", "-5"):
        os.environ["DISK_API_RPS_TEST"] = bad
        try:
            assert _env_float("DISK_API_RPS_TEST", 20.0) == 20.0
        finally:
            del os.environ["DISK_API_RPS_TEST"]


def test_env_float_reads_valid_value():
    os.environ["DISK_API_RPS_TEST"] = "7.5"
    try:
        assert _env_float("DISK_API_RPS_TEST", 20.0) == 7.5
    finally:
        del os.environ["DISK_API_RPS_TEST"]


def test_batch_shared_uses_common_destination():
    pairs = [
        TransferPair(source_disk_id="a@x.ru"),
        TransferPair(source_disk_id="b@x.ru"),
    ]

    cleaned = _validate_batch_pairs(
        pairs, dest_type="shared", common_destination="writer@x.ru"
    )

    assert [p.destination_disk_id for p in cleaned] == ["writer@x.ru", "writer@x.ru"]
