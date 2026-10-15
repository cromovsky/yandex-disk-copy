"""Retry settings are per run, strict booleans, journalled and live."""

import csv
import io
import json
import threading
from dataclasses import replace

import pytest
from fastapi.testclient import TestClient

from app import main
from app.disk_copy import DiskCopier
from app.run_journal import RunJournal
from tests.test_main import make_cfg


@pytest.fixture
def api(tmp_path, monkeypatch):
    store = RunJournal(tmp_path)
    monkeypatch.setattr(main.app.state, "run_journal", store, raising=False)
    monkeypatch.setattr(main, "JOBS", {})
    yield TestClient(main.app, base_url="http://localhost"), store
    store.close()


@pytest.mark.parametrize("model", [main.RunRequest, main.RunCrossRequest,
    main.RunBatchRequest, main.RunBatchCrossRequest, main.LocalSaveRequest,
    main.LocalSaveCrossRequest])
def test_all_request_shapes_default_to_enabled_and_reject_coerced_values(model):
    from pydantic import ValidationError
    payload = {"client_id": "ci", "client_secret": "cs", "orgid": "org", "admin_token": "admin",
               "src_client_id": "ci", "src_client_secret": "cs", "src_orgid": "org",
               "src_admin_token": "admin", "dst_client_id": "ci", "dst_client_secret": "cs",
               "source_disk_id": "src@example.org", "destination_disk_id": "dst@example.org",
               "pairs": [{"source_disk_id": "src@example.org", "destination_disk_id": "dst@example.org"}]}
    assert model.model_validate(payload).retry_enabled is True
    assert model.model_validate(dict(payload, retry_enabled=False)).retry_enabled is False
    for value in ("false", 0, 1, None):
        with pytest.raises(ValidationError):
            model.model_validate(dict(payload, retry_enabled=value))


def test_live_toggle_changes_running_job_only_and_is_saved_to_csv(api):
    client, store = api
    for ident in ("first", "second"):
        store.create(ident, "local", "single", "source@example.org", "local")
        main.JOBS[ident] = main.Job(id=ident, mode="local", journal=store)
    response = client.post("/api/run/first/retries", json={"enabled": False})
    assert response.status_code == 200
    assert response.json() == {"retry_enabled": False}
    assert client.get("/api/run/first").json()["retry_enabled"] is False
    assert client.get("/api/run/second").json()["retry_enabled"] is True
    events = store.events("first")["events"]
    assert events[-1]["stage"] == "retry_settings"
    assert events[-1]["retry_enabled"] is False
    csv_export = client.get("/api/run/first/journal")
    rows = list(csv.DictReader(io.StringIO(csv_export.text.lstrip("\ufeff"))))
    assert rows[-1]["retry_enabled"] == "False"
    main.JOBS.pop("first")
    assert client.get("/api/run/first").json()["retry_enabled"] is False


def test_live_toggle_rejects_invalid_unknown_terminal_and_cross_site_changes(api):
    client, store = api
    store.create("done", "local", "single", "src", "local")
    main.JOBS["done"] = main.Job(id="done", status="done", journal=store)
    assert client.post("/api/run/missing/retries", json={"enabled": False}).status_code == 404
    assert client.post("/api/run/done/retries", json={"enabled": False}).status_code == 409
    for invalid in ({}, {"enabled": "false"}, {"enabled": 0}, {"enabled": None}):
        assert client.post("/api/run/done/retries", json=invalid).status_code == 422
    assert client.post("/api/run/done/retries", json={"enabled": False},
                       headers={"Origin": "https://evil.example"}).status_code == 403


def test_all_pairs_share_live_policy_without_leaking_to_another_run(api, monkeypatch):
    client, store = api
    cfgs = [replace(make_cfg(), retry_enabled=False) for _ in range(2)]
    job = main.Job(id="batch", kind="batch")
    main._create_job(job, cfgs)
    first = DiskCopier(cfgs[0])
    second = DiskCopier(cfgs[1])
    outsider = DiskCopier(make_cfg())
    try:
        assert first.retry_policy.enabled is second.retry_policy.enabled is False
        assert outsider.retry_policy.enabled is True
        assert client.post("/api/run/batch/retries", json={"enabled": True}).status_code == 200
        assert first.retry_policy.enabled is second.retry_policy.enabled is True
        events = store.events(job.id)["events"]
        assert [e["retry_enabled"] for e in events if e.get("stage") == "retry_settings"] == [False, True]
    finally:
        first.close()
        second.close()
        outsider.close()


def test_late_cancelled_retry_cannot_overwrite_latest_saved_setting(api):
    _client, store = api
    store.create("history", "local", "single", "source", "local")
    for value in (False, True):
        store.append("history", {"stage": "retry_settings", "retry_enabled": value})
    store.append("history", {"stage": "download", "event_type": "retry",
                             "status": "skipped", "retry_enabled": False,
                             "error": "retry_disabled"})
    assert store.get("history")["retry_enabled"] is True
    assert store.history("local")["jobs"][0]["retry_enabled"] is True
