"""Integration regressions for durable list controls; every Disk is fake."""

import csv
import io
import time
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.migration.api import router as onprem_router
from app.migration.errors import MigrationError
from app.migration.worker import MigrationService
from app.run_journal import RunJournal
from app.source_batches.api import router as batch_router
from app.source_batches.service import BatchService
from app.source_slots import SourceSlots
from tests.test_source_batches import BODY, CONFIG, CREDENTIALS, CSV, MD5, FakeSource, create, finish_plan

SHARED_CSV = "type,email,vd_hash,path\nshared,reader@company.ru,HASH,/\n"
ONPREM_CONFIG = dict(CONFIG, destination_url="https://disk.example.org/v1/disk",
                     destination_identity="receiver", destination_vd_hash="target",
                     identity_confirmed=True, strategy="staged")
ONPREM_CREDS = dict(CREDENTIALS, destination_token="memory-target-secret")


class FakeDestination:
    def __init__(self):
        self.nodes = {}
        self.uploads = 0

    def info(self):
        return {"total_space": 1024**3, "used_space": 0,
                "permissions": ["read", "write"], "user": {"login": "receiver"}}

    def check_available(self):
        assert self.stat("disk:/")["type"] == "dir"
        return self.info()

    def stat(self, path):
        return {"path": path, "type": "dir"} if path == "disk:/" else self.nodes.get(path)

    def ensure_dir(self, path):
        self.nodes[path] = {"path": path, "type": "dir"}

    def upload(self, path, local_path, *, before_upload=None, on_progress=None, overwrite=False):
        if before_upload:
            before_upload(None)
        assert Path(local_path).read_bytes() == BODY
        self.uploads += 1
        self.nodes[path] = {"path": path, "type": "file", "size": len(BODY), "md5": MD5}
        return None

    def close(self):
        pass


@pytest.fixture
def paired(tmp_path):
    root, downloads, slots = tmp_path / "data", tmp_path / "downloads", SourceSlots()
    downloads.mkdir()
    journal = RunJournal(root)
    onprem = MigrationService(root, start_worker=False, slots=slots)
    destination = FakeDestination()
    onprem.runner.destination_factory = lambda config, credentials: destination
    onprem.runner.source_factory = FakeSource
    onprem.runner.reserve_bytes = 0
    batches = BatchService(root, journal, onprem, downloads, slots,
                           source_factory=FakeSource, start_worker=False)
    yield batches, onprem, destination
    batches.close()
    onprem.close()
    journal.close()


def ready_onprem(batches):
    detail = batches.create("onprem", SHARED_CSV, ONPREM_CONFIG, ONPREM_CREDS,
                            "onprem-review-" + str(time.monotonic_ns()))
    return finish_plan(batches, detail)


def wait_for(predicate, timeout=2):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(0.01)
    assert predicate()


def test_reattachment_keeps_child_credentials_and_freezes_restart(paired):
    batches, onprem, _ = paired
    detail = ready_onprem(batches)
    batch_id, child_id = detail["batch"]["id"], detail["sources"][0]["child_job_id"]
    batches.start_transfer(batch_id)
    onprem.store.recover()
    batches.store.recover()
    batches._credentials.clear()
    onprem.registry.clear()
    restored = batches.attach_credentials(batch_id, ONPREM_CREDS)
    assert restored["batch"]["state"] == "paused"
    assert onprem.store.get(child_id)["state"] == "paused"
    assert onprem.registry.get(child_id) == ONPREM_CREDS
    assert restored["sources"][0]["plan_complete"]
    assert not batches.slots.active_owners
    assert batches.resume(batch_id)["batch"]["state"] == "running"


def test_replan_replaces_child_without_erasing_previous_journal(paired):
    batches, onprem, _ = paired
    detail = ready_onprem(batches)
    source_id, previous = detail["sources"][0]["id"], detail["sources"][0]["child_job_id"]
    onprem.store.event(previous, "warning", {"error": "source_unavailable", "http_status": 500})
    batches.plan(detail["batch"]["id"], source_id)
    batches._plan_one(source_id)
    batches._finish_planning(detail["batch"]["id"])
    current = batches.store.source(source_id)["child_job_id"]
    assert current != previous
    assert onprem.store.get(previous)["state"] == "stopped"
    assert any(e["payload"].get("http_status") == 500 for e in onprem.store.events(previous))
    assert {previous, current} <= onprem.excluded_job_ids
    assert batches.store.get(detail["batch"]["id"])["state"] == "ready"


def test_combined_csv_keeps_onprem_http_and_retry_counters(paired):
    batches, onprem, _ = paired
    detail = ready_onprem(batches)
    child_id = detail["sources"][0]["child_job_id"]
    onprem.store.event(child_id, "transfer_stage", {
        "stage": "download_retry", "status": "waiting", "error": "source_unavailable",
        "http_status": 503, "api_error": "DiskOperationFailedError", "error_side": "saas",
        "download_attempts": 2, "max_download_attempts": 5, "retry_delay_ms": 4000,
    })
    with batches.export(detail["batch"]["id"], journal=True) as stream:
        rows = list(csv.DictReader(io.StringIO(stream.read().decode("utf-8-sig"))))
    event = next(row for row in rows if row["stage"] == "download_retry")
    assert event["http_status"] == "503" and event["api_error"] == "DiskOperationFailedError"
    assert event.get("download_attempts", event.get("attempt")) == "2"
    assert event.get("max_download_attempts", event.get("max_attempts")) == "5"
    assert event["retry_delay_ms"] == "4000"
    assert event["source_id"] == detail["sources"][0]["id"]
    assert "memory-target-secret" not in str(rows)


def test_selected_child_start_is_not_available_outside_batch(paired, monkeypatch):
    batches, onprem, _ = paired
    detail = ready_onprem(batches)
    app = FastAPI()
    app.state.source_batches, app.state.onprem = batches, onprem
    app.include_router(batch_router)
    app.include_router(onprem_router)
    monkeypatch.setenv("ONPREM_ENABLED", "1")
    monkeypatch.setenv("ONPREM_ALLOW_INSECURE_HTTP", "1")
    monkeypatch.delenv("ONPREM_ADMIN_TOKEN", raising=False)
    with TestClient(app, base_url="http://localhost") as client:
        child_id = detail["sources"][0]["child_job_id"]
        response = client.post("/api/onprem/jobs/" + child_id + "/start")
        assert response.status_code == 409
        assert batches.store.get(detail["batch"]["id"])["state"] == "ready"
        monkeypatch.setenv("ONPREM_ENABLED", "0")
        assert client.get("/api/source-batches/" + detail["batch"]["id"]).status_code == 404


def test_explicit_replan_can_check_an_excluded_source(paired):
    batches, _, _ = paired
    detail = batches.create("local", CSV + "personal,two@company.ru,,/\n", CONFIG,
                            CREDENTIALS, "deselected-review")
    sources, batch_id = detail["sources"], detail["batch"]["id"]
    batches.selection(batch_id, [sources[0]["id"]])
    batches.plan(batch_id, sources[1]["id"])
    batches.start()
    wait_for(lambda: batches.store.source(sources[1]["id"])["plan_complete"])


def test_reselecting_unplanned_source_invalidates_ready_state(paired):
    batches, _, _ = paired
    detail = batches.create("local", CSV + "personal,two@company.ru,,/\n", CONFIG,
                            CREDENTIALS, "unplanned-review")
    sources, batch_id = detail["sources"], detail["batch"]["id"]
    batches.selection(batch_id, [sources[0]["id"]])
    batches.plan(batch_id, sources[0]["id"])
    batches._plan_one(sources[0]["id"])
    batches._finish_planning(batch_id)
    assert batches.store.get(batch_id)["state"] == "ready"
    result = batches.selection(batch_id, [s["id"] for s in sources])
    assert result["batch"]["state"] != "ready"
    assert not result["sources"][1]["plan_complete"]


def test_resuming_destination_outage_does_not_repeat_historical_failure(paired):
    batches, onprem, _ = paired
    detail = ready_onprem(batches)
    batch_id, source_id = detail["batch"]["id"], detail["sources"][0]["id"]
    child_id = detail["sources"][0]["child_job_id"]
    batches.start_transfer(batch_id)
    file = next(e for e in onprem.store.entries(child_id) if e["kind"] == "file")
    onprem.store.update_entry(file["id"], state="failed", error="destination_unavailable")
    onprem.store.update_job(child_id, state="needs_attention", error="destination_unavailable")
    batches.store.update(batch_id, state="paused", error="destination_unavailable", resume_state="running")
    batches.store.update_source(source_id, state="paused", error="destination_unavailable")
    batches.resume(batch_id)
    batches._transfer_one(source_id)
    assert batches.store.get(batch_id)["state"] != "paused"
    assert batches.store.source(source_id)["state"] == "completed_with_issues"
    assert onprem.store.entry(file["id"])["error"] == "destination_unavailable"


def test_activating_child_retry_restores_siblings_or_keeps_parent_resumable(paired):
    batches, onprem, _ = paired
    detail = batches.create("onprem", SHARED_CSV + "shared,reader@company.ru,HASH2,/\n",
                            ONPREM_CONFIG, ONPREM_CREDS, "point-siblings-review")
    detail = finish_plan(batches, detail)
    batch_id, sources = detail["batch"]["id"], detail["sources"]
    batches.start_transfer(batch_id)
    batches.store.update(batch_id, state="paused", error="destination_unavailable", resume_state="running")
    for source in sources:
        batches.store.update_source(source["id"], state="paused", error="destination_unavailable")
        onprem.store.update_job(source["child_job_id"], state="paused", pause_requested=True)
    batches.activate_child_retry(sources[0]["child_job_id"])
    batch = batches.store.get(batch_id)
    sibling = batches.store.source(sources[1]["id"])
    assert batch["state"] == "paused" or sibling["state"] != "paused"


def test_manual_parent_pause_rejects_hidden_child_retry(paired):
    batches, _, _ = paired
    detail = ready_onprem(batches)
    batches.start_transfer(detail["batch"]["id"])
    batches.pause(detail["batch"]["id"])
    with pytest.raises(ValueError, match="managed_batch_paused"):
        batches.require_child_active(detail["sources"][0]["child_job_id"])


def test_child_point_retry_waits_for_same_four_source_lanes(paired, monkeypatch):
    batches, onprem, destination = paired
    detail = ready_onprem(batches)
    batch_id, source_id = detail["batch"]["id"], detail["sources"][0]["id"]
    child_id = detail["sources"][0]["child_job_id"]
    batches.start_transfer(batch_id)
    for entry in onprem.store.entries(child_id):
        onprem.store.update_entry(entry["id"], state="failed" if entry["kind"] == "file" else "done",
                                  error="source_unavailable" if entry["kind"] == "file" else None)
    onprem.store.update_job(child_id, state="completed_with_issues")
    batches.store.update(batch_id, state="completed_with_issues")
    batches.store.update_source(source_id, state="completed_with_issues")
    file = next(e for e in onprem.store.entries(child_id) if e["kind"] == "file")
    slots = batches.slots
    assert slots.try_acquire("local:existing") and slots.try_acquire("onprem:existing")
    assert slots.try_acquire("local:third") and slots.try_acquire("batch:fourth")
    app = FastAPI()
    app.state.source_batches, app.state.onprem = batches, onprem
    app.include_router(onprem_router)
    monkeypatch.setenv("ONPREM_ENABLED", "1")
    monkeypatch.setenv("ONPREM_ALLOW_INSECURE_HTTP", "1")
    monkeypatch.delenv("ONPREM_ADMIN_TOKEN", raising=False)
    with TestClient(app, base_url="http://localhost") as client:
        response = client.post("/api/onprem/jobs/" + child_id + "/entries/" + file["id"] + "/retry",
                               json={"revision": onprem.store.get(child_id)["revision"]})
        assert response.status_code == 200
    batches.start()
    time.sleep(0.1)
    assert destination.uploads == 0
    assert len(slots.active_owners) == 4
    slots.release("local:existing")
    wait_for(lambda: destination.uploads == 1)
    wait_for(lambda: batches.store.get(batch_id)["state"] == "completed")
    assert slots.active_owners == {"onprem:existing", "local:third", "batch:fourth"}
    slots.release("onprem:existing")
    slots.release("local:third")
    slots.release("batch:fourth")


def test_outage_point_retry_finishes_remaining_selected_sources(paired, monkeypatch):
    batches, onprem, destination = paired
    detail = batches.create("onprem", SHARED_CSV + "shared,reader@company.ru,HASH2,/\n",
                            ONPREM_CONFIG, ONPREM_CREDS, "point-recovery-complete")
    detail = finish_plan(batches, detail)
    batch_id, sources = detail["batch"]["id"], detail["sources"]
    batches.start_transfer(batch_id)
    target_id = sources[0]["child_job_id"]
    file = next(e for e in onprem.store.entries(target_id) if e["kind"] == "file")
    onprem.store.update_entry(file["id"], state="failed", error="destination_unavailable")
    batches.store.update(batch_id, state="paused", error="destination_unavailable", resume_state="running")
    for source in sources:
        batches.store.update_source(source["id"], state="paused", error="destination_unavailable")
        onprem.store.update_job(source["child_job_id"], state="paused", pause_requested=True)
    app = FastAPI()
    app.state.source_batches, app.state.onprem = batches, onprem
    app.include_router(onprem_router)
    monkeypatch.setenv("ONPREM_ENABLED", "1")
    monkeypatch.setenv("ONPREM_ALLOW_INSECURE_HTTP", "1")
    monkeypatch.delenv("ONPREM_ADMIN_TOKEN", raising=False)
    with TestClient(app, base_url="http://localhost") as client:
        response = client.post("/api/onprem/jobs/" + target_id + "/entries/" + file["id"] + "/retry",
                               json={"revision": onprem.store.get(target_id)["revision"]})
        assert response.status_code == 200
    batches.start()
    wait_for(lambda: batches.store.get(batch_id)["state"] == "completed")
    assert destination.uploads == 2
    assert all(s["state"] == "completed" for s in batches.store.sources(batch_id))
    assert not batches.slots.active_owners


@pytest.mark.parametrize("action,state", [("stop", "stopped"), ("pause", "paused")])
def test_interrupt_while_describing_source_cannot_create_live_orphan_child(paired, action, state):
    import threading
    batches, onprem, _ = paired
    entered, release = threading.Event(), threading.Event()

    class SlowDescription(FakeSource):
        def describe(self):
            entered.set()
            assert release.wait(2)
            return super().describe()

    batches.source_factory = SlowDescription
    detail = batches.create("onprem", SHARED_CSV, ONPREM_CONFIG, ONPREM_CREDS,
                            "stop-description-review")
    batch_id = detail["batch"]["id"]
    batches.plan(batch_id)
    batches.start()
    assert entered.wait(2)
    assert batches.store.sources(batch_id)[0]["child_job_id"] is None
    getattr(batches, action)(batch_id)
    release.set()
    wait_for(lambda: not batches._active)
    assert batches.store.get(batch_id)["state"] == state
    assert all(job["state"] == "stopped" for job in onprem.store.jobs())
    assert all(not onprem.registry.get(job["id"]) for job in onprem.store.jobs())
    assert not onprem.store.cache_in_use()


def test_pause_during_inventory_releases_child_and_resumes_with_new_plan(paired):
    import threading
    batches, onprem, _ = paired
    entered, release = threading.Event(), threading.Event()

    class SlowInventory(FakeSource):
        def walk(self, path):
            entered.set()
            assert release.wait(2)
            yield from super().walk(path)

    batches.source_factory = SlowInventory
    detail = batches.create("onprem", SHARED_CSV, ONPREM_CONFIG, ONPREM_CREDS,
                            "pause-inventory-review")
    batch_id = detail["batch"]["id"]
    batches.plan(batch_id)
    batches.start()
    assert entered.wait(2)
    previous_child = batches.store.sources(batch_id)[0]["child_job_id"]
    assert previous_child and onprem.store.get(previous_child)["state"] == "planning"
    batches.pause(batch_id)
    release.set()
    wait_for(lambda: not batches._active)
    assert batches.store.get(batch_id)["state"] == "paused"
    assert batches.store.get(batch_id)["resume_state"] == "planning"
    assert onprem.store.get(previous_child)["state"] == "stopped"
    assert not onprem.registry.get(previous_child)
    assert not onprem.store.cache_in_use()
    assert batches.resume(batch_id)["batch"]["state"] == "planning"
    wait_for(lambda: batches.store.get(batch_id)["state"] == "ready")
    current = batches.store.sources(batch_id)[0]
    assert current["plan_complete"] and current["child_job_id"] != previous_child
    assert onprem.store.get(previous_child)["state"] == "stopped"
    assert {previous_child, current["child_job_id"]} <= onprem.excluded_job_ids


@pytest.fixture
def history_client(paired, monkeypatch):
    from app import main

    batches, _, _ = paired
    monkeypatch.setattr(main.app.state, "run_journal", batches.journal, raising=False)
    monkeypatch.setattr(main.app.state, "journal_error", None, raising=False)
    monkeypatch.setattr(main.app.state, "source_batches", batches, raising=False)
    monkeypatch.setattr(main, "JOBS", {})
    # No lifespan: the fixture owns both isolated services and their storage.
    client = TestClient(main.app, base_url="http://localhost")
    yield client
    client.close()


def seed_history(journal, job_id, mode, *, active=False):
    journal.create(job_id, mode, "single", "source@company.ru", "destination@company.ru")
    journal.append(job_id, {"stage": "download", "status": "failed", "http_status": 503})
    if not active:
        journal.finish(job_id, "error", error="source_unavailable")


def test_local_history_cleanup_api_clears_both_stores_and_keeps_plans(paired, history_client):
    batches, onprem, _ = paired
    local = finish_plan(batches, create(batches))
    remote = ready_onprem(batches)
    local_id, remote_id = local["batch"]["id"], remote["batch"]["id"]
    source_id = local["sources"][0]["id"]
    journal_id = local["sources"][0]["journal_id"]
    before_plan = batches.entries(source_id)
    before_remote = batches.store.events(remote_id)
    child_id = remote["sources"][0]["child_job_id"]
    before_child = onprem.store.events(child_id)
    seed_history(batches.journal, "old-local", "local")
    seed_history(batches.journal, "old-single", "single")
    seed_history(batches.journal, "active-cross", "cross", active=True)
    batch_events = len(batches.store.events(local_id))
    legacy_events = sum(len(batches.journal.events(job_id)["events"])
                        for job_id in (journal_id, "old-local"))

    response = history_client.post("/api/history/cleanup", json={"mode": "local"})

    assert response.status_code == 200
    assert response.json()["removed_jobs"] == 2
    assert response.json()["removed_events"] == batch_events + legacy_events
    assert batches.journal.get(journal_id) is None
    assert batches.journal.get("old-local") is None
    assert batches.store.events(local_id) == []
    assert batches.store.source(source_id)["journal_id"] is None
    assert batches.entries(source_id) == before_plan
    assert batches.store.get(local_id)["state"] == "ready"
    assert batches.store.source(source_id)["plan_complete"]
    assert batches.store.events(remote_id) == before_remote
    assert onprem.store.events(child_id) == before_child
    assert batches.journal.get("old-single")["status"] == "error"
    assert batches.journal.get("active-cross")["status"] == "running"
    with batches.export(local_id, journal=True) as stream:
        assert list(csv.DictReader(io.StringIO(stream.read().decode("utf-8-sig")))) == []

    # Clearing completed journals keeps a ready list usable and opens fresh history.
    batches.start_transfer(local_id)
    renewed = batches.store.source(source_id)["journal_id"]
    assert renewed and renewed != journal_id
    assert batches.journal.get(renewed)["status"] == "running"


@pytest.mark.parametrize("state,inflight", [
    ("planning", False), ("running", False), ("paused", True),
])
def test_local_history_cleanup_api_rejects_active_batch_without_partial_deletion(
    paired, history_client, state, inflight,
):
    batches, _, _ = paired
    detail = finish_plan(batches, create(batches))
    batch_id, source_id = detail["batch"]["id"], detail["sources"][0]["id"]
    journal_id = detail["sources"][0]["journal_id"]
    batches.store.update(batch_id, state=state)
    seed_history(batches.journal, "completed-local", "local")
    before_events = batches.store.events(batch_id)
    before_journal = batches.journal.events(journal_id)
    if inflight:
        batches._active[source_id] = object()
    try:
        response = history_client.post("/api/history/cleanup", json={"mode": "local"})
    finally:
        batches._active.pop(source_id, None)
    assert response.status_code == 409 and response.json() == {"detail": "journal_in_use"}
    assert batches.store.events(batch_id) == before_events
    assert batches.journal.events(journal_id) == before_journal
    assert batches.journal.get("completed-local")
    assert batches.store.source(source_id)["journal_id"] == journal_id


@pytest.mark.parametrize("mode", ["single", "cross"])
def test_other_mode_history_cleanup_api_keeps_local_list_journals(paired, history_client, mode):
    batches, _, _ = paired
    detail = finish_plan(batches, create(batches))
    batch_id = detail["batch"]["id"]
    journal_id = detail["sources"][0]["journal_id"]
    before_events = batches.store.events(batch_id)
    before_journal = batches.journal.events(journal_id)
    seed_history(batches.journal, "finished-selected-mode", mode)
    response = history_client.post("/api/history/cleanup", json={"mode": mode})
    assert response.status_code == 200 and response.json()["removed_jobs"] == 1
    assert batches.journal.get("finished-selected-mode") is None
    assert batches.store.events(batch_id) == before_events
    assert batches.journal.events(journal_id) == before_journal
    assert batches.store.source(detail["sources"][0]["id"])["journal_id"] == journal_id
