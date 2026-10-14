import hashlib
import threading
import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.migration.errors import MigrationError
from app.run_journal import RunJournal
from app.source_batches.api import router
from app.source_batches.service import BatchService
from app.source_batches.store import parse_sources
from app.source_slots import SourceSlots

CSV = "type,email,vd_hash,path\npersonal,user@company.ru,,/\n"
CONFIG = {"source_org_id": "org", "source_client_id": "client", "page_limit": 1000}
CREDENTIALS = {"source_client_secret": "memory-secret-value"}
BODY = b"hello"
MD5 = hashlib.md5(BODY).hexdigest()


class FakeSource:
    nodes = [{"path": "disk:/", "type": "dir"},
             {"path": "disk:/folder", "type": "dir"},
             {"path": "disk:/folder/file.txt", "type": "file", "size": 5, "md5": MD5,
              "modified": "2026-10-14T10:00:00Z", "resource_id": "file-one"}]
    publications = 0
    downloads = 0
    failure = None

    def __init__(self, config, credentials):
        self.config = config

    def describe(self):
        return {"name": "Общий диск"}

    def stat(self, path):
        if self.failure:
            raise self.failure
        return next(dict(n) for n in self.nodes if n["path"] == path)

    def walk(self, path):
        for node in self.nodes:
            if path == "disk:/" or node["path"] == path or node["path"].startswith(path + "/"):
                yield dict(node)

    def chunks(self, path):
        type(self).downloads += 1
        yield BODY

    def close(self):
        pass


@pytest.fixture
def setup_service(tmp_path):
    local = tmp_path / "downloads"
    local.mkdir()
    journal = RunJournal(tmp_path / "data")
    service = BatchService(tmp_path / "data", journal, None, local, SourceSlots(),
                           source_factory=FakeSource, start_worker=False)
    yield service, local
    service.close()
    journal.close()


def create(service, csv=CSV):
    return service.create("local", csv, CONFIG, CREDENTIALS, "batch-key-" + str(time.monotonic_ns()))


def finish_plan(service, detail):
    batch_id = detail["batch"]["id"]
    service.plan(batch_id)
    for source in service.store.sources(batch_id):
        service._plan_one(source["id"])
    service._finish_planning(batch_id)
    return service.detail(batch_id)


def wait_for(service, batch_id, state, timeout=3):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        detail = service.detail(batch_id)
        if detail["batch"]["state"] == state:
            return detail
        time.sleep(.01)
    raise AssertionError(service.detail(batch_id))


@pytest.mark.parametrize("csv,code", [
    ("email,path\nuser@company.ru,/\n", "invalid_sources_columns"),
    ("type,email,vd_hash,path\n", "empty_source_list"),
    (CSV + "personal,USER@company.ru,,/folder\n", "duplicate_source"),
    (CSV.replace("user@company.ru", "disk-id"), "invalid_sources_row"),
    (CSV.replace("personal,user@company.ru,,", "shared,user@company.ru,,"), "invalid_sources_row"),
    (CSV.replace("personal,user@company.ru,,", "personal,user@company.ru,hash,"), "invalid_sources_row"),
    (CSV.replace(",/\n", ",/../escape\n"), "invalid_disk_path"),
])
def test_csv_rejects_invalid_and_overlapping_sources(csv, code):
    with pytest.raises(ValueError, match=code):
        parse_sources(csv)


def test_shared_duplicate_independent_of_reader():
    with pytest.raises(ValueError, match="duplicate_source"):
        parse_sources("type,email,vd_hash,path\nshared,a@company.ru,hash,/\nshared,b@company.ru,hash,/\n")


def test_csv_allows_nonoverlapping_waves():
    sources = parse_sources("type,email,vd_hash,path\npersonal,a@company.ru,,/one\npersonal,a@company.ru,,/two\n")
    assert len(sources) == 2


def test_manifest_complete_and_readonly(setup_service):
    service, local = setup_service
    FakeSource.downloads = 0
    detail = finish_plan(service, create(service))
    assert detail["batch"]["state"] == "ready"
    assert detail["batch"]["summary"]["required_bytes"] == 5
    assert detail["sources"][0]["plan_complete"] is True
    assert list(local.iterdir()) == []
    assert FakeSource.downloads == 0


def test_start_requires_full_plan(setup_service):
    service, _ = setup_service
    detail = create(service)
    with pytest.raises(ValueError, match="dry_run_required"):
        service.start_transfer(detail["batch"]["id"])


def test_existing_same_size_different_hash_is_conflict(setup_service):
    service, local = setup_service
    target = local / "user@company.ru" / "folder"
    target.mkdir(parents=True)
    (target / "file.txt").write_bytes(b"world")
    detail = finish_plan(service, create(service))
    assert detail["batch"]["state"] == "needs_attention"
    entries = service.entries(detail["sources"][0]["id"])["entries"]
    assert next(e for e in entries if e["kind"] == "file")["state"] == "conflict"
    assert (target / "file.txt").read_bytes() == b"world"


def test_existing_md5_match_subtracts_space(setup_service):
    service, local = setup_service
    target = local / "user@company.ru" / "folder"
    target.mkdir(parents=True)
    (target / "file.txt").write_bytes(BODY)
    detail = finish_plan(service, create(service))
    assert detail["batch"]["state"] == "ready"
    assert detail["batch"]["summary"]["required_bytes"] == 0
    assert detail["batch"]["summary"]["verified_bytes"] == 5


def test_missing_hash_blocks_complete_source(setup_service, monkeypatch):
    service, _ = setup_service
    nodes = [{k: v for k, v in n.items() if k != "md5"} for n in FakeSource.nodes]
    monkeypatch.setattr(FakeSource, "nodes", nodes)
    detail = finish_plan(service, create(service))
    assert detail["sources"][0]["plan_complete"]
    assert detail["sources"][0]["state"] == "error"
    assert detail["batch"]["state"] == "needs_attention"


def test_source_failure_isolated_and_can_be_excluded(setup_service):
    service, _ = setup_service

    class OneFails(FakeSource):
        def stat(self, path):
            if self.config["source_email"] == "bad@company.ru":
                raise MigrationError("source_unavailable", http_status=500)
            return super().stat(path)

    service.source_factory = OneFails
    detail = finish_plan(service, create(service, CSV + "personal,bad@company.ru,,/\n"))
    good, bad = detail["sources"]
    assert good["state"] == "ready"
    assert bad["error"] == "source_unavailable"
    result = service.selection(detail["batch"]["id"], [good["id"]])
    assert result["batch"]["state"] == "ready"
    assert result["batch"]["summary"]["selected_sources"] == 1
    events = service.store.events(detail["batch"]["id"])
    assert any(e.get("http_status") == 500 and e.get("error_side") == "source" for e in events)


def test_duplicate_target_folder_blocks_entire_group(setup_service):
    service, _ = setup_service
    detail = finish_plan(service, create(service, "type,email,vd_hash,path\npersonal,a@company.ru,,/folder\npersonal,a@company.ru,,/second\n"))
    assert detail["batch"]["state"] == "needs_attention"
    assert detail["batch"]["error"] == "target_folder_collision"


def test_manifest_interruption_never_ready(setup_service):
    service, _ = setup_service
    detail = create(service)
    service.plan(detail["batch"]["id"])
    service.pause(detail["batch"]["id"])
    service._plan_one(detail["sources"][0]["id"])
    result = service.detail(detail["batch"]["id"])
    assert not result["sources"][0]["plan_complete"]
    with pytest.raises(ValueError, match="dry_run_required"):
        service.start_transfer(result["batch"]["id"])
    result = service.resume(result["batch"]["id"])
    assert result["batch"]["state"] == "planning"


def test_durable_manifest_and_memory_only_credentials(setup_service):
    service, _ = setup_service
    detail = finish_plan(service, create(service))
    source_id = detail["sources"][0]["id"]
    service.store.recover()
    service._credentials.clear()
    restored = service.detail(detail["batch"]["id"])
    assert restored["batch"]["state"] == "awaiting_credentials"
    assert not restored["batch"]["credentials_available"]
    assert restored["sources"][0]["plan_complete"]
    assert service.entries(source_id)["total"] == 2
    for filename in service.store.root.iterdir():
        if filename.is_file():
            assert CREDENTIALS["source_client_secret"].encode() not in filename.read_bytes()
    service.attach_credentials(detail["batch"]["id"], CREDENTIALS)
    assert service.detail(detail["batch"]["id"])["batch"]["state"] == "ready"


def test_paused_planning_restart_preserves_resume_intent(setup_service):
    service, _ = setup_service
    detail = create(service)
    batch_id = detail["batch"]["id"]
    service.plan(batch_id)
    service.pause(batch_id)
    service.store.recover()
    service.attach_credentials(batch_id, CREDENTIALS)
    assert service.resume(batch_id)["batch"]["state"] == "planning"


def test_local_execution_revalidates_and_copies_frozen_manifest(setup_service):
    service, local = setup_service
    detail = finish_plan(service, create(service))
    batch_id, source_id = detail["batch"]["id"], detail["sources"][0]["id"]
    service.start_transfer(batch_id)
    service._transfer_one(source_id)
    assert (local / "user@company.ru/folder/file.txt").read_bytes() == BODY
    assert service.detail(batch_id)["sources"][0]["state"] == "completed"
    assert not list(local.rglob("*.part"))


def test_source_changed_after_plan_is_skipped(setup_service, monkeypatch):
    service, local = setup_service
    detail = finish_plan(service, create(service))
    changed = [dict(n, modified="new") if n["type"] == "file" else n for n in FakeSource.nodes]
    monkeypatch.setattr(FakeSource, "nodes", changed)
    service.start_transfer(detail["batch"]["id"])
    service._transfer_one(detail["sources"][0]["id"])
    entries = service.entries(detail["sources"][0]["id"])["entries"]
    assert next(e for e in entries if e["kind"] == "file")["error"] == "source_changed"
    assert not (local / "user@company.ru/folder/file.txt").exists()


def test_five_download_attempts_logged_then_skipped(setup_service):
    service, local = setup_service

    class Broken(FakeSource):
        def chunks(self, path):
            raise MigrationError("source_download_failed", http_status=503,
                                 diagnostics={"api_error": "InternalServerError"})
            yield b""

    service.source_factory = Broken
    service._delay = lambda delay, checkpoint: checkpoint()
    detail = finish_plan(service, create(service))
    service.start_transfer(detail["batch"]["id"])
    service._transfer_one(detail["sources"][0]["id"])
    entries = service.entries(detail["sources"][0]["id"])["entries"]
    file = next(e for e in entries if e["kind"] == "file")
    assert file["attempts"] == 5 and file["state"] == "failed"
    events = service.store.events(detail["batch"]["id"])
    errors = [e for e in events if e.get("stage") == "downloading" and e.get("status") == "failed"]
    assert len(errors) == 5
    assert all(e["http_status"] == 503 and e["api_error"] == "InternalServerError" for e in errors)
    assert [e["retry_delay_ms"] for e in events if e.get("stage") == "download_retry"] == [2000, 5000, 10000, 20000]
    assert not list(local.rglob("*.part"))


def test_aggregate_local_space_checked(setup_service, monkeypatch):
    service, _ = setup_service
    monkeypatch.setattr("app.source_batches.service.shutil.disk_usage", lambda p: type("Usage", (), {"free": 9})())
    detail = finish_plan(service, create(service, CSV + "personal,another@company.ru,,/\n"))
    assert detail["batch"]["error"] == "local_space_insufficient"
    assert detail["batch"]["summary"]["required_bytes"] == 10


def test_four_lanes_and_stop_cancels_fifth_source(setup_service):
    service, _ = setup_service
    gate = threading.Event()
    entered = threading.Event()
    lock = threading.Lock()
    current = maximum = 0

    class Blocking(FakeSource):
        def stat(self, path):
            nonlocal current, maximum
            if path == "disk:/":
                with lock:
                    current += 1
                    maximum = max(maximum, current)
                    if current == 4:
                        entered.set()
                gate.wait(5)
                with lock:
                    current -= 1
            return super().stat(path)

    service.source_factory = Blocking
    detail = create(service, CSV + "personal,two@company.ru,,/\npersonal,three@company.ru,,/\n"
                    "shared,reader@company.ru,four,/\nshared,reader@company.ru,five,/\n")
    service.plan(detail["batch"]["id"])
    service.start()
    try:
        assert entered.wait(2)
        assert len(service.slots.active_owners) == 4
        assert service.detail(detail["batch"]["id"])["sources"][4]["state"] == "pending"
        service.stop(detail["batch"]["id"])
    finally:
        gate.set()
    end = time.monotonic() + 2
    while service.slots.active_owners and time.monotonic() < end:
        time.sleep(.01)
    assert maximum == 4
    assert not service.slots.active_owners
    assert all(s["state"] == "stopped" for s in service.detail(detail["batch"]["id"])["sources"])


def test_csv_export_contains_source_and_is_formula_safe(setup_service):
    service, _ = setup_service
    detail = finish_plan(service, create(service))
    with service.export(detail["batch"]["id"]) as file:
        text = file.read().decode("utf-8-sig")
    assert "source_id,type,email,vd_hash" in text
    assert "user@company.ru" in text
    assert MD5 in text


def test_api_redacts_secrets_and_blocks_foreign_origin(setup_service):
    service, _ = setup_service
    app = FastAPI()
    app.state.source_batches = service
    app.include_router(router)
    client = TestClient(app)
    payload = {"mode": "local", "sources_csv": CSV, "config": CONFIG,
               "credentials": CREDENTIALS, "idempotency_key": "api-batch-key"}
    result = client.post("/api/source-batches", json=payload)
    assert result.status_code == 200
    assert "memory-secret-value" not in result.text
    assert client.post("/api/source-batches", json=payload, headers={"Origin": "https://evil.example"}).status_code == 403
    bad = client.post("/api/source-batches", json={**payload, "credentials": {"source_client_secret": 123}})
    assert bad.status_code == 422 and bad.json() == {"detail": "invalid_request"}


def test_onprem_batch_api_disabled(setup_service, monkeypatch):
    service, _ = setup_service
    app = FastAPI()
    app.state.source_batches = service
    app.include_router(router)
    monkeypatch.setenv("ONPREM_ENABLED", "0")
    client = TestClient(app)
    assert client.get("/api/source-batches?mode=onprem").status_code == 404


def test_symlink_destination_refused(setup_service, tmp_path):
    service, local = setup_service
    other = tmp_path / "outside"
    other.mkdir()
    (local / "user@company.ru").symlink_to(other, target_is_directory=True)
    detail = finish_plan(service, create(service))
    assert detail["sources"][0]["error"] == "local_destination_unsafe"
    assert list(other.iterdir()) == []

ONPREM_CONFIG = {**CONFIG, "destination_url": "https://onprem.example/v1/disk",
                  "destination_identity": "admin@company.ru", "destination_type": "shared",
                  "destination_vd_hash": "target-disk", "destination_base": "disk:/",
                  "identity_confirmed": True, "strategy": "staged"}
ONPREM_CREDS = {**CREDENTIALS, "destination_token": "memory-destination-token"}


class FakeDestination:
    stats = {"disk:/": {"type": "dir", "path": "disk:/"}}

    def __init__(self, config, credentials):
        self.config = config

    def info(self):
        return {"user": {"email": "admin@company.ru"}, "total_space": 1000000,
                "used_space": 0, "permissions": ["read", "write"], "vd_hash": "target-disk"}

    def stat(self, path):
        return self.stats.get(path)

    def check_available(self):
        return self.info()

    def close(self):
        pass


@pytest.fixture
def onprem_service(tmp_path):
    from app.migration.worker import MigrationService
    root = tmp_path / "data"
    local = tmp_path / "downloads"
    local.mkdir()
    journal = RunJournal(root)
    onprem = MigrationService(root, start_worker=False)
    onprem.runner.source_factory = FakeSource
    onprem.runner.destination_factory = FakeDestination
    onprem.runner.reserve_bytes = 0
    service = BatchService(root, journal, onprem, local, SourceSlots(),
                           source_factory=FakeSource, start_worker=False)
    yield service, onprem
    service.close()
    onprem.close()
    journal.close()


def create_onprem(service, csv=CSV):
    return service.create("onprem", csv, ONPREM_CONFIG, ONPREM_CREDS,
                          "onprem-key-" + str(time.monotonic_ns()))


def test_onprem_dry_run_reserves_four_largest_cached_files(onprem_service):
    service, onprem = onprem_service
    sizes = {"one@company.ru": 100, "two@company.ru": 90, "three@company.ru": 80,
             "four@company.ru": 70, "five@company.ru": 60}

    class SizedSource(FakeSource):
        def walk(self, path):
            for node in super().walk(path):
                if node["type"] == "file":
                    node["size"] = sizes[self.config["source_email"]]
                yield node

    service.source_factory = SizedSource
    onprem.runner.staging_budget = 349
    (onprem.runner.staging / "retained.download").write_bytes(b"0123456789")
    csv = "type,email,vd_hash,path\n" + "".join(
        f"personal,{email},,/\n" for email in sizes
    )
    detail = finish_plan(service, create_onprem(service, csv))
    assert all(source["plan_complete"] and source["state"] == "ready"
               for source in detail["sources"])
    assert detail["batch"]["error"] == "local_cache_space_insufficient"
    onprem.runner.staging_budget = 350
    service._finish_planning(detail["batch"]["id"])
    assert service.detail(detail["batch"]["id"])["batch"]["state"] == "ready"


def test_onprem_dry_run_counts_free_space_reserve_for_four_downloads(onprem_service, monkeypatch):
    service, onprem = onprem_service
    onprem.runner.reserve_bytes = 10
    onprem.runner.staging_budget = 1000
    monkeypatch.setattr("app.source_batches.service.shutil.disk_usage",
                        lambda p: type("Usage", (), {"free": 29})())
    csv = CSV + "".join(f"personal,{name}@company.ru,,/\n" for name in ("two", "three", "four"))
    detail = finish_plan(service, create_onprem(service, csv))
    assert detail["batch"]["error"] == "local_cache_space_insufficient"


def test_mixed_personal_and_shared_sources_all_reserve_staged_cache(onprem_service):
    service, onprem = onprem_service

    class SizedSource(FakeSource):
        def walk(self, path):
            for node in super().walk(path):
                if node["type"] == "file":
                    shared_id = self.config.get("source_vd_hash")
                    node["size"] = {"sixty": 60, "forty": 40}.get(shared_id, 10000)
                yield node

    service.source_factory = SizedSource
    onprem.runner.staging_budget = 110
    (onprem.runner.staging / "retained.download").write_bytes(b"0123456789")
    csv = CSV + "shared,reader@company.ru,sixty,/\nshared,reader@company.ru,forty,/\n"
    detail = service.create("onprem", csv, dict(ONPREM_CONFIG, strategy="staged"),
                            ONPREM_CREDS, "mixed-direct-cache")
    detail = finish_plan(service, detail)
    assert detail["batch"]["error"] == "local_cache_space_insufficient"
    onprem.runner.staging_budget = 10109
    service._finish_planning(detail["batch"]["id"])
    assert service.detail(detail["batch"]["id"])["batch"]["error"] == "local_cache_space_insufficient"
    onprem.runner.staging_budget = 10110
    service._finish_planning(detail["batch"]["id"])
    assert service.detail(detail["batch"]["id"])["batch"]["state"] == "ready"
    assert detail["batch"]["summary"]["required_bytes"] == 10100
    child_strategies = [onprem.store.get(source["child_job_id"])["config"]["strategy"]
                        for source in detail["sources"]]
    assert child_strategies == ["staged", "staged", "staged"]



def test_onprem_full_plan_child_excluded_and_bound(onprem_service):
    service, onprem = onprem_service
    detail = finish_plan(service, create_onprem(service))
    assert detail["batch"]["state"] == "ready", detail
    child_id = detail["sources"][0]["child_job_id"]
    assert child_id in onprem.excluded_job_ids
    assert onprem.registry.get(child_id) == ONPREM_CREDS
    assert onprem.store.get(child_id)["state"] == "ready"
    assert detail["batch"]["summary"]["required_bytes"] == 5
    service.attach_credentials(detail["batch"]["id"], ONPREM_CREDS)
    assert onprem.registry.get(child_id) == ONPREM_CREDS


def test_shared_onprem_folder_fixed_and_staged(onprem_service):
    service, onprem = onprem_service
    csv = "type,email,vd_hash,path\nshared,reader@company.ru,source-disk,/\n"
    detail = finish_plan(service, create_onprem(service, csv))
    assert detail["batch"]["state"] == "ready", detail
    source = detail["sources"][0]
    assert source["folder"] == "Общий диск (source-disk)"
    child = onprem.store.get(source["child_job_id"])
    assert child["config"]["destination_root"] == "disk:/Общий диск (source-disk)"
    assert child["config"]["source_type"] == "shared"
    assert child["config"]["strategy"] == "staged"
    assert not child["config"].get("source_public")


def test_onprem_replan_preserves_old_history_stopped(onprem_service):
    service, onprem = onprem_service
    detail = finish_plan(service, create_onprem(service))
    old_id = detail["sources"][0]["child_job_id"]
    detail = finish_plan(service, detail)
    new_id = detail["sources"][0]["child_job_id"]
    assert new_id != old_id
    assert old_id in onprem.excluded_job_ids and new_id in onprem.excluded_job_ids
    assert onprem.store.get(old_id)["state"] == "stopped"
    assert len(onprem.store.entries(old_id)) == 2


def test_parent_stop_fences_child_and_releases_paths(onprem_service):
    service, onprem = onprem_service
    detail = finish_plan(service, create_onprem(service))
    child_id = detail["sources"][0]["child_job_id"]
    service.start_transfer(detail["batch"]["id"])
    assert service.require_child_active(child_id)
    service.stop(detail["batch"]["id"])
    assert onprem.store.get(child_id)["state"] == "stopped"
    with pytest.raises(ValueError, match="managed_batch_not_active"):
        service.require_child_active(child_id)
    assert not service.require_child_active("not-managed")


def test_child_credentials_reattachment_never_autostarts_parent(onprem_service):
    service, onprem = onprem_service
    detail = finish_plan(service, create_onprem(service))
    batch_id = detail["batch"]["id"]
    child_id = detail["sources"][0]["child_job_id"]
    service.start_transfer(batch_id)
    onprem.store.update_job(child_id, state="awaiting_credentials", resume_state="running")
    service.store.recover()
    service.attach_credentials(batch_id, ONPREM_CREDS)
    assert service.detail(batch_id)["batch"]["state"] == "paused"
    assert onprem.store.get(child_id)["state"] == "paused"
    assert onprem.registry.get(child_id) == ONPREM_CREDS


def test_paused_batch_does_not_revive_running_child(onprem_service):
    service, onprem = onprem_service
    detail = finish_plan(service, create_onprem(service))
    batch_id = detail["batch"]["id"]
    service.start_transfer(batch_id)
    service.pause(batch_id)
    service.start()
    time.sleep(.3)
    assert service.detail(batch_id)["batch"]["state"] == "paused"
    assert not service._active


def test_onprem_combined_journal_flattens_diagnostics(onprem_service):
    service, onprem = onprem_service
    detail = finish_plan(service, create_onprem(service))
    child_id = detail["sources"][0]["child_job_id"]
    onprem.store.event(child_id, "transfer_stage", {"stage": "upload_file", "status": "failed",
                        "error": "destination_unavailable", "http_status": 500,
                        "api_error": "InternalServerError", "error_side": "onprem"})
    with service.export(detail["batch"]["id"], journal=True) as file:
        text = file.read().decode("utf-8-sig")
    assert "InternalServerError" in text
    assert "destination_unavailable" in text
    assert "500" in text
    assert "user@company.ru" in text


def test_dryrun_and_stop_do_not_hold_legacy_journal_forever(setup_service):
    service, _ = setup_service
    detail = finish_plan(service, create(service))
    history = service.journal.history("local")
    assert history["running_count"] == 0
    assert history["jobs"][0]["status"] == "done"
    service.stop(detail["batch"]["id"])
    assert service.journal.history("local")["running_count"] == 0


def test_transfer_new_journal_version_preserves_dryrun_history(setup_service):
    service, _ = setup_service
    detail = finish_plan(service, create(service))
    plan_journal = detail["sources"][0]["journal_id"]
    detail = service.start_transfer(detail["batch"]["id"])
    transfer_journal = detail["sources"][0]["journal_id"]
    assert transfer_journal != plan_journal
    assert service.journal.get(plan_journal)["status"] == "done"
    assert service.journal.get(transfer_journal)["status"] == "running"
    service._transfer_one(detail["sources"][0]["id"])
    assert service.journal.get(transfer_journal)["status"] == "done"


def test_saas_stat_error_logged_at_source_check_stage(setup_service):
    service, _ = setup_service
    detail = finish_plan(service, create(service))
    service.start_transfer(detail["batch"]["id"])

    class BrokenStat(FakeSource):
        def stat(self, path):
            raise MigrationError("source_unavailable", http_status=500,
                                 diagnostics={"api_error": "InternalServerError"})

    service.source_factory = BrokenStat
    service._transfer_one(detail["sources"][0]["id"])
    events = service.store.events(detail["batch"]["id"])
    errors = [e for e in events if e.get("error") == "source_unavailable"]
    assert errors and all(e["stage"] == "check_source" for e in errors)


def test_old_generation_orphan_child_never_runs_standalone(onprem_service):
    service, onprem = onprem_service
    detail = finish_plan(service, create_onprem(service))
    old_id = detail["sources"][0]["child_job_id"]
    detail = finish_plan(service, detail)
    with pytest.raises(ValueError, match="managed_batch_not_active"):
        service.require_child_active(old_id)
    assert old_id in onprem.store.managed_batch_job_ids()


def test_verified_local_file_is_rechecked_after_dryrun(setup_service, monkeypatch):
    service, local = setup_service
    target = local / "user@company.ru/folder/file.txt"
    target.parent.mkdir(parents=True)
    target.write_bytes(BODY)
    detail = finish_plan(service, create(service))
    assert detail["batch"]["summary"]["verified_bytes"] == len(BODY)
    changed = [dict(n, modified="changed-after-dryrun") if n["type"] == "file" else n
               for n in FakeSource.nodes]
    monkeypatch.setattr(FakeSource, "nodes", changed)
    FakeSource.downloads = 0
    service.start_transfer(detail["batch"]["id"])
    service._transfer_one(detail["sources"][0]["id"])
    entries = service.entries(detail["sources"][0]["id"])["entries"]
    file = next(e for e in entries if e["kind"] == "file")
    assert file["state"] == "failed" and file["error"] == "source_changed"
    assert FakeSource.downloads == 0
    assert target.read_bytes() == BODY


def test_combined_onprem_csv_contains_access_failure_before_child(onprem_service):
    service, _ = onprem_service

    class BadSource(FakeSource):
        def describe(self):
            raise MigrationError("source_unavailable", http_status=503,
                                 diagnostics={"api_error": "InternalServerError"})

    service.source_factory = BadSource
    detail = finish_plan(service, create_onprem(service))
    assert detail["sources"][0]["child_job_id"] is None
    with service.export(detail["batch"]["id"], journal=True) as file:
        text = file.read().decode("utf-8-sig")
    assert "InternalServerError" in text and "503" in text


def test_onprem_start_revalidates_verified_files(onprem_service, monkeypatch):
    service, onprem = onprem_service
    monkeypatch.setattr(FakeDestination, "stats", {
        "disk:/": {"type": "dir", "path": "disk:/"},
        "disk:/user@company.ru/folder/file.txt": {"type": "file", "size": 5, "md5": MD5}})
    detail = finish_plan(service, create_onprem(service))
    assert detail["batch"]["summary"]["verified_bytes"] == 5
    service.start_transfer(detail["batch"]["id"])
    child_id = detail["sources"][0]["child_job_id"]
    entries = onprem.store.entries(child_id)
    assert next(e for e in entries if e["kind"] == "file")["state"] == "planned"


def test_local_journal_cleanup_preserves_manifest_and_onprem_events(onprem_service):
    service, _ = onprem_service
    local = finish_plan(service, create(service))
    remote = finish_plan(service, create_onprem(service))
    local_id, remote_id = local["batch"]["id"], remote["batch"]["id"]
    remote_events = service.store.events(remote_id)
    assert service.store.events(local_id)
    with service.guard:
        service.check_local_journal_cleanup()
        service.journal.cleanup("local")
        removed = service.clear_local_journal_cache()
    assert removed > 0
    assert service.store.events(local_id) == []
    assert service.store.events(remote_id) == remote_events
    restored = service.detail(local_id)
    assert restored["batch"]["state"] == "ready"
    assert restored["sources"][0]["plan_complete"]
    assert restored["sources"][0]["journal_id"] is None
    assert service.entries(restored["sources"][0]["id"])["total"] == 2


def test_local_journal_cleanup_fences_queued_and_live_work(setup_service):
    from app.run_journal import JournalError
    service, _ = setup_service
    detail = create(service)
    service.plan(detail["batch"]["id"])
    with service.guard, pytest.raises(JournalError, match="journal_in_use"):
        service.check_local_journal_cleanup()
    service.pause(detail["batch"]["id"])
    source_id = detail["sources"][0]["id"]
    service._active[source_id] = object()
    with service.guard, pytest.raises(JournalError, match="journal_in_use"):
        service.check_local_journal_cleanup()
    service._active.pop(source_id)
    with service.guard:
        service.check_local_journal_cleanup()


def test_resume_after_restart_rejects_new_token_wrong_identity(onprem_service):
    service, onprem = onprem_service
    detail = finish_plan(service, create_onprem(service))
    batch_id = detail["batch"]["id"]
    child_id = detail["sources"][0]["child_job_id"]
    service.start_transfer(batch_id)
    service.store.recover()
    onprem.store.recover()
    service._credentials.clear()
    new_credentials = {**ONPREM_CREDS, "destination_token": "new-wrong-identity-token"}
    service.attach_credentials(batch_id, new_credentials)

    class WrongIdentity(FakeDestination):
        def info(self):
            return {**super().info(), "user": {"email": "other@company.ru"}}

    onprem.runner.destination_factory = WrongIdentity
    with pytest.raises(MigrationError, match="destination_identity_mismatch"):
        service.resume(batch_id)
    assert service.store.get(batch_id)["state"] == "paused"
    assert onprem.store.get(child_id)["state"] == "paused"


def test_outage_child_retry_identity_check_precedes_unfencing(onprem_service):
    service, onprem = onprem_service
    detail = finish_plan(service, create_onprem(service))
    batch_id = detail["batch"]["id"]
    child_id = detail["sources"][0]["child_job_id"]
    service.start_transfer(batch_id)
    service.store.update(batch_id, state="paused", resume_state="running", error="destination_unavailable")
    onprem.runner._record_destination_outage(child_id, MigrationError("destination_unavailable"))

    class WrongIdentity(FakeDestination):
        def info(self):
            return {**super().info(), "user": {"email": "other@company.ru"}}

    onprem.runner.destination_factory = WrongIdentity
    with pytest.raises(MigrationError, match="destination_identity_mismatch"):
        service.prepare_child_retry(child_id)
    assert service.store.get(batch_id)["state"] == "paused"
    assert onprem.runner._destination_outages
