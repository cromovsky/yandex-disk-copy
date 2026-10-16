import csv
import io
import sqlite3

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.migration.store import MigrationStore
from app.source_batches.api import router
from app.source_batches.store import BatchStore
from tests.test_source_batches import (
    BODY, CSV, ONPREM_CREDS, FakeDestination, FakeSource, create, create_onprem, finish_plan,
    onprem_service, setup_service,
)


def tree(service, detail):
    source_id = detail["sources"][0]["id"]
    return {entry["source_path"]: entry for entry in service.entries(source_id, 500)["entries"]}


def test_local_manifest_defaults_and_persists_selection(setup_service):
    service, _ = setup_service
    detail = finish_plan(service, create(service))
    entries = tree(service, detail)
    assert all(entry["selected"] and entry["can_select"] for entry in entries.values())
    result = service.entry_selection(detail["batch"]["id"], detail["sources"][0]["id"],
                                     entries["disk:/folder/file.txt"]["id"], False)
    assert result["batch"]["selection"] == {
        "total": 2, "selected": 1, "excluded": 1, "selected_files": 0,
        "selected_directories": 1, "selected_bytes": 0, "editable": True,
    }
    assert result["sources"][0]["summary"]["total_files"] == 1
    assert result["sources"][0]["summary"]["required_bytes"] == 0
    reopened = BatchStore(service.store.root)
    try:
        saved = reopened.entries(detail["sources"][0]["id"])["entries"]
        assert next(entry for entry in saved if entry["kind"] == "file")["selected"] is False
    finally:
        reopened.close()


def test_local_folder_cascade_covers_pages_and_restores_ancestors(setup_service, monkeypatch):
    service, _ = setup_service
    nodes = [{"path": "disk:/", "type": "dir"},
             {"path": "disk:/folder", "type": "dir"},
             {"path": "disk:/folder/nested", "type": "dir"},
             {"path": "disk:/folder-other", "type": "dir"}]
    nodes += [{"path": f"disk:/folder/nested/{index:03}.txt", "type": "file",
               "size": 5, "md5": "5d41402abc4b2a76b9719d911017c592"}
              for index in range(110)]
    monkeypatch.setattr(FakeSource, "nodes", nodes)
    detail = finish_plan(service, create(service))
    source_id, batch_id = detail["sources"][0]["id"], detail["batch"]["id"]
    entries = tree(service, detail)
    service.entry_selection(batch_id, source_id, entries["disk:/folder"]["id"], False)
    entries = tree(service, detail)
    assert all(not entry["selected"] for path, entry in entries.items()
               if path == "disk:/folder" or path.startswith("disk:/folder/"))
    assert entries["disk:/folder-other"]["selected"]
    service.entry_selection(batch_id, source_id, entries["disk:/folder/nested/109.txt"]["id"], True)
    entries = tree(service, detail)
    assert entries["disk:/folder"]["selected"] and entries["disk:/folder"]["selection_partial"]
    assert entries["disk:/folder/nested"]["selected"] and entries["disk:/folder/nested"]["selection_partial"]
    assert entries["disk:/folder/nested/109.txt"]["selected"]
    assert not entries["disk:/folder/nested/000.txt"]["selected"]
    assert service.entries(source_id, 100, 100)["total"] == 113


def test_local_empty_selection_can_be_restored_and_cannot_start(setup_service):
    service, local = setup_service
    detail = finish_plan(service, create(service))
    source_id, batch_id = detail["sources"][0]["id"], detail["batch"]["id"]
    folder = tree(service, detail)["disk:/folder"]
    result = service.entry_selection(batch_id, source_id, folder["id"], False)
    assert result["batch"]["selection"]["selected"] == 0
    assert result["batch"]["selection"]["editable"]
    with pytest.raises(ValueError, match="empty_selection"):
        service.start_transfer(batch_id)
    assert list(local.iterdir()) == []
    result = service.entry_selection(batch_id, source_id, folder["id"], True)
    assert result["batch"]["selection"]["selected"] == 2
    service.start_transfer(batch_id)
    with pytest.raises(ValueError, match="selection_not_allowed"):
        service.entry_selection(batch_id, source_id, folder["id"], False)


def test_local_excluded_file_never_downloads_and_queue_completes(setup_service, monkeypatch):
    service, local = setup_service
    monkeypatch.setattr(FakeSource, "downloads", 0)
    detail = finish_plan(service, create(service))
    source_id, batch_id = detail["sources"][0]["id"], detail["batch"]["id"]
    service.entry_selection(batch_id, source_id, tree(service, detail)["disk:/folder/file.txt"]["id"], False)
    service.start_transfer(batch_id)
    service._transfer_one(source_id)
    result = service.detail(batch_id)
    assert result["sources"][0]["state"] == "completed"
    assert result["sources"][0]["summary"]["issues"] == 0
    assert FakeSource.downloads == 0
    assert (local / "user@company.ru" / "folder").is_dir()
    assert not (local / "user@company.ru" / "folder" / "file.txt").exists()
    excluded = tree(service, detail)["disk:/folder/file.txt"]
    assert excluded["state"] == "skipped_by_user" and excluded["error"] is None
    assert not excluded["can_select"]
    journal = service.journal.get(result["sources"][0]["journal_id"])
    assert journal["summary"] == {"saved": 0, "failed": 0, "skipped": 1, "total": 1}
    with service.export(batch_id) as stream:
        rows = list(csv.DictReader(io.StringIO(stream.read().decode("utf-8-sig"))))
    assert next(row for row in rows if row["kind"] == "file")["selected"] == "False"


def test_zero_selection_source_has_no_local_side_effects(setup_service, monkeypatch):
    service, local = setup_service
    monkeypatch.setattr(FakeSource, "downloads", 0)
    detail = finish_plan(service, create(service, CSV + "personal,other@company.ru,,/\n"))
    batch_id, source_id = detail["batch"]["id"], detail["sources"][0]["id"]
    folder = tree(service, detail)["disk:/folder"]
    service.entry_selection(batch_id, source_id, folder["id"], False)
    service.start_transfer(batch_id)
    assert service.store.source(source_id)["state"] == "completed"
    assert not (local / "user@company.ru").exists()
    other = detail["sources"][1]["id"]
    service._transfer_one(other)
    assert (local / "other@company.ru" / "folder" / "file.txt").read_bytes() == BODY
    assert FakeSource.downloads == 1


def test_batch_entry_selection_api_checks_identity_and_strict_boolean(setup_service):
    service, _ = setup_service
    detail = finish_plan(service, create(service, CSV + "personal,other@company.ru,,/\n"))
    app = FastAPI()
    app.state.source_batches = service
    app.include_router(router)
    source_id, batch_id = detail["sources"][0]["id"], detail["batch"]["id"]
    file = tree(service, detail)["disk:/folder/file.txt"]
    path = f"/api/source-batches/{batch_id}/sources/{source_id}/entries/{file['id']}/selection"
    with TestClient(app) as client:
        assert client.post(path, json={"selected": "false"}).status_code == 422
        assert client.post(path.replace(source_id, detail["sources"][1]["id"]),
                           json={"selected": False}).status_code == 404
        result = client.post(path, json={"selected": False})
        assert result.status_code == 200
        assert result.json()["batch"]["selection"]["excluded"] == 1


def test_onprem_batch_selection_preserves_inventory_and_skips_zero_child(onprem_service):
    service, onprem = onprem_service
    detail = finish_plan(service, create_onprem(service, CSV + "personal,other@company.ru,,/\n"))
    source_id, batch_id = detail["sources"][0]["id"], detail["batch"]["id"]
    folder = tree(service, detail)["disk:/folder"]
    result = service.entry_selection(batch_id, source_id, folder["id"], False)
    assert result["sources"][0]["summary"]["selection"]["selected"] == 0
    assert result["sources"][0]["summary"]["total_files"] == 1
    assert service.entries(source_id)["total"] == 2
    child_id = detail["sources"][0]["child_job_id"]
    namespace_state = onprem.store.get(child_id)["namespace_state"]
    service.start_transfer(batch_id)
    assert service.store.source(source_id)["state"] == "completed"
    assert onprem.store.get(child_id)["state"] == "completed"
    assert onprem.store.get(child_id)["selection_frozen"]
    assert all(entry["state"] == "skipped_by_user" for entry in onprem.store.entries(child_id))
    assert onprem.store.get(child_id)["namespace_state"] == namespace_state


def test_selection_reduces_local_capacity_without_forgetting_inventory(setup_service, monkeypatch):
    service, _ = setup_service
    detail = finish_plan(service, create(service))
    monkeypatch.setattr("app.source_batches.service.shutil.disk_usage",
                        lambda path: type("Usage", (), {"free": 0})())
    file = tree(service, detail)["disk:/folder/file.txt"]
    result = service.entry_selection(detail["batch"]["id"], detail["sources"][0]["id"], file["id"], False)
    assert result["batch"]["state"] == "ready"
    assert result["batch"]["summary"]["total_bytes"] == 5
    assert result["batch"]["summary"]["required_bytes"] == 0


def test_excluding_local_conflict_allows_other_selected_objects(setup_service):
    service, local = setup_service
    target = local / "user@company.ru" / "folder"
    target.mkdir(parents=True)
    (target / "file.txt").write_bytes(b"world")
    detail = finish_plan(service, create(service))
    assert detail["sources"][0]["state"] == "error"
    file = tree(service, detail)["disk:/folder/file.txt"]
    assert file["can_select"]
    result = service.entry_selection(detail["batch"]["id"], detail["sources"][0]["id"], file["id"], False)
    assert result["batch"]["state"] == "ready"
    assert result["sources"][0]["summary"]["issues"] == 0
    service.start_transfer(detail["batch"]["id"])
    service._transfer_one(detail["sources"][0]["id"])
    assert (target / "file.txt").read_bytes() == b"world"
    assert service.store.source(detail["sources"][0]["id"])["state"] == "completed"


def test_onprem_complete_quota_plan_can_exclude_file(onprem_service):
    service, onprem = onprem_service

    class LimitedDestination(FakeDestination):
        def info(self):
            return dict(super().info(), total_space=2, used_space=0)

    onprem.runner.destination_factory = LimitedDestination
    detail = finish_plan(service, create_onprem(service))
    assert detail["sources"][0]["plan_complete"]
    file = tree(service, detail)["disk:/folder/file.txt"]
    assert file["can_select"]
    result = service.entry_selection(detail["batch"]["id"], detail["sources"][0]["id"], file["id"], False)
    assert result["batch"]["state"] == "ready"
    service.start_transfer(detail["batch"]["id"])


def test_zero_selection_source_does_not_collide_with_included_folder(setup_service, monkeypatch):
    service, _ = setup_service
    monkeypatch.setattr(FakeSource, "nodes", [
        {"path": "disk:/one", "type": "dir"},
        {"path": "disk:/two", "type": "dir"},
    ])
    detail = finish_plan(service, create(service, CSV.replace(",/\n", ",/one\n") + "personal,user@company.ru,,/two\n"))
    assert detail["batch"]["error"] == "target_folder_collision"
    source_id = detail["sources"][0]["id"]
    result = service.entry_selection(detail["batch"]["id"], source_id,
                                     tree(service, detail)["disk:/one"]["id"], False)
    assert result["batch"]["state"] == "ready"


@pytest.mark.parametrize("mode", ["local", "onprem"])
def test_recovered_selection_needs_no_credentials(onprem_service, mode):
    service, onprem = onprem_service
    detail = finish_plan(service, create(service) if mode == "local" else create_onprem(service))
    batch_id, source_id = detail["batch"]["id"], detail["sources"][0]["id"]
    file = tree(service, detail)["disk:/folder/file.txt"]
    service.store.recover()
    onprem.store.recover()
    service._credentials.clear()
    onprem.registry.clear()
    result = service.entry_selection(batch_id, source_id, file["id"], False)
    assert result["batch"]["state"] == "awaiting_credentials"
    assert not result["batch"]["credentials_available"]
    assert result["batch"]["selection"]["editable"]
    assert result["batch"]["selection"]["excluded"] == 1
    assert tree(service, detail)["disk:/folder/file.txt"]["selected"] is False
    restored = service.entry_selection(batch_id, source_id, file["id"], True)
    assert restored["batch"]["selection"]["excluded"] == 0


def test_old_local_entry_schema_migrates_to_selected_plan(setup_service):
    service, _ = setup_service
    detail = finish_plan(service, create(service))
    with service.store.connection() as db:
        db.execute("ALTER TABLE entries DROP COLUMN selected")
    service.store.close()
    service.store = BatchStore(service.store.root)
    assert service.detail(detail["batch"]["id"])["batch"]["selection"]["selected"] == 2
    folder = tree(service, detail)["disk:/folder"]
    result = service.entry_selection(detail["batch"]["id"], detail["sources"][0]["id"], folder["id"], False)
    assert result["batch"]["selection"]["selected"] == 0


def test_start_checks_current_selected_capacity_without_freezing_plan(setup_service, monkeypatch):
    service, _ = setup_service
    detail = finish_plan(service, create(service))
    monkeypatch.setattr("app.source_batches.service.shutil.disk_usage",
                        lambda path: type("Usage", (), {"free": 0})())
    with pytest.raises(ValueError, match="local_space_insufficient"):
        service.start_transfer(detail["batch"]["id"])
    file = tree(service, detail)["disk:/folder/file.txt"]
    assert file["can_select"]
    result = service.entry_selection(detail["batch"]["id"], detail["sources"][0]["id"], file["id"], False)
    assert result["batch"]["state"] == "ready"
    service.start_transfer(detail["batch"]["id"])


def test_start_prevalidates_later_child_before_freezing_earlier(onprem_service):
    service, onprem = onprem_service
    detail = finish_plan(service, create_onprem(service, CSV + "personal,other@company.ru,,/\n"))
    first, second = detail["sources"]
    second_file = next(entry for entry in onprem.store.entries(second["child_job_id"])
                       if entry["kind"] == "file")
    onprem.store.update_entry(second_file["id"], state="conflict", error="destination_conflict")
    with pytest.raises(ValueError, match="plan_conflicts"):
        service.start_transfer(detail["batch"]["id"])
    assert not service.store.get(detail["batch"]["id"])["started"]
    assert not onprem.store.get(first["child_job_id"])["selection_frozen"]
    assert service.store.source(first["id"])["state"] == "ready"
    assert onprem.store.get(first["child_job_id"])["selection"]["editable"]


def test_recovered_quota_selection_defers_remote_check_until_credentials(onprem_service):
    service, onprem = onprem_service

    class LimitedDestination(FakeDestination):
        def info(self):
            return dict(super().info(), total_space=2, used_space=0)

    onprem.runner.destination_factory = LimitedDestination
    detail = finish_plan(service, create_onprem(service))
    batch_id, source_id = detail["batch"]["id"], detail["sources"][0]["id"]
    file = tree(service, detail)["disk:/folder/file.txt"]
    service.store.recover()
    onprem.store.recover()
    service._credentials.clear()
    onprem.registry.clear()
    result = service.entry_selection(batch_id, source_id, file["id"], False)
    assert result["batch"]["state"] == "awaiting_credentials"
    assert result["batch"]["selection"]["editable"]
    assert result["batch"]["selection"]["selected_bytes"] == 0


def test_local_start_failure_rolls_back_freeze_before_reselection(setup_service):
    service, local = setup_service
    detail = finish_plan(service, create(service))
    batch_id, source_id = detail["batch"]["id"], detail["sources"][0]["id"]
    file = tree(service, detail)["disk:/folder/file.txt"]
    service.entry_selection(batch_id, source_id, file["id"], False)
    with service.store.connection() as db:
        db.executescript("""
            CREATE TEMP TRIGGER abort_local_start BEFORE UPDATE OF state ON sources
            WHEN NEW.state='running'
            BEGIN SELECT RAISE(ABORT, 'simulated_start_failure'); END;
        """)
    with pytest.raises(sqlite3.IntegrityError, match="simulated_start_failure"):
        service.start_transfer(batch_id)
    assert service.store.get(batch_id)["state"] == "ready"
    assert not service.store.get(batch_id)["started"]
    assert service.store.source(source_id)["state"] == "ready"
    assert tree(service, detail)["disk:/folder/file.txt"]["state"] == "planned"
    with service.store.connection() as db:
        db.execute("DROP TRIGGER abort_local_start")
    service.entry_selection(batch_id, source_id, file["id"], True)
    service.start_transfer(batch_id)
    service._transfer_one(source_id)
    assert (local / "user@company.ru" / "folder" / "file.txt").read_bytes() == BODY


@pytest.mark.parametrize("blocked", ["quota", "conflict"])
def test_reattaching_complete_attention_child_preserves_selection(onprem_service, monkeypatch, blocked):
    service, onprem = onprem_service
    if blocked == "quota":
        class LimitedDestination(FakeDestination):
            def info(self):
                return dict(super().info(), total_space=2, used_space=0)

        onprem.runner.destination_factory = LimitedDestination
    else:
        monkeypatch.setattr(FakeDestination, "stats", {
            "disk:/": {"type": "dir", "path": "disk:/"},
            "disk:/user@company.ru/folder/file.txt": {"type": "file", "size": 5, "md5": "a" * 32},
        })
    detail = finish_plan(service, create_onprem(service))
    batch_id, source_id = detail["batch"]["id"], detail["sources"][0]["id"]
    child_id = detail["sources"][0]["child_job_id"]
    service.store.recover()
    onprem.store.recover()
    service._credentials.clear()
    onprem.registry.clear()
    service.attach_credentials(batch_id, ONPREM_CREDS)
    file = tree(service, detail)["disk:/folder/file.txt"]
    assert file["can_select"]
    result = service.entry_selection(batch_id, source_id, file["id"], False)
    assert result["batch"]["state"] == "ready"
    assert result["sources"][0]["summary"]["issues"] == 0
    service.start_transfer(batch_id)
    assert onprem.store.get(child_id)["state"] == "running"


@pytest.mark.parametrize("reopen", [False, True])
@pytest.mark.parametrize("first_empty", [False, True])
def test_partial_onprem_start_can_retry_without_executing_source(onprem_service, monkeypatch, reopen, first_empty):
    service, onprem = onprem_service
    monkeypatch.setattr(FakeSource, "downloads", 0)
    detail = finish_plan(service, create_onprem(service, CSV + "personal,other@company.ru,,/\n"))
    batch_id, first, second = detail["batch"]["id"], *detail["sources"]
    entries = tree(service, detail)
    excluded = entries["disk:/folder" if first_empty else "disk:/folder/file.txt"]
    service.entry_selection(batch_id, first["id"], excluded["id"], False)
    update_job = onprem.store.update_job
    interruption = KeyboardInterrupt if reopen else RuntimeError

    def interrupt_later_child(job_id, **fields):
        if job_id == second["child_job_id"] and fields.get("state") == "running":
            raise interruption("interrupted_admission")
        return update_job(job_id, **fields)

    monkeypatch.setattr(onprem.store, "update_job", interrupt_later_child)
    with pytest.raises(interruption, match="interrupted_admission"):
        service.start_transfer(batch_id)
    monkeypatch.setattr(onprem.store, "update_job", update_job)
    assert not service.store.get(batch_id)["started"]
    partial_state = "completed" if first_empty else "running"
    assert onprem.store.get(first["child_job_id"])["state"] == (partial_state if reopen else "ready")
    assert onprem.store.get(first["child_job_id"])["selection_frozen"]
    if reopen:
        batch_root, migration_root = service.store.root, onprem.store.root
        service.store.close()
        onprem.store.close()
        service.store = BatchStore(batch_root)
        onprem.store = MigrationStore(migration_root)
        onprem.runner.store = onprem.store
        onprem.store.recover()
        service.store.recover()
        service._credentials.clear()
        onprem.registry.clear()
        restored = service.attach_credentials(batch_id, ONPREM_CREDS)
        assert restored["batch"]["state"] == "ready"
        assert all(source["state"] == "ready" for source in restored["sources"])
    else:
        assert all(source["state"] == "ready" for source in service.detail(batch_id)["sources"])
    result = service.start_transfer(batch_id)
    assert result["batch"]["state"] == "running" and result["batch"]["started"]
    assert result["sources"][0]["state"] == ("completed" if first_empty else "running")
    assert result["sources"][1]["state"] == "running"
    assert tree(service, detail)[excluded["source_path"]]["selected"] is False
    assert FakeSource.downloads == 0
    with onprem.store._connection() as db:
        assert db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM publications WHERE state!='planned'").fetchone()[0] == 0


@pytest.mark.parametrize("evidence", ["executed_directory", "attempt", "publication"])
def test_unstarted_parent_never_rewinds_child_execution_evidence(onprem_service, evidence):
    service, onprem = onprem_service
    detail = finish_plan(service, create_onprem(service))
    batch_id, source = detail["batch"]["id"], detail["sources"][0]
    child_id = source["child_job_id"]
    entries = tree(service, detail)
    onprem.store.freeze_selection(child_id)
    onprem.store.update_job(child_id, state="running")
    service.store.update_source(source["id"], state="running")
    if evidence == "executed_directory":
        onprem.store.update_entry(entries["disk:/folder"]["id"], state="done")
    elif evidence == "attempt":
        onprem.store.new_attempt(entries["disk:/folder/file.txt"]["id"], "staged")
    else:
        onprem.store.update_publication(child_id, entries["disk:/folder"]["id"], state="intent")
    with pytest.raises(ValueError, match="selection_not_allowed"):
        service.start_transfer(batch_id)
    assert onprem.store.get(child_id)["state"] == "running"
    assert service.store.source(source["id"])["state"] == "running"
    assert not service.store.get(batch_id)["started"]
