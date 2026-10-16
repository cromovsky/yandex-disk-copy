"""Reviewed selection survives reload and bounds every transfer side effect."""

import sqlite3

import pytest

from app.migration.store import MigrationStore
from tests.test_migration_api import api, REQUEST
from tests.test_migration_public_runner import public_migration
from tests.test_migration_runner import migration, finish
from tests.test_migration_store import config, file_entry


def toggle(store, job_id, entry, selected):
    return store.set_entry_selection(
        job_id, entry["id"], selected, store.get(job_id)["revision"],
    )


def test_folder_selection_cascades_across_pages_and_child_restores_parents(tmp_path):
    store = MigrationStore(tmp_path)
    job_id = store.create(config(strategy="staged"), "selection-tree")["id"]
    store.add_entries(job_id, [
        {"kind": "dir", "source_path": "disk:/Documents",
         "destination_path": "disk:/Migration/Documents", "size": 0},
        file_entry(),
        file_entry(source_path="disk:/Documents/b.txt",
                   destination_path="disk:/Migration/Documents/b.txt", size=7),
        file_entry(source_path="disk:/Documents-other/c.txt",
                   destination_path="disk:/Migration/Documents-other/c.txt", size=3),
    ], mark_ready=True)
    root, child, sibling, outside = store.entries(job_id)
    toggle(store, job_id, root, False)
    assert [e["selected"] for e in store.entries(job_id, limit=1, offset=2)] == [False]
    assert store.entry(outside["id"])["selected"] is True
    job = toggle(store, job_id, child, True)
    assert store.entry(root["id"])["selected"] is True
    assert store.entry(root["id"])["selection_partial"] is True
    assert store.entry(sibling["id"])["selected"] is False
    assert job["selection"] == {
        "total": 4, "selected": 3, "excluded": 1, "selected_files": 2,
        "selected_directories": 1, "selected_bytes": 15, "editable": True,
    }
    store.close()
    reopened = MigrationStore(tmp_path)
    assert reopened.entry(sibling["id"])["selected"] is False
    assert reopened.entry(child["id"])["can_select"] is True


def test_selection_rejects_stale_foreign_and_frozen_mutations_atomically(tmp_path):
    store = MigrationStore(tmp_path)
    first = store.create(config(strategy="staged"), "selection-first")["id"]
    second = store.create(config(strategy="staged"), "selection-second")["id"]
    for job_id in (first, second):
        store.add_entries(job_id, [file_entry()], mark_ready=True)
    entry = store.entries(first)[0]
    revision = store.get(first)["revision"]
    with pytest.raises(ValueError, match="stale_revision"):
        store.set_entry_selection(first, entry["id"], False, revision - 1)
    with pytest.raises(KeyError):
        store.set_entry_selection(second, entry["id"], False,
                                  store.get(second)["revision"])
    assert store.entry(entry["id"])["selected"] is True
    store.freeze_selection(first)
    with pytest.raises(ValueError, match="selection_not_allowed"):
        toggle(store, first, entry, False)


def test_empty_selection_is_rejected_without_freezing_and_directory_is_valid(migration):
    store, _, _, job_id, _, _ = migration
    root = store.entries(job_id)[0]
    toggle(store, job_id, root, False)
    with pytest.raises(ValueError, match="empty_selection"):
        store.freeze_selection(job_id)
    assert store.get(job_id)["selection"]["editable"] is True
    empty_dir = next(e for e in store.entries(job_id) if e["source_path"].endswith("/empty"))
    toggle(store, job_id, empty_dir, True)
    store.freeze_selection(job_id)
    assert store.get(job_id)["selection"]["selected_bytes"] == 0
    assert store.entry(empty_dir["id"])["state"] == "planned"


def test_unchecked_file_never_downloads_and_does_not_fail_queue(migration):
    store, _, runner, job_id, source, destination = migration
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    toggle(store, job_id, entry, False)
    store.freeze_selection(job_id)
    result = finish(store, runner, job_id)
    assert result["state"] == "completed"
    assert source.downloads == destination.uploads == 0
    assert store.entry(entry["id"])["state"] == "skipped_by_user"
    assert store.describe_entries(job_id, [entry])[0]["can_retry"] is False
    assert store.entry(entry["id"])["attempt_id"] is None


def test_unselected_public_root_is_never_published(public_migration):
    store, _, runner, job_id, source, destination = public_migration
    runner.step(job_id)
    root = store.entries(job_id)[0]
    # A second independent root remains selected to keep the job runnable.
    store.add_entries(job_id, [{"kind": "dir", "source_path": "disk:/Other",
                               "destination_path": "disk:/owner@example.org/Other",
                               "size": 0}], append=True)
    toggle(store, job_id, root, False)
    store.freeze_selection(job_id)
    assert finish(store, runner, job_id)["state"] == "completed"
    assert source.published == []
    assert source.downloads == destination.uploads == 0


def test_selection_api_persists_and_enforces_strict_body(api):
    client, service = api
    job_id = client.post("/api/onprem/jobs", json=REQUEST).json()["id"]
    service.store.add_entries(job_id, [file_entry()], mark_ready=True)
    entry = service.store.entries(job_id)[0]
    url = f"/api/onprem/jobs/{job_id}/entries/{entry['id']}/selection"
    revision = service.store.get(job_id)["revision"]
    assert client.post(url, json={"selected": 0, "revision": revision}).status_code == 422
    response = client.post(url, json={"selected": False, "revision": revision})
    assert response.status_code == 200
    assert response.json()["selection"]["excluded"] == 1
    assert client.post(f"/api/onprem/jobs/{job_id}/start").json()["detail"] == "empty_selection"
    assert client.post(url, json={"selected": True, "revision": revision}).json()["detail"] == "stale_revision"


def test_schema_eight_inventory_migrates_selected_without_losing_history(tmp_path):
    store = MigrationStore(tmp_path)
    job_id = store.create(config(), "old-selection")["id"]
    store.add_entries(job_id, [file_entry()], mark_ready=True)
    entry_id = store.entries(job_id)[0]["id"]
    store.close()
    with sqlite3.connect(tmp_path / "jobs.db") as db:
        db.execute("DROP INDEX IF EXISTS entries_selection")
        # Recreate the precise absence of new columns in an older database.
        for table, column in (("entries", "selected"), ("jobs", "plan_complete"),
                              ("jobs", "selection_frozen")):
            columns = {r[1] for r in db.execute(f"PRAGMA table_info({table})")}
            if column in columns:
                db.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
        db.execute("PRAGMA user_version=8")
    reopened = MigrationStore(tmp_path)
    assert reopened.entry(entry_id)["selected"] is True
    assert reopened.get(job_id)["selection"]["editable"] is True
    assert reopened.get(job_id)["total_bytes"] == 12


def test_incomplete_inventory_cannot_be_selected_after_failed_enumeration(tmp_path):
    store = MigrationStore(tmp_path)
    job_id = store.create(config(strategy="staged"), "partial-selection")["id"]
    store.add_entries(job_id, [file_entry()])
    store.update_job(job_id, state="needs_attention", error="source_unavailable")
    entry = store.entries(job_id)[0]
    assert entry["can_select"] is False
    with pytest.raises(ValueError, match="selection_not_allowed"):
        toggle(store, job_id, entry, False)


def test_complete_conflict_plan_can_exclude_problem_and_freeze(migration):
    store, _, _, job_id, _, _ = migration
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    store.update_entry(entry["id"], state="conflict", error="destination_conflict")
    store.update_job(job_id, state="needs_attention", error="plan_conflicts")
    with pytest.raises(ValueError, match="plan_conflicts"):
        store.freeze_selection(job_id)
    toggle(store, job_id, entry, False)
    store.freeze_selection(job_id)
    assert store.entry(entry["id"])["state"] == "skipped_by_user"


def test_recovered_ready_selection_is_editable_without_credentials(migration):
    store, registry, _, job_id, _, _ = migration
    registry.discard(job_id)
    store.recover()
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    toggle(store, job_id, entry, False)
    assert store.get(job_id)["state"] == "awaiting_credentials"
    assert store.get(job_id)["selection"]["editable"] is True


def test_unselected_verified_file_is_not_revalidated_or_counted_in_progress(migration):
    store, _, _, job_id, _, _ = migration
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    store.update_entry(entry["id"], state="already_verified", verification="md5")
    toggle(store, job_id, entry, False)
    assert store.get(job_id)["verified_bytes"] == 0
    store.revalidate_verified_files(job_id)
    assert store.entry(entry["id"])["state"] == "already_verified"


def test_start_revalidates_selected_quota_without_freezing_on_failure(migration):
    store, _, runner, job_id, _, destination = migration
    destination.info = lambda: {"total_space": 1, "used_space": 0,
                                "user": {"login": "receiver"}}
    from app.migration.errors import MigrationError
    with pytest.raises(MigrationError, match="destination_space_insufficient"):
        runner.validate_selection(job_id)
    assert store.get(job_id)["selection"]["editable"] is True
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    toggle(store, job_id, entry, False)
    runner.validate_selection(job_id)


def test_explicit_root_folder_restores_as_ancestor_and_reports_partial(tmp_path):
    store = MigrationStore(tmp_path)
    job_id = store.create(config(strategy="staged"), "root-selection")["id"]
    store.add_entries(job_id, [
        {"kind": "dir", "source_path": "disk:/", "destination_path": "disk:/Migration", "size": 0},
        file_entry(), file_entry(source_path="disk:/other.txt", destination_path="disk:/Migration/other.txt"),
    ], mark_ready=True)
    root, child, _ = store.entries(job_id)
    toggle(store, job_id, root, False)
    toggle(store, job_id, child, True)
    assert store.entry(root["id"])["selected"] is True
    assert store.entry(root["id"])["selection_partial"] is True


def test_direct_managed_child_reads_do_not_offer_selection(api):
    client, service = api
    job_id = client.post("/api/onprem/jobs", json=REQUEST).json()["id"]
    service.store.add_entries(job_id, [file_entry()], mark_ready=True)
    entry = service.store.entries(job_id)[0]
    service.excluded_job_ids.add(job_id)
    assert client.get(f"/api/onprem/jobs/{job_id}").json()["selection"]["editable"] is False
    assert client.get(f"/api/onprem/jobs/{job_id}/entries").json()["entries"][0]["can_select"] is False
    assert client.get(f"/api/onprem/jobs/{job_id}/entries/{entry['id']}").json()["can_select"] is False


def test_batch_internal_empty_freeze_persists_user_skips(migration):
    store, _, _, job_id, _, _ = migration
    toggle(store, job_id, store.entries(job_id)[0], False)
    store.validate_selection(job_id, allow_empty=True)
    store.freeze_selection(job_id, allow_empty=True)
    assert {e["state"] for e in store.entries(job_id)} == {"skipped_by_user"}
    assert store.get(job_id)["selection"]["editable"] is False


def test_start_recovers_a_frozen_prestart_selection_after_crash(api):
    client, service = api
    job_id = client.post("/api/onprem/jobs", json=REQUEST).json()["id"]
    service.store.add_entries(job_id, [{"kind": "dir", "source_path": "disk:/Docs",
                                      "destination_path": "disk:/owner/Docs", "size": 0}], mark_ready=True)
    service.store.freeze_selection(job_id)
    revision = service.store.get(job_id)["revision"]
    service.store.freeze_selection(job_id)
    assert service.store.get(job_id)["revision"] == revision
    assert client.post(f"/api/onprem/jobs/{job_id}/start").status_code == 200


def test_frozen_selection_reopens_prestart_but_never_reopens_running_plan(tmp_path):
    store = MigrationStore(tmp_path)
    job_id = store.create(config(strategy="staged"), "frozen-reopen")["id"]
    store.add_entries(job_id, [file_entry()], mark_ready=True)
    store.freeze_selection(job_id)
    store.close()
    reopened = MigrationStore(tmp_path)
    reopened.recover()
    assert reopened.get(job_id)["resume_state"] == "ready"
    reopened.validate_selection(job_id)
    reopened.freeze_selection(job_id)
    entry = reopened.entries(job_id)[0]
    with pytest.raises(ValueError, match="selection_not_allowed"):
        toggle(reopened, job_id, entry, False)
    reopened.update_job(job_id, state="running", resume_state=None)
    reopened.recover()
    assert reopened.get(job_id)["resume_state"] == "running"
    with pytest.raises(ValueError, match="selection_not_allowed"):
        reopened.freeze_selection(job_id)


def test_excluded_conflict_stops_counting_as_issue_and_reselection_restores_it(migration):
    store, _, _, job_id, _, _ = migration
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    store.update_entry(entry["id"], state="conflict", error="destination_conflict")
    store.update_job(job_id, state="needs_attention", error="plan_conflicts")
    excluded = toggle(store, job_id, entry, False)
    assert excluded["error"] is None
    assert excluded["state"] == "ready"
    assert excluded["counts"].get("conflict", 0) == 0
    assert excluded["total_files"] == 1
    assert store.entry(entry["id"])["state"] == "conflict"
    selected = toggle(store, job_id, entry, True)
    assert selected["error"] == "plan_conflicts"
    assert selected["state"] == "needs_attention"
    assert selected["counts"]["conflict"] == 1


def test_global_retry_cannot_bypass_empty_reviewed_selection(migration):
    store, _, runner, job_id, _, _ = migration
    store.update_job(job_id, state="needs_attention", error="destination_space_insufficient")
    toggle(store, job_id, store.entries(job_id)[0], False)
    with pytest.raises(ValueError, match="empty_selection"):
        runner.retry(job_id, "staged")
    assert store.get(job_id)["state"] == "needs_attention"


def test_resume_restores_review_without_bypassing_empty_selection_on_start(api):
    client, service = api
    job_id = client.post("/api/onprem/jobs", json=REQUEST).json()["id"]
    service.store.add_entries(job_id, [file_entry()], mark_ready=True)
    service.store.update_job(job_id, state="needs_attention", error="destination_space_insufficient")
    toggle(service.store, job_id, service.store.entries(job_id)[0], False)
    response = client.post(f"/api/onprem/jobs/{job_id}/resume")
    assert response.status_code == 200
    assert response.json()["state"] == "needs_attention"
    assert response.json()["selection_frozen"] is False
    started = client.post(f"/api/onprem/jobs/{job_id}/start")
    assert started.status_code == 409
    assert started.json()["detail"] == "empty_selection"


@pytest.mark.parametrize("frozen", [False, True])
def test_credentials_resume_restores_prestart_attention_plan_without_launch(api, frozen):
    client, service = api
    job_id = client.post("/api/onprem/jobs", json=REQUEST).json()["id"]
    service.store.add_entries(job_id, [{"kind": "dir", "source_path": "disk:/Docs",
                                      "destination_path": "disk:/owner/Docs", "size": 0}], mark_ready=True)
    if frozen:
        service.store.freeze_selection(job_id)
    service.store.update_job(job_id, state="awaiting_credentials",
                             resume_state="needs_attention", error="destination_space_insufficient")
    response = client.post(f"/api/onprem/jobs/{job_id}/resume")
    assert response.status_code == 200
    assert response.json()["state"] == "needs_attention"
    assert response.json()["error"] == "destination_space_insufficient"
    assert response.json()["selection_frozen"] is frozen
    assert response.json()["can_start_transfer"] is True
    assert service.store.entries(job_id)[0]["state"] == "planned"
    started = client.post(f"/api/onprem/jobs/{job_id}/start")
    assert started.status_code == 200
    assert started.json()["state"] == "running"
    assert started.json()["error"] is None


def test_transfer_startability_excludes_empty_incomplete_and_executed_plans(migration):
    store, _, _, job_id, _, _ = migration
    root = store.entries(job_id)[0]
    assert store.get(job_id)["can_start_transfer"] is True
    toggle(store, job_id, root, False)
    assert store.get(job_id)["can_start_transfer"] is False
    toggle(store, job_id, root, True)
    store.freeze_selection(job_id)
    store.update_entry(root["id"], state="failed", error="destination_space_insufficient")
    store.update_job(job_id, state="needs_attention", error="destination_space_insufficient")
    assert store.get(job_id)["can_start_transfer"] is False
    other = store.create(config(strategy="staged"), "incomplete-startability")["id"]
    store.add_entries(other, [file_entry()])
    store.update_job(other, state="needs_attention", error="source_unavailable")
    assert store.get(other)["can_start_transfer"] is False
