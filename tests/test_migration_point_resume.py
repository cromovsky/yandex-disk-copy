"""A per-file retry must not strand the remaining transfer behind a pause."""

import pytest

from app.migration.store import MigrationStore
from tests import test_migration_api as api_fixtures
from tests import test_migration_runner as runner_fixtures
from tests.test_migration_point_retry import queue, unknown
from tests.test_migration_runner import BODY, META
from tests.test_migration_store import file_entry, point_retry_job

api = api_fixtures.api
unplanned_migration = runner_fixtures.unplanned_migration
direct_migration = runner_fixtures.direct_migration


def test_cached_point_retry_continues_remaining_plan(direct_migration):
    store, _, runner, job_id, source, destination = direct_migration
    entry, attempt = unknown(direct_migration, cache=True)
    pending = [e for e in store.entries(job_id) if e["state"] == "planned"]
    queue(direct_migration, entry, attempt)
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] == "done"
    assert destination.upload_body == BODY
    assert source.downloads == 0
    assert store.get(job_id)["state"] == "running"
    for _ in range(10):
        runner.step(job_id)
    assert store.get(job_id)["state"] == "completed"
    assert all(store.entry(e["id"])["state"] == "done" for e in pending)


def test_explicit_pause_during_point_upload_keeps_remaining_plan(direct_migration):
    store, _, runner, job_id, _, destination = direct_migration
    entry, attempt = unknown(direct_migration, cache=True)
    upload = destination.upload

    def pause_during_upload(*args, **kwargs):
        store.update_job(job_id, pause_requested=True)
        return upload(*args, **kwargs)

    destination.upload = pause_during_upload
    queue(direct_migration, entry, attempt)
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] == "done"
    assert store.get(job_id)["state"] == "paused"
    assert store.get(job_id)["pause_requested"]
    assert any(e["state"] == "planned" for e in store.entries(job_id))


def test_point_success_with_other_unknown_continues_plan(unplanned_migration):
    store, _, runner, job_id, source, _ = unplanned_migration
    walk = source.walk
    source.walk = lambda path: [*walk(path), dict(
        META, path="disk:/Docs/b.txt", name="b.txt",
    )]
    runner.step(job_id)
    entry, old = unknown(unplanned_migration, cache=True)
    other = next(e for e in store.entries(job_id) if e["source_path"].endswith("b.txt"))
    attempt = store.new_attempt(other["id"], "staged")
    store.update_attempt(attempt["id"], phase="unknown_transfer")
    store.update_entry(other["id"], state="outcome_unknown")
    before_entry, before_attempt = store.entry(other["id"]), store.attempt(attempt["id"])
    queue(unplanned_migration, entry, old)
    runner.step(job_id)
    assert store.get(job_id)["state"] == "running"
    for _ in range(10):
        runner.step(job_id)
    assert all(e["state"] == "done" for e in store.entries(job_id) if e["kind"] == "dir")
    assert store.get(job_id)["state"] == "needs_attention"
    assert store.entry(other["id"]) == before_entry
    assert store.attempt(attempt["id"]) == before_attempt


@pytest.mark.parametrize("pause", [False, True])
def test_finish_point_retry_preserves_credential_fence(tmp_path, pause):
    store = MigrationStore(tmp_path)
    try:
        job, entry, attempt = point_retry_job(store, extra_entries=[file_entry(
            source_path="disk:/Documents/b.txt",
            destination_path="disk:/Migration/Documents/b.txt",
        )])
        queue_args = job["id"], entry["id"], attempt["id"], job["revision"]
        store.queue_point_retry(*queue_args)
        store.update_entry(entry["id"], state="done")
        store.update_job(job["id"], state="awaiting_credentials",
                         resume_state="running", error="credentials_required",
                         pause_requested=pause)
        result = store.finish_point_retry(entry["id"])
        assert result["state"] == "awaiting_credentials"
        assert result["error"] == "credentials_required"
        assert result["resume_state"] == ("paused" if pause else "running")
        assert not result["point_retry_active"]
    finally:
        store.close()


@pytest.mark.parametrize("historical_unknown", [False, True])
def test_resume_paused_plan_preserves_failed_and_unknown_files(api, historical_unknown):
    client, service = api
    source, destination = runner_fixtures.Source(), runner_fixtures.Destination()
    walk = source.walk
    source.walk = lambda path: [*walk(path), dict(
        META, path="disk:/Docs/b.txt", name="b.txt",
    )]
    service.runner.source_factory = lambda c, a: source
    service.runner.destination_factory = lambda c, a: destination
    job_id = client.post("/api/onprem/jobs", json=api_fixtures.REQUEST).json()["id"]
    service.runner.step(job_id)
    store = service.store
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    attempt = store.new_attempt(entry["id"], "staged")
    store.update_attempt(attempt["id"], phase="unknown_transfer" if historical_unknown
                         else "abandoned_transfer", operation_id="historical-operation")
    store.update_entry(entry["id"], state="outcome_unknown" if historical_unknown
                       else "failed", error="destination_unavailable")
    store.update_job(job_id, state="paused", pause_requested=True, namespace_state="owned")
    before_entry, before_attempt = store.entry(entry["id"]), store.attempt(attempt["id"])
    response = client.post(f"/api/onprem/jobs/{job_id}/resume")
    assert response.status_code == 200
    assert response.json()["state"] == "running"
    assert not response.json()["pause_requested"]
    for _ in range(10):
        service.runner.step(job_id)
    remaining_file = next(e for e in store.entries(job_id) if e["source_path"].endswith("b.txt"))
    assert remaining_file["state"] == "done"
    assert store.entry(entry["id"]) == before_entry
    assert store.attempt(attempt["id"]) == before_attempt
    assert destination.uploads == 1
    assert destination.imports == 0
    assert store.get(job_id)["state"] == ("needs_attention" if historical_unknown
                                          else "completed_with_issues")



def test_continue_with_only_unknown_still_requires_targeted_recovery(api):
    client, service = api
    job_id, entry, attempt = api_fixtures.attempted_job(api)
    service.store.update_job(job_id, state="paused", pause_requested=True)
    before_entry = service.store.entry(entry["id"])
    before_attempt = service.store.attempt(attempt["id"])
    response = client.post(f"/api/onprem/jobs/{job_id}/resume")
    assert response.status_code == 409
    assert response.json()["detail"] == "active_or_unknown_attempt"
    assert service.store.entry(entry["id"]) == before_entry
    assert service.store.attempt(attempt["id"]) == before_attempt
