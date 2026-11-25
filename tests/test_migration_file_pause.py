"""Consecutive transfers proceed immediately while manual pause and stop still apply."""

import pytest

from tests.fake_clock import FakeClock
from tests import test_migration_runner as fixtures
from tests.test_migration_runner import CONFIG, DIR, META, stage_events


unplanned_migration = fixtures.unplanned_migration


@pytest.fixture
def frozen_clock(monkeypatch):
    clock = FakeClock(start=0)
    monkeypatch.setattr("app.migration.runner.time.monotonic", clock.monotonic)
    monkeypatch.setattr("app.migration.runner.time.sleep", clock.sleep)
    return clock


@pytest.fixture
def two_files(unplanned_migration):
    store, _, runner, job_id, source, _ = unplanned_migration
    source.walk = lambda path: [dict(DIR), dict(META), dict(
        META, path="disk:/Docs/b.txt", name="b.txt",
    )]
    runner.step(job_id)
    store.update_job(job_id, state="running")
    return unplanned_migration


def first_file(migration):
    store, _, runner, job_id, _, destination = migration
    for _ in range(10):
        runner.step(job_id)
        if destination.uploads:
            break
    assert destination.uploads == 1
    assert len([e for e in store.entries(job_id) if e["kind"] == "file"
                and e["state"] == "done"]) == 1
    return migration


@pytest.mark.parametrize("retry_enabled", [True, False])
def test_second_file_starts_immediately(two_files, frozen_clock, retry_enabled):
    store, _, runner, job_id, source, destination = two_files
    runner.set_retry_enabled(job_id, retry_enabled)
    first_file(two_files)
    runner.step(job_id)
    assert destination.uploads == source.downloads == 2
    runner.step(job_id)
    assert store.get(job_id)["state"] == "completed"
    assert frozen_clock.monotonic() == 0
    assert not stage_events(store, job_id, "file_pause")


def test_manual_pause_and_resume_keep_success_and_remaining_queue(two_files):
    store, _, runner, job_id, _, destination = first_file(two_files)
    first = next(e for e in store.entries(job_id) if e["kind"] == "file"
                 and e["state"] == "done")
    store.update_job(job_id, pause_requested=True)
    runner.step(job_id)
    assert store.get(job_id)["state"] == "paused"
    assert store.entry(first["id"])["state"] == "done"
    store.update_job(job_id, state="running", pause_requested=False)
    runner.step(job_id)
    assert destination.uploads == 2
    runner.step(job_id)
    assert store.get(job_id)["state"] == "completed"


def test_stop_prevents_next_upload(two_files):
    store, _, runner, job_id, _, destination = first_file(two_files)
    store.request_stop(job_id)
    runner.stop_job(job_id)
    runner.step(job_id)
    assert store.get(job_id)["state"] == "stopped"
    assert destination.uploads == 1


def test_another_source_can_start_while_first_is_active(two_files):
    store, registry, runner, job_id, _, destination = first_file(two_files)
    config = dict(CONFIG, source_email="other@example.org", destination_root="disk:/other")
    other = store.create(config, "independent-source")["id"]
    registry.put(other, config, {"source_client_secret": "s", "destination_token": "d"})
    runner.step(other)
    store.update_job(other, state="running")
    for _ in range(10):
        runner.step(other)
        if destination.uploads == 2:
            break
    assert destination.uploads == 2


def test_accepted_operation_is_polled_and_verified_after_previous_upload(two_files):
    store, _, runner, job_id, _, destination = first_file(two_files)
    second = next(e for e in store.entries(job_id) if e["kind"] == "file"
                  and e["state"] == "planned")
    attempt = store.new_attempt(second["id"], "staged")
    store.update_attempt(attempt["id"], phase="waiting_transfer", operation_id="op-1")
    store.update_entry(second["id"], state="waiting_operation")
    destination.nodes[attempt["remote_path"]] = dict(META, path=attempt["remote_path"])
    destination.operation = "in-progress"
    runner.step(job_id)
    assert stage_events(store, job_id, "poll_transfer")
    assert store.entry(second["id"])["state"] == "waiting_operation"
    destination.operation = "success"
    runner.step(job_id)
    assert store.entry(second["id"])["state"] == "done"
    assert destination.uploads == 1
    runner.step(job_id)
    assert store.get(job_id)["state"] == "completed"


def test_directory_and_last_file_completion_are_not_delayed(two_files):
    store, _, runner, job_id, _, destination = two_files
    second = next(e for e in store.entries(job_id) if e["source_path"].endswith("b.txt"))
    store.update_entry(second["id"], state="skipped_by_user")
    first_file(two_files)
    directory = store.add_entries(job_id, [{
        "kind": "dir", "source_path": "disk:/Docs/later",
        "destination_path": "disk:/owner/Docs/later", "size": 0,
    }], append=True)[0]
    runner.step(job_id)
    assert store.entry(directory)["state"] == "done"
    runner.step(job_id)
    assert store.get(job_id)["state"] == "completed"
    assert destination.uploads == 1
    assert not stage_events(store, job_id, "file_pause")
