"""File pacing never holds the job lock or turns the queue into a manual pause."""

import pytest

from app.file_cooldown import FileCooldown
from tests.fake_clock import FakeClock
from tests import test_migration_runner as fixtures
from tests.test_migration_point_retry import queue, unknown
from tests.test_migration_runner import CONFIG, DIR, META, stage_events


unplanned_migration = fixtures.unplanned_migration
direct_migration = fixtures.direct_migration


@pytest.fixture
def paced(unplanned_migration):
    store, _, runner, job_id, source, _ = unplanned_migration
    source.walk = lambda path: [dict(DIR), dict(META), dict(
        META, path="disk:/Docs/b.txt", name="b.txt",
    )]
    clock = FakeClock()
    runner._file_cooldown = FileCooldown(seconds=20, monotonic=clock.monotonic)
    runner.step(job_id)
    store.update_job(job_id, state="running")
    return unplanned_migration, clock


def first_file(paced):
    migration, _ = paced
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
def test_second_file_waits_twenty_seconds_then_queue_finishes(paced, retry_enabled):
    migration, clock = paced
    store, _, runner, job_id, source, destination = migration
    runner.set_retry_enabled(job_id, retry_enabled)
    first_file(paced)
    before_events = len(store.events(job_id, limit=10000))
    for _ in range(10):
        runner.step(job_id)
    clock.advance(19.999)
    runner.step(job_id)
    assert destination.uploads == source.downloads == 1
    assert len(store.events(job_id, limit=10000)) == before_events + 1
    pauses = stage_events(store, job_id, "file_pause")
    assert len(pauses) == 1
    assert pauses[0]["payload"]["status"] == "waiting"
    assert pauses[0]["payload"]["retry_delay_ms"] == 20000
    assert pauses[0]["stage_label"] == "Пауза между передачами"
    clock.advance(0.001)
    runner.step(job_id)
    assert destination.uploads == source.downloads == 2
    runner.step(job_id)
    assert store.get(job_id)["state"] == "completed"
    assert clock.sleeps == []


def test_manual_pause_and_resume_keep_success_and_remaining_queue(paced):
    _, clock = paced
    store, _, runner, job_id, _, destination = first_file(paced)
    first = next(e for e in store.entries(job_id) if e["kind"] == "file"
                 and e["state"] == "done")
    store.update_job(job_id, pause_requested=True)
    runner.step(job_id)
    assert store.get(job_id)["state"] == "paused"
    assert store.entry(first["id"])["state"] == "done"
    store.update_job(job_id, state="running", pause_requested=False)
    runner.step(job_id)
    assert destination.uploads == 1
    clock.advance(20)
    runner.step(job_id)
    runner.step(job_id)
    assert destination.uploads == 2
    assert store.get(job_id)["state"] == "completed"


def test_stop_cancels_file_wait_immediately(paced):
    store, _, runner, job_id, _, destination = first_file(paced)
    store.request_stop(job_id)
    runner.stop_job(job_id)
    runner.step(job_id)
    assert store.get(job_id)["state"] == "stopped"
    assert runner._file_cooldown.remaining(job_id) == 0
    assert destination.uploads == 1


def test_another_source_does_not_share_the_cooldown(paced):
    store, registry, runner, job_id, _, destination = first_file(paced)
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
    assert runner._file_cooldown.remaining(job_id) == 20
    assert runner._file_cooldown.remaining(other) == 20


def test_accepted_operation_is_polled_and_verified_during_cooldown(paced):
    store, _, runner, job_id, _, destination = first_file(paced)
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


def test_directory_and_last_file_completion_are_not_delayed(paced):
    migration, _ = paced
    store, _, runner, job_id, _, destination = migration
    second = next(e for e in store.entries(job_id) if e["source_path"].endswith("b.txt"))
    store.update_entry(second["id"], state="skipped_by_user")
    first_file(paced)
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


def test_point_retry_transfer_starts_the_same_cooldown(direct_migration):
    store, _, runner, job_id, source, _ = direct_migration
    clock = FakeClock()
    runner._file_cooldown = FileCooldown(seconds=20, monotonic=clock.monotonic)
    entry, attempt = unknown(direct_migration, cache=True)
    queue(direct_migration, entry, attempt)
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] == "done"
    assert runner._file_cooldown.remaining(job_id) == 20
    assert not stage_events(store, job_id, "file_pause")
    assert source.downloads == 0


def test_point_check_of_an_existing_file_does_not_start_cooldown(direct_migration):
    store, _, runner, job_id, _, destination = direct_migration
    clock = FakeClock()
    runner._file_cooldown = FileCooldown(seconds=20, monotonic=clock.monotonic)
    entry, attempt = unknown(direct_migration, cache=True)
    destination.nodes[entry["destination_path"]] = dict(META, path=entry["destination_path"])
    queue(direct_migration, entry, attempt)
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] == "already_verified"
    assert runner._file_cooldown.remaining(job_id) == 0
    assert not stage_events(store, job_id, "file_pause")
