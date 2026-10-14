"""Only active local work reserves the managed worker lane."""

import threading
import time

import pytest

from app.migration.errors import MigrationError
from app.migration.runner import MigrationRunner
from app.migration.worker import MigrationService, enabled
from app.source_slots import SourceSlots
from tests.test_migration_runner import CONFIG, META, Destination, Source


@pytest.mark.parametrize("setting,expected", [
    (None, True), ("", False), (" ", False), ("0", False), (" 0 ", False),
    ("false", False), ("OFF", False), ("fasle", False), ("2", False),
    ("1", True), ("true", True), ("YES", True), ("on", True),
    (" true ", True), (" On ", True),
])
def test_onprem_is_enabled_by_default_with_explicit_disable(monkeypatch, setting, expected):
    if setting is None:
        monkeypatch.delenv("ONPREM_ENABLED", raising=False)
    else:
        monkeypatch.setenv("ONPREM_ENABLED", setting)
    assert enabled() is expected


@pytest.mark.parametrize("setting", ["", "0", " 0 ", "typo"])
def test_disabled_application_lifespan_does_not_create_migration_service(
    tmp_path, monkeypatch, setting,
):
    from fastapi.testclient import TestClient

    import app.main as main

    if setting is None:
        monkeypatch.delenv("ONPREM_ENABLED", raising=False)
    else:
        monkeypatch.setenv("ONPREM_ENABLED", setting)
    root = tmp_path / "disabled-migration-data"
    monkeypatch.setenv("ONPREM_DATA_DIR", str(root))

    def forbid_service(_root, **kwargs):
        pytest.fail("disabled mode must not start a migration service")

    monkeypatch.setattr(main, "MigrationService", forbid_service)
    with TestClient(main.app) as client:
        assert main.app.state.onprem is None
        response = client.get("/api/config")
        assert response.status_code == 200
        assert response.json()["onprem_enabled"] is False
        assert client.get("/api/onprem/jobs").status_code == 404
    assert main.app.state.onprem is None
    assert (root / "journals" / "history.db").is_file()
    assert not (root / "jobs.db").exists()
    assert not (root / "staging").exists()


class ObservedDestination(Destination):
    def __init__(self):
        super().__init__()
        self.started = threading.Event()
        self.polled = threading.Event()
        self.polls = 0

    def upload(self, path, local_path, **kwargs):
        try:
            return super().upload(path, local_path, **kwargs)
        finally:
            self.started.set()

    def poll(self, operation_id):
        self.polls += 1
        self.polled.set()
        return super().poll(operation_id)


def wait_until(condition, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.01)
    pytest.fail("managed worker did not reach the expected observable state")


@pytest.fixture
def managed_worker(tmp_path, monkeypatch, request):
    sources, destinations = {}, {}

    def make_runner(store, registry, **settings):
        return MigrationRunner(
            store, registry,
            source_factory=lambda config, credentials: sources[config["source_email"]],
            destination_factory=lambda config, credentials: destinations[
                config["source_email"]
            ],
            **settings,
        )

    monkeypatch.setattr("app.migration.worker.MigrationRunner", make_runner)
    # Legacy pause/credential scenarios deliberately saturate two lanes.
    slots = (SourceSlots() if getattr(request, "param", None) == "default"
             else SourceSlots(limit=2))
    service = MigrationService(tmp_path, start_worker=True, slots=slots)

    def add_job(name, *, destination=None, start=True, plan=True, shared=False):
        config = dict(CONFIG, source_email=name + "@example.org",
                      destination_root="disk:/" + name)
        if shared:
            config.update(destination_type="shared", destination_vd_hash="archive",
                          destination_base="disk:/",
                          destination_root="disk:/" + config["source_email"])
        source = Source()
        destination = destination or ObservedDestination()
        with service.guard:
            sources[config["source_email"]] = source
            destinations[config["source_email"]] = destination
            job = service.store.create(config, name)
            service.registry.put(job["id"], config, {
                "source_client_secret": "s", "destination_token": "d",
            })
            if plan:
                service.runner.step(job["id"])
                assert service.store.get(job["id"])["state"] == "ready"
            if start:
                service.store.update_job(job["id"], state="running")
        service.notify()
        return job["id"], source, destination

    yield service, add_job
    service.close()
    assert not service.thread.is_alive()
    assert all(destination.imports == 0 for destination in destinations.values())


def test_worker_transfers_two_sources_but_queues_a_third(
    managed_worker,
):
    service, add_job = managed_worker
    first_destination = ObservedDestination()
    first_destination.operation = "in-progress"
    first_id, _, first = add_job("first", destination=first_destination)
    assert first.started.wait(5)
    second_destination = ObservedDestination()
    second_destination.operation = "in-progress"
    second_id, second_source, second = add_job("second", destination=second_destination)
    assert second.started.wait(5)
    third_id, third_source, third = add_job("third")
    wait_until(lambda: first.polls >= 2)
    assert first.uploads == 1
    assert second.uploads == second_source.downloads == 1
    assert third.uploads == third_source.downloads == 0
    assert service.store.get(second_id)["state"] == "running"
    assert service.store.attempts(third_id) == []
    first.operation = "success"
    service.notify()
    assert third.started.wait(5)
    second.operation = "success"
    service.notify()
    wait_until(lambda: service.store.get(second_id)["state"] == "completed")
    assert service.store.get(first_id)["state"] == "completed"
    assert first.uploads == second.uploads == 1
    wait_until(lambda: service.store.get(third_id)["state"] == "completed")


@pytest.mark.parametrize("managed_worker", ["default"], indirect=True)
def test_worker_starts_four_sources_and_queues_fifth(managed_worker):
    service, add_job = managed_worker
    active = []
    try:
        for name in ("first", "second", "third", "fourth"):
            destination = ObservedDestination()
            destination.operation = "in-progress"
            job_id, source, destination = add_job(name, destination=destination)
            active.append((job_id, source, destination))
            assert destination.started.wait(2)
        fifth_id, fifth_source, fifth = add_job("fifth")
        wait_until(lambda: active[0][2].polls >= 2)
        assert len(service.slots.active_owners) == 4
        assert fifth.uploads == fifth_source.downloads == 0
        assert service.store.attempts(fifth_id) == []
        active[0][2].operation = "success"
        service.notify()
        assert fifth.started.wait(2)
    finally:
        for _, _, destination in active:
            destination.operation = "success"
        service.notify()
    wait_until(lambda: all(service.store.get(job_id)["state"] == "completed"
                           for job_id, _, _ in active))
    wait_until(lambda: service.store.get(fifth_id)["state"] == "completed")


@pytest.mark.parametrize("managed_worker", ["default"], indirect=True)
def test_local_list_and_standalone_onprem_share_four_source_cap(managed_worker, tmp_path):
    from app.run_journal import RunJournal
    from app.source_batches.service import BatchService
    from tests.test_source_batches import (
        BODY, CSV, FakeSource, create, finish_plan,
    )

    service, add_job = managed_worker
    downloads = tmp_path / "downloads"
    downloads.mkdir()
    journal = RunJournal(tmp_path)
    batches = BatchService(tmp_path, journal, service, downloads, service.slots,
                           source_factory=FakeSource, start_worker=False)
    release = threading.Event()
    two_local_started = threading.Event()
    lock = threading.Lock()
    local_active = 0

    class BlockingDownload(FakeSource):
        def chunks(self, path):
            nonlocal local_active
            with lock:
                local_active += 1
                if local_active == 2:
                    two_local_started.set()
            assert release.wait(5)
            yield BODY

    active = []
    try:
        detail = finish_plan(batches, create(batches, CSV + "shared,reader@company.ru,local-shared,/\n"))
        batches.source_factory = BlockingDownload
        batches.start_transfer(detail["batch"]["id"])
        batches.start()
        assert two_local_started.wait(2)
        for name in ("onprem-first", "onprem-second"):
            destination = ObservedDestination()
            destination.operation = "in-progress"
            job_id, source, destination = add_job(name, destination=destination)
            active.append((job_id, source, destination))
            assert destination.started.wait(2)
        fifth_id, fifth_source, fifth = add_job("onprem-fifth")
        wait_until(lambda: active[0][2].polls >= 2)
        assert len(service.slots.active_owners) == 4
        assert fifth.uploads == fifth_source.downloads == 0
        batches.stop(detail["batch"]["id"])
        release.set()
        assert fifth.started.wait(2)
        wait_until(lambda: not any(owner.startswith("batch:")
                                   for owner in service.slots.active_owners))
        assert all(source["state"] == "stopped"
                   for source in batches.detail(detail["batch"]["id"])["sources"])
    finally:
        release.set()
        for _, _, destination in active:
            destination.operation = "success"
        service.notify()
        batches.close()
        journal.close()
    wait_until(lambda: service.store.get(fifth_id)["state"] == "completed")


def test_standalone_onprem_uses_same_slots_as_local_saving(managed_worker):
    service, add_job = managed_worker
    destination = ObservedDestination()
    destination.operation = "in-progress"
    with service.slots.lease("local:simulated-download"):
        first_id, _, first = add_job("shared-cap-first", destination=destination)
        assert first.started.wait(5)
        second_id, source, second = add_job("shared-cap-second")
        wait_until(lambda: first.polls >= 2)
        assert second.uploads == source.downloads == 0
    assert second.started.wait(5)
    first.operation = "success"
    service.notify()
    wait_until(lambda: service.store.get(first_id)["state"] == "completed")
    wait_until(lambda: service.store.get(second_id)["state"] == "completed")


def test_historical_unknown_does_not_block_other_transfers_or_fresh_plans(managed_worker):
    service, add_job = managed_worker
    first_destination = ObservedDestination()
    first_destination.upload_error = MigrationError("transfer_outcome_unknown", uncertain=True)
    first_id, _, first = add_job("uncertain", destination=first_destination)
    assert first.started.wait(5)
    wait_until(lambda: service.store.get(first_id)["state"] == "needs_attention")
    before = service.store.attempts(first_id)[0]
    second_id, _, second = add_job("next-job")
    third_id, _, third = add_job("read_only_plan", start=False, plan=False)
    wait_until(lambda: service.store.get(third_id)["state"] == "ready")
    wait_until(lambda: service.store.get(second_id)["state"] == "completed")
    assert service.store.attempts(first_id)[0] == before
    assert first.uploads == second.uploads == 1
    assert third.uploads == 0


def test_missing_credentials_keep_known_operation_lane_reserved(managed_worker):
    service, add_job = managed_worker
    first_destination = ObservedDestination()
    first_destination.operation = "in-progress"
    first_id, _, first = add_job("waiting", destination=first_destination)
    assert first.polled.wait(5)
    with service.guard:
        service.registry.discard(first_id)
        service.store.update_job(first_id, state="awaiting_credentials",
                                 resume_state="running")
    second_id, second_source, second = add_job("no_access_blocked")
    third_id, _, _ = add_job("still_plannable", start=False, plan=False)
    wait_until(lambda: service.store.get(third_id)["state"] == "ready")
    attempt = service.store.attempts(first_id)[0]
    assert attempt["phase"] == "waiting_transfer"
    assert attempt["operation_id"] == "op-1"
    assert service.store.get(second_id)["state"] == "running"
    assert second.uploads == second_source.downloads == 0
    assert first.uploads == 1


@pytest.mark.parametrize("unknown", [False])
@pytest.mark.parametrize("restarted", [False, True])
def test_blocked_job_can_pause_without_remote_mutation_or_releasing_holder(
    managed_worker, unknown, restarted,
):
    service, add_job = managed_worker
    holder = ObservedDestination()
    if unknown:
        holder.upload_error = MigrationError("transfer_outcome_unknown", uncertain=True)
    else:
        holder.operation = "in-progress"
    first_id, _, first = add_job("holder", destination=holder)
    assert first.started.wait(5)
    wait_until(lambda: bool(service.store.attempts(first_id)))
    if unknown:
        wait_until(lambda: service.store.get(first_id)["state"] == "needs_attention")
    else:
        assert first.polled.wait(5)
    second_id, second_source, second = add_job("waiting_to_pause", start=False)
    first_attempt = service.store.attempts(first_id)[0]
    with service.guard, service.job_guard(first_id), service.job_guard(second_id):
        service.store.update_job(second_id, state="running", pause_requested=True)
        if restarted:
            service.store.update_job(first_id, pause_requested=True)
            service.store.recover()
            service.registry.clear()
    service.notify()
    if restarted:
        wait_until(lambda: service.store.get(second_id)["resume_state"] == "paused")
        assert service.store.get(second_id)["state"] == "awaiting_credentials"
        assert service.registry.get(second_id) is None
        assert service.store.get(first_id)["resume_state"] == (
            "needs_attention" if unknown else "running"
        )
    else:
        wait_until(lambda: service.store.get(second_id)["state"] == "paused")
    assert service.store.get(second_id)["namespace_state"] == "unclaimed"
    assert service.store.attempts(second_id) == []
    assert second.nodes == {}
    assert second.uploads == second_source.downloads == 0
    assert service.store.attempts(first_id)[0] == first_attempt
    assert first.uploads == 1
    assert service.holding_job()["id"] == first_id


@pytest.mark.parametrize("phase,blocks", [
    ("prepared", False), ("downloading", False), ("staged", False),
    ("transfer_intent", False), ("unknown_transfer", False),
    ("waiting_transfer", False), ("verifying", False),
    ("move_intent", True), ("waiting_move", True), ("unknown_move", True),
    ("verifying_final", False), ("succeeded", False), ("failed", False),
])
def test_stopped_job_releases_lane_and_preserves_remote_evidence(tmp_path, phase, blocks):
    service = MigrationService(tmp_path, start_worker=False)
    try:
        job = service.store.create(CONFIG, "stop-classification")
        service.store.add_entries(job["id"], [{
            "kind": "file", "source_path": "disk:/Docs/a.txt",
            "destination_path": "disk:/owner/Docs/a.txt", "size": META["size"],
            "md5": META["md5"],
        }])
        entry = service.store.entries(job["id"])[0]
        attempt = service.store.new_attempt(entry["id"], "url")
        service.store.update_attempt(attempt["id"], phase=phase)
        service.store.update_entry(entry["id"], state="outcome_unknown")
        service.store.update_job(job["id"], state="stopped")
        holding = service.holding_job()
        assert holding is None
        assert service.describe_job(service.store.get(job["id"]), holding)[
            "stop_blocked"
        ] is False
        assert "stop_resolvable_attempt_ids" not in service.describe_job(
            service.store.get(job["id"]), holding,
        )
        assert service.store.attempt(attempt["id"])["phase"] == phase
    finally:
        service.close()


@pytest.mark.parametrize("phase,blocks", [
    ("transfer_intent", True), ("waiting_transfer", True), ("unknown_transfer", True),
    ("verifying_final", False), ("failed", False),
])
def test_stopped_direct_upload_releases_lane_without_claiming_remote_outcome(tmp_path, phase, blocks):
    service = MigrationService(tmp_path, start_worker=False)
    try:
        job = service.store.create(CONFIG, "direct-stop-classification")
        service.store.add_entries(job["id"], [{
            "kind": "file", "source_path": META["path"],
            "destination_path": "disk:/owner/Docs/a.txt", "size": META["size"],
            "md5": META["md5"],
        }])
        entry = service.store.entries(job["id"])[0]
        attempt = service.store.new_attempt(entry["id"], "url")
        with service.store._connection(write=True) as connection:
            connection.execute("UPDATE jobs SET remote_root='' WHERE id=?", (job["id"],))
            connection.execute("UPDATE attempts SET remote_path=? WHERE id=?",
                               (entry["destination_path"], attempt["id"]))
        service.store.update_attempt(attempt["id"], phase=phase)
        service.store.update_entry(entry["id"], state="outcome_unknown")
        service.store.update_job(job["id"], state="stopped")
        holding = service.holding_job()
        described = service.describe_job(service.store.get(job["id"]), holding)
        assert holding is None
        assert described["stop_blocked"] is False
        assert "stop_resolvable_attempt_ids" not in described
        assert described["remote_pending"] is blocks
    finally:
        service.close()


def test_stopped_job_keeps_historical_final_move_without_holding_lane(tmp_path):
    service = MigrationService(tmp_path, start_worker=False)
    try:
        job = service.store.create(CONFIG, "historical-move")
        service.store.add_entries(job["id"], [{
            "kind": "file", "source_path": "disk:/Docs/a.txt",
            "destination_path": "disk:/owner/Docs/a.txt", "size": META["size"],
            "md5": META["md5"],
        }])
        entry = service.store.entries(job["id"])[0]
        old = service.store.new_attempt(entry["id"], "url")
        service.store.update_attempt(old["id"], phase="unknown_move")
        latest = service.store.new_attempt(entry["id"], "url")
        service.store.update_attempt(latest["id"], phase="succeeded")
        service.store.update_entry(entry["id"], state="done")
        service.store.update_job(job["id"], state="stopped")
        assert service.holding_job() is None
        assert service.store.attempt(old["id"])["phase"] == "unknown_move"
    finally:
        service.close()


def test_explicit_stop_releases_unknown_upload_and_never_finishes_old_file(
    managed_worker,
):
    service, add_job = managed_worker
    destination = ObservedDestination()
    destination.upload_error = MigrationError(
        "transfer_outcome_unknown", uncertain=True,
    )
    first_id, _, first = add_job("stop-unknown-upload", destination=destination)
    assert first.started.wait(5)
    wait_until(lambda: service.store.get(first_id)["state"] == "needs_attention")
    attempt = service.store.attempts(first_id)[0]
    service.store.request_stop(first_id)
    service.notify()
    wait_until(lambda: service.store.get(first_id)["state"] == "stopped")
    # The stopped state is committed before the guarded client cleanup completes.
    with service.guard:
        assert service.registry.get(first_id) is None
    assert service.store.attempts(first_id)[0] == attempt
    first.upload_error = None
    first.nodes[attempt["remote_path"]] = dict(META)
    second_id, _, second = add_job("after-stopped-upload")
    assert second.started.wait(5)
    wait_until(lambda: service.store.get(second_id)["state"] == "completed")
    assert first.moves == 0
    assert first.uploads == 1
    assert service.store.attempts(first_id)[0] == attempt


def test_stop_is_accepted_during_inflight_upload_and_acknowledged_after_response(
    managed_worker,
):
    service, add_job = managed_worker
    entered, release = threading.Event(), threading.Event()
    destination = ObservedDestination()
    original = destination.upload

    def upload_waiting(path, local_path, **kwargs):
        before_upload = kwargs.pop("before_upload", None)
        if before_upload:
            before_upload("op-1")
        entered.set()
        assert release.wait(5)
        return original(path, local_path, **kwargs)

    destination.upload = upload_waiting
    first_id, _, first = add_job("inflight-stop", destination=destination)
    try:
        assert entered.wait(5)
        assert not service.job_guard(first_id).acquire(blocking=False)
        service.store.request_stop(first_id)
        service.notify()
        assert service.store.get(first_id)["stop_requested"] is True
        assert service.store.get(first_id)["state"] != "stopped"
    finally:
        release.set()
    wait_until(lambda: service.store.get(first_id)["state"] == "stopped")
    attempt = service.store.attempts(first_id)[0]
    assert attempt["phase"] == "waiting_transfer"
    assert attempt["operation_id"] == "op-1"
    assert first.uploads == 1
    assert first.polls == first.moves == 0
    assert service.holding_job() is None


def test_stopped_unknown_move_does_not_block_fresh_transfer(
    managed_worker,
):
    service, add_job = managed_worker
    destination = ObservedDestination()
    destination.move_error = MigrationError("move_outcome_unknown", uncertain=True)
    first_id, _, first = add_job("stop-unknown-move", destination=destination)
    wait_until(lambda: service.store.get(first_id)["state"] == "needs_attention")
    service.store.request_stop(first_id)
    service.notify()
    wait_until(lambda: service.store.get(first_id)["state"] == "stopped")
    second_id, second_source, second = add_job(
        "after-stopped-move", start=False, plan=False,
    )
    wait_until(lambda: service.store.get(second_id)["state"] == "ready")
    with service.guard:
        service.store.update_job(second_id, state="running")
    service.notify()
    wait_until(lambda: service.store.get(second_id)["state"] == "completed")
    assert service.describe_job(service.store.get(second_id), service.holding_job())[
        "blocked_by"
    ] is None
    assert second.uploads == 1
    assert first.moves == 1


@pytest.mark.parametrize("shared", [False, True])
def test_stopped_legacy_waiting_operation_never_polls_in_background(managed_worker, shared):
    service, add_job = managed_worker
    destination = ObservedDestination()
    destination.operation = "in-progress"
    first_id, _, first = add_job("legacy-stop", destination=destination, shared=shared)
    assert first.polled.wait(5)
    with service.guard, service.job_guard(first_id):
        service.store.update_job(first_id, state="stopped")
        attempt = service.store.attempts(first_id)[0]
        # Model a persisted URL operation without submitting a new URL import.
        with service.store._connection(write=True) as connection:
            connection.execute("UPDATE attempts SET strategy='url' WHERE id=?", (attempt["id"],))
        before = service.store.attempts(first_id)[0]
        polls = first.polls
    service.notify()
    second_id, _, second = add_job("after-legacy-stop")
    wait_until(lambda: service.store.get(second_id)["state"] == "completed")
    assert second.uploads == 1
    assert first.polls == polls
    assert service.store.attempts(first_id)[0] == before
    assert service.registry.get(first_id) is None
