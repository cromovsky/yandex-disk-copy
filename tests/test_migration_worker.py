"""The managed worker reserves one lane for accepted or uncertain operations."""

import threading
import time

import pytest

from app.migration.errors import MigrationError
from app.migration.runner import MigrationRunner
from app.migration.worker import MigrationService, enabled
from tests.test_migration_runner import CONFIG, META, Destination, Source


@pytest.mark.parametrize("setting,expected", [
    (None, False), ("", False), (" ", False), ("0", False), (" 0 ", False),
    ("false", False), ("OFF", False), ("fasle", False), ("2", False),
    ("1", True), ("true", True), ("YES", True), ("on", True),
    (" true ", True), (" On ", True),
])
def test_onprem_requires_explicit_opt_in(monkeypatch, setting, expected):
    if setting is None:
        monkeypatch.delenv("ONPREM_ENABLED", raising=False)
    else:
        monkeypatch.setenv("ONPREM_ENABLED", setting)
    assert enabled() is expected


@pytest.mark.parametrize("setting", [None, "", "0", " 0 ", "typo"])
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

    def forbid_service(_root):
        pytest.fail("disabled mode must not start a migration service")

    monkeypatch.setattr(main, "MigrationService", forbid_service)
    with TestClient(main.app) as client:
        assert main.app.state.onprem is None
        response = client.get("/api/config")
        assert response.status_code == 200
        assert response.json()["onprem_enabled"] is False
        assert client.get("/api/onprem/jobs").status_code == 404
    assert main.app.state.onprem is None
    assert not root.exists()


class ObservedDestination(Destination):
    def __init__(self):
        super().__init__()
        self.started = threading.Event()
        self.polled = threading.Event()
        self.polls = 0

    def import_url(self, path, url):
        try:
            return super().import_url(path, url)
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
def managed_worker(tmp_path, monkeypatch):
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
    service = MigrationService(tmp_path, start_worker=True)

    def add_job(name, *, destination=None, start=True, plan=True):
        config = dict(CONFIG, source_email=name + "@example.org",
                      destination_root="disk:/" + name)
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


def test_worker_does_not_send_other_job_while_first_operation_is_in_progress(
    managed_worker,
):
    service, add_job = managed_worker
    first_destination = ObservedDestination()
    first_destination.operation = "in-progress"
    first_id, _, first = add_job("first", destination=first_destination)
    assert first.started.wait(5)
    second_id, second_source, second = add_job("second")
    wait_until(lambda: first.polls >= 2)
    assert first.imports == 1
    assert second.imports == second_source.downloads == 0
    assert service.store.get(second_id)["state"] == "running"
    assert service.store.attempts(second_id) == []
    first.operation = "success"
    service.notify()
    assert second.started.wait(5)
    wait_until(lambda: service.store.get(second_id)["state"] == "completed")
    assert service.store.get(first_id)["state"] == "completed"
    assert first.imports == second.imports == 1


def test_unknown_operation_blocks_other_transfers_but_worker_can_plan_jobs(
    managed_worker,
):
    service, add_job = managed_worker
    first_destination = ObservedDestination()
    first_destination.import_error = MigrationError(
        "transfer_outcome_unknown", uncertain=True,
    )
    first_id, _, first = add_job("uncertain", destination=first_destination)
    assert first.started.wait(5)
    wait_until(lambda: service.store.get(first_id)["state"] == "needs_attention")
    second_id, second_source, second = add_job("blocked")
    third_id, _, third = add_job("read_only_plan", start=False, plan=False)
    wait_until(lambda: service.store.get(third_id)["state"] == "ready")
    assert service.store.get(second_id)["state"] == "running"
    assert service.store.attempts(second_id) == []
    assert second.imports == second_source.downloads == third.imports == 0
    assert first.imports == 1
    with service.guard:
        attempt = service.store.attempts(first_id)[0]
        service.store.update_attempt(
            attempt["id"], phase="failed", error="remote_operation_failed",
        )
        service.store.update_entry(
            attempt["entry_id"], state="failed", error="remote_operation_failed",
        )
        service.store.update_job(first_id, state="completed_with_issues")
    service.notify()
    assert second.started.wait(5)
    wait_until(lambda: service.store.get(second_id)["state"] == "completed")
    assert first.imports == 1
    assert second.imports == 1


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
    assert second.imports == second_source.downloads == 0
    assert first.imports == 1


@pytest.mark.parametrize("unknown", [False, True])
@pytest.mark.parametrize("restarted", [False, True])
def test_blocked_job_can_pause_without_remote_mutation_or_releasing_holder(
    managed_worker, unknown, restarted,
):
    service, add_job = managed_worker
    holder = ObservedDestination()
    if unknown:
        holder.import_error = MigrationError("transfer_outcome_unknown", uncertain=True)
    else:
        holder.operation = "in-progress"
    first_id, _, first = add_job("holder", destination=holder)
    assert first.started.wait(5)
    wait_until(lambda: bool(service.store.attempts(first_id)))
    if unknown:
        wait_until(lambda: service.store.get(first_id)["state"] == "needs_attention")
    else:
        assert first.polled.wait(5)
    second_id, second_source, second = add_job("waiting_to_pause")
    first_attempt = service.store.attempts(first_id)[0]
    with service.guard:
        service.store.update_job(second_id, pause_requested=True)
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
    assert second.imports == second_source.downloads == 0
    assert service.store.attempts(first_id)[0] == first_attempt
    assert first.imports == 1
    assert service.holding_job()["id"] == first_id


@pytest.mark.parametrize("phase,blocks", [
    ("prepared", False), ("downloading", False), ("staged", False),
    ("transfer_intent", False), ("unknown_transfer", False),
    ("waiting_transfer", False), ("verifying", False),
    ("move_intent", True), ("waiting_move", True), ("unknown_move", True),
    ("verifying_final", False), ("succeeded", False), ("failed", False),
])
def test_stopped_job_reserves_only_unresolved_final_moves(tmp_path, phase, blocks):
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
        assert (holding is not None) is blocks
        assert service.describe_job(service.store.get(job["id"]), holding)[
            "stop_blocked"
        ] is blocks
        assert service.describe_job(service.store.get(job["id"]), holding)[
            "stop_resolvable_attempt_ids"
        ] == ([attempt["id"]] if blocks else [])
        assert service.store.attempt(attempt["id"])["phase"] == phase
    finally:
        service.close()


def test_stopped_job_checks_historical_final_move_not_only_current_entry(tmp_path):
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
        assert service.holding_job()["id"] == job["id"]
    finally:
        service.close()


def test_explicit_stop_releases_unknown_import_and_never_finishes_old_file(
    managed_worker,
):
    service, add_job = managed_worker
    destination = ObservedDestination()
    destination.import_error = MigrationError(
        "transfer_outcome_unknown", uncertain=True,
    )
    first_id, _, first = add_job("stop-unknown-import", destination=destination)
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
    first.import_error = None
    first.nodes[attempt["remote_path"]] = dict(META)
    second_id, _, second = add_job("after-stopped-import")
    assert second.started.wait(5)
    wait_until(lambda: service.store.get(second_id)["state"] == "completed")
    assert first.moves == 0
    assert first.imports == 1
    assert service.store.attempts(first_id)[0] == attempt


def test_stop_is_accepted_during_inflight_import_and_acknowledged_after_response(
    managed_worker,
):
    service, add_job = managed_worker
    entered, release = threading.Event(), threading.Event()
    destination = ObservedDestination()
    original = destination.import_url

    def import_waiting(path, url):
        entered.set()
        assert release.wait(5)
        return original(path, url)

    destination.import_url = import_waiting
    first_id, _, first = add_job("inflight-stop", destination=destination)
    try:
        assert entered.wait(5)
        assert not service.guard.acquire(blocking=False)
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
    assert first.moves == 0
    assert service.holding_job() is None


def test_stopped_unknown_move_holds_lane_but_does_not_starve_fresh_plan(
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
    assert service.describe_job(service.store.get(second_id), service.holding_job())[
        "blocked_by"
    ]["job_id"] == first_id
    assert service.store.attempts(second_id) == []
    assert second.imports == second_source.downloads == 0
    assert first.moves == 1
