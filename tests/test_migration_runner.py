"""Durable migration: prove outcomes before success or replay."""

import hashlib
from pathlib import Path

import pytest

from app.migration.credentials import CredentialRegistry
from app.migration.errors import MigrationError
from app.migration.runner import MigrationRunner
from app.migration.store import MigrationStore


CONFIG = {
    "source_org_id": "org", "source_client_id": "client",
    "source_email": "owner@example.org", "source_path": "disk:/Docs",
    "destination_url": "https://disk.example.org/v1/disk",
    "destination_identity": "receiver", "destination_root": "disk:/owner",
    "strategy": "url", "identity_confirmed": True,
}
BODY = b"a file\x00with data"
META = {
    "path": "disk:/Docs/a.txt", "name": "a.txt", "type": "file",
    "size": len(BODY), "md5": hashlib.md5(BODY).hexdigest(),
    "modified": "2026-10-11T10:00:00Z", "resource_id": "file-1",
}
DIR = {"path": "disk:/Docs", "name": "Docs", "type": "dir"}


class Source:
    def __init__(self):
        self.metadata = dict(META)
        self.downloads = 0

    def stat(self, path):
        return dict(DIR if path == "disk:/Docs" else self.metadata)

    def walk(self, path):
        return [dict(DIR), dict(self.metadata), {
            "path": "disk:/Docs/empty", "name": "empty", "type": "dir",
        }]

    def download_url(self, path):
        self.downloads += 1
        return "https://signed.example.org/file?secret=never-persist"

    def chunks(self, path):
        self.downloads += 1
        yield BODY[:4]
        yield BODY[4:]

    def close(self):
        pass


class Destination:
    def __init__(self):
        self.nodes = {}
        self.imports = 0
        self.moves = 0
        self.operation = "success"
        self.import_error = None
        self.move_error = None
        self.upload_body = None

    def info(self):
        return {"total_space": 1000000, "used_space": 0,
                "user": {"login": "receiver"}}

    def stat(self, path):
        return self.nodes.get(path)

    def ensure_dir(self, path):
        if path in self.nodes and self.nodes[path]["type"] != "dir":
            raise MigrationError("destination_conflict")
        self.nodes[path] = {"type": "dir", "path": path}

    def claim_dir(self, path):
        if path in self.nodes:
            raise MigrationError("technical_namespace_exists")
        self.nodes[path] = {"type": "dir", "path": path}

    def import_url(self, path, url):
        self.imports += 1
        if self.import_error:
            raise self.import_error
        self.nodes[path] = dict(META, path=path)
        return "op-1"

    def upload(self, path, local_path):
        self.upload_body = Path(local_path).read_bytes()
        self.nodes[path] = dict(META, path=path)
        return None

    def poll(self, ref):
        assert ref == "op-1"
        return self.operation

    def move(self, src, dst):
        self.moves += 1
        if self.move_error:
            raise self.move_error
        if dst in self.nodes:
            raise MigrationError("destination_conflict")
        self.nodes[dst] = dict(self.nodes.pop(src), path=dst)
        return None

    def close(self):
        pass


@pytest.fixture
def migration(tmp_path):
    store = MigrationStore(tmp_path)
    registry = CredentialRegistry()
    source, destination = Source(), Destination()
    runner = MigrationRunner(
        store, registry, source_factory=lambda c, a: source,
        destination_factory=lambda c, a: destination,
        reserve_bytes=0,
    )
    job = store.create(CONFIG, "create-once")
    registry.put(job["id"], CONFIG, {
        "source_client_secret": "source-secret",
        "destination_token": "destination-secret",
    })
    runner.step(job["id"])
    assert store.get(job["id"])["state"] == "ready"
    yield store, registry, runner, job["id"], source, destination
    store.close()


def finish(store, runner, job_id):
    store.update_job(job_id, state="running")
    for _ in range(20):
        runner.step(job_id)
        if store.get(job_id)["state"] != "running":
            break
    return store.get(job_id)


@pytest.fixture
def unplanned_migration(tmp_path):
    store = MigrationStore(tmp_path)
    registry = CredentialRegistry()
    source, destination = Source(), Destination()
    runner = MigrationRunner(
        store, registry, source_factory=lambda c, a: source,
        destination_factory=lambda c, a: destination, reserve_bytes=0,
    )
    job = store.create(CONFIG, "unplanned")
    registry.put(job["id"], CONFIG, {
        "source_client_secret": "s", "destination_token": "d",
    })
    yield store, registry, runner, job["id"], source, destination
    runner.close()
    store.close()


def start_accepted_operation(store, runner, job_id, destination):
    destination.operation = "in-progress"
    store.update_job(job_id, state="running")
    for _ in range(10):
        runner.step(job_id)
        if store.attempts(job_id):
            break
    attempts = store.attempts(job_id)
    assert len(attempts) == 1
    assert attempts[0]["operation_id"] == "op-1"
    return attempts[0]


def test_url_mode_preserves_folder_and_empty_dirs_without_local_bytes(migration):
    store, _, runner, job_id, source, dst = migration
    job = finish(store, runner, job_id)
    assert job["state"] == "completed"
    assert dst.nodes["disk:/owner/Docs/a.txt"]["md5"] == META["md5"]
    assert dst.nodes["disk:/owner/Docs/empty"]["type"] == "dir"
    assert dst.imports == 1
    assert source.downloads == 1
    assert not list((Path(store.root) / "staging").glob("*"))


def run_until_attempt(store, runner, job_id):
    store.update_job(job_id, state="running")
    for _ in range(10):
        runner.step(job_id)
        if store.attempts(job_id):
            return store.attempts(job_id)[0]
    pytest.fail("file attempt was not reached")


def test_stop_after_import_response_preserves_reference_without_poll_or_move(
    migration,
):
    store, _, runner, job_id, _, dst = migration
    original = dst.import_url

    def import_and_stop(path, url):
        operation = original(path, url)
        store.request_stop(job_id)
        return operation

    dst.import_url = import_and_stop
    dst.poll = lambda operation: pytest.fail("stopped import must not be polled")
    attempt = run_until_attempt(store, runner, job_id)
    assert attempt["phase"] == "waiting_transfer"
    assert attempt["operation_id"] == "op-1"
    assert store.get(job_id)["stop_requested"] is True
    for _ in range(3):
        runner.step(job_id)
    assert dst.imports == 1
    assert dst.moves == 0
    assert "disk:/owner/Docs/a.txt" not in dst.nodes


def test_stop_after_successful_poll_prevents_final_move(migration):
    store, _, runner, job_id, _, dst = migration
    attempt = start_accepted_operation(store, runner, job_id, dst)

    def poll_and_stop(operation):
        store.request_stop(job_id)
        return "success"

    dst.poll = poll_and_stop
    runner.step(job_id)
    assert store.attempt(attempt["id"])["phase"] == "verifying"
    assert dst.moves == 0
    assert "disk:/owner/Docs/a.txt" not in dst.nodes


def test_stop_during_staged_download_never_uploads_partial_file(migration):
    store, _, runner, job_id, source, dst = migration
    runner.default_strategy = "staged"
    consumed = []

    def chunks(path):
        consumed.append("first")
        yield BODY[:4]
        store.request_stop(job_id)
        consumed.append("last")
        yield BODY[4:]
        pytest.fail("download must stop at the next chunk boundary")

    source.chunks = chunks
    attempt = run_until_attempt(store, runner, job_id)
    assert attempt["phase"] == "downloading"
    local = Path(store.root) / "staging" / attempt["local_name"]
    assert local.read_bytes() == BODY[:4]
    assert dst.upload_body is None
    assert dst.moves == 0
    assert consumed == ["first", "last"]
    assert runner.cleanup(job_id)["removed"] == 0
    assert local.exists()


def test_stop_during_lost_move_records_unknown_without_resurrecting_job(migration):
    store, _, runner, job_id, _, dst = migration

    def move_and_stop(src, target):
        dst.moves += 1
        store.request_stop(job_id)
        raise MigrationError("move_outcome_unknown", uncertain=True)

    dst.move = move_and_stop
    attempt = run_until_attempt(store, runner, job_id)
    assert attempt["phase"] == "unknown_move"
    assert store.entry(attempt["entry_id"])["state"] == "outcome_unknown"
    assert store.get(job_id)["stop_requested"] is True
    runner.step(job_id)
    assert dst.moves == 1


def test_stop_during_auth_failure_keeps_request_durable(migration):
    store, registry, runner, job_id, _, dst = migration

    def import_and_stop(path, url):
        store.request_stop(job_id)
        raise MigrationError("credentials_required", auth=True)

    dst.import_url = import_and_stop
    attempt = run_until_attempt(store, runner, job_id)
    assert attempt["phase"] == "transfer_intent"
    assert store.get(job_id)["stop_requested"] is True
    assert registry.get(job_id) is None
    runner.step(job_id)
    assert dst.moves == 0


def test_stop_during_planning_does_not_publish_ready_plan(unplanned_migration):
    store, _, runner, job_id, source, dst = unplanned_migration
    original = source.walk

    def walk_and_stop(path):
        nodes = original(path)
        yield nodes[0]
        store.request_stop(job_id)
        yield nodes[1]

    source.walk = walk_and_stop
    runner.step(job_id)
    assert store.get(job_id)["stop_requested"] is True
    assert store.get(job_id)["state"] != "ready"
    assert store.entries(job_id) == []
    assert dst.nodes == {}


@pytest.mark.parametrize("status,phase", [
    ("in-progress", "waiting_move"), ("success", "verifying_final"),
    ("failed", "failed"),
])
def test_stopped_final_move_can_reconcile_read_only(migration, status, phase):
    store, _, runner, job_id, source, dst = migration
    attempt = start_accepted_operation(store, runner, job_id, dst)
    store.update_attempt(attempt["id"], phase="waiting_move")
    store.update_entry(attempt["entry_id"], state="committing")
    store.update_job(job_id, state="stopped")
    before = dict(dst.nodes)
    downloads = source.downloads
    dst.operation = status
    runner.reconcile_stopped_move(job_id)
    assert store.get(job_id)["state"] == "stopped"
    assert store.attempt(attempt["id"])["phase"] == phase
    assert store.attempt(attempt["id"])["operation_id"] == "op-1"
    assert dst.moves == 0
    assert dst.nodes == before
    assert source.downloads == downloads


def test_stopped_final_move_after_restart_only_constructs_destination(migration):
    store, registry, runner, job_id, _, dst = migration
    attempt = start_accepted_operation(store, runner, job_id, dst)
    store.update_attempt(attempt["id"], phase="waiting_move")
    store.update_entry(attempt["entry_id"], state="committing")
    store.update_job(job_id, state="stopped")
    runner.close()
    runner.source_factory = lambda config, credentials: pytest.fail(
        "stopped polling must not construct SaaS source",
    )
    dst.operation = "success"
    runner.reconcile_stopped_move(job_id)
    assert store.attempt(attempt["id"])["phase"] == "verifying_final"
    assert store.get(job_id)["state"] == "stopped"
    assert registry.get(job_id) is None
    assert dst.moves == 0


def test_destination_only_client_is_cached_rotated_and_closed(migration):
    _, registry, runner, job_id, source, dst = migration
    job = runner.store.get(job_id)
    runner.close()
    closed = []
    dst.close = lambda: closed.append("destination")
    source.close = lambda: closed.append("source")
    created = []
    runner.destination_factory = lambda config, credentials: (
        created.append("destination") or dst
    )
    runner.source_factory = lambda config, credentials: (
        created.append("source") or source
    )
    assert runner.destination_client(job) is dst
    assert runner.destination_client(job) is dst
    assert created == ["destination"]
    registry.put(job_id, CONFIG, {
        "source_client_secret": "new-source", "destination_token": "new-token",
    })
    assert runner.destination_client(job) is dst
    assert closed == ["destination"]
    assert created == ["destination", "destination"]
    assert runner.clients(job) == (source, dst)
    assert created == ["destination", "destination", "source"]
    runner.release_clients(job_id)
    assert closed == ["destination", "source", "destination"]
    assert registry.get(job_id) is None


def test_stopped_unknown_import_is_never_reconciled_or_finalized(migration):
    store, _, runner, job_id, _, dst = migration
    dst.import_error = MigrationError("transfer_outcome_unknown", uncertain=True)
    assert finish(store, runner, job_id)["state"] == "needs_attention"
    attempt = store.attempts(job_id)[0]
    store.update_job(job_id, state="stopped")
    dst.import_error = None
    dst.nodes[attempt["remote_path"]] = dict(META)
    runner.reconcile_stopped_move(job_id)
    runner.step(job_id)
    assert store.attempt(attempt["id"]) == attempt
    assert dst.moves == 0
    assert dst.imports == 1


def test_fresh_restart_checks_existing_files_without_retransmission(migration):
    store, registry, runner, job_id, _, dst = migration
    assert finish(store, runner, job_id)["state"] == "completed"
    child = store.restart(job_id, CONFIG, "fresh-plan-after-success")
    assert child["remote_root"] != store.get(job_id)["remote_root"]
    registry.put(child["id"], CONFIG, {
        "source_client_secret": "s", "destination_token": "d",
    })
    runner.step(child["id"])
    result = finish(store, runner, child["id"])
    assert result["state"] == "completed"
    assert result["counts"] == {"already_verified": 1}
    assert dst.imports == dst.moves == 1
    assert store.get(job_id)["state"] == "completed"


def test_fresh_restart_never_reuses_abandoned_unknown_attempt(migration):
    store, registry, runner, job_id, _, dst = migration
    dst.import_error = MigrationError("transfer_outcome_unknown", uncertain=True)
    assert finish(store, runner, job_id)["state"] == "needs_attention"
    attempt = store.attempts(job_id)[0]
    store.request_stop(job_id)
    runner.stop_job(job_id)
    child = store.restart(job_id, CONFIG, "fresh-plan-after-stop")
    registry.put(child["id"], CONFIG, {
        "source_client_secret": "s", "destination_token": "d",
    })
    dst.import_error = None
    runner.step(child["id"])
    assert finish(store, runner, child["id"])["state"] == "completed"
    assert store.get(job_id)["state"] == "stopped"
    assert store.attempt(attempt["id"]) == attempt
    latest = store.attempts(child["id"])[0]
    assert latest["remote_path"].startswith(child["remote_root"] + "/")
    assert latest["remote_path"] != attempt["remote_path"]
    assert dst.imports == 2
    assert dst.moves == 1


def test_stopped_destination_validation_does_not_require_original_saas_source(
    migration,
):
    _, _, runner, _, _, dst = migration
    runner.source_factory = lambda config, credentials: pytest.fail(
        "stopped final-move recovery must not require SaaS",
    )
    result = runner.preflight_destination(CONFIG, {
        "source_client_secret": "no-longer-valid", "destination_token": "valid",
    })
    assert result["identity"] == "receiver"
    assert dst.imports == dst.moves == 0


def test_staged_stream_verifies_and_removes_only_own_local_file(migration):
    store, _, runner, job_id, _, dst = migration
    runner.default_strategy = "staged"
    sentinel = Path(store.root) / "staging" / "unrelated.part"
    sentinel.parent.mkdir(exist_ok=True)
    sentinel.write_bytes(b"keep")
    assert finish(store, runner, job_id)["state"] == "completed"
    assert dst.upload_body == BODY
    assert dst.imports == 0
    assert sentinel.read_bytes() == b"keep"
    assert list(sentinel.parent.iterdir()) == [sentinel]


def test_lost_import_response_never_reimports_even_after_restart(migration):
    store, registry, runner, job_id, _, dst = migration
    dst.import_error = MigrationError("transfer_outcome_unknown", uncertain=True)
    assert finish(store, runner, job_id)["state"] == "needs_attention"
    assert dst.imports == 1
    store.recover()
    registry.clear()
    registry.put(job_id, CONFIG, {
        "source_client_secret": "s", "destination_token": "d",
    })
    store.update_job(job_id, state="running")
    runner.step(job_id)
    assert store.get(job_id)["state"] == "needs_attention"
    assert dst.imports == 1
    assert any(e["state"] == "outcome_unknown"
               for e in store.entries(job_id))


def test_known_operation_is_polled_not_resubmitted_after_restart(migration):
    store, _, runner, job_id, _, dst = migration
    dst.operation = "in-progress"
    store.update_job(job_id, state="running")
    for _ in range(4):
        runner.step(job_id)
    assert dst.imports == 1
    assert dst.moves == 0
    store.recover()
    store.update_job(job_id, state="running")
    dst.operation = "success"
    for _ in range(10):
        runner.step(job_id)
    assert store.get(job_id)["state"] == "completed"
    assert dst.imports == 1


def test_changed_source_is_not_skipped_as_matching_old_destination(migration):
    store, _, runner, job_id, src, dst = migration
    dst.nodes["disk:/owner/Docs/a.txt"] = dict(META)
    src.metadata["modified"] = "2026-10-11T11:00:00Z"
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    file = next(e for e in store.entries(job_id) if e["kind"] == "file")
    assert file["state"] == "source_changed"
    assert dst.imports == 0


def test_conflicting_final_file_is_never_overwritten(migration):
    store, _, runner, job_id, _, dst = migration
    dst.nodes["disk:/owner/Docs/a.txt"] = dict(META, md5="f" * 32)
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert dst.nodes["disk:/owner/Docs/a.txt"]["md5"] == "f" * 32
    assert dst.imports == 0
    assert dst.moves == 0


def test_missing_destination_hash_cannot_be_reported_as_success(migration):
    store, _, runner, job_id, _, dst = migration
    original = dst.import_url

    def import_without_hash(path, url):
        operation = original(path, url)
        dst.nodes[path].pop("md5")
        return operation

    dst.import_url = import_without_hash
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert dst.moves == 0
    assert any(e["state"] == "verification_unavailable"
               for e in store.entries(job_id))


def test_pause_keeps_polling_already_accepted_operation(migration):
    store, _, runner, job_id, _, dst = migration
    dst.operation = "in-progress"
    store.update_job(job_id, state="running")
    for _ in range(4):
        runner.step(job_id)
    store.update_job(job_id, pause_requested=True)
    runner.step(job_id)
    assert store.get(job_id)["state"] == "running"
    dst.operation = "success"
    for _ in range(5):
        runner.step(job_id)
    assert store.get(job_id)["state"] == "paused"
    assert dst.moves == 1


def test_lost_move_response_does_not_infer_success_from_final_file(migration):
    store, _, runner, job_id, _, dst = migration
    original = dst.move

    def lost(src, final):
        original(src, final)
        raise MigrationError("transfer_outcome_unknown", uncertain=True)

    dst.move = lost
    assert finish(store, runner, job_id)["state"] == "needs_attention"
    assert "disk:/owner/Docs/a.txt" in dst.nodes
    store.update_job(job_id, state="running")
    runner.step(job_id)
    assert store.get(job_id)["state"] == "needs_attention"
    assert dst.moves == 1


def test_staging_budget_refuses_file_before_download(migration):
    store, _, runner, job_id, src, dst = migration
    runner.default_strategy = "staged"
    runner.staging_budget = 1
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert src.downloads == 0
    assert dst.upload_body is None


def test_final_hash_is_rechecked_after_move(migration):
    store, _, runner, job_id, _, dst = migration
    original = dst.move

    def move_corrupt(src, final):
        original(src, final)
        dst.nodes[final]["md5"] = "0" * 32

    dst.move = move_corrupt
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert any(e["state"] == "failed" for e in store.entries(job_id))


def test_lost_directory_creation_response_can_reconcile_without_orphan_unknown(
    migration,
):
    store, _, runner, job_id, _, destination = migration
    original = destination.ensure_dir
    calls = []

    def lose_once(path):
        original(path)
        if not calls:
            calls.append(path)
            raise MigrationError("destination_unavailable", uncertain=True)

    destination.ensure_dir = lose_once
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert not any(e["state"] == "outcome_unknown"
                   for e in store.entries(job_id))
    runner.retry(job_id, "url")
    for _ in range(10):
        runner.step(job_id)
    assert store.get(job_id)["state"] == "completed"
    assert destination.imports == 1


def test_unknown_attempt_directory_mkdir_is_not_unknown_file_import(migration):
    store, _, runner, job_id, _, destination = migration
    original = destination.ensure_dir
    failed = []

    def lose_once(path):
        original(path)
        if path.startswith("disk:/.disk-copy-transfer-") and not failed:
            failed.append(path)
            raise MigrationError("destination_unavailable", uncertain=True)

    destination.ensure_dir = lose_once
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert destination.imports == 0
    runner.retry(job_id, "url")
    for _ in range(10):
        runner.step(job_id)
    assert store.get(job_id)["state"] == "completed"
    assert destination.imports == 1


@pytest.mark.parametrize("strategy", ["url", "staged"])
def test_crash_after_success_intent_before_entry_commit_reconciles_without_replay(
    migration, monkeypatch, strategy,
):
    store, _, runner, job_id, _, dst = migration
    runner.default_strategy = strategy
    original = store.update_entry

    def crash_before_file_done(entry_id, **fields):
        if fields.get("state") == "done" and store.entry(entry_id)["kind"] == "file":
            raise SystemExit("simulated_process_exit")
        return original(entry_id, **fields)

    monkeypatch.setattr(store, "update_entry", crash_before_file_done)
    with pytest.raises(SystemExit):
        finish(store, runner, job_id)
    assert store.attempts(job_id)[0]["phase"] == "succeeded"
    assert store.entries(job_id, state="committing")
    monkeypatch.setattr(store, "update_entry", original)
    store.recover()
    recovered = finish(store, runner, job_id)
    assert recovered["state"] == "completed"
    assert recovered["verified_bytes"] == len(BODY)
    assert dst.moves == 1
    assert dst.imports == (1 if strategy == "url" else 0)
    assert not list(runner.staging.iterdir())


def test_crash_after_confirmed_failed_operation_before_entry_commit_does_not_hang(
    migration, monkeypatch,
):
    store, _, runner, job_id, _, dst = migration
    attempt = start_accepted_operation(store, runner, job_id, dst)
    original = store.update_entry

    def crash_before_failed_entry(entry_id, **fields):
        if fields.get("state") == "failed":
            raise SystemExit("simulated_process_exit")
        return original(entry_id, **fields)

    dst.operation = "failed"
    monkeypatch.setattr(store, "update_entry", crash_before_failed_entry)
    with pytest.raises(SystemExit):
        runner.step(job_id)
    assert store.attempt(attempt["id"])["phase"] == "failed"
    assert store.entry(attempt["entry_id"])["state"] == "waiting_operation"
    monkeypatch.setattr(store, "update_entry", original)
    store.recover()
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert store.entry(attempt["entry_id"])["state"] == "failed"
    assert dst.imports == 1
    assert dst.moves == 0


def test_polling_outage_retains_operation_until_confirmed_result(migration):
    store, _, runner, job_id, _, dst = migration
    attempt = start_accepted_operation(store, runner, job_id, dst)
    original_poll = dst.poll

    def unavailable(ref):
        assert ref == "op-1"
        raise MigrationError("destination_unavailable")

    dst.poll = unavailable
    runner.step(job_id)
    preserved = store.attempt(attempt["id"])
    assert preserved["operation_id"] == "op-1"
    assert preserved["phase"] == "waiting_transfer"
    assert store.entry(attempt["entry_id"])["state"] == "waiting_operation"
    assert store.get(job_id)["state"] in {"running", "needs_attention"}
    assert dst.imports == 1
    with pytest.raises(ValueError):
        runner.retry(job_id, "staged")
    dst.poll, dst.operation = original_poll, "success"
    assert finish(store, runner, job_id)["state"] == "completed"
    assert dst.imports == 1
    assert dst.moves == 1


def test_unknown_internal_error_after_import_intent_is_not_a_retryable_failure(
    migration,
):
    store, _, runner, job_id, _, dst = migration

    def unexpected_after_request(path, url):
        dst.imports += 1
        raise RuntimeError("signed URL must not enter the database")

    dst.import_url = unexpected_after_request
    assert finish(store, runner, job_id)["state"] == "needs_attention"
    attempt = store.attempts(job_id)[0]
    assert attempt["phase"] == "unknown_transfer"
    assert store.entry(attempt["entry_id"])["state"] == "outcome_unknown"
    assert dst.imports == 1
    with pytest.raises(ValueError):
        runner.retry(job_id, "staged")


def test_preexisting_technical_namespace_is_not_reused(unplanned_migration):
    store, _, runner, job_id, _, dst = unplanned_migration
    technical = store.get(job_id)["remote_root"]
    dst.nodes[technical] = {"type": "dir", "path": technical}
    dst.nodes[technical + "/keep.txt"] = {"type": "file", "md5": "f" * 32}
    runner.step(job_id)
    assert store.get(job_id)["state"] == "needs_attention"
    assert store.get(job_id)["error"] == "technical_namespace_exists"
    assert store.entries(job_id) == []
    assert dst.nodes[technical + "/keep.txt"]["md5"] == "f" * 32
    assert dst.imports == dst.moves == 0


def test_technical_root_created_after_plan_is_not_claimed_or_reused(migration):
    store, _, runner, job_id, _, dst = migration
    technical = store.get(job_id)["remote_root"]
    dst.nodes[technical] = {"type": "dir", "path": technical}
    dst.nodes[technical + "/keep.txt"] = {"type": "file", "md5": "f" * 32}
    store.update_job(job_id, state="running")
    for _ in range(5):
        runner.step(job_id)
        if store.get(job_id)["state"] != "running":
            break
    assert store.get(job_id)["state"] == "needs_attention"
    assert dst.imports == dst.moves == 0
    assert store.attempts(job_id) == []
    assert dst.nodes[technical + "/keep.txt"]["md5"] == "f" * 32


def test_unicode_normalization_collision_rejects_entire_plan(unplanned_migration):
    store, _, runner, job_id, src, dst = unplanned_migration
    src.walk = lambda path: [
        dict(DIR), dict(META, path="disk:/Docs/caf\u00e9.txt"),
        dict(META, path="disk:/Docs/cafe\u0301.txt"),
    ]
    runner.step(job_id)
    assert store.get(job_id)["state"] == "needs_attention"
    assert store.get(job_id)["error"] == "path_collision"
    assert store.entries(job_id) == []
    assert dst.imports == 0


def test_source_tree_outside_selected_folder_is_not_planned(unplanned_migration):
    store, _, runner, job_id, src, dst = unplanned_migration
    src.walk = lambda path: [dict(DIR), dict(META, path="disk:/Other/private.txt")]
    runner.step(job_id)
    assert store.get(job_id)["error"] == "invalid_source_tree"
    assert store.entries(job_id) == []
    assert dst.imports == 0


def test_recipient_identity_mismatch_blocks_plan_before_any_writes(unplanned_migration):
    store, _, runner, job_id, _, dst = unplanned_migration
    dst.info = lambda: {
        "total_space": 1000000, "used_space": 0,
        "user": {"login": "someone_else"},
    }
    runner.step(job_id)
    assert store.get(job_id)["error"] == "destination_identity_mismatch"
    assert store.entries(job_id) == []
    assert dst.nodes == {}


def test_quota_accounts_for_full_plan_and_largest_technical_copy(unplanned_migration):
    store, _, runner, job_id, _, dst = unplanned_migration
    dst.info = lambda: {
        "total_space": len(BODY), "used_space": 0,
        "user": {"login": "receiver"},
    }
    runner.step(job_id)
    assert store.get(job_id)["error"] == "destination_space_insufficient"
    assert store.entries(job_id) == []
    assert dst.nodes == {}


def test_missing_quota_is_a_warning_not_zero_available_space(unplanned_migration):
    store, _, runner, job_id, _, dst = unplanned_migration
    dst.info = lambda: {"user": {"login": "receiver"}}
    runner.step(job_id)
    assert store.get(job_id)["state"] == "ready"
    assert any(
        event["payload"].get("error") == "quota_unavailable"
        for event in store.events(job_id)
    )
    assert store.get(job_id)["total_bytes"] == len(BODY)


def test_missing_source_hash_is_an_issue_without_transfer(unplanned_migration):
    store, _, runner, job_id, src, dst = unplanned_migration
    src.metadata.pop("md5")
    runner.step(job_id)
    assert store.get(job_id)["state"] == "ready"
    result = finish(store, runner, job_id)
    assert result["state"] == "completed_with_issues"
    file = next(entry for entry in store.entries(job_id) if entry["kind"] == "file")
    assert file["state"] == "verification_unavailable"
    assert file["error"] == "source_hash_unavailable"
    assert result["verified_bytes"] == 0
    assert dst.imports == 0


def test_plan_retains_full_fingerprint_for_future_source_comparison(migration):
    store, _, _, job_id, _, _ = migration
    file = next(entry for entry in store.entries(job_id) if entry["kind"] == "file")
    assert file["source_path"] == "disk:/Docs/a.txt"
    assert file["destination_path"] == "disk:/owner/Docs/a.txt"
    assert file["size"] == len(BODY)
    assert file["md5"] == META["md5"]
    assert file["modified"] == "2026-10-11T10:00:00Z"
    assert file["resource_id"] == "file-1"


def test_single_file_selection_preserves_only_its_name(unplanned_migration):
    store, registry, runner, _, src, _ = unplanned_migration
    config = dict(CONFIG, source_path="disk:/Docs/a.txt")
    job = store.create(config, "single-file")
    registry.put(job["id"], config, {
        "source_client_secret": "s", "destination_token": "d",
    })
    src.walk = lambda path: [dict(META)]
    runner.step(job["id"])
    assert store.get(job["id"])["state"] == "ready"
    entries = store.entries(job["id"])
    assert len(entries) == 1
    assert entries[0]["destination_path"] == "disk:/owner/a.txt"


def test_whole_disk_selection_keeps_top_level_paths(unplanned_migration):
    store, registry, runner, _, src, _ = unplanned_migration
    config = dict(CONFIG, source_path="disk:/")
    job = store.create(config, "whole-disk")
    registry.put(job["id"], config, {
        "source_client_secret": "s", "destination_token": "d",
    })
    original_stat = src.stat
    src.stat = lambda path: (
        {"path": "disk:/", "type": "dir"} if path == "disk:/"
        else original_stat(path)
    )
    src.walk = lambda path: [
        {"path": "disk:/", "type": "dir"}, dict(DIR), dict(META),
    ]
    runner.step(job["id"])
    assert store.get(job["id"])["state"] == "ready"
    assert [entry["destination_path"] for entry in store.entries(job["id"])] == [
        "disk:/owner/Docs", "disk:/owner/Docs/a.txt",
    ]


def test_plan_cannot_target_its_own_technical_namespace(unplanned_migration):
    store, registry, runner, _, src, dst = unplanned_migration
    config = dict(CONFIG, source_path="disk:/", destination_root="disk:/")
    job = store.create(config, "technical-collision")
    registry.put(job["id"], config, {
        "source_client_secret": "s", "destination_token": "d",
    })
    src.stat = lambda path: {"path": "disk:/", "type": "dir"}
    src.walk = lambda path: [{"path": job["remote_root"], "type": "dir"}]
    runner.step(job["id"])
    assert store.get(job["id"])["error"] == "technical_path_collision"
    assert store.entries(job["id"]) == []
    assert dst.nodes == {}


def test_cleanup_after_terminal_entry_commit_removes_only_recorded_local_object(
    migration, monkeypatch,
):
    store, _, runner, job_id, _, dst = migration
    runner.default_strategy = "staged"
    sentinel = runner.staging / "not-owned.part"
    sentinel.write_bytes(b"keep")
    original_cleanup = runner.cleanup

    def crash_before_cleanup(*args, **kwargs):
        raise SystemExit("simulated_process_exit")

    monkeypatch.setattr(runner, "cleanup", crash_before_cleanup)
    with pytest.raises(SystemExit):
        finish(store, runner, job_id)
    attempt = store.attempts(job_id)[0]
    assert attempt["phase"] == "succeeded"
    assert store.entry(attempt["entry_id"])["state"] == "done"
    own_file = runner.staging / attempt["local_name"]
    assert own_file.read_bytes() == BODY
    monkeypatch.setattr(runner, "cleanup", original_cleanup)
    store.recover()
    result = runner.cleanup(job_id, successes_only=True)
    assert result["removed"] == 1
    assert not own_file.exists()
    assert sentinel.read_bytes() == b"keep"
    assert dst.moves == 1


def prepare_failed_staged_upload(store, runner, job_id, destination):
    runner.default_strategy = "staged"
    runner.staging_budget = len(BODY)
    original_upload = destination.upload
    calls = []

    def upload_once_fails(path, local_path):
        calls.append((path, local_path))
        if len(calls) == 1:
            raise MigrationError("destination_rate_limited")
        return original_upload(path, local_path)

    destination.upload = upload_once_fails
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    attempt = store.attempts(job_id)[0]
    assert attempt["phase"] == "failed"
    assert (runner.staging / attempt["local_name"]).read_bytes() == BODY
    return attempt, calls


def test_staged_retry_reuses_verified_bytes_when_budget_fits_one_file(migration):
    store, _, runner, job_id, source, destination = migration
    previous, uploads = prepare_failed_staged_upload(
        store, runner, job_id, destination,
    )
    assert source.downloads == 1
    runner.retry(job_id, "staged")
    attempts = store.attempts(job_id)
    assert len(attempts) == 2
    assert attempts[0]["local_name"] is None
    assert attempts[1]["local_name"] == previous["local_name"]
    assert attempts[1]["remote_path"] != previous["remote_path"]
    assert finish(store, runner, job_id)["state"] == "completed"
    assert source.downloads == 1
    assert len(uploads) == 2
    assert destination.upload_body == BODY
    assert not list(runner.staging.iterdir())


def test_source_change_before_staged_retry_blocks_reuse_upload(migration):
    store, _, runner, job_id, source, destination = migration
    _, uploads = prepare_failed_staged_upload(store, runner, job_id, destination)
    source.metadata["modified"] = "2026-10-11T11:00:00Z"
    runner.retry(job_id, "staged")
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    file = next(entry for entry in store.entries(job_id) if entry["kind"] == "file")
    assert file["state"] == "source_changed"
    assert source.downloads == 1
    assert len(uploads) == 1
    assert destination.upload_body is None
    assert destination.moves == 0


@pytest.mark.parametrize("namespace_state", ["intent", "unknown"])
def test_unknown_namespace_claim_is_not_replayed_after_restart(
    migration, namespace_state,
):
    store, registry, runner, job_id, source, destination = migration
    claims = []
    original = destination.claim_dir

    def record_claim(path):
        claims.append(path)
        return original(path)

    destination.claim_dir = record_claim
    store.update_job(job_id, state="running", namespace_state=namespace_state)
    store.recover()
    registry.clear()
    registry.put(job_id, CONFIG, {
        "source_client_secret": "s", "destination_token": "d",
    })
    store.update_job(job_id, state="running")
    runner.step(job_id)
    assert store.get(job_id)["state"] == "needs_attention"
    assert store.get(job_id)["error"] == "technical_namespace_outcome_unknown"
    assert claims == []
    assert source.downloads == destination.imports == destination.moves == 0
    assert store.attempts(job_id) == []


def test_empty_root_plan_is_ready_and_finishes_without_transfer(unplanned_migration):
    store, registry, runner, _, source, destination = unplanned_migration
    config = dict(CONFIG, source_path="disk:/")
    job = store.create(config, "empty-root")
    registry.put(job["id"], config, {
        "source_client_secret": "s", "destination_token": "d",
    })
    source.stat = lambda path: {"path": "disk:/", "type": "dir"}
    source.walk = lambda path: []
    runner.step(job["id"])
    assert store.get(job["id"])["state"] == "ready"
    assert store.entries(job["id"]) == []
    result = finish(store, runner, job["id"])
    assert result["state"] == "completed"
    assert result["total_bytes"] == result["total_files"] == 0
    assert source.downloads == destination.imports == destination.moves == 0


def test_pause_before_first_file_does_not_start_transfer(migration):
    store, _, runner, job_id, source, destination = migration
    store.update_job(job_id, state="running", pause_requested=True)
    runner.step(job_id)
    assert store.get(job_id)["state"] == "paused"
    assert source.downloads == destination.imports == destination.moves == 0
    assert store.attempts(job_id) == []
    assert all(entry["state"] == "planned" for entry in store.entries(job_id))
