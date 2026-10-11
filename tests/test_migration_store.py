"""Persistent migration intents, replay and credential secrecy."""

from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.migration.credentials import CredentialRegistry
from app.migration.store import MigrationStore


def config(**overrides):
    value = {
        "source_org_id": "123",
        "source_client_id": "client-1",
        "source_email": "user@example.org",
        "source_path": "disk:/Documents",
        "destination_url": "https://disk.example.org/v1/disk",
        "destination_identity": "recipient@example.org",
        "destination_root": "disk:/Migration",
        "strategy": "url",
        "identity_confirmed": True,
    }
    value.update(overrides)
    return value


def file_entry(**overrides):
    value = {
        "kind": "file",
        "source_path": "disk:/Documents/a.txt",
        "destination_path": "disk:/Migration/Documents/a.txt",
        "size": 12,
        "md5": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "modified": "2026-10-11T10:00:00+00:00",
        "resource_id": "resource-1",
    }
    value.update(overrides)
    return value


def test_idempotent_creation_survives_reopen_and_rejects_changed_config(tmp_path):
    root = tmp_path / "private"
    store = MigrationStore(root)
    created = store.create(config(), "request-1")
    store.close()
    reopened = MigrationStore(root)
    reversed_config = dict(reversed(list(config().items())))
    assert reopened.create(reversed_config, "request-1") == created
    with pytest.raises(ValueError):
        reopened.create(config(destination_root="disk:/Other"), "request-1")
    assert len(reopened.jobs()) == 1
    assert created["state"] == "queued"
    assert created["revision"] == 0
    assert created["counts"] == {}
    assert created["remote_root"] == f"disk:/.disk-copy-transfer-{created['id']}"


def test_query_operation_checkpoint_and_audit_survive_reopen(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "query-operation")
    store.add_entries(job["id"], [file_entry()])
    attempt = store.new_attempt(store.entries(job["id"])[0]["id"], "url")
    store.update_attempt(
        attempt["id"], phase="waiting_transfer", operation_id="?id=op_123",
    )
    store.close()
    reopened = MigrationStore(tmp_path)
    try:
        assert reopened.attempt(attempt["id"])["operation_id"] == "?id=op_123"
        assert reopened.events(job["id"])[-1]["payload"]["operation_id"] == (
            "?id=op_123"
        )
    finally:
        reopened.close()


@pytest.mark.parametrize("reference", [
    "?id=op_123&token=secret", "?id=one&id=two", "/operations?id=op_123",
    "https://host/operations?id=op_123",
])
def test_operation_checkpoint_rejects_urls_and_extra_parameters(tmp_path, reference):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "unsafe-operation")
    store.add_entries(job["id"], [file_entry()])
    attempt = store.new_attempt(store.entries(job["id"])[0]["id"], "url")
    revision = store.get(job["id"])["revision"]
    with pytest.raises(ValueError, match="invalid_operation_id"):
        store.update_attempt(attempt["id"], operation_id=reference)
    assert store.attempt(attempt["id"])["operation_id"] is None
    assert store.get(job["id"])["revision"] == revision


def test_store_rejects_secrets_urls_and_raw_exception_messages(tmp_path):
    store = MigrationStore(tmp_path)
    with pytest.raises(ValueError):
        store.create(config(destination_token="secret-token"), "secret")
    with pytest.raises(ValueError):
        store.create(config(destination_url="https://disk.test/?token=secret"), "url")
    job = store.create(config(), "safe")
    with pytest.raises(ValueError):
        store.update_job(job["id"], error="GET https://host/?token=secret-token")
    with pytest.raises(ValueError):
        store.event(job["id"], "error", {"destination_token": "secret-token"})
    with pytest.raises(ValueError):
        store.event(job["id"], "error", {"error": "https://host/?token=secret"})
    raw = b"".join(path.read_bytes() for path in tmp_path.glob("jobs.db*"))
    assert b"secret-token" not in raw


def test_plan_insertion_is_atomic_and_reports_only_verified_file_bytes(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "plan")
    with pytest.raises(ValueError):
        store.add_entries(job["id"], [file_entry(), file_entry(size=-1)])
    assert store.entries(job["id"]) == []
    store.add_entries(job["id"], [
        file_entry(),
        file_entry(
            kind="dir", source_path="disk:/Documents/empty",
            destination_path="disk:/Migration/Documents/empty", size=0,
            md5=None, resource_id=None,
        ),
    ])
    file, directory = store.entries(job["id"])
    store.update_entry(directory["id"], state="done")
    summary = store.get(job["id"])
    assert summary["counts"] == {"planned": 1}
    assert summary["directory_counts"] == {"done": 1}
    assert summary["total_bytes"] == 12
    assert summary["verified_bytes"] == 0
    store.update_entry(file["id"], state="already_verified", verification="md5")
    summary = store.get(job["id"])
    assert summary["counts"] == {"already_verified": 1}
    assert summary["verified_bytes"] == 12
    assert summary["total_files"] == summary["total_directories"] == 1


def test_duplicate_destination_paths_roll_back_whole_plan(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "duplicate")
    with pytest.raises(ValueError):
        store.add_entries(job["id"], [file_entry(), file_entry()])
    assert store.entries(job["id"]) == []


def test_attempt_intent_is_persisted_before_resume_and_never_reuses_path(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "attempt")
    store.add_entries(job["id"], [file_entry()])
    entry = store.entries(job["id"])[0]
    first = store.new_attempt(entry["id"], "url")
    assert first["phase"] == "prepared"
    assert first["remote_path"] == (
        f"{job['remote_root']}/{entry['id']}/{first['id']}/a.txt"
    )
    store.update_attempt(first["id"], phase="import_intent")
    store.update_job(job["id"], state="running")
    store.close()
    reopened = MigrationStore(tmp_path)
    reopened.recover()
    restored = reopened.get(job["id"])
    assert restored["state"] == "awaiting_credentials"
    assert restored["resume_state"] == "running"
    assert reopened.attempt(first["id"])["phase"] == "import_intent"
    assert reopened.entry(entry["id"])["attempt_id"] == first["id"]
    second = reopened.new_attempt(entry["id"], "staged")
    assert second["remote_path"] != first["remote_path"]
    assert len(reopened.attempts(job["id"])) == 2


@pytest.mark.parametrize("state,resume", [
    ("planning", "queued"), ("ready", "ready"),
    ("paused", "paused"), ("needs_attention", "needs_attention"),
])
def test_recovery_remembers_pre_restart_work_without_mutating_entries(
    tmp_path, state, resume,
):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "recover")
    store.add_entries(job["id"], [file_entry()])
    entry = store.entries(job["id"])[0]
    store.update_entry(entry["id"], state="outcome_unknown")
    store.update_job(job["id"], state=state)
    store.recover()
    assert store.get(job["id"])["resume_state"] == resume
    assert store.entry(entry["id"])["state"] == "outcome_unknown"
    revision = store.get(job["id"])["revision"]
    store.recover()
    assert store.get(job["id"])["revision"] == revision


def test_terminal_jobs_are_not_reopened_by_recovery(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "terminal")
    store.update_job(job["id"], state="completed")
    before = store.get(job["id"])
    store.recover()
    assert store.get(job["id"]) == before


def test_events_are_replayable_and_mutations_increment_revision_atomically(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "events")
    store.update_job(job["id"], state="planning")
    store.add_entries(job["id"], [file_entry()])
    entry = store.entries(job["id"])[0]
    store.update_entry(entry["id"], state="failed", error="import_failed")
    all_events = store.events(job["id"])
    assert all_events == store.events(job["id"])
    assert store.events(job["id"], after=all_events[0]["sequence"]) == all_events[1:]
    assert all_events[-1]["payload"]["entry_id"] == entry["id"]
    assert all_events[-1]["payload"]["error"] == "import_failed"
    assert store.get(job["id"])["revision"] == len(all_events) - 1
    revision = store.get(job["id"])["revision"]
    with pytest.raises(ValueError, match="stale_revision"):
        store.update_job(job["id"], state="running", expected_revision=revision - 1)
    assert store.get(job["id"])["revision"] == revision


def test_concurrent_updates_have_no_lost_events_or_revision(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "threads")
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(
            lambda index: store.event(job["id"], "progress", {"count": index}),
            range(40),
        ))
    assert store.get(job["id"])["revision"] == 40
    events = store.events(job["id"])
    assert len(events) == 41
    assert len({event["sequence"] for event in events}) == 41


def test_files_are_private_and_database_has_schema_and_integrity_constraints(tmp_path):
    root = tmp_path / "private"
    store = MigrationStore(root)
    store.create(config(), "permissions")
    assert root.stat().st_mode & 0o777 == 0o700
    assert (root / "jobs.db").stat().st_mode & 0o777 == 0o600
    with sqlite3.connect(root / "jobs.db") as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        names = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'",
        )}
        assert {"jobs", "entries", "attempts", "events"} <= names


def test_registry_rejects_binding_change_and_never_returns_mutable_owned_values():
    registry = CredentialRegistry()
    credentials = {
        "source_client_secret": "source-secret",
        "destination_token": "destination-secret",
    }
    registry.put("job-1", config(), credentials)
    credentials["destination_token"] = "changed-outside"
    obtained = registry.get("job-1")
    assert obtained["destination_token"] == "destination-secret"
    obtained["destination_token"] = "changed-copy"
    assert registry.get("job-1")["destination_token"] == "destination-secret"
    with pytest.raises(ValueError):
        registry.put("job-1", config(destination_identity="other"), credentials)
    registry.discard("job-1")
    assert registry.get("job-1") is None
    with pytest.raises(ValueError):
        registry.put("job-1", config(source_email="other@example.org"), credentials)
    registry.clear()
    assert registry.get("job-1") is None


def test_registry_rejects_extra_or_empty_credentials_without_echoing_secrets():
    registry = CredentialRegistry()
    with pytest.raises(ValueError) as error:
        registry.put("j", config(), {"source_client_secret": "x", "token": "secret"})
    assert "secret" not in str(error.value)
    with pytest.raises(ValueError):
        registry.put("j", config(), {
            "source_client_secret": "", "destination_token": "x",
        })


def test_attempt_rejects_signed_urls_and_unowned_local_paths(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "safe-attempt")
    store.add_entries(job["id"], [file_entry()])
    attempt = store.new_attempt(store.entries(job["id"])[0]["id"], "staged")
    for fields in [
        {"operation_id": "https://disk.example.org/?token=secret"},
        {"local_name": "../../elsewhere"},
        {"local_name": "/tmp/elsewhere"},
        {"local_name": "https://disk.example.org/?token=secret"},
        {"local_name": "owned\\outside"},
    ]:
        with pytest.raises(ValueError):
            store.update_attempt(attempt["id"], **fields)
    store.update_attempt(
        attempt["id"], local_name=f"{job['id']}/{attempt['id']}.part",
        operation_id="op-123",
    )
    assert store.attempt(attempt["id"])["operation_id"] == "op-123"


def test_other_entries_cannot_claim_an_attempt(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "ownership")
    store.add_entries(job["id"], [
        file_entry(), file_entry(destination_path="disk:/Migration/b.txt"),
    ])
    first, second = store.entries(job["id"])
    attempt = store.new_attempt(first["id"], "url")
    with pytest.raises(ValueError, match="attempt_entry_mismatch"):
        store.update_entry(second["id"], attempt_id=attempt["id"])
    assert store.entry(second["id"])["attempt_id"] is None


def test_entries_and_event_pagination_preserve_order_and_filter(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "pagination")
    store.add_entries(job["id"], [
        file_entry(source_path=f"disk:/a-{index}", destination_path=f"disk:/b-{index}")
        for index in range(3)
    ])
    entries = store.entries(job["id"])
    store.update_entry(entries[1]["id"], state="conflict")
    assert store.entries(job["id"], limit=1, offset=1)[0]["id"] == entries[1]["id"]
    assert store.entries(job["id"], state="conflict")[0]["id"] == entries[1]["id"]
    assert len(store.events(job["id"], limit=1)) == 1
    assert "secret" not in json.dumps(store.get(job["id"]))


def test_plan_and_ready_state_commit_atomically(tmp_path, monkeypatch):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "atomic-ready")
    store.update_job(job["id"], state="planning", error="source_unavailable")
    original_event = store._event

    def interrupted_event(*args, **kwargs):
        raise SystemExit("simulated_process_exit")

    monkeypatch.setattr(store, "_event", interrupted_event)
    with pytest.raises(SystemExit):
        store.add_entries(job["id"], [file_entry()], mark_ready=True)
    assert store.entries(job["id"]) == []
    assert store.get(job["id"])["state"] == "planning"
    monkeypatch.setattr(store, "_event", original_event)
    store.add_entries(job["id"], [file_entry()], mark_ready=True)
    assert store.get(job["id"])["state"] == "ready"
    assert store.get(job["id"])["error"] is None
    assert len(store.entries(job["id"])) == 1
    event = store.events(job["id"])[-1]
    assert event["payload"] == {"count": 1, "state": "ready"}


def test_old_schema_migration_preserves_jobs_and_adds_claim_state(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "old-schema")
    store.close()
    with sqlite3.connect(tmp_path / "jobs.db") as connection:
        connection.execute("ALTER TABLE jobs DROP COLUMN namespace_state")
        connection.execute("PRAGMA user_version=1")
    upgraded = MigrationStore(tmp_path)
    assert upgraded.get(job["id"])["namespace_state"] == "unclaimed"
    assert upgraded.get(job["id"])["config"] == config()
    upgraded.update_job(job["id"], namespace_state="intent")
    assert upgraded.get(job["id"])["namespace_state"] == "intent"
    with pytest.raises(ValueError):
        upgraded.update_job(job["id"], namespace_state="unsafe")


def test_resolution_journal_accepts_digest_but_never_freeform_proof(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "resolution")
    event = store.event(job["id"], "attempt_resolved", {
        "terminal": "success", "evidence_digest": "0" * 64,
    })
    assert event["payload"]["evidence_digest"] == "0" * 64
    with pytest.raises(ValueError):
        store.event(job["id"], "attempt_resolved", {
            "evidence_digest": "server log https://host/?token=secret",
        })


def unknown_attempt(store, *, phase="unknown_transfer"):
    job = store.create(config(), "atomic-resolution")
    store.add_entries(job["id"], [file_entry()])
    entry = store.entries(job["id"])[0]
    attempt = store.new_attempt(entry["id"], "url")
    store.update_attempt(attempt["id"], phase=phase)
    store.update_entry(entry["id"], state="outcome_unknown",
                       error="transfer_outcome_unknown")
    store.update_job(job["id"], state="needs_attention",
                     error="transfer_outcome_unknown")
    return store.get(job["id"]), store.entry(entry["id"]), attempt


def terminal_resolution(**overrides):
    value = {
        "phase": "verifying", "operation_id": None, "entry_state": "verifying",
        "error": None, "status": "success",
        "evidence_kind": "administrator_terminal_server_evidence",
        "evidence_digest": "1" * 64,
    }
    value.update(overrides)
    return value


@pytest.mark.parametrize("old_phase,fields", [
    ("unknown_transfer", terminal_resolution()),
    ("unknown_move", terminal_resolution(
        phase="verifying_final", entry_state="committing",
    )),
    ("move_intent", terminal_resolution(
        phase="failed", entry_state="failed", status="failed",
        error="remote_operation_failed",
    )),
    ("transfer_intent", terminal_resolution(
        phase="waiting_transfer", operation_id="op-123", status="in-progress",
        evidence_kind="operation_reference", evidence_digest=None,
    )),
    ("unknown_transfer", terminal_resolution(
        phase="failed", operation_id="op-failed-123", entry_state="failed",
        status="failed", error="remote_operation_failed",
        evidence_kind="operation_reference", evidence_digest=None,
    )),
    ("unknown_move", terminal_resolution(
        phase="waiting_move", operation_id="op-123", entry_state="committing",
        evidence_kind="operation_reference", evidence_digest=None,
    )),
])
def test_resolution_commits_attempt_entry_job_and_audit_in_one_revision(
    tmp_path, old_phase, fields,
):
    store = MigrationStore(tmp_path)
    job, entry, attempt = unknown_attempt(store, phase=old_phase)
    events_before = store.events(job["id"])
    result = store.resolve_attempt(
        job["id"], attempt["id"], job["revision"], **fields,
    )
    assert result["revision"] == job["revision"] + 1
    assert result["state"] == "needs_attention"
    assert result["error"] is None
    assert store.attempt(attempt["id"])["phase"] == fields["phase"]
    assert store.attempt(attempt["id"])["operation_id"] == fields["operation_id"]
    assert store.entry(entry["id"])["state"] == fields["entry_state"]
    assert store.entry(entry["id"])["error"] == fields["error"]
    events = store.events(job["id"])
    assert len(events) == len(events_before) + 1
    assert events[-1]["type"] == "administrative_resolution"
    assert events[-1]["payload"]["attempt_id"] == attempt["id"]
    assert events[-1]["payload"]["status"] == fields["status"]
    assert events[-1]["payload"]["operation_id"] == fields["operation_id"]


def test_resolution_audit_failure_rolls_back_all_state_changes(tmp_path, monkeypatch):
    store = MigrationStore(tmp_path)
    job, entry, attempt = unknown_attempt(store)
    saved_attempt = store.attempt(attempt["id"])
    events = store.events(job["id"])

    def crash_during_audit(*args, **kwargs):
        raise SystemExit("simulated_process_exit")

    monkeypatch.setattr(store, "_event", crash_during_audit)
    with pytest.raises(SystemExit):
        store.resolve_attempt(
            job["id"], attempt["id"], job["revision"], **terminal_resolution(),
        )
    assert store.get(job["id"]) == job
    assert store.entry(entry["id"]) == entry
    assert store.attempt(attempt["id"]) == saved_attempt
    assert store.events(job["id"]) == events


def test_resolution_revision_cas_rejects_stale_administrator_action(tmp_path):
    store = MigrationStore(tmp_path)
    job, entry, attempt = unknown_attempt(store)
    saved_attempt = store.attempt(attempt["id"])
    with pytest.raises(ValueError, match="stale_revision"):
        store.resolve_attempt(
            job["id"], attempt["id"], job["revision"] - 1, **terminal_resolution(),
        )
    assert store.get(job["id"]) == job
    assert store.entry(entry["id"]) == entry
    assert store.attempt(attempt["id"]) == saved_attempt


def test_resolution_requires_attempt_to_belong_to_job_and_be_current(tmp_path):
    store = MigrationStore(tmp_path)
    job, entry, attempt = unknown_attempt(store)
    other = store.create(config(destination_root="disk:/Other"), "another-job")
    with pytest.raises(ValueError, match="attempt_not_unknown"):
        store.resolve_attempt(
            other["id"], attempt["id"], other["revision"], **terminal_resolution(),
        )
    store.new_attempt(entry["id"], "staged")
    current = store.get(job["id"])
    with pytest.raises(ValueError, match="attempt_not_unknown"):
        store.resolve_attempt(
            job["id"], attempt["id"], current["revision"], **terminal_resolution(),
        )
    assert store.attempt(attempt["id"])["phase"] == "unknown_transfer"


@pytest.mark.parametrize("fields", [
    terminal_resolution(evidence_digest="raw https://host/?token=secret"),
    terminal_resolution(evidence_kind="arbitrary_proof"),
    terminal_resolution(error="GET https://host/?token=secret"),
    terminal_resolution(phase="done", entry_state="done"),
    terminal_resolution(status="in-progress"),
    terminal_resolution(operation_id="https://host/operations/op-123"),
    terminal_resolution(phase="verifying_final", entry_state="committing"),
])
def test_invalid_resolution_cannot_change_or_expose_unknown_attempt(tmp_path, fields):
    store = MigrationStore(tmp_path)
    job, entry, attempt = unknown_attempt(store)
    events = store.events(job["id"])
    with pytest.raises(ValueError):
        store.resolve_attempt(job["id"], attempt["id"], job["revision"], **fields)
    assert store.get(job["id"]) == job
    assert store.entry(entry["id"]) == entry
    assert store.events(job["id"]) == events


@pytest.mark.parametrize("version", [0, 1, 2, 3])
def test_stop_schema_migration_preserves_history_and_defaults(tmp_path, version):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "stop-schema")
    store.add_entries(job["id"], [file_entry()])
    before = store.entries(job["id"])
    store.close()
    with sqlite3.connect(tmp_path / "jobs.db") as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(jobs)")}
        if version < 3:
            for column in ("stop_requested", "restart_of"):
                if column in columns:
                    connection.execute(f"ALTER TABLE jobs DROP COLUMN {column}")
        connection.execute(f"PRAGMA user_version={version}")
    reopened = MigrationStore(tmp_path)
    recovered = reopened.get(job["id"])
    assert recovered["stop_requested"] is False
    assert recovered["restart_of"] is None
    assert recovered["config"] == config()
    assert reopened.entries(job["id"]) == before
    with sqlite3.connect(tmp_path / "jobs.db") as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3


@pytest.mark.parametrize("state", [
    "queued", "planning", "ready", "running", "paused", "awaiting_credentials",
    "needs_attention", "completed_with_issues",
])
def test_request_stop_is_durable_idempotent_and_preserves_attempts(tmp_path, state):
    store = MigrationStore(tmp_path)
    job, entry, attempt = unknown_attempt(store)
    store.update_job(job["id"], state=state)
    before_entry = store.entry(entry["id"])
    before_attempt = store.attempt(attempt["id"])
    stopped = store.request_stop(job["id"])
    assert stopped["stop_requested"] is True
    assert stopped["state"] == state
    assert store.request_stop(job["id"]) == stopped
    events = store.events(job["id"])
    assert events[-1]["type"] == "stop_requested"
    assert events[-1]["payload"] == {"stop_requested": True}
    store.close()
    reopened = MigrationStore(tmp_path)
    reopened.recover()
    assert reopened.get(job["id"]) == stopped
    assert reopened.entry(entry["id"]) == before_entry
    assert reopened.attempt(attempt["id"]) == before_attempt
    reopened.update_job(job["id"], state="stopped", resume_state=None)
    acknowledged = reopened.get(job["id"])
    reopened.recover()
    assert reopened.get(job["id"]) == acknowledged
    assert reopened.request_stop(job["id"]) == acknowledged


def test_completed_job_does_not_accept_stop(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "completed-stop")
    store.update_job(job["id"], state="completed")
    before = store.get(job["id"])
    with pytest.raises(ValueError, match="job_not_stoppable"):
        store.request_stop(job["id"])
    assert store.get(job["id"]) == before


@pytest.mark.parametrize("acknowledge", [False, True])
def test_stopped_job_cannot_be_resurrected_or_resolved(tmp_path, acknowledge):
    store = MigrationStore(tmp_path)
    job, entry, attempt = unknown_attempt(store)
    store.request_stop(job["id"])
    if acknowledge:
        store.update_job(job["id"], state="stopped")
    before = store.get(job["id"])
    before_entry = store.entry(entry["id"])
    before_attempt = store.attempt(attempt["id"])
    for action in (
        lambda: store.update_job(job["id"], state="running"),
        lambda: store.update_job(job["id"], stop_requested=False),
        lambda: store.new_attempt(entry["id"], "staged"),
        lambda: store.resolve_attempt(
            job["id"], attempt["id"], before["revision"], **terminal_resolution(),
        ),
    ):
        with pytest.raises(ValueError, match="job_stopped"):
            action()
    assert store.get(job["id"]) == before
    assert store.entry(entry["id"]) == before_entry
    assert store.attempt(attempt["id"]) == before_attempt


def test_stop_prevents_plan_creation(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "stopped-plan")
    store.request_stop(job["id"])
    with pytest.raises(ValueError, match="job_stopped"):
        store.add_entries(job["id"], [file_entry()], mark_ready=True)
    assert store.entries(job["id"]) == []


@pytest.mark.parametrize("state", ["stopped", "completed", "completed_with_issues"])
def test_restart_creates_idempotent_linked_fresh_plan_and_namespace(tmp_path, state):
    store = MigrationStore(tmp_path)
    parent, entry, attempt = unknown_attempt(store)
    if state == "stopped":
        store.request_stop(parent["id"])
    store.update_job(parent["id"], state=state)
    old_entry = store.entry(entry["id"])
    old_attempt = store.attempt(attempt["id"])
    child = store.restart(parent["id"], config(), "fresh-restart")
    assert child["id"] != parent["id"]
    assert child["restart_of"] == parent["id"]
    assert child["state"] == "queued"
    assert child["revision"] == 0
    assert child["stop_requested"] is False
    assert child["remote_root"] != parent["remote_root"]
    assert child["namespace_state"] == "unclaimed"
    assert child["config"] == parent["config"]
    assert store.entries(child["id"]) == store.attempts(child["id"]) == []
    assert store.entry(entry["id"]) == old_entry
    assert store.attempt(attempt["id"]) == old_attempt
    parent_after = store.get(parent["id"])
    assert store.restart(parent["id"], config(), "fresh-restart") == child
    assert store.get(parent["id"]) == parent_after
    assert store.events(parent["id"])[-1]["payload"] == {
        "restart_job_id": child["id"],
    }
    assert store.events(child["id"])[0]["payload"] == {
        "state": "queued", "restart_of": parent["id"],
    }
    store.close()
    reopened = MigrationStore(tmp_path)
    assert reopened.get(child["id"]) == child


def test_restart_rejects_active_parent_changed_config_and_key_reuse(tmp_path):
    store = MigrationStore(tmp_path)
    parent = store.create(config(), "restart-parent")
    with pytest.raises(ValueError, match="job_not_restartable"):
        store.restart(parent["id"], config(), "restart-active")
    store.request_stop(parent["id"])
    with pytest.raises(ValueError, match="job_not_restartable"):
        store.restart(parent["id"], config(), "restart-pending")
    store.update_job(parent["id"], state="stopped")
    with pytest.raises(ValueError, match="restart_config_mismatch"):
        store.restart(parent["id"], config(strategy="staged"), "restart-new-config")
    child = store.restart(parent["id"], config(), "restart-shared-key")
    other = store.create(config(), "restart-other-parent")
    store.update_job(other["id"], state="completed")
    with pytest.raises(ValueError, match="idempotency_parent_mismatch"):
        store.restart(other["id"], config(), "restart-shared-key")
    with pytest.raises(ValueError, match="idempotency_parent_mismatch"):
        store.create(config(), "restart-shared-key")
    with pytest.raises(ValueError, match="idempotency_parent_mismatch"):
        store.restart(parent["id"], config(), "restart-other-parent")
    assert len(store.jobs()) == 3
    assert store.get(child["id"])["restart_of"] == parent["id"]


def test_restart_audit_failure_rolls_back_parent_and_child(tmp_path, monkeypatch):
    store = MigrationStore(tmp_path)
    parent = store.create(config(), "restart-atomic")
    store.update_job(parent["id"], state="completed")
    before = store.get(parent["id"])
    events = store.events(parent["id"])
    original_event = store._event

    def crash_on_parent_audit(connection, job_id, *args, **kwargs):
        if job_id == parent["id"]:
            raise SystemExit("simulated_process_exit")
        return original_event(connection, job_id, *args, **kwargs)

    monkeypatch.setattr(store, "_event", crash_on_parent_audit)
    with pytest.raises(SystemExit):
        store.restart(parent["id"], config(), "restart-atomic-child")
    assert store.jobs() == [before]
    assert store.events(parent["id"]) == events


@pytest.mark.parametrize("phase", ["move_intent", "waiting_move", "unknown_move"])
@pytest.mark.parametrize("status", ["success", "failed"])
def test_stopped_final_move_resolution_records_outcome_without_resurrection(
    tmp_path, phase, status,
):
    store = MigrationStore(tmp_path)
    job, entry, attempt = unknown_attempt(store, phase=phase)
    store.update_attempt(attempt["id"], operation_id="?id=original-operation")
    store.request_stop(job["id"])
    store.update_job(job["id"], state="stopped")
    before = store.get(job["id"])
    fields = terminal_resolution(phase="verifying_final", entry_state="committing")
    if status == "failed":
        fields = terminal_resolution(
            phase="failed", entry_state="failed", status="failed",
            error="remote_operation_failed",
        )
    result = store.resolve_attempt(
        job["id"], attempt["id"], before["revision"], **fields,
    )
    assert result["state"] == "stopped"
    assert result["stop_requested"] is True
    assert result["revision"] == before["revision"] + 1
    assert store.attempt(attempt["id"])["phase"] == fields["phase"]
    assert store.entry(entry["id"])["state"] == fields["entry_state"]


def test_active_waiting_move_still_rejects_manual_resolution(tmp_path):
    store = MigrationStore(tmp_path)
    job, _, attempt = unknown_attempt(store, phase="waiting_move")
    with pytest.raises(ValueError, match="attempt_not_unknown"):
        store.resolve_attempt(
            job["id"], attempt["id"], job["revision"],
            **terminal_resolution(phase="verifying_final", entry_state="committing"),
        )


@pytest.mark.parametrize("phase", ["move_intent", "waiting_move", "unknown_move"])
def test_stopped_final_move_rejects_substitution_of_known_operation(tmp_path, phase):
    store = MigrationStore(tmp_path)
    job, _, attempt = unknown_attempt(store, phase=phase)
    store.update_attempt(attempt["id"], operation_id="?id=original-operation")
    store.request_stop(job["id"])
    store.update_job(job["id"], state="stopped")
    before = store.attempt(attempt["id"])
    with pytest.raises(ValueError, match="operation_reference_mismatch"):
        store.resolve_attempt(
            job["id"], attempt["id"], store.get(job["id"])["revision"],
            **terminal_resolution(
                phase="verifying_final", entry_state="committing",
                operation_id="?id=unrelated-operation",
                evidence_kind="operation_reference", evidence_digest=None,
            ),
        )
    assert store.attempt(attempt["id"]) == before
