"""Persistent migration intents, replay and credential secrecy."""

from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.migration.credentials import CredentialRegistry
from app.migration.store import MigrationStore, canonical_config


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


def shared_config(**overrides):
    value = config(
        destination_type="shared", destination_vd_hash="archive_123",
        destination_base="disk:/Archive",
        destination_root="disk:/Archive/user@example.org",
    )
    value.update(overrides)
    return value


def legacy_shared_job(store, key="legacy-shared", **overrides):
    job = store.create(shared_config(**overrides), key)
    with store._connection(write=True) as connection:
        connection.execute(
            "UPDATE jobs SET remote_root=?,namespace_state='unclaimed' WHERE id=?",
            (f"disk:/.disk-copy-transfer-{job['id']}", job["id"]),
        )
    return store.get(job["id"])


def schema_six(store):
    store.close()
    with sqlite3.connect(store.path) as connection:
        connection.executescript("""
            CREATE TABLE attempts_v6 (
                id TEXT PRIMARY KEY, entry_id TEXT NOT NULL REFERENCES entries(id),
                strategy TEXT NOT NULL CHECK(strategy IN ('url','staged')),
                phase TEXT NOT NULL, remote_path TEXT NOT NULL UNIQUE,
                operation_id TEXT, local_name TEXT, error TEXT,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            INSERT INTO attempts_v6 SELECT * FROM attempts ORDER BY rowid;
            DROP TABLE attempts;
            ALTER TABLE attempts_v6 RENAME TO attempts;
            PRAGMA user_version=6;
        """)


def database_rows(path):
    with sqlite3.connect(path) as connection:
        return {
            table: connection.execute(f"SELECT rowid,* FROM {table} ORDER BY rowid")
            .fetchall()
            for table in ("jobs", "entries", "attempts", "publications", "events")
        }


@pytest.mark.parametrize("strategy", ["url", "staged"])
def test_shared_jobs_upload_directly_and_failed_retries_keep_original_path(
    tmp_path, strategy,
):
    store = MigrationStore(tmp_path)
    job = store.create(shared_config(strategy=strategy), "direct-shared")
    assert job["remote_root"] == ""
    assert job["namespace_state"] == "owned"
    store.add_entries(job["id"], [file_entry()])
    entry = store.entries(job["id"])[0]
    first = store.new_attempt(entry["id"], strategy)
    assert first["remote_path"] == entry["destination_path"]
    store.update_attempt(first["id"], phase="failed", error="remote_operation_failed")
    second = store.new_attempt(entry["id"], strategy)
    assert second["id"] != first["id"]
    assert second["remote_path"] == first["remote_path"]
    assert store.attempt(first["id"])["error"] == "remote_operation_failed"
    store.close()
    reopened = MigrationStore(tmp_path)
    assert reopened.create(shared_config(strategy=strategy), "direct-shared") == (
        reopened.get(job["id"])
    )
    assert reopened.entry(entry["id"])["attempt_id"] == second["id"]
    assert reopened.attempts(job["id"])[0]["id"] == first["id"]


def test_two_shared_jobs_may_reference_the_same_final_path(tmp_path):
    store = MigrationStore(tmp_path)
    attempts = []
    for key in ("first-direct-job", "second-direct-job"):
        job = store.create(shared_config(), key)
        store.add_entries(job["id"], [file_entry()])
        attempts.append(store.new_attempt(store.entries(job["id"])[0]["id"], "url"))
    assert attempts[0]["id"] != attempts[1]["id"]
    assert attempts[0]["remote_path"] == attempts[1]["remote_path"]
    assert attempts[0]["remote_path"] == file_entry()["destination_path"]


@pytest.mark.parametrize("point_retry", [False, True])
@pytest.mark.parametrize("phase", [
    "prepared", "transfer_intent", "waiting_transfer", "unknown_transfer",
    "verifying", "succeeded",
])
def test_direct_attempt_replacement_requires_failure_or_explicit_point_retry(
    tmp_path, point_retry, phase,
):
    store = MigrationStore(tmp_path)
    job = store.create(shared_config(), "unsafe-direct-retry")
    store.add_entries(job["id"], [file_entry()])
    entry = store.entries(job["id"])[0]
    attempt = store.new_attempt(entry["id"], "url")
    store.update_attempt(attempt["id"], phase=phase)
    store.update_entry(entry["id"], point_retry_requested=point_retry)
    before = database_rows(store.path)
    if point_retry and phase in {"transfer_intent", "waiting_transfer", "unknown_transfer"}:
        replacement = store.new_attempt(entry["id"], "url", point_retry=True)
        assert replacement["id"] != attempt["id"]
        assert store.attempt(attempt["id"])["phase"] == "abandoned_transfer"
    else:
        with pytest.raises(ValueError, match="^attempt_not_retryable$"):
            store.new_attempt(entry["id"], "url", point_retry=point_retry)
        assert database_rows(store.path) == before


def test_schema_six_upgrade_preserves_all_history_paths_and_foreign_keys(tmp_path):
    store = MigrationStore(tmp_path)
    job = legacy_shared_job(store, source_public=True)
    store.add_entries(job["id"], [file_entry()])
    entry = store.entries(job["id"])[0]
    attempt = store.new_attempt(entry["id"], "staged")
    store.update_attempt(attempt["id"], phase="unknown_transfer",
                         operation_id="legacy-operation", local_name="cache.ready")
    schema_six(store)
    before = database_rows(store.path)
    reopened = MigrationStore(tmp_path)
    assert database_rows(store.path) == before
    assert reopened.get(job["id"])["remote_root"] == job["remote_root"]
    assert reopened.attempt(attempt["id"])["remote_path"] == attempt["remote_path"]
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 8
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert any(row[2:5] == ("attempts", "attempt_id", "id")
                   for row in connection.execute("PRAGMA foreign_key_list(entries)"))
    direct = reopened.create(shared_config(), "after-upgrade")
    reopened.add_entries(direct["id"], [file_entry()])
    direct_entry = reopened.entries(direct["id"])[0]
    first = reopened.new_attempt(direct_entry["id"], "url")
    reopened.update_attempt(first["id"], phase="failed")
    assert reopened.new_attempt(direct_entry["id"], "url")["remote_path"] == (
        direct_entry["destination_path"]
    )


def test_schema_seven_upgrade_rolls_back_if_existing_foreign_keys_are_invalid(tmp_path):
    store = MigrationStore(tmp_path)
    job = legacy_shared_job(store)
    store.add_entries(job["id"], [file_entry()])
    schema_six(store)
    with sqlite3.connect(store.path) as connection:
        connection.execute("UPDATE entries SET attempt_id='missing-attempt'")
    before = database_rows(store.path)
    with pytest.raises(sqlite3.IntegrityError, match="migration_foreign_key_check_failed"):
        MigrationStore(tmp_path)
    assert database_rows(store.path) == before
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 6
        assert "remote_path TEXT NOT NULL UNIQUE" in connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='attempts'",
        ).fetchone()[0]


def test_future_store_schema_is_rejected_without_mutation(tmp_path):
    store = MigrationStore(tmp_path)
    store.create(shared_config(), "future-schema")
    store.close()
    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA user_version=9")
    before = database_rows(store.path)
    with pytest.raises(ValueError, match="^unsupported_store_version$"):
        MigrationStore(tmp_path)
    assert database_rows(store.path) == before
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 9


def direct_unknown_attempt(store, *, phase="unknown_transfer"):
    job = store.create(shared_config(), "direct-unknown")
    store.add_entries(job["id"], [file_entry()])
    entry = store.entries(job["id"])[0]
    attempt = store.new_attempt(entry["id"], "url")
    store.update_attempt(attempt["id"], phase=phase)
    store.update_entry(entry["id"], state="outcome_unknown")
    job = store.update_job(job["id"], state="needs_attention")
    return job, store.entry(entry["id"]), attempt


@pytest.mark.parametrize("reuse_local", [False, True])
def test_direct_point_retry_after_failed_operation_keeps_path_and_cache_custody(
    tmp_path, reuse_local,
):
    store = MigrationStore(tmp_path)
    job, entry, attempt = direct_unknown_attempt(store)
    store.queue_point_retry(job["id"], entry["id"], attempt["id"], job["revision"])
    store.update_attempt(attempt["id"], phase="failed", operation_id="old-operation",
                         local_name="cache.ready", error="remote_operation_failed")
    new = store.new_attempt(entry["id"], "url", point_retry=True,
                            reuse_local=reuse_local)
    old = store.attempt(attempt["id"])
    assert new["id"] != old["id"]
    assert new["remote_path"] == old["remote_path"] == entry["destination_path"]
    assert old["phase"] == "failed"
    assert old["operation_id"] == "old-operation"
    assert old["error"] == "remote_operation_failed"
    assert old["local_name"] == (None if reuse_local else "cache.ready")
    assert new["local_name"] == ("cache.ready" if reuse_local else None)


@pytest.mark.parametrize("phase", ["transfer_intent", "unknown_transfer", "failed"])
def test_direct_point_copy_confirmation_marks_the_final_path_succeeded(tmp_path, phase):
    store = MigrationStore(tmp_path)
    job, entry, attempt = direct_unknown_attempt(store)
    store.queue_point_retry(job["id"], entry["id"], attempt["id"], job["revision"])
    store.update_attempt(attempt["id"], phase=phase, operation_id="old-operation",
                         error="destination_timeout")
    store.confirm_point_copy(entry["id"])
    assert store.entry(entry["id"])["state"] == "already_verified"
    assert store.attempt(attempt["id"])["phase"] == "succeeded"
    assert store.attempt(attempt["id"])["operation_id"] == "old-operation"
    assert store.attempt(attempt["id"])["error"] == "destination_timeout"
    assert store.events(job["id"])[-1]["payload"]["phase"] == "succeeded"


def test_restart_legacy_shared_job_creates_direct_job_without_rewriting_old_paths(tmp_path):
    store = MigrationStore(tmp_path)
    parent = legacy_shared_job(store)
    store.add_entries(parent["id"], [file_entry()])
    entry = store.entries(parent["id"])[0]
    attempt = store.new_attempt(entry["id"], "url")
    store.update_job(parent["id"], state="completed_with_issues")
    entry = store.entry(entry["id"])
    child = store.restart(parent["id"], shared_config(), "direct-restart")
    assert child["remote_root"] == ""
    assert child["namespace_state"] == "owned"
    assert child["restart_of"] == parent["id"]
    assert store.entry(entry["id"]) == entry
    assert store.attempt(attempt["id"]) == attempt
    assert store.get(parent["id"])["remote_root"] == parent["remote_root"]
    assert store.restart(parent["id"], shared_config(), "direct-restart") == child


@pytest.mark.parametrize("factory", [config, shared_config])
def test_false_public_source_keeps_legacy_canonical_binding(tmp_path, factory):
    value = factory()
    assert canonical_config({**value, "source_public": False}) == value
    store = MigrationStore(tmp_path)
    before = store.create(value, "legacy-public-omitted")
    assert store.create({**value, "source_public": False},
                        "legacy-public-omitted") == before
    store.add_entries(before["id"], [file_entry()])
    assert store.publications(before["id"]) == []


@pytest.mark.parametrize("value", [None, 0, 1, "true", {}, []])
def test_public_source_must_be_boolean(value):
    with pytest.raises(ValueError, match="^invalid_source_public$"):
        canonical_config(shared_config(strategy="staged", source_public=value))


@pytest.mark.parametrize("value", [
    config(strategy="url", source_public=True),
    config(strategy="staged", source_public=True),
])
def test_public_source_rejects_personal_destination(value):
    with pytest.raises(ValueError, match="^invalid_source_public$"):
        canonical_config(value)


@pytest.mark.parametrize("strategy", ["url", "staged"])
@pytest.mark.parametrize("root,items,expected", [
    ("disk:/Documents", [
        file_entry(kind="dir", source_path="disk:/Documents",
                   destination_path="disk:/Archive/user@example.org/Documents"),
        file_entry(),
        file_entry(kind="dir", source_path="disk:/Documents/nested",
                   destination_path="disk:/Migration/Documents/nested"),
    ], ["disk:/Documents"]),
    ("disk:/Documents/a.txt", [file_entry()], ["disk:/Documents/a.txt"]),
    ("disk:/", [
        file_entry(kind="dir", source_path="disk:/Documents",
                   destination_path="disk:/Archive/user@example.org/Documents"),
        file_entry(),
        file_entry(source_path="disk:/top.txt",
                   destination_path="disk:/Archive/user@example.org/top.txt"),
        file_entry(kind="dir", source_path="disk:/empty",
                   destination_path="disk:/Archive/user@example.org/empty"),
    ], ["disk:/Documents", "disk:/top.txt", "disk:/empty"]),
    ("disk:/", [], []),
])
def test_publications_seed_only_selected_or_top_level_roots(
    tmp_path, root, items, expected, strategy,
):
    store = MigrationStore(tmp_path)
    value = shared_config(strategy=strategy, source_public=True, source_path=root)
    job = store.create(value, "public-root-plan")
    store.add_entries(job["id"], items, mark_ready=True)
    rows = store.publications(job["id"])
    assert [row["source_path"] for row in rows] == expected
    assert all(row["state"] == "planned" and row["error"] is None for row in rows)
    entries = {entry["id"]: entry for entry in store.entries(job["id"])}
    for row in rows:
        for key in ("source_path", "kind", "resource_id", "size", "md5", "modified"):
            assert row[key] == entries[row["entry_id"]][key]
    assert store.get(job["id"])["config"] == value


def test_publication_intent_and_events_survive_recover(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(
        shared_config(strategy="staged", source_public=True,
                      source_path="disk:/Documents/a.txt"),
        "durable-publication",
    )
    store.add_entries(job["id"], [file_entry()], mark_ready=True)
    entry_id = store.publications(job["id"])[0]["entry_id"]
    store.update_publication(job["id"], entry_id, state="intent")
    before = store.publications(job["id"])
    event = store.events(job["id"])[-1]
    assert event["type"] == "publication_checkpoint"
    assert event["payload"] == {"entry_id": entry_id, "phase": "intent", "error": None}
    store.close()
    reopened = MigrationStore(tmp_path)
    reopened.recover()
    assert reopened.publications(job["id"]) == before
    assert reopened.events(job["id"])[-2] == event
    reopened.update_publication(job["id"], entry_id, state="published")
    assert reopened.publications(job["id"])[0]["state"] == "published"


def test_publication_updates_are_bound_stopped_and_safe(tmp_path):
    store = MigrationStore(tmp_path)
    value = shared_config(strategy="staged", source_public=True,
                          source_path="disk:/Documents/a.txt")
    first = store.create(value, "first-publication")
    second = store.create(value, "second-publication")
    store.add_entries(first["id"], [file_entry()])
    entry_id = store.publications(first["id"])[0]["entry_id"]
    before = store.publications(first["id"])
    events = store.events(first["id"])
    with pytest.raises(ValueError, match="^publication_job_mismatch$"):
        store.update_publication(second["id"], entry_id, state="intent")
    with pytest.raises(ValueError, match="^invalid_publication_state$"):
        store.update_publication(first["id"], entry_id, state="public_key=secret")
    with pytest.raises(ValueError, match="^invalid_safe_code$"):
        store.update_publication(first["id"], entry_id, state="failed",
                                 error="https://signed.example/?token=never-persist")
    assert store.publications(first["id"]) == before
    assert store.events(first["id"]) == events
    store.request_stop(first["id"])
    for acknowledged in (False, True):
        if acknowledged:
            store.update_job(first["id"], state="stopped")
        with pytest.raises(ValueError, match="^job_stopped$"):
            store.update_publication(first["id"], entry_id, state="intent")
    assert store.publications(first["id"]) == before


def test_publication_terminal_state_cannot_regress_and_failed_can_retry(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(
        shared_config(strategy="staged", source_public=True,
                      source_path="disk:/Documents/a.txt"),
        "publication-transitions",
    )
    store.add_entries(job["id"], [file_entry()])
    entry_id = store.publications(job["id"])[0]["entry_id"]
    store.update_publication(job["id"], entry_id, state="intent")
    store.update_publication(job["id"], entry_id, state="failed",
                             error="source_unavailable")
    store.update_publication(job["id"], entry_id, state="intent")
    assert store.publications(job["id"])[0]["error"] is None
    store.update_publication(job["id"], entry_id, state="published")
    before = store.publications(job["id"])
    for state in ("planned", "intent", "failed"):
        with pytest.raises(ValueError, match="^invalid_publication_transition$"):
            store.update_publication(job["id"], entry_id, state=state)
    assert store.publications(job["id"]) == before


@pytest.mark.parametrize("previous_state", ["planned", "failed"])
def test_existing_publication_can_be_confirmed_without_new_intent(
    tmp_path, previous_state,
):
    store = MigrationStore(tmp_path)
    job = store.create(
        shared_config(strategy="staged", source_public=True,
                      source_path="disk:/Documents/a.txt"),
        "existing-source-publication",
    )
    store.add_entries(job["id"], [file_entry()])
    entry_id = store.publications(job["id"])[0]["entry_id"]
    if previous_state == "failed":
        store.update_publication(job["id"], entry_id, state="failed",
                                 error="source_unavailable")
    store.update_publication(job["id"], entry_id, state="published")
    assert store.publications(job["id"])[0]["state"] == "published"
    assert store.publications(job["id"])[0]["error"] is None


def test_publication_checkpoint_and_event_roll_back_together(tmp_path, monkeypatch):
    store = MigrationStore(tmp_path)
    job = store.create(
        shared_config(strategy="staged", source_public=True,
                      source_path="disk:/Documents/a.txt"),
        "publication-atomic-checkpoint",
    )
    store.add_entries(job["id"], [file_entry()])
    before = store.publications(job["id"])
    before_job, before_events = store.get(job["id"]), store.events(job["id"])

    def fail_event(*args, **kwargs):
        raise RuntimeError("event unavailable")

    monkeypatch.setattr(store, "_event", fail_event)
    with pytest.raises(RuntimeError, match="event unavailable"):
        store.update_publication(job["id"], before[0]["entry_id"], state="intent")
    assert store.publications(job["id"]) == before
    assert store.get(job["id"]) == before_job
    assert store.events(job["id"]) == before_events


def test_publication_plan_seed_rolls_back_with_entries(tmp_path, monkeypatch):
    store = MigrationStore(tmp_path)
    job = store.create(
        shared_config(strategy="staged", source_public=True,
                      source_path="disk:/Documents/a.txt"),
        "publication-atomic-plan",
    )

    def fail_event(*args, **kwargs):
        raise RuntimeError("event unavailable")

    monkeypatch.setattr(store, "_event", fail_event)
    with pytest.raises(RuntimeError, match="event unavailable"):
        store.add_entries(job["id"], [file_entry()], mark_ready=True)
    assert store.entries(job["id"]) == []
    assert store.publications(job["id"]) == []
    assert store.get(job["id"])["state"] == "queued"


def test_schema_four_upgrade_adds_publications_without_history_changes(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "legacy-publication-upgrade")
    store.add_entries(job["id"], [file_entry()])
    before_job = store.update_job(job["id"], state="completed")
    before_entries, before_events = store.entries(job["id"]), store.events(job["id"])
    store.close()
    with sqlite3.connect(tmp_path / "jobs.db") as connection:
        connection.execute("DROP TABLE IF EXISTS publications")
        connection.execute("PRAGMA user_version=4")
    reopened = MigrationStore(tmp_path)
    assert reopened.get(job["id"]) == before_job
    assert reopened.entries(job["id"]) == before_entries
    assert reopened.events(job["id"]) == before_events
    assert reopened.publications(job["id"]) == []
    with sqlite3.connect(tmp_path / "jobs.db") as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 8


def test_transfer_stage_context_keeps_historical_attempt_and_shared_paths(tmp_path):
    store = MigrationStore(tmp_path)
    job = legacy_shared_job(store, "detailed-journal")
    store.add_entries(job["id"], [file_entry(
        destination_path="disk:/Archive/user@example.org/Documents/a.txt",
    )])
    entry = store.entries(job["id"])[0]
    first = store.new_attempt(entry["id"], "url")
    payload = {
        "entry_id": entry["id"], "attempt_id": first["id"],
        "stage": "poll_transfer", "status": "failed",
        "error": "destination_rate_limited", "http_status": 429,
        "duration_ms": 345, "operation_id": "?id=operation-1",
    }
    event = store.event(job["id"], "transfer_stage", payload)
    second = store.new_attempt(entry["id"], "staged")
    store.event(job["id"], "transfer_stage", {
        "entry_id": entry["id"], "stage": "check_source", "status": "started",
    })
    stages = [item for item in store.events(job["id"])
              if item["type"] == "transfer_stage"]
    assert stages[0]["sequence"] == event["sequence"]
    assert stages[0]["payload"] == payload
    assert stages[0]["context"] == {
        "source": entry["source_path"],
        "destination": "vd:archive_123:" + entry["destination_path"],
        "kind": "file", "size": 12, "strategy": "url",
        "remote_path": "vd:archive_123:" + first["remote_path"],
    }
    assert stages[0]["stage_label"] == "Ожидание загрузки"
    assert stages[0]["status_label"] == "Ошибка"
    assert "лимит" in stages[0]["error_message"].lower()
    assert stages[1]["context"]["remote_path"] is None
    assert second["remote_path"] not in json.dumps(stages[1])
    assert "context" not in store.events(job["id"])[0]


def test_transfer_stage_uses_folder_override_before_entries_exist(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(shared_config(), "planning-journal")
    store.event(job["id"], "transfer_stage", {
        "stage": "planning", "status": "failed", "error": "source_not_found",
        "source_path": "disk:/Documents/Архив", "http_status": 404,
    })
    stage = store.events(job["id"])[-1]
    assert stage["context"]["source"] == "disk:/Documents/Архив"
    assert stage["context"]["destination"] == (
        "vd:archive_123:disk:/Archive/user@example.org"
    )


@pytest.mark.parametrize("fields", [
    {"stage": "invented_stage"}, {"status": "invented_status"},
    {"http_status": "secret"}, {"http_status": True}, {"http_status": 700},
    {"duration_ms": -1}, {"duration_ms": False},
    {"source_path": "https://signed.example.org/?token=never-persist"},
    {"source_path": "disk:/../private"},
    {"source_path": "disk:/Documents//not-canonical"},
    {"remote_status": "token=secret"},
    {"operation_id": "https://disk.example.org/?token=secret"},
    {"operation_id": 123}, {"entry_id": True}, {"error": 400},
])
def test_transfer_stage_rejects_unsafe_or_invalid_fields(tmp_path, fields):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "invalid-journal")
    payload = {"stage": "planning", "status": "started", **fields}
    with pytest.raises(ValueError):
        store.event(job["id"], "transfer_stage", payload)
    assert len(store.events(job["id"])) == 1


def test_journal_paging_stays_at_initial_sequence_cutoff(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "snapshot-journal")
    for _ in range(4):
        store.event(job["id"], "warning", {"error": "source_changed"})
    cutoff = store.last_sequence(job["id"])
    store.event(job["id"], "warning", {"error": "internal_error"})
    first = store.events(job["id"], limit=2, through=cutoff, journal=True)
    second = store.events(job["id"], after=first[-1]["sequence"], limit=10,
                          through=cutoff, journal=True)
    assert len(first + second) == 5
    assert (first + second)[-1]["sequence"] == cutoff
    assert all(item["payload"].get("error") != "internal_error"
               for item in first + second)


@pytest.mark.parametrize("version", [0, 1, 2, 3])
def test_download_retry_migration_preserves_terminal_history(tmp_path, version):
    store = MigrationStore(tmp_path)
    job = store.create(config(strategy="staged"), "download-retry-migration")
    store.add_entries(job["id"], [file_entry()])
    entry = store.entries(job["id"])[0]
    attempt = store.new_attempt(entry["id"], "staged")
    store.update_entry(entry["id"], state="done", verification="size_md5")
    store.update_attempt(attempt["id"], phase="done")
    before_job = store.update_job(job["id"], state="completed")
    before_entry = store.entry(entry["id"])
    before_events = store.events(job["id"])
    store.close()
    with sqlite3.connect(tmp_path / "jobs.db") as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(entries)")}
        for column in ("download_attempts", "retry_at_ms"):
            if column in columns:
                connection.execute(f"ALTER TABLE entries DROP COLUMN {column}")
        connection.execute(f"PRAGMA user_version={version}")
    reopened = MigrationStore(tmp_path)
    assert reopened.get(job["id"]) == before_job
    assert reopened.entry(entry["id"]) == {
        **before_entry, "download_attempts": 0, "retry_at_ms": None,
    }
    assert reopened.attempt(attempt["id"])["phase"] == "done"
    assert reopened.events(job["id"]) == before_events
    with sqlite3.connect(tmp_path / "jobs.db") as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 8


def test_download_retry_counter_wait_and_failure_survive_reopen(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(strategy="staged"), "persistent-download-retry")
    store.add_entries(job["id"], [file_entry()])
    entry = store.entries(job["id"])[0]
    assert entry["download_attempts"] == 0
    assert entry["retry_at_ms"] is None
    retry_at = 1800000000000
    store.update_entry(entry["id"], download_attempts=2, retry_at_ms=retry_at,
                       error="source_timeout")
    payload = {
        "entry_id": entry["id"], "stage": "download_retry", "status": "waiting",
        "error": "source_timeout", "download_attempts": 2,
        "max_download_attempts": 5, "retry_delay_ms": 8000,
        "retry_at_ms": retry_at,
    }
    store.event(job["id"], "transfer_stage", payload)
    store.close()
    reopened = MigrationStore(tmp_path)
    assert reopened.entry(entry["id"])["download_attempts"] == 2
    assert reopened.entry(entry["id"])["retry_at_ms"] == retry_at
    event = reopened.events(job["id"])[-1]
    assert event["payload"] == payload
    assert event["stage_label"] == "Повтор скачивания"
    assert "врем" in event["error_message"].lower()
    reopened.update_entry(entry["id"], download_attempts=5, retry_at_ms=None,
                          state="failed", error="source_timeout")
    skipped = reopened.event(job["id"], "transfer_stage", {
        **payload, "download_attempts": 5, "status": "skipped",
        "retry_at_ms": None, "retry_delay_ms": 0,
    })
    assert reopened.events(job["id"])[-1]["status_label"] == "Пропущено"
    assert skipped["payload"]["error"] == "source_timeout"


@pytest.mark.parametrize("fields", [
    {"download_attempts": -1}, {"download_attempts": 6},
    {"download_attempts": True}, {"download_attempts": None},
    {"download_attempts": "token=secret"}, {"download_attempts": 1.5},
    {"retry_at_ms": -1}, {"retry_at_ms": True}, {"retry_at_ms": "secret"},
    {"retry_at_ms": 1.5},
])
def test_entry_rejects_invalid_retry_counters_and_waits(tmp_path, fields):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "invalid-entry-download-retry")
    store.add_entries(job["id"], [file_entry()])
    entry = store.entries(job["id"])[0]
    before_events = store.events(job["id"])
    with pytest.raises(ValueError):
        store.update_entry(entry["id"], **fields)
    assert store.entry(entry["id"]) == entry
    assert store.events(job["id"]) == before_events


@pytest.mark.parametrize("fields", [
    {"download_attempts": -1}, {"download_attempts": 6},
    {"download_attempts": True}, {"download_attempts": None},
    {"max_download_attempts": 0}, {"max_download_attempts": 6},
    {"max_download_attempts": False}, {"max_download_attempts": None},
    {"retry_delay_ms": -1}, {"retry_delay_ms": True},
    {"retry_delay_ms": "token=secret"}, {"retry_delay_ms": 1.5},
    {"retry_at_ms": -1}, {"retry_at_ms": False},
    {"retry_at_ms": "https://signed.example.org/?token=secret"},
])
def test_download_retry_events_reject_invalid_or_unsafe_values(tmp_path, fields):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "invalid-download-retry-event")
    with pytest.raises(ValueError):
        store.event(job["id"], "transfer_stage", {
            "stage": "download_retry", "status": "waiting", **fields,
        })
    assert len(store.events(job["id"])) == 1


def test_shared_binding_survives_reopen_and_rejects_retargeting(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(shared_config(), "shared-request")
    store.close()
    store = MigrationStore(tmp_path)
    try:
        assert store.create(shared_config(), "shared-request")["config"] == (
            shared_config()
        )
        with pytest.raises(ValueError, match="idempotency_config_mismatch"):
            store.create(shared_config(destination_vd_hash="another"),
                         "shared-request")
        assert store.get(job["id"])["config"]["destination_vd_hash"] == "archive_123"
    finally:
        store.close()


@pytest.mark.parametrize("overrides", [
    {"destination_vd_hash": "wrong/hash"},
    {"destination_vd_hash": ""},
    {"destination_base": "disk:/../Other"},
    {"destination_root": "disk:/Other/user@example.org"},
    {"destination_type": "unexpected"},
])
def test_shared_config_rejects_wrong_disk_or_email_folder(overrides):
    value = shared_config()
    value.update(overrides)
    with pytest.raises(ValueError, match="invalid_destination_shared_disk"):
        canonical_config(value)


def test_legacy_personal_config_retains_exact_shape():
    assert canonical_config(config()) == config()


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
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 8
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        names = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'",
        )}
        assert {"jobs", "entries", "attempts", "events", "publications"} <= names


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


def test_historical_resolution_journal_is_readable_but_no_longer_writable(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "historical-resolution")
    payload = {"terminal": "success", "evidence_digest": "0" * 64}
    with store._connection(write=True) as connection:
        connection.execute(
            "INSERT INTO events(job_id,type,payload,created_at) VALUES(?,?,?,?)",
            (job["id"], "attempt_resolved", json.dumps(payload), job["created_at"]),
        )
    assert store.events(job["id"])[-1]["payload"] == payload
    for key, value in payload.items():
        with pytest.raises(ValueError, match="invalid_event_fields"):
            store.event(job["id"], "attempt_resolved", {key: value})


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
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 8


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


@pytest.mark.parametrize("state", [
    "queued", "planning", "ready", "running", "paused", "awaiting_credentials",
    "needs_attention", "completed_with_issues", "stopped",
])
def test_full_stop_preserves_server_outcome_and_cancels_local_point_retry(tmp_path, state):
    store = MigrationStore(tmp_path)
    job, entry, attempt = direct_unknown_attempt(store)
    store.update_attempt(attempt["id"], operation_id="still-pending", local_name="ready.part")
    cache = tmp_path / "staging" / "ready.part"
    cache.parent.mkdir()
    cache.write_bytes(b"verified local file")
    store.update_entry(entry["id"], point_retry_requested=True, point_retry_attempts=2,
                       retry_at_ms=1800000030000)
    store.update_job(job["id"], state=state)
    before = store.attempt(attempt["id"])
    result = store.request_full_stop(job["id"])
    assert result["state"] == state
    assert result["stop_requested"] is result["full_stop_requested"] is True
    assert not result["point_retry_active"]
    current = store.entry(entry["id"])
    assert current["state"] == "outcome_unknown"
    assert current["attempt_id"] == attempt["id"]
    assert current["point_retry_attempts"] == 2
    assert current["point_retry_requested"] is False
    assert current["retry_at_ms"] is None
    assert store.attempt(attempt["id"]) == before
    assert cache.read_bytes() == b"verified local file"
    assert store.events(job["id"])[-1]["type"] == "full_stop_requested"
    events = store.events(job["id"])
    assert store.request_full_stop(job["id"]) == result
    assert store.events(job["id"]) == events
    store.close()
    reopened = MigrationStore(tmp_path)
    reopened.recover()
    assert reopened.get(job["id"]) == result
    assert reopened.attempt(attempt["id"]) == before
    assert cache.read_bytes() == b"verified local file"


def test_completed_job_rejects_full_stop_without_modification(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(shared_config(), "finished-full-stop")
    store.update_job(job["id"], state="completed")
    before = store.get(job["id"])
    with pytest.raises(ValueError, match="job_not_stoppable"):
        store.request_full_stop(job["id"])
    assert store.get(job["id"]) == before
    assert before["full_stop_requested"] is False


def test_schema_seven_upgrade_adds_full_stop_without_rewriting_history(tmp_path):
    store = MigrationStore(tmp_path)
    job, entry, attempt = direct_unknown_attempt(store)
    store.request_stop(job["id"])
    store.close()
    with sqlite3.connect(tmp_path / "jobs.db") as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(jobs)")}
        if "full_stop_requested" in columns:
            connection.execute("ALTER TABLE jobs DROP COLUMN full_stop_requested")
        connection.execute("PRAGMA user_version=7")
        job_before = connection.execute("SELECT id,config,state,revision,stop_requested "
                                        "FROM jobs").fetchall()
        history_before = {
            table: connection.execute(f"SELECT rowid,* FROM {table}").fetchall()
            for table in ("entries", "attempts", "events")
        }
    reopened = MigrationStore(tmp_path)
    assert reopened.get(job["id"])["full_stop_requested"] is False
    assert reopened.get(job["id"])["stop_requested"] is True
    assert reopened.entry(entry["id"])["attempt_id"] == attempt["id"]
    with sqlite3.connect(tmp_path / "jobs.db") as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 8
        assert connection.execute("SELECT id,config,state,revision,stop_requested "
                                  "FROM jobs").fetchall() == job_before
        for table, rows in history_before.items():
            assert connection.execute(f"SELECT rowid,* FROM {table}").fetchall() == rows
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


@pytest.mark.parametrize("phase", [
    "transfer_intent", "unknown_transfer", "waiting_transfer",
])
def test_full_stop_allows_new_same_path_attempt_without_losing_server_history(tmp_path, phase):
    store = MigrationStore(tmp_path)
    old, entry, attempt = direct_unknown_attempt(store, phase=phase)
    store.update_attempt(attempt["id"], operation_id="still-pending-upload",
                         local_name="cached.part")
    before = store.attempt(attempt["id"])
    store.request_full_stop(old["id"])
    new = store.create(shared_config(), "reuse-stopped-file-path")
    store.add_entries(new["id"], [file_entry()])
    new_entry = store.entries(new["id"])[0]
    next_attempt = store.new_attempt(new_entry["id"], "url")
    assert next_attempt["remote_path"] == before["remote_path"]
    assert next_attempt["id"] != before["id"]
    assert store.attempt(attempt["id"]) == before
    assert store.entry(entry["id"])["state"] == "outcome_unknown"


@pytest.mark.parametrize("state", [
    "planned", "transferring", "waiting_operation", "verifying", "committing",
    "downloading", "staged", "outcome_unknown",
])
@pytest.mark.parametrize("kind", ["file", "dir"])
def test_local_cancellation_records_cancelled_state_without_faking_remote_failure(
    tmp_path, state, kind,
):
    store = MigrationStore(tmp_path)
    job = store.create(shared_config(), "local-cancel")
    store.add_entries(job["id"], [file_entry(kind=kind)])
    entry = store.entries(job["id"])[0]
    attempt = store.new_attempt(entry["id"], "staged")
    store.update_attempt(attempt["id"], phase="unknown_transfer",
                         operation_id="server-still-running", local_name="verified.part",
                         error="destination_timeout")
    cache = tmp_path / "staging" / "verified.part"
    cache.parent.mkdir()
    cache.write_bytes(b"already downloaded")
    store.update_entry(entry["id"], state=state, point_retry_requested=True,
                       point_retry_attempts=2, retry_at_ms=1800000030000)
    store.request_full_stop(job["id"])
    store.update_job(job["id"], state="stopped")
    before = store.attempt(attempt["id"])
    assert store.cancel_local_entries(job["id"]) == 1
    current = store.entry(entry["id"])
    assert current["state"] == "cancelled"
    assert current["error"] == "local_operation_cancelled"
    assert current["point_retry_requested"] is False
    assert current["retry_at_ms"] is None
    assert current["point_retry_attempts"] == 2
    assert current["attempt_id"] == attempt["id"]
    assert store.attempt(attempt["id"]) == before
    assert cache.read_bytes() == b"already downloaded"
    events = store.events(job["id"])
    assert events[-1]["type"] == "local_operations_cancelled"
    assert events[-1]["payload"] == {"count": 1}
    assert store.cancel_local_entries(job["id"]) == 0
    assert store.events(job["id"]) == events
    store.close()
    reopened = MigrationStore(tmp_path)
    assert reopened.entry(entry["id"]) == current
    assert reopened.attempt(attempt["id"]) == before
    assert reopened.cancel_local_entries(job["id"]) == 0


@pytest.mark.parametrize("state", ["done", "already_verified", "failed", "conflict", "cancelled"])
def test_local_cancellation_preserves_completed_and_terminal_results(tmp_path, state):
    store = MigrationStore(tmp_path)
    job, entry, attempt = direct_unknown_attempt(store)
    store.update_entry(entry["id"], state=state, error="destination_conflict"
                       if state == "conflict" else None)
    store.request_full_stop(job["id"])
    store.update_job(job["id"], state="stopped")
    before = store.entry(entry["id"])
    assert store.cancel_local_entries(job["id"]) == 0
    assert store.entry(entry["id"]) == before
    assert store.attempt(attempt["id"])["phase"] == "unknown_transfer"


@pytest.mark.parametrize("stop_kind", ["none", "legacy", "full_unacknowledged"])
def test_local_cancellation_requires_fully_stopped_job(tmp_path, stop_kind):
    store = MigrationStore(tmp_path)
    job, entry, attempt = direct_unknown_attempt(store)
    if stop_kind == "legacy":
        store.request_stop(job["id"])
        store.update_job(job["id"], state="stopped")
    elif stop_kind == "full_unacknowledged":
        store.request_full_stop(job["id"])
    before = store.entry(entry["id"])
    with pytest.raises(ValueError, match="job_not_fully_stopped"):
        store.cancel_local_entries(job["id"])
    assert store.entry(entry["id"]) == before
    assert store.attempt(attempt["id"])["phase"] == "unknown_transfer"


def test_release_old_reserved_conflicts_preserves_real_conflicts_and_job_state(tmp_path):
    store = MigrationStore(tmp_path)
    job = store.create(shared_config(), "old-reservation-conflicts")
    store.add_entries(job["id"], [file_entry(
        destination_path="disk:/Archive/" + name, source_path="disk:/Documents/" + name,
    ) for name in ("blocked.txt", "real-conflict.txt", "failed.txt")])
    entries = store.entries(job["id"])
    by_name = {entry["destination_path"].rsplit("/", 1)[1]: entry for entry in entries}
    blocked = by_name["blocked.txt"]
    real = by_name["real-conflict.txt"]
    failed = by_name["failed.txt"]
    attempt = store.new_attempt(blocked["id"], "url")
    store.update_attempt(attempt["id"], phase="failed", error="destination_path_reserved")
    store.update_entry(blocked["id"], state="conflict", error="destination_path_reserved")
    store.update_entry(real["id"], state="conflict", error="destination_conflict")
    store.update_entry(failed["id"], state="failed", error="destination_path_reserved")
    store.update_job(job["id"], state="completed_with_issues")
    before_attempt = store.attempt(attempt["id"])
    before_real = store.entry(real["id"])
    before_failed = store.entry(failed["id"])
    assert store.release_cancelled_path_conflicts() == 1
    changed = store.entry(blocked["id"])
    assert changed["state"] == "failed"
    assert changed["error"] == "stopped_job_path_released"
    assert store.get(job["id"])["state"] == "completed_with_issues"
    assert store.attempt(attempt["id"]) == before_attempt
    assert store.entry(real["id"]) == before_real
    assert store.entry(failed["id"]) == before_failed
    events = store.events(job["id"])
    assert events[-1]["type"] == "entry_updated"
    assert events[-1]["payload"] == {
        "entry_id": blocked["id"], "state": "failed", "error": "stopped_job_path_released",
    }
    assert store.release_cancelled_path_conflicts() == 0
    assert store.events(job["id"]) == events


@pytest.mark.parametrize("acknowledge", [False, True])
def test_stopped_job_cannot_be_resurrected(tmp_path, acknowledge):
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


def point_retry_job(store, *, phase="unknown_transfer", state="needs_attention",
                    extra_entries=()):
    job = store.create(config(), "point-retry-intent")
    store.add_entries(job["id"], [file_entry(), *extra_entries])
    entry = store.entries(job["id"])[0]
    attempt = store.new_attempt(entry["id"], "url")
    store.update_attempt(attempt["id"], phase=phase)
    store.update_entry(entry["id"], state="outcome_unknown",
                       error="transfer_outcome_unknown")
    job = store.update_job(job["id"], state=state, namespace_state="owned")
    return job, store.entry(entry["id"]), attempt


def test_point_retry_atomically_queues_only_selected_entry_and_keeps_evidence(tmp_path):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store, extra_entries=[file_entry(
        source_path="disk:/Documents/b.txt",
        destination_path="disk:/Migration/Documents/b.txt", resource_id="resource-2",
    )])
    second = store.entries(job["id"])[1]
    other_attempt = store.new_attempt(second["id"], "url")
    store.update_attempt(other_attempt["id"], phase="unknown_transfer")
    store.update_entry(second["id"], state="outcome_unknown")
    job = store.get(job["id"])
    before_second = store.entry(second["id"])
    before_attempt = store.attempt(attempt["id"])
    before_count = len(store.events(job["id"]))
    result = store.queue_point_retry(job["id"], entry["id"], attempt["id"],
                                     job["revision"])
    queued = store.entry(entry["id"])
    assert result["state"] == "running"
    assert result["revision"] == job["revision"] + 1
    assert result["resume_state"] is None
    assert result["pause_requested"] is False
    assert result["point_retry_active"] is True
    assert queued["point_retry_requested"] is True
    assert queued["point_retry_attempts"] == 0
    assert queued["retry_at_ms"] is None
    assert queued["error"] == "transfer_outcome_unknown"
    assert store.entry(second["id"]) == before_second
    assert store.attempt(attempt["id"]) == before_attempt
    events = store.events(job["id"])
    assert len(events) == before_count + 1
    assert events[-1]["type"] == "point_retry_requested"
    assert events[-1]["payload"] == {
        "entry_id": entry["id"], "attempt_id": attempt["id"],
        "stage": "point_retry", "status": "started", "max_point_retry_attempts": 3,
    }
    store.close()
    reopened = MigrationStore(tmp_path)
    assert reopened.entry(entry["id"])["point_retry_requested"] is True
    assert reopened.get(job["id"])["point_retry_active"] is True


@pytest.mark.parametrize("phase", [
    "unknown_transfer", "unknown_move", "transfer_intent", "move_intent",
])
@pytest.mark.parametrize("state", ["needs_attention", "paused", "awaiting_credentials"])
def test_point_retry_accepts_live_unknown_phases(tmp_path, phase, state):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store, phase=phase, state=state)
    if state == "awaiting_credentials":
        job = store.update_job(job["id"], resume_state="needs_attention")
    assert store.queue_point_retry(job["id"], entry["id"], attempt["id"],
                                   job["revision"])["state"] == "running"


@pytest.mark.parametrize("field,value,error", [
    ("state", "stopped", "job_stopped"),
    ("state", "running", "job_not_point_retryable"),
    ("state", "completed", "job_not_point_retryable"),
    ("namespace_state", "unknown", "technical_namespace_outcome_unknown"),
    ("stop_requested", True, "job_stopped"),
])
def test_point_retry_guards_job_before_mutating(tmp_path, field, value, error):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store)
    job = store.update_job(job["id"], **{field: value})
    before = store.entry(entry["id"])
    events = store.events(job["id"])
    with pytest.raises(ValueError, match=f"^{error}$"):
        store.queue_point_retry(job["id"], entry["id"], attempt["id"],
                                job["revision"])
    assert store.get(job["id"]) == job
    assert store.entry(entry["id"]) == before
    assert store.events(job["id"]) == events


def test_point_retry_rejects_stale_foreign_attempt_or_active_entry(tmp_path):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store)
    with pytest.raises(ValueError, match="^stale_revision$"):
        store.queue_point_retry(job["id"], entry["id"], attempt["id"],
                                job["revision"] - 1)
    foreign = store.create(config(), "point-retry-foreign")
    store.add_entries(foreign["id"], [file_entry()])
    foreign_entry = store.entries(foreign["id"])[0]
    foreign_attempt = store.new_attempt(foreign_entry["id"], "url")
    for candidate_entry, candidate_attempt in [
        (entry, foreign_attempt), (foreign_entry, foreign_attempt),
    ]:
        with pytest.raises(ValueError, match="^attempt_not_retryable$"):
            store.queue_point_retry(job["id"], candidate_entry["id"],
                                    candidate_attempt["id"], job["revision"])
    store.update_entry(entry["id"], point_retry_requested=True)
    current = store.get(job["id"])
    with pytest.raises(ValueError, match="^active_or_unknown_attempt$"):
        store.queue_point_retry(job["id"], entry["id"], attempt["id"],
                                current["revision"])


def test_point_retry_audit_failure_rolls_back_intent_and_job(tmp_path, monkeypatch):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store)
    before = store.entry(entry["id"])
    monkeypatch.setattr(store, "_event", lambda *a, **k: (_ for _ in ()).throw(
        RuntimeError("simulated-audit-failure"),
    ))
    with pytest.raises(RuntimeError, match="simulated-audit-failure"):
        store.queue_point_retry(job["id"], entry["id"], attempt["id"],
                                job["revision"])
    assert store.get(job["id"]) == job
    assert store.entry(entry["id"]) == before


def test_schema_five_upgrade_keeps_unknown_history_and_retry_defaults(tmp_path):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store)
    before_entries, before_events = store.entries(job["id"]), store.events(job["id"])
    store.close()
    with sqlite3.connect(tmp_path / "jobs.db") as connection:
        connection.execute("ALTER TABLE entries DROP COLUMN point_retry_requested")
        connection.execute("ALTER TABLE entries DROP COLUMN point_retry_attempts")
        connection.execute("PRAGMA user_version=5")
    reopened = MigrationStore(tmp_path)
    assert reopened.get(job["id"]) == job
    assert reopened.entries(job["id"]) == before_entries
    assert reopened.events(job["id"]) == before_events
    assert reopened.attempt(attempt["id"])["phase"] == "unknown_transfer"
    with sqlite3.connect(tmp_path / "jobs.db") as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 8


@pytest.mark.parametrize("fields", [
    {"point_retry_attempts": -1}, {"point_retry_attempts": 4},
    {"point_retry_attempts": True}, {"point_retry_attempts": None},
    {"point_retry_requested": 1}, {"point_retry_requested": "yes"},
])
def test_point_retry_rejects_invalid_entry_fields(tmp_path, fields):
    store = MigrationStore(tmp_path)
    job, entry, _ = point_retry_job(store)
    with pytest.raises(ValueError, match="^invalid_entry_value$"):
        store.update_entry(entry["id"], **fields)


@pytest.mark.parametrize("fields", [
    {"point_retry_attempts": -1}, {"point_retry_attempts": 4},
    {"point_retry_attempts": True}, {"max_point_retry_attempts": 0},
    {"max_point_retry_attempts": 4}, {"point_retry_requested": 1},
])
def test_point_retry_rejects_invalid_event_counters(tmp_path, fields):
    store = MigrationStore(tmp_path)
    job = store.create(config(), "invalid-point-retry-event")
    with pytest.raises(ValueError, match="^invalid_event_value$"):
        store.event(job["id"], "point_retry", fields)


@pytest.mark.parametrize("other_state", [
    "transferring", "waiting_operation", "verifying", "committing",
    "downloading", "staged",
])
def test_point_retry_rejects_other_active_entry(tmp_path, other_state):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store, extra_entries=[file_entry(
        source_path="disk:/Documents/b.txt",
        destination_path="disk:/Migration/Documents/b.txt",
    )])
    other = store.entries(job["id"])[1]
    store.update_entry(other["id"], state=other_state)
    job = store.get(job["id"])
    with pytest.raises(ValueError, match="^active_or_unknown_attempt$"):
        store.queue_point_retry(job["id"], entry["id"], attempt["id"],
                                job["revision"])
    assert not store.entry(entry["id"])["point_retry_requested"]


@pytest.mark.parametrize("fields", [
    {"state": "planned"}, {"state": "done"},
])
def test_point_retry_requires_current_unknown_entry(tmp_path, fields):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store)
    store.update_entry(entry["id"], **fields)
    job = store.get(job["id"])
    with pytest.raises(ValueError, match="^attempt_not_retryable$"):
        store.queue_point_retry(job["id"], entry["id"], attempt["id"],
                                job["revision"])
    assert not store.entry(entry["id"])["point_retry_requested"]


@pytest.mark.parametrize("reuse_local", [False, True])
def test_point_retry_replacement_atomically_preserves_history_and_cache_custody(
    tmp_path, reuse_local,
):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store)
    store.update_attempt(attempt["id"], local_name="owned-cache/file.ready",
                         operation_id="old-operation", error="destination_timeout")
    job = store.get(job["id"])
    store.queue_point_retry(job["id"], entry["id"], attempt["id"], job["revision"])
    store.update_entry(entry["id"], point_retry_attempts=1)
    new = store.new_attempt(entry["id"], "url", point_retry=True,
                            reuse_local=reuse_local)
    old, current = store.attempt(attempt["id"]), store.entry(entry["id"])
    assert old["phase"] == "abandoned_transfer"
    assert old["error"] == "destination_timeout"
    assert old["operation_id"] == "old-operation"
    assert old["local_name"] == (None if reuse_local else "owned-cache/file.ready")
    assert new["local_name"] == ("owned-cache/file.ready" if reuse_local else None)
    assert new["phase"] == ("staged" if reuse_local else "prepared")
    assert new["remote_path"] != old["remote_path"]
    assert current["attempt_id"] == new["id"]
    assert current["state"] == "transferring"
    assert current["error"] is None
    assert current["point_retry_requested"] is True
    assert current["point_retry_attempts"] == 1
    store.close()
    reopened = MigrationStore(tmp_path)
    assert reopened.entry(entry["id"])["attempt_id"] == new["id"]
    assert reopened.attempt(new["id"])["local_name"] == new["local_name"]


@pytest.mark.parametrize("phase", ["prepared", "staged", "succeeded"])
def test_point_retry_replacement_rejects_unsafe_old_attempt(tmp_path, phase):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store, phase=phase)
    store.update_entry(entry["id"], point_retry_requested=True)
    with pytest.raises(ValueError, match="^attempt_not_retryable$"):
        store.new_attempt(entry["id"], "url", point_retry=True)
    assert store.entry(entry["id"])["attempt_id"] == attempt["id"]
    assert store.attempt(attempt["id"])["phase"] == phase


def test_point_retry_replacement_audit_failure_keeps_old_cache_and_attempt(
    tmp_path, monkeypatch,
):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store)
    store.update_entry(entry["id"], point_retry_requested=True)
    store.update_attempt(attempt["id"], local_name="local/ready")
    before_entry = store.entry(entry["id"])
    before_attempt = store.attempt(attempt["id"])
    monkeypatch.setattr(store, "_event", lambda *a, **k: (_ for _ in ()).throw(
        RuntimeError("simulated-audit-failure"),
    ))
    with pytest.raises(RuntimeError, match="simulated-audit-failure"):
        store.new_attempt(entry["id"], "url", point_retry=True, reuse_local=True)
    assert store.entry(entry["id"]) == before_entry
    assert store.attempt(attempt["id"]) == before_attempt
    assert len(store.attempts(job["id"])) == 1


@pytest.mark.parametrize("state,other_state,expected_job,error", [
    ("done", "planned", "running", None),
    ("done", "outcome_unknown", "needs_attention", "transfer_outcome_unknown"),
    ("outcome_unknown", "done", "needs_attention", "destination_timeout"),
    ("done", "done", "completed", None),
    ("failed", "done", "completed_with_issues", None),
])
def test_point_retry_finish_atomically_updates_entry_job_and_event(
    tmp_path, state, other_state, expected_job, error,
):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store, extra_entries=[file_entry(
        source_path="disk:/Documents/b.txt",
        destination_path="disk:/Migration/Documents/b.txt",
    )])
    store.queue_point_retry(job["id"], entry["id"], attempt["id"], job["revision"])
    other = store.entries(job["id"])[1]
    store.update_entry(other["id"], state=other_state)
    store.update_entry(entry["id"], state=state, point_retry_attempts=3,
                       error="destination_timeout" if state != "done" else None,
                       retry_at_ms=1800000000000)
    before = store.get(job["id"])
    result = store.finish_point_retry(entry["id"])
    assert result["state"] == expected_job
    assert result["error"] == error
    assert result["point_retry_active"] is False
    assert result["revision"] == before["revision"] + 1
    current = store.entry(entry["id"])
    assert current["point_retry_requested"] is False
    assert current["point_retry_attempts"] == 3
    assert current["retry_at_ms"] is None
    event = store.events(job["id"])[-1]
    assert event["type"] == "transfer_stage"
    assert event["payload"]["stage"] == "point_retry"
    assert event["payload"]["status"] == ("succeeded" if state == "done" else "skipped")
    store.close()
    reopened = MigrationStore(tmp_path)
    assert reopened.get(job["id"])["state"] == expected_job
    assert reopened.get(job["id"])["point_retry_active"] is False


def test_point_retry_finish_failure_preserves_pending_intent(tmp_path, monkeypatch):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store)
    store.queue_point_retry(job["id"], entry["id"], attempt["id"], job["revision"])
    store.update_entry(entry["id"], state="done", point_retry_attempts=1)
    before_entry, before_job = store.entry(entry["id"]), store.get(job["id"])
    monkeypatch.setattr(store, "_event", lambda *a, **k: (_ for _ in ()).throw(
        RuntimeError("simulated-audit-failure"),
    ))
    with pytest.raises(RuntimeError, match="simulated-audit-failure"):
        store.finish_point_retry(entry["id"])
    assert store.get(job["id"]) == before_job
    assert store.entry(entry["id"]) == before_entry


@pytest.mark.parametrize("stop_state,stop_requested", [
    ("stopped", False), ("running", True),
])
def test_point_retry_finish_never_overrides_stop(tmp_path, stop_state, stop_requested):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store)
    store.queue_point_retry(job["id"], entry["id"], attempt["id"], job["revision"])
    store.update_job(job["id"], state=stop_state, stop_requested=stop_requested)
    result = store.finish_point_retry(entry["id"])
    assert result["state"] == stop_state
    assert result["stop_requested"] is stop_requested
    assert result["point_retry_active"] is False


@pytest.mark.parametrize("phase", ["transfer_intent", "unknown_transfer", "failed"])
def test_point_copy_confirmation_atomically_preserves_old_operation_evidence(
    tmp_path, phase,
):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store)
    store.queue_point_retry(job["id"], entry["id"], attempt["id"], job["revision"])
    store.update_entry(entry["id"], point_retry_attempts=3)
    store.update_attempt(attempt["id"], phase=phase, error="destination_timeout",
                         operation_id="old-upload-operation")
    store.confirm_point_copy(entry["id"])
    current, old = store.entry(entry["id"]), store.attempt(attempt["id"])
    assert current["state"] == "already_verified"
    assert current["verification"] == "md5"
    assert current["error"] is None
    assert current["point_retry_requested"] is True
    assert current["point_retry_attempts"] == 3
    assert old["phase"] == "abandoned_transfer"
    assert old["operation_id"] == "old-upload-operation"
    assert old["error"] == "destination_timeout"
    events = store.events(job["id"])
    assert [e["type"] for e in events[-2:]] == ["entry_updated", "attempt_updated"]
    store.close()
    reopened = MigrationStore(tmp_path)
    assert reopened.entry(entry["id"])["state"] == "already_verified"
    assert reopened.attempt(attempt["id"])["phase"] == "abandoned_transfer"


def test_point_copy_confirmation_rolls_back_both_changes_and_first_audit(
    tmp_path, monkeypatch,
):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store)
    store.queue_point_retry(job["id"], entry["id"], attempt["id"], job["revision"])
    before_entry = store.entry(entry["id"])
    before_attempt = store.attempt(attempt["id"])
    before_events, before_job = store.events(job["id"]), store.get(job["id"])
    original_event = store._event
    count = 0

    def fail_second_audit(*args, **kwargs):
        nonlocal count
        count += 1
        if count == 2:
            raise RuntimeError("simulated-second-audit-failure")
        return original_event(*args, **kwargs)

    monkeypatch.setattr(store, "_event", fail_second_audit)
    with pytest.raises(RuntimeError, match="simulated-second-audit-failure"):
        store.confirm_point_copy(entry["id"])
    assert store.entry(entry["id"]) == before_entry
    assert store.attempt(attempt["id"]) == before_attempt
    assert store.events(job["id"]) == before_events
    assert store.get(job["id"]) == before_job


@pytest.mark.parametrize("phase", ["unknown_move", "move_intent", "waiting_move"])
def test_point_copy_confirmation_preserves_old_move_as_abandoned(tmp_path, phase):
    store = MigrationStore(tmp_path)
    job, entry, attempt = point_retry_job(store, phase=phase)
    store.update_entry(entry["id"], point_retry_requested=True)
    store.confirm_point_copy(entry["id"])
    assert store.attempt(attempt["id"])["phase"] == "abandoned_move"
    assert store.entry(entry["id"])["state"] == "already_verified"


@pytest.mark.parametrize("strategy", ["url", "staged"])
def test_restart_shared_normalizes_only_child_destination_and_publication(tmp_path, strategy):
    store = MigrationStore(tmp_path)
    original = shared_config(strategy=strategy)
    parent = store.create(original, "fixed-shared-parent")
    store.add_entries(parent["id"], [file_entry()])
    entry = store.entries(parent["id"])[0]
    attempt = store.new_attempt(entry["id"], strategy)
    store.update_job(parent["id"], state="completed_with_issues")
    before_entry = store.entry(entry["id"])
    child = store.restart(parent["id"], original, "fixed-shared-child")
    assert child["config"] == {
        **original, "destination_base": "disk:/",
        "destination_root": "disk:/user@example.org", "source_public": True,
    }
    assert store.get(parent["id"])["config"] == original
    assert store.entry(entry["id"]) == before_entry
    assert store.attempt(attempt["id"]) == attempt
    assert store.restart(parent["id"], original, "fixed-shared-child") == child
    with pytest.raises(ValueError, match="restart_config_mismatch"):
        store.restart(parent["id"], child["config"], "fixed-shared-child")


def test_restart_replay_keeps_existing_historical_child_configuration(tmp_path):
    store = MigrationStore(tmp_path)
    original = shared_config()
    parent = store.create(original, "historical-parent")
    store.update_job(parent["id"], state="completed")
    with store._connection(write=True) as connection:
        serialized = connection.execute(
            "SELECT config FROM jobs WHERE id=?", (parent["id"],),
        ).fetchone()["config"]
        child = store._create_job(
            connection, serialized, "historical-child", restart_of=parent["id"],
        )
    assert store.restart(parent["id"], original, "historical-child") == child
    assert store.get(child["id"])["config"] == original
