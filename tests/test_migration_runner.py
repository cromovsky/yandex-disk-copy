"""Durable migration: prove outcomes before success or replay."""

import hashlib
import os
import time
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
    "strategy": "staged", "identity_confirmed": True,
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
        self.uploads = 0
        self.upload_error = None
        self.moves = 0
        self.operation = "success"
        self.import_error = None
        self.move_error = None
        self.upload_body = None

    def info(self):
        return {"total_space": 1000000, "used_space": 0,
                "user": {"login": "receiver"}}

    def stat(self, path):
        return {"type": "dir", "path": path} if path == "disk:/" else self.nodes.get(path)

    def ensure_dir(self, path):
        if path in self.nodes and self.nodes[path]["type"] != "dir":
            raise MigrationError("destination_conflict")
        self.nodes[path] = {"type": "dir", "path": path}

    def claim_dir(self, path):
        if path in self.nodes:
            raise MigrationError("technical_namespace_exists")
        self.nodes[path] = {"type": "dir", "path": path}

    def import_url(self, path, url, *, overwrite=False):
        self.imports += 1
        if self.import_error:
            raise self.import_error
        self.nodes[path] = dict(META, path=path)
        return "op-1"

    def upload(self, path, local_path, *, before_upload=None, on_progress=None, overwrite=False):
        if before_upload:
            before_upload(None)
        self.uploads += 1
        if self.upload_error:
            raise self.upload_error
        self.upload_body = Path(local_path).read_bytes()
        if on_progress:
            on_progress(0)
            on_progress(len(self.upload_body))
        self.nodes[path] = dict(META, path=path)
        return "op-1"

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


def legacy_job(store, job_id):
    """Existing namespace jobs retain their original upload and replay rules."""
    with store._connection(write=True) as connection:
        connection.execute(
            "UPDATE jobs SET remote_root=?,namespace_state='unclaimed' WHERE id=?",
            (f"disk:/.disk-copy-transfer-{job_id}", job_id),
        )


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
        now = max(time.time(), max(((e.get("retry_at_ms") or 0) / 1000
                                   for e in store.entries(job_id)), default=0))
        with pytest.MonkeyPatch.context() as clock:
            clock.setattr("app.migration.runner.time.time", lambda: now)
            runner.step(job_id)
        if store.get(job_id)["state"] != "running":
            break
    return store.get(job_id)


def cached_attempt(store, runner, job_id, *, phase="staged", count=1):
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    attempt = store.new_attempt(entry["id"], "staged")
    name = attempt["id"] + ".part"
    (runner.staging / name).write_bytes(BODY)
    store.update_attempt(attempt["id"], phase=phase, local_name=name,
                         operation_id="op-1" if phase == "unknown_transfer" else None)
    store.update_entry(entry["id"], state="outcome_unknown"
                       if phase == "unknown_transfer" else "staged",
                       download_attempts=count)
    return store.entry(entry["id"]), store.attempt(attempt["id"])


def test_global_cache_cleanup_clears_all_jobs_orphans_and_links_only(migration, tmp_path):
    store, _, runner, first_id, _, _ = migration
    first_entry, first_attempt = cached_attempt(store, runner, first_id)
    store.update_job(first_id, state="paused")
    other = store.create(CONFIG, "other-cached-job")
    store.add_entries(other["id"], [{
        "kind": "file", "source_path": META["path"],
        "destination_path": "disk:/owner/a.txt", "size": len(BODY),
        "md5": META["md5"], "modified": META["modified"],
        "resource_id": META["resource_id"],
    }])
    other_entry, other_attempt = cached_attempt(
        store, runner, other["id"], phase="unknown_transfer", count=5,
    )
    store.update_job(other["id"], state="needs_attention")
    orphan = runner.staging / "orphan.part"
    orphan.write_bytes(b"orphan")
    outside = tmp_path / "download-kept.bin"
    outside.write_bytes(b"keep me")
    (runner.staging / "outside-link").symlink_to(outside)
    (runner.staging / "missing-link").symlink_to(tmp_path / "missing")
    previous_events = {j: store.events(j) for j in (first_id, other["id"])}

    assert runner.cleanup_cache() == {"removed": 5, "removed_bytes": 2 * len(BODY) + 6}
    assert not list(runner.staging.iterdir())
    assert outside.read_bytes() == b"keep me"
    assert store.path.is_file()
    assert store.attempt(first_attempt["id"])["local_name"] is None
    updated = store.attempt(other_attempt["id"])
    assert updated["local_name"] is None
    assert updated["phase"] == "unknown_transfer"
    assert updated["operation_id"] == "op-1"
    assert store.entry(other_entry["id"]) == other_entry
    assert store.entry(first_entry["id"])["download_attempts"] == 0
    for job_id in (first_id, other["id"]):
        events = store.events(job_id)
        assert events[:len(previous_events[job_id])] == previous_events[job_id]
        assert events[-1]["type"] == "local_cleanup"
        assert events[-1]["payload"]["count"] == 1
        journal = store.events(job_id, journal=True)[-1]
        assert journal["stage_label"] == "Очистка локального кеша"
        assert journal["status_label"] == "Успешно"


def test_global_cache_cleanup_has_no_job_requirement(tmp_path):
    store = MigrationStore(tmp_path)
    runner = MigrationRunner(store, CredentialRegistry())
    try:
        (runner.staging / "orphan.part").write_bytes(BODY)
        assert runner.cleanup_cache() == {"removed": 1, "removed_bytes": len(BODY)}
        assert runner.cleanup_cache() == {"removed": 0, "removed_bytes": 0}
    finally:
        store.close()


@pytest.mark.parametrize("bad_object", ["directory", "root_symlink"])
def test_global_cache_cleanup_rejects_unsafe_tree_before_deleting(
    migration, tmp_path, bad_object,
):
    store, _, runner, job_id, _, _ = migration
    _, attempt = cached_attempt(store, runner, job_id)
    store.update_job(job_id, state="paused")
    protected = runner.staging / attempt["local_name"]
    if bad_object == "directory":
        (runner.staging / "unexpected").mkdir()
    else:
        replacement = tmp_path / "outside-staging"
        runner.staging.rename(replacement)
        runner.staging.symlink_to(replacement, target_is_directory=True)
        protected = replacement / attempt["local_name"]
    before = store.attempt(attempt["id"])
    with pytest.raises(MigrationError, match="invalid_staging_object"):
        runner.cleanup_cache()
    assert protected.read_bytes() == BODY
    assert store.attempt(attempt["id"]) == before


def test_global_cache_cleanup_io_failure_keeps_database_recovery_information(
    migration, monkeypatch,
):
    store, _, runner, job_id, _, _ = migration
    _, attempt = cached_attempt(store, runner, job_id)
    store.update_job(job_id, state="paused")
    before = store.attempt(attempt["id"])

    def fail_unlink(*args, **kwargs):
        raise OSError("private path and credential must never escape")

    monkeypatch.setattr(os, "unlink", fail_unlink)
    with pytest.raises(MigrationError) as error:
        runner.cleanup_cache()
    assert error.value.code == "cache_cleanup_failed"
    assert "private" not in str(error.value)
    assert store.attempt(attempt["id"]) == before
    assert not any(e["type"] == "local_cleanup" for e in store.events(job_id))


def test_global_cache_cleanup_allows_redownload_after_fifth_successful_download(migration):
    store, _, runner, job_id, source, destination = migration
    entry, attempt = cached_attempt(store, runner, job_id, count=5)
    store.update_job(job_id, state="awaiting_credentials", resume_state="running")
    runner.cleanup_cache()
    assert store.attempt(attempt["id"])["phase"] == "staged"
    assert store.entry(entry["id"])["download_attempts"] == 0
    assert finish(store, runner, job_id)["state"] == "completed"
    assert source.downloads == 1
    assert destination.upload_body == BODY


@pytest.mark.parametrize("phase", ["failed", "downloading", "unknown_transfer"])
def test_global_cache_cleanup_preserves_other_attempt_counters(migration, phase):
    store, _, runner, job_id, _, _ = migration
    entry, attempt = cached_attempt(store, runner, job_id, phase=phase, count=5)
    if phase == "downloading":
        (runner.staging / attempt["local_name"]).write_bytes(BODY[:4])
    store.update_job(job_id, state="paused")
    runner.cleanup_cache()
    assert store.entry(entry["id"])["download_attempts"] == 5
    assert store.attempt(attempt["id"])["phase"] == phase


def test_global_cache_cleanup_never_uses_database_reference_as_delete_path(migration, tmp_path):
    store, _, runner, job_id, _, _ = migration
    _, attempt = cached_attempt(store, runner, job_id)
    store.update_job(job_id, state="paused")
    outside = tmp_path / "keep.bin"
    outside.write_bytes(BODY)
    with store._connection(write=True) as connection:
        connection.execute("UPDATE attempts SET local_name='../keep.bin' WHERE id=?",
                           (attempt["id"],))
    runner.cleanup_cache()
    assert outside.read_bytes() == BODY
    assert store.attempt(attempt["id"])["local_name"] is None


def test_global_cache_cleanup_partial_failure_updates_only_removed_references(
    migration, monkeypatch,
):
    store, _, runner, job_id, _, _ = migration
    entry, attempt = cached_attempt(store, runner, job_id, count=5)
    store.update_job(job_id, state="paused")
    (runner.staging / attempt["local_name"]).rename(runner.staging / "000-cache.part")
    store.update_attempt(attempt["id"], local_name="000-cache.part")
    (runner.staging / "zzz-error.part").write_bytes(BODY)
    original = os.unlink

    def fail_second(name, **kwargs):
        if name == "zzz-error.part":
            raise OSError("private filesystem error")
        return original(name, **kwargs)

    monkeypatch.setattr(os, "unlink", fail_second)
    with pytest.raises(MigrationError, match="cache_cleanup_failed"):
        runner.cleanup_cache()
    assert not (runner.staging / "000-cache.part").exists()
    assert (runner.staging / "zzz-error.part").read_bytes() == BODY
    assert store.attempt(attempt["id"])["local_name"] is None
    assert store.entry(entry["id"])["download_attempts"] == 0
    event = store.events(job_id, journal=True)[-1]
    assert event["type"] == "local_cleanup"
    assert event["payload"]["status"] == "failed"
    assert event["payload"]["error"] == "cache_cleanup_failed"


def test_global_cache_cleanup_pins_directory_even_if_its_path_is_replaced(
    migration, tmp_path, monkeypatch,
):
    store, _, runner, job_id, _, _ = migration
    _, attempt = cached_attempt(store, runner, job_id)
    store.update_job(job_id, state="paused")
    outside = tmp_path / "unrelated"
    outside.mkdir()
    sentinel = outside / attempt["local_name"]
    sentinel.write_bytes(BODY)
    pinned = tmp_path / "original-cache"
    original = os.unlink

    def replace_then_unlink(name, **kwargs):
        runner.staging.rename(pinned)
        runner.staging.symlink_to(outside, target_is_directory=True)
        return original(name, **kwargs)

    monkeypatch.setattr(os, "unlink", replace_then_unlink)
    assert runner.cleanup_cache() == {"removed": 1, "removed_bytes": len(BODY)}
    assert sentinel.read_bytes() == BODY
    assert not list(pinned.iterdir())


def test_global_cache_cleanup_recovers_crash_between_unlink_and_db_update(migration):
    store, _, runner, job_id, source, destination = migration
    entry, attempt = cached_attempt(store, runner, job_id, count=5)
    (runner.staging / attempt["local_name"]).unlink()
    assert finish(store, runner, job_id)["state"] == "completed"
    assert source.downloads == 1
    assert store.entry(entry["id"])["download_attempts"] == 1
    assert destination.upload_body == BODY


def test_global_cache_cleanup_recovery_does_not_reset_budget_for_bad_copy(migration):
    store, _, runner, job_id, source, destination = migration
    entry, attempt = cached_attempt(store, runner, job_id, count=5)
    (runner.staging / attempt["local_name"]).write_bytes(b"damaged")
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert source.downloads == 0
    assert store.entry(entry["id"])["download_attempts"] == 5
    assert destination.upload_body is None


@pytest.mark.parametrize("crash_after_unlink", [False, True])
def test_global_cache_cleanup_recovers_fsynced_copy_before_staged_checkpoint(
    migration, monkeypatch, crash_after_unlink,
):
    store, _, runner, job_id, source, destination = migration
    entry, attempt = cached_attempt(store, runner, job_id, phase="downloading", count=5)
    store.update_job(job_id, state="awaiting_credentials", resume_state="running")
    if crash_after_unlink:
        with monkeypatch.context() as crash:
            def fail(*args, **kwargs):
                raise OSError("simulated crash before cleanup database checkpoint")
            crash.setattr(store, "clear_cache_references", fail)
            with pytest.raises(MigrationError, match="cache_cleanup_failed"):
                runner.cleanup_cache()
        assert store.attempt(attempt["id"])["local_name"] == attempt["local_name"]
    else:
        runner.cleanup_cache()
        assert store.attempt(attempt["id"])["local_name"] is None
    assert store.attempt(attempt["id"])["phase"] == "staged"
    assert not (runner.staging / attempt["local_name"]).exists()
    assert finish(store, runner, job_id)["state"] == "completed"
    assert source.downloads == 1
    assert store.entry(entry["id"])["download_attempts"] == 1
    assert destination.upload_body == BODY


def test_staged_download_retries_then_succeeds(migration):
    store, _, runner, job_id, source, destination = migration
    runner.default_strategy = "staged"
    original = source.chunks
    failures = []

    def flaky(path):
        if len(failures) < 2:
            failures.append(path)
            yield BODY[:2]
            raise MigrationError("source_stream_interrupted", http_status=503)
        yield from original(path)

    source.chunks = flaky
    assert finish(store, runner, job_id)["state"] == "completed"
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    assert entry["download_attempts"] == 3
    assert entry["retry_at_ms"] is None
    assert destination.upload_body == BODY
    assert not list(runner.staging.iterdir())
    retries = stage_events(store, job_id, "download_retry")
    assert [e["payload"]["download_attempts"] for e in retries] == [1, 2]
    assert all(e["payload"]["http_status"] == 503 for e in retries)


def test_five_download_failures_skip_and_continue(migration):
    store, _, runner, job_id, source, destination = migration
    runner.default_strategy = "staged"
    calls = []

    def failing(path):
        calls.append(path)
        yield BODY[:1]
        raise MigrationError("source_download_failed", http_status=502)

    source.chunks = failing
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    assert len(calls) == entry["download_attempts"] == 5
    assert entry["error"] == "source_download_failed"
    assert entry["state"] == "failed"
    assert not list(runner.staging.iterdir())
    assert destination.upload_body is None
    assert destination.nodes["disk:/owner/Docs/empty"]["type"] == "dir"
    retries = stage_events(store, job_id, "download_retry")
    assert [e["payload"]["status"] for e in retries] == ["waiting"] * 4 + ["skipped"]
    assert retries[-1]["payload"]["error"] == entry["error"]
    runner.retry(job_id, "staged")
    assert store.entry(entry["id"])["download_attempts"] == 0


def test_download_wait_is_persistent_quiet_and_stoppable(migration, monkeypatch):
    store, _, runner, job_id, source, _ = migration
    runner.default_strategy = "staged"
    calls = []

    def failing(path):
        calls.append(path)
        raise MigrationError("source_timeout")
        yield

    source.chunks = failing
    attempt = run_until_attempt(store, runner, job_id)
    entry = store.entry(attempt["entry_id"])
    assert entry["download_attempts"] == 1
    assert attempt["phase"] == "download_retry_wait"
    assert entry["retry_at_ms"] > int(time.time() * 1000)
    count = len(store.events(job_id, limit=10000))
    for _ in range(3):
        runner.step(job_id)
    assert len(calls) == 1
    assert len(store.events(job_id, limit=10000)) == count
    reopened = MigrationStore(store.root)
    assert reopened.entry(entry["id"])["retry_at_ms"] == entry["retry_at_ms"]
    reopened.close()
    runner.stop_job(job_id)
    assert runner.final_pending(job_id) == []
    runner.step(job_id)
    assert len(calls) == 1


def test_historical_url_terminal_failure_never_allocates_another_attempt(migration):
    store, _, runner, job_id, source, destination = migration
    previous = historical_url_operation(store, job_id, destination)
    destination.operation = "failed"
    runner.step(job_id)
    assert store.attempt(previous["id"])["phase"] == "failed"
    assert store.entry(previous["entry_id"])["error"] == "destination_url_import_failed"
    runner.step(job_id)
    assert store.get(job_id)["error"] == "onprem_staged_required"
    assert len(store.attempts(job_id)) == 1
    assert destination.imports == source.downloads == 0
    assert not stage_events(store, job_id, "download_retry")


def test_historical_url_failure_keeps_confirmed_operation_without_retry_allocation(
    migration, monkeypatch,
):
    store, _, runner, job_id, source, destination = migration
    previous = historical_url_operation(store, job_id, destination)
    destination.operation = "failed"
    monkeypatch.setattr(store, "new_attempt", lambda *args, **kwargs: pytest.fail("URL retry allocation"))
    runner.step(job_id)
    before = store.attempt(previous["id"])
    assert before["phase"] == "failed"
    assert before["operation_id"] == "op-1"
    store.recover()
    runner.step(job_id)
    assert store.attempt(previous["id"]) == before
    assert destination.imports == source.downloads == 0


def stage_events(store, job_id, stage=None):
    events = store.events(job_id, limit=10000)
    return [event for event in events if event["type"] == "transfer_stage"
            and (stage is None or event["payload"]["stage"] == stage)]


def test_journal_records_directory_and_verified_staged_transfer(migration):
    store, _, runner, job_id, _, _ = migration
    assert finish(store, runner, job_id)["state"] == "completed"
    events = stage_events(store, job_id)
    stages = {event["payload"]["stage"] for event in events}
    assert {"planning", "create_directory", "check_source", "downloading",
            "upload_file", "poll_transfer", "verify_temporary",
            "move_file", "verify_destination", "cleanup"} <= stages
    for event in events:
        payload = event["payload"]
        if payload["status"] != "started":
            assert payload["duration_ms"] >= 0
    assert "never-persist" not in str(events)



@pytest.mark.parametrize("uncertain", [False, True])
def test_journal_identifies_upload_failure_before_attempt_phase_changes(
    migration, uncertain,
):
    store, _, runner, job_id, _, destination = migration
    error = MigrationError("destination_unavailable", uncertain=uncertain)
    error.http_status = 503
    destination.upload_error = error
    finish(store, runner, job_id)
    events = stage_events(store, job_id, "upload_file")
    assert len(events) == 2
    payload = events[-1]["payload"]
    assert payload["status"] == "failed"
    assert payload["error"] == "destination_unavailable"
    assert payload["http_status"] == 503
    assert payload["entry_id"] and payload["attempt_id"]
    assert events[-1]["context"]["source"] == META["path"]



def test_journal_identifies_failed_directory(migration):
    store, _, runner, job_id, _, destination = migration
    original = destination.ensure_dir

    def refuse_folder(path):
        if path == "disk:/owner/Docs":
            raise MigrationError("destination_read_only")
        return original(path)

    destination.ensure_dir = refuse_folder
    finish(store, runner, job_id)
    failures = [event for event in stage_events(store, job_id, "create_directory")
                if event["payload"]["status"] == "failed"]
    assert len(failures) == 1
    assert failures[0]["context"]["kind"] == "dir"
    assert failures[0]["context"]["destination"] == "disk:/owner/Docs"
    assert failures[0]["payload"]["error"] == "destination_read_only"


def test_journal_reports_hash_absence_as_verification_failure(migration):
    store, _, runner, job_id, _, destination = migration
    original = destination.stat

    def no_hash(path):
        value = original(path)
        return {k: v for k, v in value.items() if k != "md5"} if value else value

    destination.stat = no_hash
    finish(store, runner, job_id)
    failure = stage_events(store, job_id, "verify_temporary")[-1]["payload"]
    assert failure["status"] == "failed"
    assert failure["error"] == "destination_hash_unavailable"


def test_journal_reports_staging_budget_and_remote_terminal_failure(migration):
    store, _, runner, job_id, _, _ = migration
    runner.default_strategy = "staged"
    runner.staging_budget = 1
    finish(store, runner, job_id)
    failure = stage_events(store, job_id, "downloading")[-1]["payload"]
    assert failure["status"] == "failed"
    assert failure["error"] == "staging_space_insufficient"


def test_journal_reports_remote_failed_operation(migration):
    store, _, runner, job_id, _, destination = migration
    destination.operation = "failed"
    finish(store, runner, job_id)
    failure = stage_events(store, job_id, "poll_transfer")[-1]["payload"]
    assert failure["status"] == "failed"
    assert failure["remote_status"] == "failed"
    assert failure["error"] == "remote_operation_failed"



def test_journal_identifies_file_without_size_during_planning(unplanned_migration):
    store, _, runner, job_id, source, _ = unplanned_migration
    source.metadata.pop("size")
    runner.step(job_id)
    failure = stage_events(store, job_id, "planning")[-1]
    assert failure["payload"]["status"] == "failed"
    assert failure["payload"]["error"] == "source_size_unavailable"
    assert failure["context"]["source"] == META["path"]


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


@pytest.fixture
def direct_migration(unplanned_migration):
    store, registry, runner, _, source, destination = unplanned_migration
    config = dict(CONFIG, destination_type="shared", destination_vd_hash="archive",
                  destination_base="disk:/Base",
                  destination_root="disk:/Base/owner@example.org")
    job_id = store.create(config, "new-direct-shared")["id"]
    assert store.get(job_id)["remote_root"] == ""
    registry.put(job_id, config, {
        "source_client_secret": "s", "destination_token": "d",
    })
    runner.step(job_id)
    assert store.get(job_id)["state"] == "ready"
    return store, registry, runner, job_id, source, destination


@pytest.mark.parametrize("strategy", ["url", "staged"])
def test_direct_transfer_writes_original_filename_without_technical_dirs_or_move(
    direct_migration, strategy,
):
    store, _, runner, job_id, _, destination = direct_migration
    runner.default_strategy = strategy
    if strategy == "url":
        assert finish(store, runner, job_id)["state"] == "completed_with_issues"
        assert store.entries(job_id, state="failed")[0]["error"] == "onprem_staged_required"
        assert destination.imports == destination.uploads == 0
        return
    assert finish(store, runner, job_id)["state"] == "completed"
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    assert store.attempts(job_id)[0]["remote_path"] == entry["destination_path"]
    assert destination.nodes[entry["destination_path"]]["md5"] == META["md5"]
    assert destination.moves == 0
    assert not any(".disk-copy" in path for path in destination.nodes)
    assert not stage_events(store, job_id, "claim_namespace")
    assert not stage_events(store, job_id, "verify_temporary")
    assert stage_events(store, job_id, "verify_destination")



@pytest.mark.parametrize("strategy", ["url", "staged"])
def test_direct_prepared_retry_does_not_overwrite_conflicting_final_file(
    direct_migration, strategy,
):
    store, _, runner, job_id, source, destination = direct_migration
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    attempt = store.new_attempt(entry["id"], strategy)
    final = dict(META, path=entry["destination_path"], md5="f" * 32)
    destination.nodes[entry["destination_path"]] = final
    if strategy == "url":
        assert finish(store, runner, job_id)["state"] == "needs_attention"
        assert store.get(job_id)["error"] == "onprem_staged_required"
        assert destination.nodes[entry["destination_path"]] == final
        assert destination.imports == destination.uploads == source.downloads == 0
        assert store.attempt(attempt["id"])["phase"] == "prepared"
        return
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert store.entry(entry["id"])["state"] == "conflict"
    assert destination.nodes[entry["destination_path"]] == final
    assert destination.imports == destination.moves == source.downloads == 0
    assert destination.upload_body is None
    assert store.attempt(attempt["id"])["phase"] == "failed"



@pytest.mark.parametrize("status", ["success", "failed"])
def test_direct_accepted_upload_is_never_polled_after_stop(direct_migration, status):
    store, registry, runner, job_id, _, destination = direct_migration
    attempt = start_accepted_operation(store, runner, job_id, destination)
    runner.stop_job(job_id)
    assert runner.final_pending(job_id) == [store.attempt(attempt["id"])]
    assert registry.get(job_id) is not None
    destination.operation = status
    destination.poll = lambda reference: pytest.fail("stopped job must not poll")
    before = store.attempt(attempt["id"])
    for _ in range(3):
        runner.step(job_id)
    assert store.get(job_id)["state"] == "stopped"
    assert store.attempt(attempt["id"]) == before
    assert runner.final_pending(job_id) == [before]
    assert destination.uploads == 1
    assert destination.imports == 0
    assert destination.moves == 0



def test_direct_historical_url_failure_preserves_original_path_without_retry(direct_migration):
    store, _, runner, job_id, source, destination = direct_migration
    previous = historical_url_operation(store, job_id, destination)
    destination.operation = "failed"
    runner.step(job_id)
    runner.step(job_id)
    assert store.attempt(previous["id"])["phase"] == "failed"
    assert store.attempt(previous["id"])["remote_path"] == store.entry(previous["entry_id"])["destination_path"]
    assert len(store.attempts(job_id)) == 1
    assert destination.imports == destination.moves == source.downloads == 0
    assert store.get(job_id)["error"] == "onprem_staged_required"


def test_direct_historical_url_failure_never_overwrites_partial_final_file(direct_migration):
    store, _, runner, job_id, _, destination = direct_migration
    previous = historical_url_operation(store, job_id, destination)
    destination.nodes[previous["remote_path"]]["md5"] = "0" * 32
    destination.operation = "failed"
    runner.step(job_id)
    runner.step(job_id)
    entry = store.entry(previous["entry_id"])
    assert entry["state"] == "failed"
    assert destination.nodes[entry["destination_path"]]["md5"] == "0" * 32
    assert destination.imports == destination.uploads == destination.moves == 0
    assert len(store.attempts(job_id)) == 1


def test_direct_confirmed_failed_url_history_recovers_without_attempt_allocation(
    direct_migration, monkeypatch,
):
    store, _, runner, job_id, source, destination = direct_migration
    previous = historical_url_operation(store, job_id, destination)
    store.update_attempt(previous["id"], phase="failed", error="destination_url_import_failed")
    store.update_entry(previous["entry_id"], state="downloading", retry_at_ms=1)
    monkeypatch.setattr(store, "new_attempt", lambda *args, **kwargs: pytest.fail("URL retry allocation"))
    store.recover()
    store.update_job(job_id, state="running")
    before = store.attempt(previous["id"])
    runner.step(job_id)
    assert store.get(job_id)["error"] == "onprem_staged_required"
    assert store.attempt(previous["id"]) == before
    assert destination.imports == source.downloads == 0


def test_direct_known_preconnect_failure_retry_reuses_verified_local_cache(
    direct_migration,
):
    store, _, runner, job_id, source, destination = direct_migration
    runner.default_strategy = "staged"
    paths = []
    original = destination.upload

    def upload(path, local_path, *, before_upload, on_progress=None):
        paths.append(path)
        if len(paths) == 1:
            before_upload("op-unused")
            raise MigrationError("destination_connection_failed", diagnostics={
                "error_side": "onprem", "transport_error": "dns",
            })
        return original(path, local_path, before_upload=before_upload,
                        on_progress=on_progress)

    destination.upload = upload
    assert finish(store, runner, job_id)["state"] == "needs_attention"
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    old = store.attempt(entry["attempt_id"])
    assert old["phase"] == "failed"
    assert entry["state"] == "failed"
    assert old["operation_id"] == "op-unused"
    assert entry["error"] == "destination_connection_failed"
    assert not runner.final_pending(job_id)
    assert source.downloads == 1
    assert (runner.staging / old["local_name"]).read_bytes() == BODY
    failed = [e for e in stage_events(store, job_id, "upload_file")
              if e["payload"]["status"] == "failed"]
    assert failed[-1]["payload"]["transport_error"] == "dns"
    source.chunks = lambda path: pytest.fail("verified cache must not be downloaded again")
    runner.retry(job_id, "staged")
    assert finish(store, runner, job_id)["state"] == "completed"
    attempts = store.attempts(job_id)
    assert len(attempts) == 2
    assert attempts[1]["id"] != old["id"]
    assert paths == [entry["destination_path"]] * 2
    assert source.downloads == 1
    assert destination.upload_body == BODY
    assert destination.imports == destination.moves == 0
    assert not list(runner.staging.iterdir())


def test_direct_quota_does_not_require_second_remote_copy(direct_migration):
    store, registry, runner, original_id, _, destination = direct_migration
    config = store.get(original_id)["config"]
    job_id = store.create(config, "exact-direct-quota")["id"]
    registry.put(job_id, config, {
        "source_client_secret": "s", "destination_token": "d",
    })
    destination.info = lambda: {"total_space": len(BODY), "used_space": 0,
                                "user": {"login": "receiver"}}
    runner.step(job_id)
    assert store.get(job_id)["state"] == "ready"


def historical_url_operation(store, job_id, destination, *, phase="waiting_transfer", count=1):
    """Seed remote evidence from an older build; never submit an import in tests."""
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    for directory in store.entries(job_id):
        if directory["kind"] == "dir":
            store.update_entry(directory["id"], state="done")
    attempt = store.new_attempt(entry["id"], "url")
    store.update_attempt(attempt["id"], phase=phase, operation_id="op-1")
    store.update_entry(entry["id"], state="waiting_operation", download_attempts=count)
    store.update_job(job_id, state="running", namespace_state="owned")
    destination.nodes[attempt["remote_path"]] = dict(META, path=attempt["remote_path"])
    return store.attempt(attempt["id"])


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


def test_staged_mode_preserves_folder_and_empty_dirs_and_cleans_local_bytes(migration):
    store, _, runner, job_id, source, dst = migration
    job = finish(store, runner, job_id)
    assert job["state"] == "completed"
    assert dst.nodes["disk:/owner/Docs/a.txt"]["md5"] == META["md5"]
    assert dst.nodes["disk:/owner/Docs/empty"]["type"] == "dir"
    assert dst.uploads == 1
    assert dst.imports == 0
    assert source.downloads == 1
    assert not list((Path(store.root) / "staging").glob("*"))



def run_until_attempt(store, runner, job_id):
    store.update_job(job_id, state="running")
    for _ in range(10):
        runner.step(job_id)
        if store.attempts(job_id):
            return store.attempts(job_id)[0]
    pytest.fail("file attempt was not reached")


def test_stop_after_upload_response_preserves_reference_without_poll_or_move(
    migration,
):
    store, _, runner, job_id, _, dst = migration
    original = dst.upload

    def upload_and_stop(path, local, **kwargs):
        operation = original(path, local, **kwargs)
        store.request_stop(job_id)
        return operation

    dst.upload = upload_and_stop
    dst.poll = lambda operation: pytest.fail("stopped import must not be polled")
    attempt = run_until_attempt(store, runner, job_id)
    assert attempt["phase"] == "waiting_transfer"
    assert attempt["operation_id"] == "op-1"
    assert store.get(job_id)["stop_requested"] is True
    for _ in range(3):
        runner.step(job_id)
    assert dst.uploads == 1
    assert dst.imports == 0
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

    def upload_and_stop(path, local, *, before_upload, **kwargs):
        before_upload(None)
        store.request_stop(job_id)
        raise MigrationError("credentials_required", auth=True)

    dst.upload = upload_and_stop
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


@pytest.mark.parametrize("status", ["in-progress", "success", "failed"])
def test_stopped_final_move_never_polls_or_finalizes(migration, status):
    store, _, runner, job_id, source, dst = migration
    attempt = start_accepted_operation(store, runner, job_id, dst)
    store.update_attempt(attempt["id"], phase="waiting_move")
    store.update_entry(attempt["entry_id"], state="committing")
    store.update_job(job_id, state="stopped")
    before = dict(dst.nodes)
    downloads = source.downloads
    dst.operation = status
    dst.poll = lambda reference: pytest.fail("stopped job must not poll")
    for _ in range(3):
        runner.step(job_id)
    assert store.get(job_id)["state"] == "stopped"
    assert store.attempt(attempt["id"])["phase"] == "waiting_move"
    assert store.attempt(attempt["id"])["operation_id"] == "op-1"
    assert dst.moves == 0
    assert dst.nodes == before
    assert source.downloads == downloads


def test_stopped_final_move_after_restart_constructs_no_clients(migration):
    store, registry, runner, job_id, _, dst = migration
    attempt = start_accepted_operation(store, runner, job_id, dst)
    store.update_attempt(attempt["id"], phase="waiting_move")
    store.update_entry(attempt["entry_id"], state="committing")
    store.update_job(job_id, state="stopped")
    runner.close()
    runner.source_factory = lambda config, credentials: pytest.fail(
        "stopped job must not construct SaaS source",
    )
    runner.destination_factory = lambda config, credentials: pytest.fail(
        "stopped job must not construct On-premises destination",
    )
    dst.operation = "success"
    runner.step(job_id)
    assert store.attempt(attempt["id"])["phase"] == "waiting_move"
    assert store.get(job_id)["state"] == "stopped"
    assert registry.get(job_id) is not None
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
    dst.upload_error = MigrationError("transfer_outcome_unknown", uncertain=True)
    assert finish(store, runner, job_id)["state"] == "needs_attention"
    attempt = store.attempts(job_id)[0]
    store.update_job(job_id, state="stopped")
    dst.upload_error = None
    dst.nodes[attempt["remote_path"]] = dict(META)
    runner.step(job_id)
    assert store.attempt(attempt["id"]) == attempt
    assert dst.moves == 0
    assert dst.uploads == 1



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
    assert dst.uploads == dst.moves == 1
    assert store.get(job_id)["state"] == "completed"




def test_fresh_restart_never_reuses_abandoned_unknown_attempt(migration):
    store, registry, runner, job_id, _, dst = migration
    dst.upload_error = MigrationError("transfer_outcome_unknown", uncertain=True)
    assert finish(store, runner, job_id)["state"] == "needs_attention"
    attempt = store.attempts(job_id)[0]
    store.request_stop(job_id)
    runner.stop_job(job_id)
    child = store.restart(job_id, CONFIG, "fresh-plan-after-stop")
    registry.put(child["id"], CONFIG, {
        "source_client_secret": "s", "destination_token": "d",
    })
    dst.upload_error = None
    runner.step(child["id"])
    assert finish(store, runner, child["id"])["state"] == "completed"
    assert store.get(job_id)["state"] == "stopped"
    assert store.attempt(attempt["id"]) == attempt
    latest = store.attempts(child["id"])[0]
    assert latest["remote_path"].startswith(child["remote_root"] + "/")
    assert latest["remote_path"] != attempt["remote_path"]
    assert dst.uploads == 2
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


def test_shared_preflight_shows_selected_disk_quota_permissions_and_full_path(
    unplanned_migration,
):
    _, _, runner, _, _, dst = unplanned_migration
    config = dict(CONFIG, destination_type="shared", destination_vd_hash="archive",
                  destination_base="disk:/Base",
                  destination_root="disk:/Base/owner@example.org")
    dst.info = lambda: {
        "user": {"login": "receiver"}, "vd_hash": "archive", "name": "Архив",
        "permissions": ["read", "write"],
        "total_space": 200, "used_space": 50,
    }
    result = runner.preflight(config, {})["destination"]
    assert result == {
        "identity": "receiver", "free_bytes": 150, "type": "shared",
        "vd_hash": "archive", "name": "Архив",
        "permissions": ["read", "write"],
        "path": "vd:archive:disk:/Base/owner@example.org",
    }


def test_staged_lost_put_keeps_operation_checkpoint_and_never_replays(migration):
    store, _, runner, job_id, _, dst = migration
    runner.default_strategy = "staged"
    puts = []

    def lost_put(path, local_path, *, before_upload=None, on_progress=None):
        before_upload("upload-op")
        checkpoint = store.attempts(job_id)[0]
        assert checkpoint["phase"] == "transfer_intent"
        assert checkpoint["operation_id"] == "upload-op"
        puts.append(path)
        raise MigrationError("destination_unavailable", uncertain=True)

    dst.upload = lost_put
    result = finish(store, runner, job_id)
    assert result["state"] == "needs_attention"
    attempt = store.attempts(job_id)[0]
    assert attempt["phase"] == "abandoned_transfer"
    assert attempt["operation_id"] == "upload-op"
    assert store.entry(attempt["entry_id"])["state"] == "failed"
    assert (runner.staging / attempt["local_name"]).read_bytes() == BODY
    store.recover()
    for _ in range(3):
        runner.step(job_id)
    assert len(puts) == 1
    assert (runner.staging / attempt["local_name"]).read_bytes() == BODY


def test_stop_after_get_upload_checkpoint_prevents_put(migration):
    store, _, runner, job_id, _, dst = migration
    runner.default_strategy = "staged"

    def stopped_before_put(path, local_path, *, before_upload=None, on_progress=None):
        store.request_stop(job_id)
        before_upload("upload-op")
        pytest.fail("PUT must be prevented by the callback checkpoint")

    dst.upload = stopped_before_put
    attempt = run_until_attempt(store, runner, job_id)
    assert attempt["operation_id"] == "upload-op"
    assert attempt["phase"] == "transfer_intent"
    assert store.get(job_id)["stop_requested"] is True


def test_get_upload_auth_failure_requires_explicit_retry_before_any_put(migration):
    store, registry, runner, job_id, source, dst = migration
    runner.default_strategy = "staged"
    original = dst.upload
    calls = []

    def get_auth_failure(path, local_path, *, before_upload=None, on_progress=None):
        calls.append(path)
        if len(calls) == 1:
            raise MigrationError("destination_auth_failed", auth=True)
        return original(path, local_path, before_upload=before_upload,
                        on_progress=on_progress)

    dst.upload = get_auth_failure
    assert finish(store, runner, job_id)["state"] == "awaiting_credentials"
    attempt = store.attempts(job_id)[0]
    assert attempt["phase"] == "failed"
    assert attempt["operation_id"] is None
    assert dst.upload_body is None
    entry = store.entry(attempt["entry_id"])
    assert entry["state"] == "failed"
    assert store.get(job_id)["resume_state"] == "needs_attention"
    assert (runner.staging / attempt["local_name"]).read_bytes() == BODY
    registry.put(job_id, CONFIG, {
        "source_client_secret": "s", "destination_token": "fresh-token",
    })
    runner.step(job_id)
    assert store.get(job_id)["state"] == "awaiting_credentials"
    assert len(calls) == 1
    assert dst.upload_body is None
    store.update_job(job_id, state=store.get(job_id)["resume_state"], resume_state=None)
    assert store.get(job_id)["state"] == "needs_attention"
    source.stat = lambda path: pytest.fail("point retry must use the planned MD5")
    source.chunks = lambda path: pytest.fail("point retry must reuse verified cache")
    store.queue_point_retry(job_id, entry["id"], attempt["id"],
                            store.get(job_id)["revision"])
    for _ in range(10):
        runner.step(job_id)
        if not store.get(job_id)["point_retry_active"]:
            break
    assert store.entry(entry["id"])["state"] == "done"
    assert dst.upload_body == BODY
    assert len(calls) == 2
    assert source.downloads == 1


@pytest.mark.parametrize("strategy", ["url", "staged"])
def test_shared_job_preserves_employee_tree_and_own_technical_namespace(
    tmp_path, strategy,
):
    config = dict(CONFIG, destination_type="shared", destination_vd_hash="archive",
                  destination_base="disk:/Base",
                  destination_root="disk:/Base/owner@example.org", strategy=strategy)
    store, registry = MigrationStore(tmp_path), CredentialRegistry()
    source, destination = Source(), Destination()
    seen = []

    def destination_factory(binding, credentials):
        seen.append(dict(binding))
        return destination

    runner = MigrationRunner(
        store, registry, source_factory=lambda c, a: source,
        destination_factory=destination_factory, reserve_bytes=0,
    )
    try:
        job = store.create(config, "shared-tree")
        legacy_job(store, job["id"])
        job = store.get(job["id"])
        registry.put(job["id"], config, {
            "source_client_secret": "s", "destination_token": "d",
        })
        runner.step(job["id"])
        if strategy == "url":
            assert store.get(job["id"])["state"] == "needs_attention"
            assert store.get(job["id"])["error"] == "onprem_staged_required"
            assert destination.nodes == {}
            return
        assert store.get(job["id"])["state"] == "ready"
        assert finish(store, runner, job["id"])["state"] == "completed"
        assert destination.nodes["disk:/Base/owner@example.org/Docs/a.txt"]["md5"] == (
            META["md5"]
        )
        assert destination.nodes["disk:/Base/owner@example.org/Docs/empty"]["type"] == (
            "dir"
        )
        attempt = store.attempts(job["id"])[0]
        assert attempt["remote_path"].startswith(job["remote_root"] + "/")
        assert seen == [config]
    finally:
        runner.close()
        store.close()



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
    dst.upload_error = MigrationError("transfer_outcome_unknown", uncertain=True)
    assert finish(store, runner, job_id)["state"] == "needs_attention"
    assert dst.uploads == 1
    store.recover()
    registry.clear()
    registry.put(job_id, CONFIG, {
        "source_client_secret": "s", "destination_token": "d",
    })
    store.update_job(job_id, state="running")
    runner.step(job_id)
    assert store.get(job_id)["state"] == "needs_attention"
    assert dst.uploads == 1
    assert any(e["state"] == "outcome_unknown"
               for e in store.entries(job_id))



def test_known_operation_is_polled_not_resubmitted_after_restart(migration):
    store, _, runner, job_id, _, dst = migration
    dst.operation = "in-progress"
    store.update_job(job_id, state="running")
    for _ in range(4):
        runner.step(job_id)
    assert dst.uploads == 1
    assert dst.moves == 0
    store.recover()
    store.update_job(job_id, state="running")
    dst.operation = "success"
    for _ in range(10):
        runner.step(job_id)
    assert store.get(job_id)["state"] == "completed"
    assert dst.uploads == 1



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
    original = dst.upload

    def upload_without_hash(path, local, **kwargs):
        operation = original(path, local, **kwargs)
        dst.nodes[path].pop("md5")
        return operation

    dst.upload = upload_without_hash
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
    assert finish(store, runner, job_id)["state"] == "needs_attention"
    assert not any(e["state"] == "outcome_unknown"
                   for e in store.entries(job_id))
    runner.retry(job_id, "staged")
    for _ in range(10):
        runner.step(job_id)
    assert store.get(job_id)["state"] == "completed"
    assert destination.uploads == 1



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
    assert finish(store, runner, job_id)["state"] == "needs_attention"
    assert destination.imports == 0
    runner.retry(job_id, "staged")
    for _ in range(10):
        runner.step(job_id)
    assert store.get(job_id)["state"] == "completed"
    assert destination.uploads == 1



@pytest.mark.parametrize("strategy", ["url", "staged"])
def test_crash_after_success_intent_before_entry_commit_reconciles_without_replay(
    migration, monkeypatch, strategy,
):
    store, _, runner, job_id, _, dst = migration
    if strategy == "url":
        historical_url_operation(store, job_id, dst)
    else:
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
    assert dst.imports == 0
    assert dst.uploads == (0 if strategy == "url" else 1)
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
    store.update_entry(attempt["entry_id"], download_attempts=5)
    monkeypatch.setattr(store, "update_entry", crash_before_failed_entry)
    with pytest.raises(SystemExit):
        runner.step(job_id)
    assert store.attempt(attempt["id"])["phase"] == "failed"
    assert store.entry(attempt["entry_id"])["state"] == "waiting_operation"
    monkeypatch.setattr(store, "update_entry", original)
    store.recover()
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert store.entry(attempt["entry_id"])["state"] == "failed"
    assert dst.uploads == 1
    assert dst.moves == 0



def test_polling_outage_preserves_operation_without_background_recovery(migration):
    store, _, runner, job_id, _, dst = migration
    attempt = start_accepted_operation(store, runner, job_id, dst)
    calls = []

    def unavailable(ref):
        assert ref == "op-1"
        calls.append(ref)
        raise MigrationError("destination_unavailable")

    dst.poll = unavailable
    runner.step(job_id)
    preserved = store.attempt(attempt["id"])
    assert preserved["operation_id"] == "op-1"
    assert preserved["phase"] == "abandoned_transfer"
    assert store.entry(attempt["entry_id"])["state"] == "failed"
    assert store.get(job_id)["state"] == "needs_attention"
    assert dst.uploads == 1
    for _ in range(3):
        runner.step(job_id)
    assert calls == ["op-1"]
    assert store.attempt(attempt["id"]) == preserved
    assert dst.uploads == 1
    assert dst.moves == 0



def test_unknown_internal_error_after_upload_intent_is_not_a_retryable_failure(
    migration,
):
    store, _, runner, job_id, _, dst = migration

    def unexpected_after_request(path, local, **kwargs):
        kwargs["before_upload"](None)
        dst.uploads += 1
        raise RuntimeError("signed URL must not enter the database")

    dst.upload = unexpected_after_request
    assert finish(store, runner, job_id)["state"] == "needs_attention"
    attempt = store.attempts(job_id)[0]
    assert attempt["phase"] == "unknown_transfer"
    assert store.entry(attempt["entry_id"])["state"] == "outcome_unknown"
    assert dst.uploads == 1
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

    def upload_once_fails(path, local_path, *, before_upload=None, on_progress=None):
        calls.append((path, local_path))
        if len(calls) == 1:
            raise MigrationError("destination_rate_limited")
        return original_upload(path, local_path, before_upload=before_upload,
                               on_progress=on_progress)

    destination.upload = upload_once_fails
    assert finish(store, runner, job_id)["state"] == "needs_attention"
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


@pytest.mark.parametrize("resume_state", [
    "paused", "needs_attention", "completed_with_issues",
])
def test_retry_reattached_failed_job_restores_download_budget(
    migration, resume_state,
):
    store, _, runner, job_id, source, destination = migration
    for entry in store.entries(job_id):
        if entry["kind"] == "dir":
            store.update_entry(entry["id"], state="done")
    file = next(e for e in store.entries(job_id) if e["kind"] == "file")
    previous = store.new_attempt(file["id"], "staged")
    store.update_attempt(previous["id"], phase="failed",
                         error="source_download_failed")
    store.update_entry(file["id"], state="failed", download_attempts=5,
                       retry_at_ms=1000, error="source_download_failed")
    store.update_job(job_id, state="awaiting_credentials",
                     resume_state=resume_state, error="credentials_required")

    runner.retry(job_id, "staged")

    job = store.get(job_id)
    current = store.entry(file["id"])
    assert job["state"] == "running"
    assert job["resume_state"] is None
    assert job["error"] is None
    assert current["state"] == "planned"
    assert current["download_attempts"] == 0
    assert current["retry_at_ms"] is None
    assert current["error"] is None
    assert current["attempt_id"] != previous["id"]
    assert store.attempt(current["attempt_id"])["strategy"] == "staged"
    assert store.attempt(previous["id"])["phase"] == "failed"
    assert all(e["state"] == "done" for e in store.entries(job_id)
               if e["kind"] == "dir")
    assert source.downloads == destination.imports == destination.moves == 0



@pytest.mark.parametrize("entry_state", [
    "transferring", "waiting_operation", "verifying", "committing",
    "downloading", "staged", "outcome_unknown",
])
def test_retry_reattached_job_never_replays_active_or_unknown(
    migration, entry_state,
):
    store, _, runner, job_id, _, destination = migration
    file = next(e for e in store.entries(job_id) if e["kind"] == "file")
    attempt = store.new_attempt(file["id"], "staged")
    store.update_attempt(attempt["id"], phase="waiting_transfer",
                         operation_id="accepted-operation")
    store.update_entry(file["id"], state=entry_state)
    store.update_job(job_id, state="awaiting_credentials",
                     resume_state="needs_attention")
    before_job, before_entry = store.get(job_id), store.entry(file["id"])
    before_attempt = store.attempt(attempt["id"])

    with pytest.raises(ValueError, match="^active_or_unknown_attempt$"):
        runner.retry(job_id, "staged")

    assert store.get(job_id) == before_job
    assert store.entry(file["id"]) == before_entry
    assert store.attempts(job_id) == [before_attempt]
    assert destination.imports == destination.moves == 0



@pytest.mark.parametrize("state,resume_state", [
    ("awaiting_credentials", "running"),
    ("awaiting_credentials", "queued"),
    ("awaiting_credentials", "ready"),
    ("awaiting_credentials", "stopped"),
    ("awaiting_credentials", None),
    ("stopped", "paused"),
])
def test_retry_cannot_use_unretryable_saved_state(migration, state, resume_state):
    store, _, runner, job_id, _, _ = migration
    file = next(e for e in store.entries(job_id) if e["kind"] == "file")
    store.update_entry(file["id"], state="failed", download_attempts=5)
    store.update_job(job_id, state=state, resume_state=resume_state)
    before_job, before_entry = store.get(job_id), store.entry(file["id"])

    with pytest.raises(ValueError, match="^job_not_retryable$"):
        runner.retry(job_id, "staged")

    assert store.get(job_id) == before_job
    assert store.entry(file["id"]) == before_entry
    assert store.attempts(job_id) == []


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
