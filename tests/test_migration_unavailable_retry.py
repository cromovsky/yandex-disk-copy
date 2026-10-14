"""Unavailable Onprem stops locally; explicit retry uses the confirmed cache."""

import pytest

from app.migration.errors import MigrationError
from tests import test_migration_runner as fixtures
from tests.test_migration_point_retry import unknown, queue
from tests.test_migration_runner import BODY, META, cached_attempt, stage_events


unplanned_migration = fixtures.unplanned_migration
direct_migration = fixtures.direct_migration


def root_disk(destination):
    original = destination.stat

    def stat(path):
        return {"type": "dir", "path": "disk:/"} if path == "disk:/" else original(path)
    destination.stat = stat


@pytest.mark.parametrize("phase", ["waiting_transfer", "waiting_move"])
def test_unavailable_operation_stops_without_polling_again_and_preserves_cache(
    direct_migration, phase,
):
    store, _, runner, job_id, _, destination = direct_migration
    entry, attempt = cached_attempt(store, runner, job_id)
    store.update_attempt(attempt["id"], phase=phase, operation_id="op-unavailable")
    store.update_entry(entry["id"], state="waiting_operation")
    store.update_job(job_id, state="running")
    root_disk(destination)
    polls = []

    def unavailable(reference):
        polls.append(reference)
        raise MigrationError("destination_unavailable", http_status=503,
                             diagnostics={"error_side": "onprem", "api_error": "Unavailable"})
    destination.poll = unavailable
    runner.step(job_id)
    current = store.entry(entry["id"])
    assert current["state"] == "failed"
    assert current["error"] == "destination_unavailable"
    assert store.get(job_id)["state"] == "needs_attention"
    updated = store.attempt(attempt["id"])
    assert updated["phase"] == ("abandoned_move" if phase == "waiting_move"
                                else "abandoned_transfer")
    assert updated["operation_id"] == "op-unavailable"
    assert (runner.staging / updated["local_name"]).read_bytes() == BODY
    for _ in range(3):
        runner.step(job_id)
    assert polls == ["op-unavailable"]
    failed = stage_events(store, job_id,
                          "poll_move" if phase == "waiting_move" else "poll_transfer")
    assert failed[-1]["payload"]["status"] == "failed"
    assert failed[-1]["payload"]["http_status"] == 503


def test_ambiguous_upload_is_a_local_failure_with_cache_and_remote_audit(direct_migration):
    store, _, runner, job_id, _, destination = direct_migration
    entry, attempt = cached_attempt(store, runner, job_id)
    store.update_job(job_id, state="running")

    def upload(path, local_path, *, before_upload, on_progress=None):
        before_upload("op-accepted")
        raise MigrationError("destination_timeout", uncertain=True, diagnostics={
            "error_side": "onprem", "transport_error": "read_timeout",
        })
    destination.upload = upload
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] == "failed"
    assert store.get(job_id)["state"] == "needs_attention"
    updated = store.attempt(attempt["id"])
    assert updated["phase"] == "abandoned_transfer"
    assert updated["operation_id"] == "op-accepted"
    assert (runner.staging / updated["local_name"]).read_bytes() == BODY
    event = stage_events(store, job_id, "upload_file")[-1]["payload"]
    assert event["status"] == "failed"
    assert event["transport_error"] == "read_timeout"


def test_old_unknown_retry_probes_actual_disk_and_uploads_cache_without_saas(direct_migration):
    store, _, runner, job_id, source, destination = direct_migration
    entry, old = unknown(direct_migration, operation="old-op", cache=True)
    checks, uploads = [], []
    original_stat = destination.stat

    def stat(path):
        checks.append(path)
        return {"type": "dir", "path": "disk:/"} if path == "disk:/" else original_stat(path)

    def upload(path, local_path, *, before_upload, on_progress=None, overwrite=False):
        uploads.append((path, local_path.read_bytes(), overwrite))
        before_upload(None)
        destination.nodes[path] = dict(META, path=path)
        return None
    destination.stat = stat
    destination.upload = upload
    destination.poll = lambda ref: pytest.fail("old remote operation must not be polled")
    source.stat = source.chunks = lambda *a: pytest.fail("confirmed cache works when SaaS is down")
    queue(direct_migration, entry, old)
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] == "done"
    assert checks[0] == "disk:/"
    assert uploads == [(entry["destination_path"], BODY, True)]
    assert store.attempt(old["id"])["phase"] == "abandoned_transfer"
    assert not list(runner.staging.iterdir())
    assert source.downloads == destination.imports == destination.moves == 0


def test_root_disk_error_prevents_retry_even_when_personal_info_is_available(direct_migration):
    store, _, runner, job_id, source, destination = direct_migration
    entry, old = unknown(direct_migration, operation="old-op", cache=True)
    calls = []

    def unavailable(path):
        calls.append(path)
        raise MigrationError("destination_unavailable", http_status=500)
    destination.stat = unavailable
    queue(direct_migration, entry, old)
    runner.step(job_id)
    assert calls == ["disk:/"]
    assert store.entry(entry["id"])["state"] == "failed"
    assert not store.entry(entry["id"])["point_retry_requested"]
    assert store.get(job_id)["state"] == "needs_attention"
    assert len(store.attempts(job_id)) == 1
    assert (runner.staging / old["local_name"]).read_bytes() == BODY
    assert source.downloads == destination.imports == destination.moves == 0


def test_retry_replaces_only_its_own_interrupted_partial_upload(direct_migration):
    store, _, runner, job_id, _, destination = direct_migration
    entry, old = unknown(direct_migration, cache=True)
    root_disk(destination)
    destination.nodes[entry["destination_path"]] = dict(
        META, path=entry["destination_path"], md5="0" * 32, size=3,
    )
    writes = []

    def upload(path, local_path, *, before_upload, on_progress=None, overwrite=False):
        writes.append(overwrite)
        before_upload(None)
        destination.nodes[path] = dict(META, path=path)
    destination.upload = upload
    queue(direct_migration, entry, old)
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] == "done"
    assert writes == [True]


def test_retry_missing_cache_reports_it_without_fetching_saas(direct_migration):
    store, _, runner, job_id, source, destination = direct_migration
    entry, old = unknown(direct_migration, cache=True)
    root_disk(destination)
    (runner.staging / old["local_name"]).unlink()
    source.stat = source.chunks = lambda *a: pytest.fail("missing cache must not silently download")
    queue(direct_migration, entry, old)
    runner.step(job_id)
    current = store.entry(entry["id"])
    assert current["state"] == "failed"
    assert current["error"] == "staging_file_unavailable"
    assert not current["point_retry_requested"]
    assert destination.upload_body is None


def test_matching_final_retry_finishes_without_source_access_or_upload(direct_migration):
    store, _, runner, job_id, source, destination = direct_migration
    entry, old = unknown(direct_migration, cache=True, operation="old-op")
    root_disk(destination)
    destination.nodes[entry["destination_path"]] = dict(META, path=entry["destination_path"])
    source.stat = lambda *a: pytest.fail("cached retry uses the saved manifest")
    destination.poll = lambda *a: pytest.fail("MD5 verification does not poll an old operation")
    queue(direct_migration, entry, old)
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] in {"done", "already_verified"}
    assert len(store.attempts(job_id)) == 1
    assert destination.upload_body is None
    assert not list(runner.staging.iterdir())


def test_repeated_probe_outage_keeps_provenance_for_own_partial_overwrite(direct_migration):
    store, _, runner, job_id, _, destination = direct_migration
    entry, old = unknown(direct_migration, cache=True, phase="abandoned_transfer")
    store.update_entry(entry["id"], state="failed")
    root_disk(destination)
    available_info = destination.info
    destination.info = lambda: (_ for _ in ()).throw(MigrationError("destination_timeout"))
    queue(direct_migration, entry, old)
    runner.step(job_id)
    assert store.attempt(old["id"])["phase"] == "abandoned_transfer"
    destination.info = available_info
    destination.nodes[entry["destination_path"]] = dict(
        META, path=entry["destination_path"], md5="0" * 32,
    )
    writes = []

    def upload(path, local_path, *, before_upload, on_progress=None, overwrite=False):
        writes.append(overwrite)
        before_upload(None)
        destination.nodes[path] = dict(META, path=path)
    destination.upload = upload
    queue(direct_migration, store.entry(entry["id"]), store.attempt(old["id"]))
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] == "done"
    assert writes == [True]


def test_bulk_retry_refuses_abandoned_attempt_before_changing_other_entries(direct_migration):
    store, _, runner, job_id, _, _ = direct_migration
    entry, old = unknown(direct_migration, cache=True, phase="abandoned_transfer")
    store.update_entry(entry["id"], state="failed")
    for other in store.entries(job_id):
        if other["id"] != entry["id"]:
            store.update_entry(other["id"], state="failed", error="destination_timeout")
    before = store.entries(job_id)
    with pytest.raises(ValueError, match="^point_retry_required$"):
        runner.retry(job_id, "staged")
    assert store.entries(job_id) == before
    assert len(store.attempts(job_id)) == 1
    assert (runner.staging / old["local_name"]).read_bytes() == BODY


def test_preupload_failure_keeps_old_partial_overwrite_provenance(direct_migration):
    store, _, runner, job_id, _, destination = direct_migration
    entry, old = unknown(direct_migration, cache=True, phase="abandoned_transfer")
    store.update_entry(entry["id"], state="failed")
    root_disk(destination)
    destination.nodes[entry["destination_path"]] = dict(
        META, path=entry["destination_path"], size=3, md5="0" * 32,
    )
    original_ensure = destination.ensure_dir
    destination.ensure_dir = lambda path: (_ for _ in ()).throw(
        MigrationError("destination_unavailable", http_status=503),
    )
    queue(direct_migration, entry, old)
    runner.step(job_id)
    current = store.entry(entry["id"])
    new = store.attempt(current["attempt_id"])
    assert new["id"] != old["id"]
    assert new["phase"] == "failed"
    assert (runner.staging / new["local_name"]).read_bytes() == BODY
    destination.ensure_dir = original_ensure
    writes = []

    def upload(path, local_path, *, before_upload, on_progress=None, overwrite=False):
        writes.append(overwrite)
        before_upload(None)
        destination.nodes[path] = dict(META, path=path)
    destination.upload = upload
    queue(direct_migration, current, new)
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] == "done"
    assert writes == [True]


def test_historical_url_retry_cannot_submit_or_grant_overwrite(direct_migration):
    store, _, runner, job_id, source, destination = direct_migration
    entry, old = unknown(direct_migration, strategy="url", phase="failed")
    store.update_entry(entry["id"], state="failed", error="source_timeout")
    store.update_attempt(old["id"], error="source_timeout")
    root_disk(destination)
    before = store.attempt(old["id"])
    destination.import_url = lambda *args, **kwargs: pytest.fail("historical URL retry must not submit")
    queue(direct_migration, entry, before)
    runner.step(job_id)
    assert store.get(job_id)["error"] == "onprem_staged_required"
    assert store.attempt(old["id"]) == before
    assert len(store.attempts(job_id)) == 1
    assert destination.imports == destination.uploads == source.downloads == 0


def test_failed_preupload_retry_never_overwrites_an_unrelated_conflicting_file(direct_migration):
    store, _, runner, job_id, _, destination = direct_migration
    entry, old = unknown(direct_migration, cache=True, phase="failed")
    store.update_entry(entry["id"], state="failed")
    root_disk(destination)
    final = dict(META, path=entry["destination_path"], md5="0" * 32)
    destination.nodes[entry["destination_path"]] = final
    queue(direct_migration, entry, old)
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] == "conflict"
    assert destination.nodes[entry["destination_path"]] == final
    assert destination.upload_body is None
    assert len(store.attempts(job_id)) == 1
