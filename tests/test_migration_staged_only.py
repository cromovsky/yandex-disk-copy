"""Only local staging may submit files; old URL evidence stays readable."""

import json

import pytest

from app.migration.credentials import CredentialRegistry
from app.migration.errors import MigrationError
from app.migration.runner import MigrationRunner
from app.migration.store import MigrationStore
from app.source_batches.store import clean_config
from tests.test_migration_api import api, request_copy
from tests.test_migration_runner import CONFIG, META, Destination, Source
from tests.test_source_batches import finish_plan
from tests.test_source_batches_review import (
    ONPREM_CONFIG, ONPREM_CREDS, SHARED_CSV, paired, ready_onprem,
)


@pytest.mark.parametrize("path", ["/jobs", "/preflight"])
def test_new_url_requests_are_rejected_before_disk_access(api, monkeypatch, path):
    client, service = api
    payload = request_copy()
    payload["config"]["strategy"] = "url"
    if path == "/preflight":
        payload.pop("idempotency_key")
    monkeypatch.setattr(service.runner, "preflight", lambda *args: (_ for _ in ()).throw(MigrationError("unexpected_disk_access")))
    response = client.post("/api/onprem" + path, json=payload)
    assert (response.status_code, response.json().get("detail")) == (
        422, "onprem_staged_required",
    )
    assert service.store.jobs() == []


def test_new_request_defaults_to_staging(api):
    client, _ = api
    response = client.post("/api/onprem/jobs", json=request_copy())
    assert response.status_code == 200
    assert response.json()["config"]["strategy"] == "staged"


@pytest.mark.parametrize("action,state,payload", [
    ("start", "ready", {}),
    ("resume", "paused", {}),
    ("retry", "completed_with_issues", {"strategy": "staged"}),
    ("entries/{entry_id}/retry", "completed_with_issues", {"revision": 0}),
    ("restart", "stopped", None),
])
def test_old_url_jobs_cannot_start_or_retry_but_keep_history(
    api, monkeypatch, action, state, payload,
):
    client, service = api
    job = service.store.create(dict(CONFIG, strategy="url"), "old-url-history")
    service.registry.put(job["id"], job["config"], request_copy()["credentials"])
    service.store.add_entries(job["id"], [{
        "kind": "file", "source_path": META["path"], "destination_path": "disk:/owner/a.txt",
        "size": META["size"], "md5": META["md5"], "modified": META["modified"],
        "resource_id": META["resource_id"],
    }])
    entry = service.store.entries(job["id"])[0]
    attempt = service.store.new_attempt(entry["id"], "url")
    service.store.update_attempt(attempt["id"], phase="failed", error="destination_url_import_failed")
    service.store.update_entry(entry["id"], state="failed", error="destination_url_import_failed")
    service.store.update_job(job["id"], state=state)
    if payload is None:
        payload = request_copy()
        payload["idempotency_key"] = "old-url-restarted"
    elif "revision" in payload:
        payload = dict(payload, revision=service.store.get(job["id"])["revision"])
    before = service.store.get(job["id"]), service.store.events(job["id"])
    monkeypatch.setattr(service.runner, "preflight", lambda *args: (_ for _ in ()).throw(MigrationError("unexpected_disk_access")))
    response = client.post(
        f"/api/onprem/jobs/{job['id']}/" + action.format(entry_id=entry["id"]), json=payload,
    )
    assert (response.status_code, response.json().get("detail")) == (409, "onprem_staged_required")
    assert (service.store.get(job["id"]), service.store.events(job["id"])) == before
    assert client.get(f"/api/onprem/jobs/{job['id']}").json()["config"]["strategy"] == "url"
    for export in ("report", "journal"):
        result = client.get(f"/api/onprem/jobs/{job['id']}/{export}")
        assert result.status_code == 200
        assert "url" in result.text
    assert client.post(f"/api/onprem/jobs/{job['id']}/stop", json={}).status_code == 200


@pytest.mark.parametrize("strategy", ["url", "other", None])
def test_csv_rejects_nonstaged_onprem_config(strategy):
    with pytest.raises(ValueError, match="onprem_staged_required"):
        clean_config("onprem", dict(ONPREM_CONFIG, strategy=strategy))


@pytest.mark.parametrize("action,state", [("plan", "draft"), ("start_transfer", "ready"), ("resume", "paused")])
def test_old_url_csv_cannot_be_replanned_started_or_resumed(paired, action, state):
    batches, _, _ = paired
    detail = ready_onprem(batches)
    batch_id = detail["batch"]["id"]
    config = dict(detail["batch"]["config"], strategy="url")
    with batches.store.connection() as db:
        db.execute("UPDATE batches SET config=? WHERE id=?", (json.dumps(config), batch_id))
    batches.store.update(batch_id, state=state)
    before = batches.store.get(batch_id), batches.store.sources(batch_id)
    with pytest.raises(ValueError, match="onprem_staged_required"):
        getattr(batches, action)(batch_id)
    assert (batches.store.get(batch_id), batches.store.sources(batch_id)) == before
    spool = batches.export(batch_id, journal=True)
    assert spool.read()
    spool.close()
    assert batches.stop(batch_id)["batch"]["state"] == "stopped"


@pytest.fixture
def legacy_runtime(tmp_path):
    store = MigrationStore(tmp_path)
    registry = CredentialRegistry()
    source, destination = Source(), Destination()
    runner = MigrationRunner(store, registry, source_factory=lambda c, a: source,
                             destination_factory=lambda c, a: destination, reserve_bytes=0)
    job = store.create(dict(CONFIG, strategy="url"), "historical-url")
    registry.put(job["id"], job["config"], {
        "source_client_secret": "test-source", "destination_token": "test-target",
    })
    store.add_entries(job["id"], [{
        "kind": "file", "source_path": META["path"], "destination_path": "disk:/owner/a.txt",
        "size": META["size"], "md5": META["md5"], "modified": META["modified"],
        "resource_id": META["resource_id"],
    }])
    store.update_job(job["id"], state="running")
    yield store, runner, job["id"], source, destination
    runner.close()
    store.close()


@pytest.mark.parametrize("phase", [None, "prepared", "download_retry_wait", "failed"])
def test_saved_url_work_never_submits_a_new_import(legacy_runtime, phase):
    store, runner, job_id, source, destination = legacy_runtime
    entry = store.entries(job_id)[0]
    if phase is not None:
        attempt = store.new_attempt(entry["id"], "url")
        store.update_attempt(attempt["id"], phase=phase, error="destination_url_import_failed")
        store.update_entry(entry["id"], state="downloading", retry_at_ms=1)
    for _ in range(3):
        runner.step(job_id)
    assert destination.imports == 0
    assert source.downloads == 0
    assert store.get(job_id)["state"] == "needs_attention"
    assert store.get(job_id)["error"] == "onprem_staged_required"
    assert store.get(job_id)["config"]["strategy"] == "url"


@pytest.mark.parametrize("status", ["success", "failed"])
def test_saved_accepted_url_operation_is_polled_without_resubmission(legacy_runtime, status):
    store, runner, job_id, source, destination = legacy_runtime
    entry = store.entries(job_id)[0]
    attempt = store.new_attempt(entry["id"], "url")
    store.update_attempt(attempt["id"], phase="waiting_transfer", operation_id="op-1")
    store.update_entry(entry["id"], state="waiting_operation", download_attempts=1)
    destination.operation = status
    if status == "success":
        destination.nodes[attempt["remote_path"]] = dict(META, path=attempt["remote_path"])
    runner.step(job_id)
    runner.step(job_id)
    assert destination.imports == 0
    assert source.downloads == 0
    assert len(store.attempts(job_id)) == 1
    assert store.attempt(attempt["id"])["phase"] == ("succeeded" if status == "success" else "failed")


def test_public_onprem_enabled_without_runtime_override(monkeypatch):
    from app.main import config

    monkeypatch.delenv("ONPREM_ENABLED", raising=False)
    assert config()["onprem_enabled"] is True


def test_old_url_credentials_require_new_dry_run_before_disk_access(api, monkeypatch):
    client, service = api
    job = service.store.create(dict(CONFIG, strategy="url"), "old-url-credentials")
    service.store.update_job(job["id"], state="awaiting_credentials", resume_state="ready")
    before = service.store.get(job["id"]), service.store.events(job["id"])
    monkeypatch.setattr(service.runner, "preflight", lambda *args: (_ for _ in ()).throw(
        MigrationError("unexpected_disk_access"),
    ))
    payload = request_copy()
    payload.pop("idempotency_key")
    response = client.put(f"/api/onprem/jobs/{job['id']}/credentials", json=payload)
    assert (response.status_code, response.json().get("detail")) == (409, "onprem_staged_required")
    assert service.registry.get(job["id"]) is None
    assert (service.store.get(job["id"]), service.store.events(job["id"])) == before


@pytest.mark.parametrize("legacy", ["batch_and_child", "child", "attempt"])
@pytest.mark.parametrize("action", ["credentials", "recovered_credentials", "selection"])
def test_legacy_csv_mutations_are_rejected_before_state_or_registry_changes(
    paired, legacy, action,
):
    batches, onprem, _ = paired
    sources_csv = "type,email,vd_hash,path\npersonal,one@company.ru,,/\npersonal,two@company.ru,,/\n"
    detail = finish_plan(batches, batches.create(
        "onprem", sources_csv, ONPREM_CONFIG, ONPREM_CREDS, "legacy-csv-mutations",
    ))
    batch_id = detail["batch"]["id"]
    child_id = detail["sources"][0]["child_job_id"]
    if legacy == "batch_and_child":
        config = dict(batches.store.get(batch_id)["config"], strategy="url")
        with batches.store.connection() as db:
            db.execute("UPDATE batches SET config=? WHERE id=?", (json.dumps(config), batch_id))
    if legacy in {"batch_and_child", "child"}:
        config = dict(onprem.store.get(child_id)["config"], strategy="url")
        previous_entries = onprem.store.entries(child_id)
        child = onprem.store.create(config, "historical-csv-child")
        child_id = child["id"]
        fields = ("kind", "source_path", "destination_path", "size", "md5", "modified", "resource_id")
        onprem.store.add_entries(child_id, [
            {key: entry[key] for key in fields} for entry in previous_entries
        ], mark_ready=True)
        batches.store.update_source(detail["sources"][0]["id"], child_job_id=child_id)
        onprem.registry.put(child_id, config, ONPREM_CREDS)
    else:
        entry = next(item for item in onprem.store.entries(child_id) if item["kind"] == "file")
        attempt = onprem.store.new_attempt(entry["id"], "url")
        onprem.store.update_attempt(attempt["id"], phase="failed", error="destination_url_import_failed")
        onprem.store.update_entry(entry["id"], state="failed", error="destination_url_import_failed")
    if action == "recovered_credentials":
        onprem.store.recover()
        batches.store.recover()
        onprem.registry.clear()
        batches._credentials.clear()
        batches._redactors.clear()

    def snapshot():
        return (
            batches.store.get(batch_id), batches.store.sources(batch_id),
            batches.store.events(batch_id), onprem.store.get(child_id),
            onprem.store.entries(child_id), onprem.store.attempts(child_id),
            onprem.store.events(child_id), onprem.registry.get(child_id),
            dict(batches._credentials), set(batches._redactors),
        )

    before = snapshot()
    with pytest.raises(ValueError, match="onprem_staged_required"):
        if action == "selection":
            batches.selection(batch_id, [detail["sources"][1]["id"]])
        else:
            batches.attach_credentials(batch_id, ONPREM_CREDS)
    assert snapshot() == before
    source = batches.detail(batch_id)["sources"][0]
    assert source["child_strategy"] == onprem.store.get(child_id)["config"]["strategy"]
