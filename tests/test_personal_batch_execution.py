import csv
import io
import threading
from concurrent.futures import Future, ThreadPoolExecutor

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.migration.api import router as job_router
from app.migration.errors import MigrationError
from app.migration.worker import MigrationService
from app.source_batches.service import BatchService
from tests.test_source_batches import (BODY, CONFIG, CREDENTIALS, MD5, FakeDestination, FakeSource,
                                       finish_plan, onprem_service, wait_for)


class PersonalDestination(FakeDestination):
    supports_personal_service_application = True

    def info(self):
        email = self.config["destination_identity"]
        return {"user": {"uid": self.owners[email], "email": email},
                "total_space": self.quota[email], "used_space": 0}


@pytest.fixture
def pairs(onprem_service):
    service, onprem = onprem_service
    class Target(PersonalDestination):
        owners = {"x@example.test": "101", "y@example.test": "102"}
        quota = dict.fromkeys(owners, 100)

    class Source(FakeSource):
        nodes = [*FakeSource.nodes, {"path": "disk:/file.txt", "type": "file",
                                     "size": 5, "md5": MD5}]

    service.source_factory = onprem.runner.source_factory = Source
    onprem.runner.destination_factory = Target
    return service, onprem, Target


def plan(service, *rows):
    csv = "source_email,destination_email,path,destination_folder\n" + "\n".join(rows)
    config = dict(CONFIG, destination_url="https://cloud-api.example.test/v1/disk",
                  destination_client_id="target-client", destination_type="personal", destination_auth="service_application")
    creds = dict(CREDENTIALS, destination_owner_token="owner-token", destination_client_secret="target-secret")
    return finish_plan(service, service.create("onprem", csv, config, creds, "personal-batch-test"))


def job_client(service, migration, monkeypatch):
    monkeypatch.setenv("ONPREM_ENABLED", "1")
    monkeypatch.setenv("ONPREM_ADMIN_TOKEN", "test-key")
    app = FastAPI()
    app.state.source_batches, app.state.onprem = service, migration
    app.include_router(job_router)
    return TestClient(app, base_url="https://utility.example", headers={"X-Onprem-Key": "test-key"})


def test_prefix_and_source_ancestors_are_checked_without_writes(pairs):
    service, onprem, dest = pairs
    dest.stats = {"disk:/": {"type": "dir"}, "disk:/archive": {"type": "file"}}
    result = plan(service, "a@cloud.example.test,x@example.test,/folder/file.txt,/archive/sub",
                  "b@cloud.example.test,y@example.test,/folder/file.txt,/new")
    left, right = result["sources"]
    assert left["error"] == "destination_conflict"
    assert right["state"] == "ready"
    child = onprem.store.get(right["child_job_id"])
    assert child["destination_verified_uid"] == "102"
    assert [e["destination_path"] for e in onprem.store.entries(child["id"])] == ["disk:/new/folder/file.txt"]


@pytest.mark.parametrize("rows,alias,code", [
    (["a@cloud.example.test,x@example.test,/folder,", "A@cloud.example.test,y@example.test,/file.txt,"], False, "personal_mapping_conflict"),
    (["a@cloud.example.test,x@example.test,/folder,", "b@cloud.example.test,x@example.test,/file.txt,/other"], False, "personal_mapping_conflict"),
    (["a@cloud.example.test,x@example.test,/folder,", "b@cloud.example.test,y@example.test,/file.txt,/other"], True, "personal_mapping_conflict"),
    (["a@cloud.example.test,x@example.test,/folder,", "a@cloud.example.test,x@example.test,/folder/file.txt,/other"], False, "overlapping_source_paths"),
])
def test_row_conflicts_cleared_by_selection(pairs, rows, alias, code):
    service, _, target = pairs
    if alias:
        target.owners["y@example.test"] = "101"
    result = plan(service, *rows)
    assert result["batch"]["error"] == code
    for row in result["sources"]:
        assert result["batch"]["mapping_issues"][row["id"]] == {"mapping_error": code, "conflict_rows": [1, 2],
            **({"conflict_path": "disk:/folder"} if code == "overlapping_source_paths" else {})}
    source = result["sources"][0]
    result = service.selection(result["batch"]["id"], [source["id"]])
    assert result["batch"]["state"] == "ready"
    assert not result["batch"]["mapping_issues"]


@pytest.mark.parametrize("prefix,conflict", [("/folder", "disk:/folder/file.txt"),
                                            ("/folder/file.txt", "disk:/folder/file.txt")])
def test_collisions_with_selected_files_can_be_resolved(pairs, prefix, conflict):
    service, onprem, _ = pairs
    result = plan(service, "a@cloud.example.test,x@example.test,/folder,", "a@cloud.example.test,x@example.test,/file.txt," + prefix)
    assert result["batch"]["error"] == "destination_path_collision"
    assert all(i["conflict_path"] == conflict for i in result["batch"]["mapping_issues"].values())
    source = result["sources"][1]
    entry = onprem.store.entries(source["child_job_id"])[0]
    result = service.entry_selection(result["batch"]["id"], source["id"], entry["id"], False)
    assert result["batch"]["state"] == "ready"


def test_failing_source_does_not_hide_another_disks_quota_error(pairs):
    service, _, target = pairs
    target.quota["y@example.test"] = 0
    result = plan(service, "a@cloud.example.test,x@example.test,/missing,", "b@cloud.example.test,y@example.test,/file.txt,")
    missing, full = result["sources"]
    assert result["batch"]["state"] == "needs_attention" and missing["error"]
    assert full["mapping_error"] == "destination_space_insufficient" and full["conflict_rows"] == [2]
    with service.export(result["batch"]["id"]) as report:
        first = next(csv.DictReader(io.StringIO(report.read().decode("utf-8-sig"))))
    assert (first["csv_row"], first["source_path"], first["source_error"]) == ("1", "disk:/missing", missing["error"])


def test_quota_for_disjoint_rows_on_same_disk_is_summed(pairs):
    service, _, target = pairs
    target.quota["x@example.test"] = 8
    result = plan(service, "a@cloud.example.test,x@example.test,/folder,", "a@cloud.example.test,x@example.test,/file.txt,")
    assert result["batch"]["error"] == "destination_space_insufficient"
    assert all(i["conflict_rows"] == [1, 2] for i in result["batch"]["mapping_issues"].values())


def test_independent_disks_share_cache_only_and_validate_every_owner(pairs):
    service, onprem, target = pairs
    target.quota = dict.fromkeys(target.owners, 6)
    onprem.runner.staging_budget = 9
    result = plan(service, "a@cloud.example.test,x@example.test,/file.txt,", "b@cloud.example.test,y@example.test,/file.txt,")
    batch_id = result["batch"]["id"]
    assert result["batch"]["error"] == "local_cache_space_insufficient"
    onprem.runner.staging_budget = 10
    service._finish_planning(batch_id)
    assert service.store.get(batch_id)["state"] == "ready"
    target.owners["y@example.test"] = "999"
    with pytest.raises((MigrationError, ValueError), match="destination_identity_mismatch"):
        service.start_transfer(batch_id)
    assert not service.store.get(batch_id)["started"]
    target.owners["y@example.test"] = "102"
    service._finish_planning(batch_id)
    service.start_transfer(batch_id)
    service.pause(batch_id)
    target.owners["y@example.test"] = "999"
    with pytest.raises(MigrationError, match="destination_identity_mismatch"):
        service.resume(batch_id)
    assert service.store.get(batch_id)["state"] == "paused"


@pytest.mark.parametrize("action", ["create", "attach", "start", "resume"])
def test_slow_destination_check_allows_stop_and_discards_its_result(pairs, monkeypatch, action):
    service, onprem, _ = pairs
    row = "a@cloud.example.test,x@example.test,/file.txt,"
    detail = plan(service, row)
    batch = detail["batch"]
    batch_id = batch["id"]
    credentials = dict(service._credentials_for(batch_id), destination_owner_token="replacement")
    other = service.create("local", "type,email,vd_hash,path\npersonal,other@cloud.example.test,,/", CONFIG,
                           CREDENTIALS, "independent-list")["batch"]["id"]
    if action == "resume":
        service.start_transfer(batch_id)
        service.pause(batch_id)
    entered, release = threading.Event(), threading.Event()

    def delayed_disk(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        if action == "attach":
            raise MigrationError("destination_identity_mismatch", auth=True)
        return 100

    monkeypatch.setattr(service, "_check_disk", delayed_disk)
    actions = {
        "create": lambda: service.create("onprem", "source_email,destination_email,path,destination_folder\n" + row,
                                         batch["config"], credentials, "personal-batch-test"),
        "attach": lambda: service.attach_credentials(batch_id, credentials),
        "start": lambda: service.start_transfer(batch_id),
        "resume": lambda: service.resume(batch_id),
    }
    with ThreadPoolExecutor(max_workers=2) as workers:
        check = workers.submit(actions[action])
        try:
            assert entered.wait(2)
            assert workers.submit(service.stop, other).result(timeout=2)["batch"]["state"] == "stopped"
            assert workers.submit(service.stop, batch_id).result(timeout=2)["batch"]["state"] == "stopped"
        finally:
            release.set()
        with pytest.raises(ValueError, match="source_busy"):
            check.result(timeout=2)
    assert not service.store.get(batch_id)["mapping_issues"]
    assert service.store.get(batch_id)["state"] == "stopped"
    assert onprem.store.get(detail["sources"][0]["child_job_id"])["state"] == "stopped"
    assert batch_id not in service._credentials


def test_final_dry_run_discards_result_after_credentials_change(pairs, monkeypatch):
    service, migration, _ = pairs
    planned = plan(service, "a@cloud.example.test,x@example.test,/file.txt,")
    source, = planned["sources"]
    batch_id = planned["batch"]["id"]
    original = service._credentials_for(batch_id)
    replacement = dict(original, destination_owner_token="replacement")
    entered, release = threading.Event(), threading.Event()
    check_disk = service._check_disk

    def delayed_old_check(batch, row, credentials, checkpoint=None):
        if credentials == original:
            entered.set()
            assert release.wait(5)
            raise MigrationError("destination_auth_failed", auth=True)
        return check_disk(batch, row, credentials, checkpoint)

    monkeypatch.setattr(service, "_check_disk", delayed_old_check)
    with ThreadPoolExecutor(max_workers=1) as workers:
        checking = workers.submit(service.selection, batch_id, [source["id"]])
        try:
            assert entered.wait(2)
            service.attach_credentials(batch_id, replacement)
        finally:
            release.set()
        checking.result(timeout=2)
    previous_check = service.store.get(batch_id)
    assert previous_check["state"] in {"planning", "ready"} and not previous_check["mapping_issues"]
    service._finish_planning(batch_id)
    assert service.store.get(batch_id)["state"] == "ready"
    assert migration.registry.get(source["child_job_id"]) == replacement


def test_attach_with_busy_child_keeps_every_credential(pairs):
    service, migration, _ = pairs
    planned = plan(service, "a@cloud.example.test,x@example.test,/file.txt,", "b@cloud.example.test,y@example.test,/file.txt,")
    batch_id = planned["batch"]["id"]
    children = [row["child_job_id"] for row in planned["sources"]]
    original = service._credentials_for(batch_id)
    replacement = dict(original, destination_owner_token="replacement")
    issue = {planned["sources"][0]["id"]: {"mapping_error": "destination_auth_failed",
                                          "conflict_rows": [1], "credential_check": True}}
    service.store.update(batch_id, mapping_issues=issue)
    with ThreadPoolExecutor(max_workers=1) as workers:
        with migration.job_guard(children[-1]):
            attaching = workers.submit(service.attach_credentials, batch_id, replacement)
            with pytest.raises(ValueError, match="source_busy"):
                attaching.result(timeout=2)
    assert service._credentials_for(batch_id) == original
    assert all(migration.registry.get(child) == original for child in children)
    assert service.store.get(batch_id)["mapping_issues"] == issue


def test_resume_waits_for_pausing_source_to_finish(pairs):
    service, migration, _ = pairs
    planned = plan(service, "a@cloud.example.test,x@example.test,/file.txt,")
    batch_id = planned["batch"]["id"]
    source, = planned["sources"]
    service.start_transfer(batch_id)
    service.pause(batch_id)
    task = Future()
    task.set_running_or_notify_cancel()
    service._active[source["id"]] = task
    with pytest.raises(ValueError, match="source_busy"):
        service.resume(batch_id)
    assert service.store.get(batch_id)["state"] == "paused"
    assert migration.store.get(source["child_job_id"])["pause_requested"]
    task.set_result(None)
    resumed = service.resume(batch_id)
    assert resumed["batch"]["state"] == resumed["sources"][0]["state"] == "running"
    assert not migration.store.get(source["child_job_id"])["pause_requested"]


def test_replacing_credentials_checks_all_pinned_owners_before_installing(pairs):
    service, onprem, target = pairs
    result = plan(service, "a@cloud.example.test,x@example.test,/file.txt,", "b@cloud.example.test,y@example.test,/file.txt,/import")
    batch_id = result["batch"]["id"]
    children = [r["child_job_id"] for r in result["sources"]]
    previous = onprem.registry.get(children[0])
    replacement = dict(previous, destination_owner_token="replacement")
    target.owners["y@example.test"] = "999"
    with pytest.raises(MigrationError, match="destination_identity_mismatch"):
        service.attach_credentials(batch_id, replacement)
    assert all(onprem.registry.get(child) == previous for child in children)
    issue = service.store.get(batch_id)["mapping_issues"][result["sources"][1]["id"]]
    assert issue["conflict_rows"] == [2] and issue["mapping_error"] == "destination_identity_mismatch"
    target.owners["y@example.test"] = "102"
    assert not service.attach_credentials(batch_id, previous)["batch"]["mapping_issues"]


def test_batch_recovery_skips_completed_recipient_after_owner_change(pairs):
    service, onprem, target = pairs
    result = plan(service, "a@cloud.example.test,x@example.test,/file.txt,", "b@cloud.example.test,y@example.test,/file.txt,/import")
    batch_id = result["batch"]["id"]
    completed, remaining = result["sources"]
    service.start_transfer(batch_id)
    child = completed["child_job_id"]
    onprem.store.update_entry(onprem.store.entries(child)[0]["id"], state="done")
    onprem.store.update_job(child, state="completed")
    service.store.update_source(completed["id"], state="completed")
    creds = dict(onprem.registry.get(child), destination_owner_token="replacement")
    service.store.recover()
    onprem.store.recover()
    onprem.registry.clear()
    service._credentials.clear()
    target.owners["x@example.test"] = "999"
    restored = service.attach_credentials(batch_id, creds)
    assert restored["batch"]["state"] == "paused"
    service.resume(batch_id)
    assert onprem.store.get(child)["state"] == "completed"
    assert onprem.registry.get(child) is None
    assert onprem.store.get(remaining["child_job_id"])["state"] == "running"
    assert onprem.registry.get(remaining["child_job_id"]) == creds


def test_point_retry_reattaches_csv_prefix_and_checks_only_affected_owner(pairs, monkeypatch):
    service, onprem, target = pairs
    result = plan(service, "a@cloud.example.test,x@example.test,/file.txt,", "b@cloud.example.test,y@example.test,/file.txt,/import")
    batch_id = result["batch"]["id"]
    service.start_transfer(batch_id)
    left, right = result["sources"]
    job_id = right["child_job_id"]
    entry = onprem.store.entries(job_id)[0]
    onprem.store.update_entry(entry["id"], state="failed", error="destination_unavailable")
    job = onprem.store.update_job(job_id, state="paused", error="destination_unavailable", resume_state="running")
    service.store.update(batch_id, state="paused", error="destination_unavailable", resume_state="running")
    for row in (left, right):
        service.store.update_source(row["id"], state="paused", error="destination_unavailable")

    client = job_client(service, onprem, monkeypatch)
    base = f"/api/onprem/jobs/{job_id}"
    form = {"config": job["config"], "credentials": onprem.registry.get(job_id), "point_retry_entry_id": entry["id"]}
    endpoint = f"{base}/entries/{entry['id']}/retry"
    target.owners["y@example.test"] = "999"
    for method, url, data in [(client.put, base + "/credentials", form), (client.post, endpoint, {"revision": job["revision"]})]:
        reply = method(url, json=data)
        assert reply.status_code == 409 and reply.json()["detail"] == "destination_identity_mismatch"
    assert not onprem.store.entry(entry["id"])["point_retry_requested"]
    target.owners["x@example.test"], target.owners["y@example.test"] = "999", "102"
    assert client.put(base + "/credentials", json=form).status_code == 200
    revision = onprem.store.get(job_id)["revision"]
    reply = client.post(endpoint, json={"revision": revision})
    assert reply.status_code == 200, reply.text
    assert onprem.store.entry(entry["id"])["point_retry_requested"]
    assert service.store.source(left["id"])["state"] == "paused"


@pytest.mark.parametrize("child_state,resume_state,error,batch_state", [
    ("awaiting_credentials", "running", "destination_identity_mismatch", "awaiting_credentials"),
    ("needs_attention", "running", "destination_unavailable", "paused"),
    ("needs_attention", None, "transfer_outcome_unknown", "completed_with_issues"),
])
def test_runtime_failure_allows_other_disk_to_finish(pairs, monkeypatch, child_state, resume_state, error, batch_state):
    service, onprem, _ = pairs
    result = plan(service, "a@cloud.example.test,x@example.test,/file.txt,", "b@cloud.example.test,y@example.test,/file.txt,")
    batch_id = result["batch"]["id"]
    failed, unaffected = result["sources"]
    service.start_transfer(batch_id)

    def step(child):
        if child == failed["child_job_id"]:
            onprem.store.update_job(child, state=child_state, resume_state=resume_state, error=error)
        else:
            onprem.store.update_job(child, state="completed")

    monkeypatch.setattr(onprem.runner, "step", step)
    service._transfer_one(failed["id"])
    assert service.store.get(batch_id)["state"] == "running"
    service.start()
    blocked = wait_for(service, batch_id, batch_state)
    assert blocked["sources"][0]["error"] == error
    assert service.store.source(unaffected["id"])["state"] == "completed"


@pytest.mark.parametrize("interruption", ["before_retry", "during_retry", "user_paused"])
def test_recovered_file_retry_finishes_with_only_its_own_credentials(pairs, monkeypatch, interruption):
    service, onprem, target = pairs
    result = plan(service, "a@cloud.example.test,x@example.test,/file.txt,", "b@cloud.example.test,y@example.test,/file.txt,/archive")
    batch_id = result["batch"]["id"]
    service.start_transfer(batch_id)
    left, right = result["sources"]
    child = right["child_job_id"]
    credentials = onprem.registry.get(child)
    entry = onprem.store.entries(child)[0]
    attempt = onprem.store.new_attempt(entry["id"], "staged")
    cache_name = attempt["id"] + ".part"
    (onprem.runner.staging / cache_name).write_bytes(BODY)
    onprem.store.update_attempt(attempt["id"], phase="failed", local_name=cache_name, error="destination_unavailable")
    onprem.store.update_entry(entry["id"], state="failed", error="destination_unavailable")
    onprem.store.update_entry(onprem.store.entries(left["child_job_id"])[0]["id"], state="done")
    for row, state in ((left, "completed"), (right, "completed_with_issues")):
        onprem.store.update_job(row["child_job_id"], state=state)
        service.store.update_source(row["id"], state=state)
    service.store.update(batch_id, state="completed_with_issues")
    if interruption != "before_retry":
        revision = onprem.store.get(child)["revision"]
        onprem.store.queue_point_retry(child, entry["id"], attempt["id"], revision)
        service.activate_child_retry(child)
        if interruption == "user_paused":
            service.pause(batch_id)
    root, local_root, slots = service.store.root, service.local_root, service.slots
    service.close()
    onprem.close()
    uploads = []

    class RecoveringTarget(target):
        stats = {"disk:/": {"type": "dir"}}

        def info(self):
            assert self.config["destination_identity"] == "y@example.test"
            return super().info()

        def ensure_dir(self, path):
            self.stats[path] = {"type": "dir"}

        def upload(self, path, local, *, before_upload, on_progress):
            before_upload(None)
            uploads.append((path, local.read_bytes()))
            self.stats[path] = {"type": "file", "size": len(BODY), "md5": MD5}

    target.owners["x@example.test"] = "999"
    migration = MigrationService(root, start_worker=False)
    restored = BatchService(root, None, migration, local_root, slots, start_worker=False)
    migration.runner.destination_factory = RecoveringTarget
    migration.runner.source_factory = FakeSource
    client = job_client(restored, migration, monkeypatch)
    try:
        job = migration.store.get(child)
        base = f"/api/onprem/jobs/{child}"
        form = {"config": job["config"], "credentials": credentials, "point_retry_entry_id": entry["id"]}
        reattach = client.put(base + "/credentials", json=form)
        if interruption == "user_paused":
            assert reattach.status_code == 409
            assert client.post(base + "/resume").status_code == 409
            assert not uploads
            return
        assert reattach.status_code == 200, reattach.text
        if interruption == "before_retry":
            payload = {"attempt_id": attempt["id"], "revision": migration.store.get(child)["revision"]}
            assert client.post(f"{base}/entries/{entry['id']}/retry", json=payload).status_code == 200
        else:
            assert client.post(base + "/resume").status_code == 200
        restored.start()
        wait_for(restored, batch_id, "completed")
        assert uploads == [("disk:/archive/file.txt", BODY)]
        assert migration.store.entry(entry["id"])["state"] == "done"
        assert not migration.registry.get(left["child_job_id"])
        assert not list(migration.runner.staging.iterdir())
    finally:
        restored.close()
        migration.close()
