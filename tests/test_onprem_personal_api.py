"""Single-recipient plans must bind access to the reviewed Disk before transfer."""

import pytest

from app.migration.api import AccessRequest, access_data
from app.migration.credentials import CredentialRegistry
from app.migration.destination import OnPremDiskDestination
from tests.test_migration_api import api
from tests.test_migration_personal_execution import PersonalDestination
from tests.test_migration_public_runner import PublicSource
from tests.test_migration_runner import BODY, META
from tests.test_migration_store import file_entry


def personal_request(auth="service_application", recipient=None):
    data = {
        "config": {
            "source_org_id": "cloud", "source_client_id": "cloud-app",
            "source_email": "aivanov@cloud.example", "source_path": "/",
            "destination_url": "https://cloud-api.office.example",
            "destination_type": "personal", "destination_identity": "apetrov@office.example",
            "destination_oauth_url": "https://oauth.office.example/token",
            "destination_auth": "service_application", "destination_client_id": "office-app",
            "source_public": True, "strategy": "staged",
        },
        "credentials": {
            "source_client_secret": "fixture-cloud-secret",
            "destination_client_secret": "fixture-office-secret",
            "destination_owner_token": "fixture-owner-token",
        },
    }
    if auth == "admin_token":
        data["config"]["destination_url"] = "https://disk-api.office.example"
        data["config"]["destination_auth"] = auth
        data["config"]["destination_identity"] = recipient or "apertov@office.example"
        del data["config"]["destination_client_id"], data["config"]["destination_oauth_url"]
        data["credentials"] = {"source_client_secret": "fixture-cloud-secret",
                               "destination_token": "fixture-onprem-admin-token"}
    return data


@pytest.mark.parametrize("route", ["preflight", "jobs"])
def test_unavailable_grant_rejects_before_planning_or_sending_credentials(api, route):
    client, service = api
    data = personal_request()
    service.runner.destination_factory = object
    if route == "jobs":
        data["idempotency_key"] = "personal-request"
    response = client.post(f"/api/onprem/{route}", json=data)
    assert (response.status_code, response.json()["detail"]) == (409, "onprem_personal_grant_unavailable")
    assert "fixture-" not in response.text
    assert service.store.jobs() == []


@pytest.mark.parametrize("changes", [
    {"destination_root": "/aivanov"}, {"destination_verified_uid": "123"},
    {"identity_confirmed": True}, {"source_public": False},
    {"destination_vd_hash": "shared"}, {"destination_type": "shared"},
    {"destination_identity": "a/b"}, {"destination_identity": " "},
    {"destination_client_id": ""}, {"source_type": "shared"},
    {"destination_oauth_url": None}, {"destination_identity": "short-login"},
])
def test_personal_config_cannot_disguise_a_different_transfer(api, changes):
    client, service = api
    data = personal_request()
    data["config"].update(changes)
    response = client.post("/api/onprem/jobs", json={**data, "idempotency_key": "invalid-personal"})
    assert response.status_code == 422
    assert "fixture-" not in response.text
    assert service.store.jobs() == []


def test_explicit_new_personal_target_requires_service_application(api):
    client, service = api
    data = personal_request("admin_token")
    del data["config"]["destination_auth"]
    data["config"].update(identity_confirmed=True, source_public=False)
    result = client.post("/api/onprem/jobs", json={**data, "idempotency_key": "new-personal"})
    assert (result.status_code, result.json().get("detail")) == (422, "invalid_personal_destination")
    assert not service.store.jobs()


@pytest.mark.parametrize("change", [
    {"destination_identity": "942"}, {"destination_identity": "operator"},
    {"identity_confirmed": True}, {"destination_auth": "admin_token"},
    {"destination_oauth_url": "https://oauth.other.example/token"},
])
def test_shared_exchange_rejects_mixed_identity_and_endpoints(api, change):
    client, service = api
    data = personal_request()
    data["config"].update(destination_type="shared", destination_vd_hash="team",
                          destination_url="https://cloud-api.office.example/v1/disk",
                          destination_identity="operator@office.example",
                          destination_oauth_url="https://oauth.office.example/token")
    data["config"].pop("destination_root", None)
    data["config"].update(change)
    result = client.post("/api/onprem/jobs", json={**data, "idempotency_key": "invalid-shared"})
    assert result.status_code == 422
    assert service.store.jobs() == []


def test_shared_service_requires_its_own_adapter_capability(api):
    client, service = api
    data = personal_request()
    data["config"].update(destination_type="shared", destination_vd_hash="team",
                          destination_url="https://cloud-api.office.example/v1/disk",
                          destination_identity="operator@office.example")
    data["config"].pop("destination_oauth_url")
    data["config"].pop("destination_root", None)
    service.runner.destination_factory = type("PersonalOnly", (), {"supports_personal_service_application": True})
    result = client.post("/api/onprem/jobs", json={**data, "idempotency_key": "unsupported-shared"})
    assert (result.status_code, result.json().get("detail")) == (409, "onprem_shared_grant_unavailable")
    assert not service.store.jobs()


def test_disk_host_alone_binds_the_same_job_as_its_explicit_oauth_endpoint(api):
    client, _ = api
    data = personal_request()
    data["config"].pop("destination_oauth_url")
    data["config"]["destination_url"] = "https://CLOUD-API.Team.Office.Example:443"
    payload = {**data, "idempotency_key": "one-installation"}
    created = client.post("/api/onprem/jobs", json=payload)
    assert created.status_code == 200
    job = created.json()
    assert job["config"]["destination_url"] == "https://cloud-api.team.office.example/v1/disk"
    assert job["config"]["destination_oauth_url"] == "https://oauth.team.office.example/token"
    data["config"]["destination_oauth_url"] = "https://OAUTH.Team.Office.Example:443/token"
    assert client.post("/api/onprem/jobs", json=payload).json()["id"] == job["id"]


@pytest.mark.parametrize("route", ["preflight", "jobs"])
def test_personal_grant_rejects_another_oauth_server(api, monkeypatch, route):
    client, service = api
    monkeypatch.setattr(service.runner, "preflight", lambda *_: pytest.fail("unexpected preflight"))
    data = personal_request()
    data["config"]["destination_oauth_url"] = "https://oauth.different.example/token"
    if route == "jobs":
        data["idempotency_key"] = "unbound-endpoint"
    response = client.post("/api/onprem/" + route, json=data)
    assert (response.status_code, response.json()["detail"]) == (422, "invalid_destination_oauth_url")
    assert not service.store.jobs()


def test_historical_oauth_mismatch_cannot_be_rebound_or_restarted(api, monkeypatch):
    client, service = api
    data = personal_request()
    job = client.post("/api/onprem/jobs", json={**data, "idempotency_key": "historical-oauth"}).json()
    base = "/api/onprem/jobs/" + job["id"]
    service.registry.discard(job["id"])
    # Simulate the pair stored by the older version before independent endpoints were rejected.
    with service.store._connection(write=True) as connection:
        connection.execute("UPDATE jobs SET config=json_set(config,'$.destination_oauth_url',?),state='ready' WHERE id=?",
                           ("https://oauth.different.example/token", job["id"]))
    monkeypatch.setattr(service.runner, "preflight", lambda *_a, **_kw: pytest.fail("unexpected preflight"))
    assert client.get(base).json()["config"]["destination_oauth_url"] == "https://oauth.different.example/token"
    assert client.get(base + "/report").status_code == 200
    for route, body in (("start", {}), ("resume", {}), ("retry", {}), ("entries/file/retry", {"revision": 0})):
        result = client.post(base + "/" + route, json=body)
        assert (result.status_code, result.json()["detail"]) == (409, "invalid_destination_oauth_url")
    del data["config"]["destination_oauth_url"]
    rebound = client.put(base + "/credentials", json=data)
    assert (rebound.status_code, rebound.json()["detail"]) == (409, "invalid_destination_oauth_url")
    assert service.registry.get(job["id"]) is None
    service.store.update_job(job["id"], state="stopped")
    restart = client.post(base + "/restart", json={**data, "idempotency_key": "old-oauth-restart"})
    assert (restart.status_code, restart.json()["detail"]) == (409, "invalid_destination_oauth_url")
    assert len(service.store.jobs()) == 1


@pytest.mark.parametrize("disk_url", [
    "https://disk-api.office.example", "https://cloud-api", "https://cloud-api..office.example",
    "https://cloud-api.bad-.example", "https://cloud-api.office%2eexample",
    "https://cloud-api.office.example:8443", "https://cloud-api.office.example:0",
    "https://127.0.0.1", "https://[::1]",
    "http://cloud-api.office.example", "https://name@cloud-api.office.example",
    "https://cloud-api.office.example/v1/disk?query", "https://cloud-api.office.example/#",
])
def test_unrecognized_disk_origin_is_rejected_before_personal_preflight(api, monkeypatch, disk_url):
    client, service = api
    monkeypatch.setattr(service.runner, "preflight", lambda *_: pytest.fail("unexpected preflight"))
    data = personal_request()
    data["config"].pop("destination_oauth_url")
    data["config"]["destination_url"] = disk_url
    response = client.post("/api/onprem/preflight", json=data)
    assert (response.status_code, response.json()["detail"]) == (422, "invalid_personal_disk_api_url")
    assert "fixture-" not in response.text
    assert not service.store.jobs()


def test_manual_token_cannot_be_used_in_place_of_delegated_access(api):
    client, _ = api
    data = personal_request()
    config, credentials = access_data(AccessRequest.model_validate(data))
    data["credentials"]["destination_token"] = "fixture-user-token"
    for keep_secret in (True, False):
        if not keep_secret:
            del data["credentials"]["destination_client_secret"]
        response = client.post("/api/onprem/preflight", json=data)
        assert response.status_code == 422
        assert "fixture-" not in response.text
        with pytest.raises(ValueError):
            CredentialRegistry().put("example", config, data["credentials"])
    assert "destination_token" not in credentials


def legacy_admin_job(service, data, key):
    config, credentials = access_data(AccessRequest.model_validate(data))
    job = service.store.create(config, key)
    service.registry.put(job["id"], config, credentials)
    return job


def create_personal(client, service, data, key):
    if data["config"]["destination_auth"] == "admin_token":
        return legacy_admin_job(service, data, key)
    return client.post("/api/onprem/jobs", json={**data, "idempotency_key": key}).json()


class AvailableDestination:
    supports_personal_service_application = True
    supports_personal_admin_token = True


def test_new_admin_job_is_rejected_and_existing_admin_job_remains_readable(api):
    client, service = api
    assert service.runner.destination_factory is OnPremDiskDestination
    data = personal_request("admin_token")
    response = client.post("/api/onprem/jobs", json={**data, "idempotency_key": "admin-personal"})
    assert response.status_code == 422
    job = legacy_admin_job(service, data, "legacy-admin")
    assert job["config"]["destination_auth"] == "admin_token"
    assert job["config"]["destination_type"] == "personal"
    assert job["config"]["destination_identity"] == "apertov@office.example"
    assert job["config"]["destination_root"] == "disk:/"
    assert job["config"]["identity_confirmed"] is False
    assert "destination_client_id" not in job["config"]
    assert service.registry.get(job["id"]) == data["credentials"]
    for path in service.store.path.parent.glob(service.store.path.name + "*"):
        assert b"fixture-" not in path.read_bytes()
    assert "fixture-" not in client.get("/api/onprem/jobs").text
    service.store.update_job(job["id"], state="completed")
    restart = client.post(f"/api/onprem/jobs/{job['id']}/restart", json={
        **data, "idempotency_key": "another-admin-job",
    })
    assert (restart.status_code, restart.json().get("detail")) == (422, "invalid_personal_destination")
    assert len(service.store.jobs()) == 1


@pytest.mark.parametrize(("path", "stored"), [
    ("/Проекты/2026/", "disk:/Проекты/2026"),
    ("/Проекты/2026/Смета.xlsx", "disk:/Проекты/2026/Смета.xlsx"),
])
def test_service_pilot_binds_job_to_selected_source_path(api, path, stored):
    client, _ = api
    data = personal_request()
    data["config"]["source_path"] = path
    payload = {**data, "idempotency_key": "scoped-personal-plan"}
    response = client.post("/api/onprem/jobs", json=payload)
    assert response.status_code == 200
    job = response.json()
    assert job["config"]["source_path"] == stored
    data["config"]["source_path"] = "/"
    duplicate = client.post("/api/onprem/jobs", json=payload)
    assert duplicate.status_code == 409
    rebound = client.put(f"/api/onprem/jobs/{job['id']}/credentials", json=data)
    assert (rebound.status_code, rebound.json()["detail"]) == (409, "credential_binding_mismatch")


@pytest.mark.parametrize("change", ["client_id", "client_secret", "source_admin_only", "manual_confirmation", "shared"])
def test_admin_pilot_rejects_mixed_access_or_destination(api, change):
    client, service = api
    data = personal_request("admin_token")
    if change == "client_id":
        data["config"]["destination_client_id"] = "another-app"
    elif change == "client_secret":
        data["credentials"]["destination_client_secret"] = "fixture-incompatible"
    elif change == "source_admin_only":
        data["credentials"]["source_admin_token"] = data["credentials"].pop("destination_token")
    elif change == "manual_confirmation":
        data["config"]["identity_confirmed"] = True
    else:
        data["config"].update(destination_type="shared", destination_vd_hash="shared")
    response = client.post("/api/onprem/jobs", json={**data, "idempotency_key": "invalid-admin-pilot"})
    assert response.status_code == 422 and "fixture-" not in response.text
    assert service.store.jobs() == []


def test_plan_binding_idempotency_restart_and_secret_storage(api):
    client, service = api
    service.runner.destination_factory = AvailableDestination
    data = personal_request()
    response = client.post("/api/onprem/jobs", json={**data, "idempotency_key": "personal-binding"})
    assert response.status_code == 200
    job = response.json()
    assert job["config"]["destination_root"] == "disk:/"
    assert job["config"]["destination_identity"] == "apetrov@office.example"
    assert job["config"]["identity_confirmed"] is False
    assert service.registry.get(job["id"]) == data["credentials"]
    for field, other in (("destination_identity", "vbelov@office.example"), ("destination_client_id", "other-app")):
        changed = {**data, "config": {**data["config"], field: other}}
        rebound = client.put(f"/api/onprem/jobs/{job['id']}/credentials", json=changed)
        assert rebound.json()["detail"] == "credential_binding_mismatch"
        duplicate = client.post("/api/onprem/jobs", json={**changed, "idempotency_key": "personal-binding"})
        assert duplicate.status_code == 409
    service.store.update_job(job["id"], state="stopped")
    restart = client.post(f"/api/onprem/jobs/{job['id']}/restart", json={
        **data, "config": job["config"], "idempotency_key": "restart-personal",
    })
    assert restart.status_code == 200
    assert restart.json()["config"] == job["config"]
    assert restart.json()["id"] != job["id"]
    assert restart.json()["state"] == "queued"
    assert "fixture-" not in response.text + restart.text + client.get("/api/onprem/jobs").text
    for path in service.store.path.parent.glob(service.store.path.name + "*"):
        assert b"fixture-" not in path.read_bytes()


def test_partial_plan_cannot_start_from_resume_or_file_retries(api):
    client, service = api
    service.runner.destination_factory = AvailableDestination
    data = {**personal_request(), "idempotency_key": "incomplete-personal"}
    job = client.post("/api/onprem/jobs", json=data).json()
    job_id = job["id"]
    entry_id, = service.store.add_entries(job_id, [file_entry()], append=True)
    service.store.update_job(job_id, state="needs_attention", error="destination_unavailable")
    base = f"/api/onprem/jobs/{job_id}"
    assert client.post(f"{base}/start").status_code == 409
    assert client.post(f"{base}/retry", json={}).status_code == 409
    point = client.post(f"{base}/entries/{entry_id}/retry", json={"revision": 0})
    assert point.status_code == 409
    resumed = client.post(f"{base}/resume")
    assert resumed.status_code == 200
    assert resumed.json()["state"] == "queued"
    assert resumed.json()["selection_frozen"] is False


@pytest.fixture(params=[
    ("admin_token", "apertov@office.example"), ("admin_token", "11300000000000001"),
    ("service_application", "apetrov@office.example"),
], ids=["admin-upn", "admin-uid", "service-app"])
def disks(api, request):
    client, service = api
    target = PersonalDestination()
    auth, identity = request.param
    data = personal_request(auth, identity)
    if auth == "admin_token":
        target.login = identity
        field = "uid" if identity.isdecimal() else "login"
        target.info = lambda: {"user": {field: target.login}, "total_space": 1000000, "used_space": 0}
    source = PublicSource()
    source_metadata = source.stat
    source.stat = lambda path: {"type": "dir", "path": path} if path == "disk:/" else source_metadata(path)

    def recipient(config, credentials):
        assert config["destination_identity"] == identity
        assert credentials == data["credentials"]
        return target

    recipient.supports_personal_service_application = True
    recipient.supports_personal_admin_token = True
    service.runner.source_factory = lambda config, credentials: source
    service.runner.destination_factory = recipient
    return client, service, source, target, data


def test_review_select_pause_and_resume_through_job_api(disks):
    client, service, source, target, data = disks
    preflight = client.post("/api/onprem/preflight", json=data)
    if data["config"]["destination_auth"] == "admin_token":
        assert preflight.status_code == 422
    else:
        assert preflight.json()["destination"]["verified_uid"] == target.uid
    job = create_personal(client, service, data, "api-through-transfer")
    job_id = job["id"]
    base = f"/api/onprem/jobs/{job_id}"
    service.runner.step(job_id)
    ready = client.get(base).json()
    assert ready["state"] == "ready"
    assert not source.published and not target.nodes
    entries = client.get(f"{base}/entries").json()["entries"]
    omitted = next(e for e in entries if e["source_path"] == "disk:/Docs/empty")
    selection = client.post(f"{base}/entries/{omitted['id']}/selection", json={
        "selected": False, "revision": ready["revision"],
    })
    assert selection.status_code == 200
    assert client.post(f"{base}/start").json()["state"] == "running"
    assert client.post(f"{base}/pause").status_code == 200
    service.runner.step(job_id)
    assert client.get(base).json()["state"] == "paused"
    assert not source.published and not target.nodes
    assert client.post(f"{base}/resume").json()["state"] == "running"
    for _ in range(20):
        service.runner.step(job_id)
        if service.store.get(job_id)["state"] == "completed":
            break
    assert client.get(base).json()["state"] == "completed"
    assert target.uploads == 1 and target.upload_body == BODY
    assert target.nodes[META["path"]]["md5"] == META["md5"]
    assert omitted["destination_path"] not in target.nodes
    for export in ("journal", "report"):
        response = client.get(f"{base}/{export}")
        assert response.status_code == 200 and "fixture-" not in response.text


@pytest.mark.parametrize("action", ["resume", "retry", "credentials", "point_retry"])
def test_recovery_rejects_wrong_login_even_with_same_credentials(disks, action):
    client, service, source, target, data = disks
    job_id = create_personal(client, service, data, "pause-personal")["id"]
    base = f"/api/onprem/jobs/{job_id}"
    service.runner.step(job_id)
    assert client.post(f"{base}/start").json()["state"] == "running"
    assert client.post(f"{base}/pause").status_code == 200
    service.runner.step(job_id)
    if data["config"]["destination_auth"] == "service_application":
        target.uid = "11300000000000002"
    else:
        target.login = "9007199254740993" if target.login.isdecimal() else "another@office.example"
    if action == "point_retry":
        job = service.store.get(job_id)
        entry = next(e for e in service.store.entries(job_id) if e["kind"] == "file")
        response = client.post(f"{base}/entries/{entry['id']}/retry",
                               json={"revision": job["revision"]})
    elif action == "credentials":
        response = client.put(f"{base}/{action}", json=data)
    else:
        response = client.post(f"{base}/{action}", json={})
    assert response.status_code == 409
    assert response.json()["detail"] == "destination_identity_mismatch"
    assert service.registry.get(job_id) is None
    service.runner.step(job_id)
    assert client.get(base).json()["state"] == "paused"
    assert not source.published and not target.nodes
