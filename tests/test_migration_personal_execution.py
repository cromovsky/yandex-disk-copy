import pytest

from app.migration.credentials import CredentialRegistry
from app.migration.destination import OnPremDiskDestination
from app.migration.errors import MigrationError
from app.migration.runner import MigrationRunner
from app.migration.source import SaasDiskSource
from app.migration.store import MigrationStore, canonical_config
from tests.test_migration_clients import Limiter, Response, Session
from tests.test_migration_public_runner import PublicSource
from tests.test_migration_runner import BODY, CONFIG, META, Destination, cached_attempt, finish


PERSONAL = dict(CONFIG, destination_type="personal", destination_auth="service_application",
                destination_url="https://cloud-api.office.example/v1/disk",
                source_email="aivanov@cloud.example", source_path="disk:/",
                destination_client_id="migration-client", destination_identity="apetrov@office.example",
                destination_oauth_url="https://oauth.office.example/token",
                destination_root="disk:/", source_public=True, identity_confirmed=False)
CREDS = {"source_client_secret": "source-fixture", "destination_client_secret": "app-fixture",
         "destination_owner_token": "owner-fixture"}
ADMIN = {k: v for k, v in PERSONAL.items() if k not in {"destination_client_id", "destination_oauth_url"}}
ADMIN.update(destination_url=CONFIG["destination_url"], destination_auth="admin_token",
             destination_identity="apetrov@office.example")
ADMIN_CREDS = {"source_client_secret": "source-fixture", "destination_token": "admin-fixture"}


class PersonalDestination(Destination):
    def __init__(self):
        super().__init__()
        self.login = "apetrov@office.example"
        self.uid = "11300000000000001"

    def info(self):
        return dict(super().info(), user={"login": self.login, "email": self.login, "uid": self.uid})


@pytest.fixture
def source_path(request):
    return getattr(request, "param", "disk:/")


@pytest.fixture
def personal(tmp_path, request, source_path):
    store, registry = MigrationStore(tmp_path), CredentialRegistry()
    source, target = PublicSource(), PersonalDestination()
    source_stat = source.stat
    source.stat = lambda path: {"path": path, "type": "dir"} if path == "disk:/" else source_stat(path)
    runner = MigrationRunner(store, registry, source_factory=lambda *_: source,
                             destination_factory=lambda *_: target, reserve_bytes=0)
    config, creds = PERSONAL, CREDS
    if getattr(request, "param", None) == "admin_token":
        config, creds = ADMIN, ADMIN_CREDS
        target.login = config["destination_identity"]
        target.info = lambda: {"total_space": 1000000, "used_space": 0, "user": {"login": target.login}}
    config = dict(config, source_path=source_path)
    job = store.create(config, "single-personal")
    registry.put(job["id"], config, creds)
    yield runner, job["id"], source, target
    runner.close()
    store.close()


@pytest.fixture
def admin_http(monkeypatch, personal):
    runner, _, _, _ = personal
    runner.destination_factory = OnPremDiskDestination
    session = Session([])
    monkeypatch.setattr("app.migration.destination.requests.Session", lambda: session)
    monkeypatch.setattr("app.migration.destination.get_onprem_limiter", lambda url: Limiter())
    return session


@pytest.mark.parametrize("personal", ["service_application", "admin_token"], indirect=True)
def test_verified_recipient_receives_same_paths_from_staging(personal):
    runner, job_id, source, target = personal
    config = runner.store.get(job_id)["config"]
    result = runner.preflight(config, runner.registry.get(job_id))
    assert result["destination"]["type"] == "personal"
    assert result["destination"]["identity"] == config["destination_identity"]
    assert "manual_identity_binding" not in result["warnings"]
    runner.step(job_id)
    assert runner.store.get(job_id)["state"] == "ready"
    assert not runner.store.get(job_id)["remote_root"]
    entries = runner.store.entries(job_id)
    assert all(e["destination_path"] == e["source_path"] for e in entries)
    assert source.published == [] and not target.nodes and not source.downloads

    runner.validate_selection(job_id)
    runner.store.freeze_selection(job_id)
    assert finish(runner.store, runner, job_id)["state"] == "completed"
    assert target.upload_body == BODY
    assert target.uploads == 1 and target.moves == 0
    assert target.nodes[META["path"]]["md5"] == META["md5"]
    assert runner.store.attempts(job_id)[0]["remote_path"] == META["path"]
    if config["destination_auth"] == "service_application":
        assert result["destination"]["verified_uid"] == runner.store.get(job_id)["destination_verified_uid"]


@pytest.mark.parametrize("personal", ["service_application", "admin_token"], indirect=True)
@pytest.mark.parametrize("source_path,expected", [
    ("disk:/Projects/2026/a.txt", {"disk:/Projects/2026/a.txt"}),
    ("disk:/Projects/2026", {"disk:/Projects/2026", "disk:/Projects/2026/a.txt",
                             "disk:/Projects/2026/empty"}),
    ("disk:/Projects/2026/empty", {"disk:/Projects/2026/empty"}),
], indirect=["source_path"])
def test_personal_subset_keeps_paths_and_publishes_only_selection(personal, source_path, expected):
    runner, job_id, source, target = personal
    store = runner.store
    source_file = "disk:/Projects/2026/a.txt"
    tree = {
        path: {"type": "dir", "path": path} for path in (
            "disk:/Projects", "disk:/Projects/2026", "disk:/Projects/2026/empty",
            "disk:/Projects/other", "disk:/unrelated",
        )
    }
    tree[source_file] = dict(META, path=source_file)
    source.stat = tree.__getitem__
    source.walk = lambda root: (
        node for path, node in tree.items() if path == root or path.startswith(root + "/")
    )
    target.nodes["disk:/Projects"] = tree["disk:/Projects"]
    preflight = runner.preflight(store.get(job_id)["config"], runner.registry.get(job_id))
    assert preflight["source"] == {"path": source_path, "type": tree[source_path]["type"]}
    runner.step(job_id)
    assert store.get(job_id)["state"] == "ready"
    assert {(e["source_path"], e["destination_path"]) for e in store.entries(job_id)} == {
        (path, path) for path in expected
    }
    assert [p["source_path"] for p in store.publications(job_id)] == [source_path]
    assert set(target.nodes) == {"disk:/Projects"}
    assert not source.published and not source.downloads
    runner.validate_selection(job_id)
    store.freeze_selection(job_id)
    assert finish(store, runner, job_id)["state"] == "completed"
    assert source.published == [source_path]
    assert set(target.nodes) >= expected
    assert source.links == ([source_file] if source_file in expected else [])
    assert "disk:/Projects/other" not in target.nodes and "disk:/unrelated" not in target.nodes


@pytest.mark.parametrize("personal", ["admin_token"], indirect=True)
@pytest.mark.parametrize("source_path", [META["path"]], indirect=True)
def test_personal_subset_stops_on_conflicting_destination_parent(personal):
    runner, job_id, source, target = personal
    target.nodes["disk:/Docs"] = dict(META, path="disk:/Docs")
    source.walk = lambda _: [META]
    runner.step(job_id)
    assert runner.store.get(job_id)["error"] == "destination_conflict"
    assert not runner.store.get(job_id)["plan_complete"]
    assert not source.published and not source.downloads and not target.uploads


@pytest.mark.parametrize("info,error", [
    ({"user": {"login": "administrator@office.example"}}, "destination_identity_mismatch"),
    ({"user": {"login": "apetrov@other.example"}}, "destination_identity_mismatch"),
    ({"user": {"login": "APETROV@office.example"}}, "destination_identity_mismatch"),
    ({"user": {"login": "apetrov"}}, "destination_identity_unverified"),
    ({"total_space": 1000}, "destination_identity_unverified"),
    ({"user": {"email": ADMIN["destination_identity"]}}, "destination_identity_unverified"),
    ({"user": "apetrov"}, "destination_identity_unverified"),
])
def test_admin_token_reads_only_disk_info_when_recipient_not_proven(personal, admin_http, info, error):
    runner, _, source, _ = personal
    admin_http.responses.append(Response(payload=info))
    source.stat = lambda *_: pytest.fail("The SaaS source must wait for destination proof")
    with pytest.raises(MigrationError, match=error):
        runner.preflight(ADMIN, ADMIN_CREDS)
    assert [(method, url) for method, url, kwargs in admin_http.calls] == [
        ("GET", ADMIN["destination_url"] + "/"),
    ]
    assert admin_http.calls[0][2]["headers"] == {"Authorization": "OAuth " + ADMIN_CREDS["destination_token"]}
    assert admin_http.calls[0][2]["allow_redirects"] is False
    assert not source.published


@pytest.mark.parametrize("identifier,user", [
    (ADMIN["destination_identity"], {"login": ADMIN["destination_identity"]}),
    ("18446744073709551615", {"uid": "18446744073709551615"}),
    ("09007199254740993", {"uid": 9007199254740993}),
])
def test_admin_token_preflight_uses_personal_root_and_same_origin(personal, admin_http, identifier, user):
    runner, _, _, _ = personal
    admin_http.responses.extend([
        Response(payload={"user": user, "total_space": 1000, "used_space": 10}),
        Response(payload={"type": "dir", "path": "disk:/"}),
    ])
    config = canonical_config(dict(ADMIN, destination_identity=identifier))
    result = runner.preflight(config, ADMIN_CREDS)
    assert result["destination"] == {"type": "personal", "identity": identifier,
                                     "free_bytes": 990, "path": "disk:/"}
    assert [(method, url) for method, url, kwargs in admin_http.calls] == [
        ("GET", ADMIN["destination_url"] + "/"), ("GET", ADMIN["destination_url"] + "/resources"),
    ]
    assert admin_http.calls[1][2]["params"]["path"] == "disk:/"


@pytest.mark.parametrize("user,error", [
    ({"uid": 9007199254740992}, "destination_identity_mismatch"),
    ({"uid": "18446744073709551616"}, "destination_identity_unverified"),
    ({"uid": True}, "destination_identity_unverified"),
    ({"uid": 9007199254740993.0}, "destination_identity_unverified"),
    ({"uid": "１２３"}, "destination_identity_unverified"),
    ({"login": "9007199254740993"}, "destination_identity_unverified"),
])
def test_admin_uid_proof_requires_the_returned_uint64_user_uid(personal, admin_http, user, error):
    runner, _, _, _ = personal
    admin_http.responses.append(Response(payload={"user": user}))
    with pytest.raises(MigrationError, match=error):
        runner.preflight(dict(ADMIN, destination_identity="9007199254740993"), ADMIN_CREDS)
    assert [call[0] for call in admin_http.calls] == ["GET"]


@pytest.mark.parametrize("personal", ["admin_token"], indirect=True)
def test_admin_token_unknown_owner_after_reattach_blocks_before_publication(personal):
    runner, job_id, source, target = personal
    runner.step(job_id)
    runner.validate_selection(job_id)
    runner.store.freeze_selection(job_id)
    runner.store.update_job(job_id, state="running")
    runner.registry.put(job_id, ADMIN, dict(ADMIN_CREDS, destination_token="replacement-fixture"))
    target.login = None
    runner.step(job_id)
    assert runner.store.get(job_id)["error"] == "destination_identity_unverified"
    assert not source.published and not source.downloads and not target.nodes


def test_admin_token_rejects_service_application_fields():
    with pytest.raises(ValueError):
        canonical_config(dict(ADMIN, destination_client_id="migration-client"))


@pytest.mark.parametrize("value", [
    "", "apetrov", "@office.example", "apetrov@", "a@b@c", "１８", "-1",
    "18446744073709551616", "a/b@office.example", "a b@office.example",
])
def test_admin_destination_accepts_only_a_complete_upn_or_uint64(value):
    with pytest.raises(ValueError):
        canonical_config(dict(ADMIN, destination_identity=value))


@pytest.mark.parametrize("uid,email,error", [
    (None, PERSONAL["destination_identity"], "destination_identity_unverified"),
    ("11300000000000001", "other@office.example", "destination_identity_mismatch"),
])
def test_proof_precedes_planning_and_publication(personal, uid, email, error):
    runner, job_id, source, target = personal
    target.uid, target.login = uid, email
    with pytest.raises(MigrationError, match=error):
        runner.preflight(PERSONAL, CREDS)
    runner.step(job_id)
    assert runner.store.get(job_id)["error"] == error
    assert not runner.store.entries(job_id) and not source.published and not target.nodes


def test_replacing_credentials_rechecks_before_write(personal):
    runner, job_id, source, target = personal
    runner.step(job_id)
    runner.validate_selection(job_id)
    runner.store.freeze_selection(job_id)
    runner.store.update_job(job_id, state="running")
    runner.registry.put(job_id, PERSONAL, dict(CREDS, destination_client_secret="replacement"))
    target.uid = "123"
    runner.step(job_id)
    assert runner.store.get(job_id)["error"] == "destination_identity_mismatch"
    assert not source.published and not source.downloads and not target.nodes


def test_personal_plan_compares_existing_files(personal):
    runner, job_id, source, target = personal
    foreign = dict(META, md5="1" * 32)
    target.nodes[META["path"]] = foreign
    runner.step(job_id)
    assert runner.store.get(job_id)["error"] == "plan_conflicts"
    entry = next(e for e in runner.store.entries(job_id) if e["kind"] == "file")
    assert entry["error"] == "destination_conflict"
    assert not source.downloads and not source.published and not target.uploads
    assert target.nodes[META["path"]] == foreign


@pytest.mark.parametrize("conflict", [True, False])
def test_point_retry_preserves_other_content_and_reuses_verified_cache(personal, conflict):
    runner, job_id, source, target = personal
    store = runner.store
    runner.step(job_id)
    store.freeze_selection(job_id)
    entry, attempt = cached_attempt(store, runner, job_id, phase="unknown_transfer")
    local = runner.staging / attempt["local_name"]
    store.update_job(job_id, state="needs_attention")
    outcomes = iter(("failed", "success"))
    target.poll = lambda _: next(outcomes)
    if conflict:
        target.nodes[META["path"]] = dict(META, md5="1" * 32)
    store.queue_point_retry(job_id, entry["id"], attempt["id"], store.get(job_id)["revision"])
    runner.step(job_id)
    if conflict:
        assert store.entry(entry["id"])["error"] == "destination_conflict"
        assert target.nodes[META["path"]]["md5"] == "1" * 32
        assert not target.uploads and local.read_bytes() == BODY
    else:
        assert store.entry(entry["id"])["state"] == "done"
        assert target.upload_body == BODY
        assert not source.downloads and not local.exists()


def test_personal_worker_cannot_skip_explicit_start(personal):
    runner, job_id, source, target = personal
    runner.step(job_id)
    runner.store.update_job(job_id, state="running")
    runner.step(job_id)
    assert runner.store.get(job_id)["error"] == "plan_not_ready"
    assert not source.published and not target.nodes


@pytest.mark.parametrize("override", [
    {"destination_root": "disk:/aivanov"}, {"destination_identity": "bad/login"},
    {"destination_client_id": "  "}, {"strategy": "url"}, {"source_type": "shared"},
    {"source_path": "../subfolder"}, {"source_public": False},
    {"identity_confirmed": True}, {"destination_token": "fixture-token"},
])
def test_personal_config_rejects_invalid_bindings(override):
    with pytest.raises(ValueError):
        canonical_config(dict(PERSONAL, **override))


def test_unconfirmed_grant_never_uses_supplied_disk_token():
    with pytest.raises(MigrationError, match="onprem_personal_grant_unavailable"):
        OnPremDiskDestination({k: v for k, v in PERSONAL.items() if k != "destination_oauth_url"},
                              dict(CREDS, destination_token="fixture-token"))


def test_outage_is_scoped_to_recipient():
    assert MigrationRunner._destination_key(PERSONAL) != MigrationRunner._destination_key(
        dict(PERSONAL, destination_identity="other"),
    )
    assert MigrationRunner._destination_key(CONFIG) == (CONFIG["destination_url"], "", "personal")
    assert MigrationRunner._destination_key(dict(ADMIN, destination_identity="00123")) == (
        MigrationRunner._destination_key(dict(ADMIN, destination_identity="123"))
    )


def test_stopped_pending_upload_does_not_reserve_another_recipients_path(personal):
    runner, original, source, first = personal
    store = runner.store
    runner.step(original)
    store.freeze_selection(original)
    _, attempt = cached_attempt(store, runner, original, phase="unknown_transfer")
    runner.stop_job(original)
    assert [a["id"] for a in runner.final_pending(original)] == [attempt["id"]]
    runner._record_destination_outage(original, MigrationError("destination_unavailable"))

    other = dict(PERSONAL, destination_identity="bvasilev@office.example")
    with pytest.raises(ValueError, match="restart_config_mismatch"):
        store.restart(original, other, "changed-recipient")
    second = PersonalDestination()
    second.login = other["destination_identity"]
    runner.destination_factory = lambda *_: second
    new = store.create(other, "other-recipient")["id"]
    runner.registry.put(new, other, CREDS)
    runner.step(new)
    runner.validate_selection(new)
    store.freeze_selection(new)
    assert finish(store, runner, new)["state"] == "completed"
    assert second.nodes[META["path"]]["md5"] == META["md5"]
    assert not first.uploads and not first.nodes


def test_reassigned_email_cannot_start_reviewed_plan(personal):
    runner, job_id, source, target = personal
    runner.step(job_id)
    assert runner.store.get(job_id)["destination_verified_uid"] == "11300000000000001"
    target.uid = "11300000000000002"
    with pytest.raises(MigrationError, match="destination_identity_mismatch"):
        runner.validate_selection(job_id)
    assert not source.published and not target.uploads


def test_reopened_worker_checks_uid_before_polling_accepted_upload(personal):
    runner, job_id, source, target = personal
    store = runner.store
    runner.step(job_id)
    runner.validate_selection(job_id)
    store.freeze_selection(job_id)
    store.update_job(job_id, state="running")
    target.operation = "in-progress"
    for _ in range(12):
        runner.step(job_id)
        if target.uploads:
            break
    attempt, = store.attempts(job_id)
    assert target.uploads == 1 and attempt["phase"] == "waiting_transfer"
    sent = list(source.published)
    runner.release_clients(job_id)
    runner.registry.put(job_id, PERSONAL, CREDS)
    target.uid = "11300000000000002"
    target.poll = lambda _: pytest.fail("polled under a different owner")
    runner.step(job_id)
    assert store.get(job_id)["error"] == "destination_identity_mismatch"
    assert store.attempt(attempt["id"])["phase"] == "waiting_transfer"
    assert target.uploads == 1 and source.published == sent


@pytest.mark.parametrize("operation_id,status", [
    ("still-running", "in-progress"), ("gone", 404), ("completed", "success"), (None, None),
])
@pytest.mark.parametrize("source_path", [META["path"]], indirect=True)
def test_point_retry_checks_uncertain_upload_before_resending(personal, operation_id, status):
    runner, job_id, source, target = personal
    source.walk = lambda _: [META]
    store = runner.store
    runner.step(job_id)
    runner.validate_selection(job_id)
    store.freeze_selection(job_id)
    store.update_job(job_id, state="running")
    sent, polled = [], []

    def lost_response(path, local, *, before_upload, **kwargs):
        before_upload(operation_id)
        sent.append(path)
        raise MigrationError("destination_timeout", uncertain=True)

    def poll(identifier):
        polled.append(identifier)
        if status == 404:
            raise MigrationError("destination_not_found", http_status=404)
        return status

    target.upload, target.poll = lost_response, poll
    for _ in range(10):
        runner.step(job_id)
        if sent:
            break
    entry = next(e for e in store.entries(job_id) if e["kind"] == "file")
    attempt, = store.attempts(job_id)
    assert attempt["operation_id"] == operation_id
    store.queue_point_retry(job_id, entry["id"], attempt["id"], store.get(job_id)["revision"])
    runner.step(job_id)
    assert sent == [META["path"]]
    assert polled == ([operation_id] if operation_id else [])
    assert len(store.attempts(job_id)) == 1
    # Once the original contents become visible, another check can finish the file.
    target.nodes[META["path"]] = dict(META)
    store.queue_point_retry(job_id, entry["id"], attempt["id"], store.get(job_id)["revision"])
    runner.step(job_id)
    assert store.entry(entry["id"])["state"] == "already_verified"
    assert sent == [META["path"]]


@pytest.mark.parametrize("status,expected", [(403, "awaiting_credentials"), (503, "needs_attention")])
def test_refresh_failure_keeps_staged_file_resumable_with_retries_off(personal, status, expected):
    runner, job_id, source, target = personal
    store = runner.store
    runner.step(job_id)
    runner.validate_selection(job_id)
    store.freeze_selection(job_id)
    store.update_job(job_id, state="running")
    runner.set_retry_enabled(job_id, False)
    upload = target.upload

    def failed_refresh(*args, **kwargs):
        error = MigrationError("destination_auth_failed" if status == 403 else "destination_unavailable",
                               auth=status == 403, http_status=status)
        error._migration_stage = "check_destination_access"
        raise error

    target.upload = failed_refresh
    for _ in range(10):
        runner.step(job_id)
        if store.attempts(job_id):
            break
    attempt, = store.attempts(job_id)
    assert store.get(job_id)["state"] == expected
    assert store.get(job_id)["resume_state"] == "running"
    assert attempt["phase"] == "staged" and not target.uploads
    downloads = source.downloads
    target.upload = upload
    runner.registry.put(job_id, PERSONAL, CREDS)
    runner.ensure_personal_recipient(store.get(job_id))
    assert finish(store, runner, job_id)["state"] == "completed"
    assert source.downloads == downloads and target.uploads == 1


@pytest.mark.parametrize("source_path", [META["path"]], indirect=True)
def test_accepted_upload_with_expired_operation_is_verified_by_final_file(personal):
    runner, job_id, source, target = personal
    source.walk = lambda _: [META]
    store = runner.store
    runner.step(job_id)
    runner.validate_selection(job_id)
    store.freeze_selection(job_id)
    store.update_job(job_id, state="running")
    target.operation = "in-progress"
    runner.step(job_id)
    attempt, = store.attempts(job_id)
    assert attempt["phase"] == "waiting_transfer"

    def expired(_):
        raise MigrationError("destination_not_found", http_status=404)

    target.poll = expired
    final = target.nodes.pop(META["path"])
    runner.step(job_id)
    assert store.get(job_id)["state"] == "needs_attention"
    assert store.attempt(attempt["id"])["phase"] == "waiting_transfer"
    target.nodes[META["path"]] = final
    assert finish(store, runner, job_id)["state"] == "completed"
    assert target.uploads == 1 and len(store.attempts(job_id)) == 1


@pytest.mark.parametrize("source_path", [META["path"]], indirect=True)
def test_recipient_rechecked_after_wait_before_publication_put(personal):
    runner, job_id, source, target = personal
    store = runner.store
    source.walk = lambda _: [META]
    runner.step(job_id)
    runner.validate_selection(job_id)
    store.freeze_selection(job_id)
    store.update_job(job_id, state="running")
    runner.set_retry_enabled(job_id, False)
    job = store.get(job_id)
    real = SaasDiskSource(job["config"], CREDS)
    real._owner_token = lambda: "fixture-source"
    real.session.close()
    real.session = Session([Response()])
    real._publication_metadata = lambda path: (META, "existing" if real.session.calls else None)
    original = target.uid

    class ReassignedDuringWait(Limiter):
        def acquire(self, method, **kwargs):
            assert method == "PUT"
            target.uid = "42"

    real.limiter = ReassignedDuringWait()
    with pytest.raises(MigrationError, match="destination_identity_mismatch") as blocked:
        runner._prepare_publications(job, real)
    assert blocked.value.auth and not blocked.value.uncertain
    assert not real.session.calls and not real._unknown_publications
    assert store.publications(job_id)[0]["state"] != "intent"
    assert all(e["state"] == "planned" for e in store.entries(job_id))

    target.uid = original
    real.limiter = Limiter()
    send = real.session.request

    def accepted(*args, **kwargs):
        target.uid = "42"
        return send(*args, **kwargs)

    real.session.request = accepted
    runner._prepare_publications(job, real)
    assert [method for method, _, _ in real.session.calls] == ["PUT"]
    assert store.publications(job_id)[0]["state"] == "published"
    real.close()
