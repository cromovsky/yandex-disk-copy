"""Persistent legacy run history, replay, cleanup and secret boundaries."""

import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app import main
from app.run_journal import JournalError, RunJournal
from tests.test_main import make_cfg


@pytest.fixture
def journal(tmp_path):
    store = RunJournal(tmp_path)
    yield store
    store.close()


@pytest.fixture
def journal_api(journal, monkeypatch):
    monkeypatch.setattr(main.app.state, "run_journal", journal, raising=False)
    monkeypatch.setattr(main.app.state, "journal_error", None, raising=False)
    monkeypatch.setattr(main, "JOBS", {})
    with TestClient(main.app, base_url="http://localhost") as client:
        # Lifespan must use this isolated store, not production environment defaults.
        yield client, journal


def seed(store, job_id="run-1", mode="single"):
    return store.create(job_id, mode, "single", "src@example.org", "dst@example.org")


def test_reopen_recovers_running_job_and_keeps_completed_history(tmp_path):
    store = RunJournal(tmp_path)
    seed(store, "completed")
    store.append("completed", {"event_type": "stage", "stage": "upload", "status": "succeeded"})
    store.finish("completed", "done", summary={"saved": 1, "failed": 0})
    seed(store, "in-flight", "local")
    store.close()
    reopened = RunJournal(tmp_path)
    try:
        reopened.recover()
        assert reopened.get("completed")["status"] == "done"
        assert reopened.get("completed")["summary"] == {"saved": 1, "failed": 0}
        assert reopened.get("in-flight")["status"] == "interrupted"
        assert reopened.events("in-flight")["events"][-1]["status"] == "interrupted"
    finally:
        reopened.close()


def test_events_are_non_consuming_ordered_and_tail_has_full_cutoff(journal):
    seed(journal)
    for number in range(520):
        journal.append("run-1", {"message": f"event {number}"})
    first = journal.events("run-1", tail=500, limit=500)
    assert len(first["events"]) == 500
    assert first == journal.events("run-1", tail=500, limit=500)
    cursor = first["events"][-1]["sequence"]
    journal.append("run-1", {"message": "new"})
    assert [e["message"] for e in journal.events("run-1", after=cursor)["events"]] == ["new"]


def test_concurrent_pairs_keep_independent_order_and_context(journal):
    seed(journal)

    def write(pair):
        for number in range(30):
            journal.append("run-1", {"message": str(number), "pair_index": pair})
    with ThreadPoolExecutor(max_workers=5) as executor:
        list(executor.map(write, range(1, 6)))
    events = journal.events("run-1", limit=1000)["events"]
    assert len({e["sequence"] for e in events}) == len(events)
    for pair in range(1, 6):
        messages = [e["message"] for e in events if e.get("pair_index") == pair]
        assert messages == [str(n) for n in range(30)]


def test_cleanup_only_selected_mode_and_blocks_only_active_same_mode(journal, tmp_path):
    for job_id, mode in [("single-run", "single"), ("cross-run", "cross"), ("local-run", "local")]:
        seed(journal, job_id, mode)
        journal.append(job_id, {"message": "kept or deleted"})
    journal.finish("single-run", "done")
    result = journal.cleanup("single")
    assert result["removed_jobs"] == 1
    assert result["removed_events"] >= 1
    assert result["removed_bytes"] >= 0
    assert journal.get("single-run") is None
    assert journal.get("cross-run")["status"] == "running"
    with pytest.raises(JournalError, match="journal_in_use"):
        journal.cleanup("cross")
    assert journal.cleanup("single")["removed_jobs"] == 0


def test_symlink_directory_or_database_never_followed(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    root = tmp_path / "root"
    root.mkdir()
    (root / "journals").symlink_to(outside)
    with pytest.raises(JournalError):
        RunJournal(root)
    assert list(outside.iterdir()) == []


def test_logger_and_api_never_persist_secrets_results_or_public_links(journal, monkeypatch):
    seed(journal)
    cfg = make_cfg()
    cfg.source.client_secret = "source-secret-test"
    cfg.source.admin_token = "admin-token-test"

    class Copier:
        def __init__(self, cfg, log):
            self.log = log

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def run(self):
            self.log.add_secret("oauth-issued-token")
            self.log.add_secret("https://signed.example/x?token=private")
            self.log("source-secret-test admin-token-test oauth-issued-token "
                     "https://signed.example/x?token=private")
            self.log.event({"stage": "upload", "status": "failed", "http_status": 500,
                            "api_error": "InternalServerError", "message": "Ошибка загрузки",
                            "response_body": "body-secret",
                            "headers": {"Authorization": "header-secret"}})
            return {"saved": [{"public_key": "https://public.example/private"}], "fails": []}
    monkeypatch.setattr(main, "DiskCopier", Copier)
    job = main.Job(id="run-1", journal=journal)
    main._run_job(job, cfg)
    serialized = json.dumps(journal.events(job.id), ensure_ascii=False)
    serialized += journal.path.read_bytes().decode("utf-8", errors="ignore")
    for secret in ("source-secret-test", "admin-token-test", "oauth-issued-token",
                   "signed.example", "body-secret", "header-secret",
                   "public.example", "public_key"):
        assert secret not in serialized
    assert journal.get(job.id)["summary"] == {"saved": 1, "failed": 0}
    assert "result" not in journal.get(job.id)


def test_late_journal_failure_does_not_abort_transfer(journal, monkeypatch):
    seed(journal)

    class Copier:
        def __init__(self, cfg, log):
            self.log = log

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def run(self):
            self.log.event({"stage": "upload", "status": "succeeded"})
            return {"saved": ["file"], "fails": []}
    monkeypatch.setattr(main, "DiskCopier", Copier)

    def failure(*args, **kwargs):
        raise JournalError("journal_unavailable")
    monkeypatch.setattr(journal, "append", failure)
    job = main.Job(id="run-1", journal=journal)
    main._run_job(job, make_cfg())
    assert job.status == "done"
    assert job.journal_error == "journal_write_failed"
    assert journal.get(job.id)["journal_error"] == "journal_write_failed"


def test_batch_partial_file_failure_is_visible(journal, monkeypatch):
    seed(journal)

    class Copier:
        def __init__(self, cfg, log):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def run(self):
            return {"saved": ["a"], "fails": ["b"]}
    monkeypatch.setattr(main, "DiskCopier", Copier)
    job = main.Job(id="run-1", journal=journal, kind="batch",
                   pairs=[main.PairState(1, "src@example.org", "dst@example.org")])
    main._run_batch(job, [make_cfg()])
    assert job.status == "done"
    assert job.error
    assert job.pairs[0].error
    assert journal.get(job.id)["summary"]["failed"] == 1


def test_history_api_csv_replay_and_mode_cleanup(journal, monkeypatch):
    monkeypatch.setattr(main.app.state, "run_journal", journal, raising=False)
    monkeypatch.setattr(main, "JOBS", {})
    seed(journal)
    journal.append("run-1", {"stage": "download", "message": "=SUM(A1)", "http_status": 503})
    journal.finish("run-1", "done")
    client = TestClient(main.app, base_url="http://localhost")
    assert client.get("/api/history?mode=single").json()["jobs"][0]["id"] == "run-1"
    assert client.get("/api/run/run-1").json()["status"] == "done"
    assert client.get("/api/run/run-1/events").status_code == 200
    csv = client.get("/api/run/run-1/journal")
    assert csv.content.startswith(b"\xef\xbb\xbf")
    assert "'=SUM(A1)" in csv.text
    first = client.get("/api/run/run-1/stream").text
    assert first == client.get("/api/run/run-1/stream").text
    assert "id: " in first and "event: done" in first
    assert client.post("/api/history/cleanup", json={"mode": "single"},
                       headers={"Origin": "https://evil.example"}).status_code == 403
    assert client.post("/api/history/cleanup", json={"mode": "single"},
                       headers={"Sec-Fetch-Site": "cross-site"}).status_code == 403
    response = client.post("/api/history/cleanup", json={"mode": "single"})
    assert response.status_code == 200
    assert response.json()["removed_jobs"] == 1
    assert client.get("/api/run/run-1").status_code == 404


def test_running_count_is_independent_of_history_page(journal, monkeypatch):
    monkeypatch.setattr(main.app.state, "run_journal", journal, raising=False)
    monkeypatch.setattr(main, "JOBS", {})
    seed(journal, "old-running")
    for number in range(3):
        seed(journal, f"finished-{number}")
        journal.finish(f"finished-{number}", "done")
    response = TestClient(main.app).get("/api/history?mode=single&limit=1").json()
    assert response["running_count"] == 1
    assert response["active_jobs"][0]["id"] == "old-running"
    assert response["has_more"]


def test_context_preserves_real_filename_characters_and_csv_rolls_to_disk(journal):
    seed(journal)
    path = "disk:/{Отчёт}/<черновик>.txt"
    journal.append("run-1", {"source": path, "destination": path})
    assert journal.events("run-1")["events"][-1]["source"] == path
    for _ in range(350):
        journal.append("run-1", {"message": "я" * 4096})
    exported = journal.csv("run-1")
    try:
        assert exported._rolled
        assert exported.read(3) == b"\xef\xbb\xbf"
        assert "<черновик>" in exported.read(1024).decode("utf-8", errors="ignore")
        journal.finish("run-1", "done")
        journal.cleanup("single")
        assert exported.read(32)  # Export survives deleting its source history.
    finally:
        exported.close()


def test_terminal_durable_jobs_release_raw_results_secrets_and_queue(journal, monkeypatch):
    seed(journal)

    class Copier:
        def __init__(self, cfg, log):
            self.log = log

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def run(self):
            self.log.add_secret("issued-token")
            for _ in range(50):
                self.log("progress")
            return {"saved": [{"public_key": "https://public.example/key"}], "fails": []}
    monkeypatch.setattr(main, "DiskCopier", Copier)
    job = main.Job(id="run-1", journal=journal)
    main._run_job(job, make_cfg())
    assert job.result is None
    assert job.queue.empty()
    assert not job.redactor._secrets
    assert job.summary == {"saved": 1, "failed": 0}


def test_initial_storage_failure_never_starts_transfer(journal, monkeypatch):
    monkeypatch.setattr(main.app.state, "run_journal", journal, raising=False)
    monkeypatch.setattr(main, "JOBS", {})

    def failure(*args):
        raise JournalError("journal_unavailable")
    monkeypatch.setattr(journal, "create", failure)
    monkeypatch.setattr(main.threading, "Thread",
                        lambda *a, **k: pytest.fail("must persist before thread"))
    with pytest.raises(JournalError, match="journal_unavailable"):
        main._start_job(make_cfg())
    assert main.JOBS == {}


def test_storage_close_mid_transfer_is_reported_without_throwing(journal):
    seed(journal)
    job = main.Job(id="run-1", journal=journal)
    log = main.JobLog(job)
    journal.close()
    log.event({"stage": "download", "status": "succeeded"})
    assert job.journal_error == "journal_write_failed"


@pytest.mark.parametrize("route,expected", [
    ("/api/run", "single"), ("/api/run-cross", "cross"),
    ("/api/save-local", "local"), ("/api/save-local-cross", "local"),
])
def test_mode_is_route_selected_even_when_credentials_match(monkeypatch, route, expected):
    observed = []
    monkeypatch.setattr(main, "_start_job",
                        lambda cfg, mode="single": observed.append(mode) or {"job_id": "stub"})
    payload = {"client_id": "client", "client_secret": "secret", "orgid": "org",
               "admin_token": "admin", "source_disk_id": "src@example.org",
               "destination_disk_id": "dst@example.org"}
    if "cross" in route:
        payload.update({"src_" + key: payload[key]
                        for key in ("client_id", "client_secret", "orgid", "admin_token")})
        payload.update(dst_client_id="client", dst_client_secret="secret")
    response = TestClient(main.app).post(route, json=payload)
    assert response.status_code == 200
    assert observed == [expected]


def test_cleanup_guard_tracks_active_job_in_selected_mode_only(journal, monkeypatch):
    monkeypatch.setattr(main.app.state, "run_journal", journal, raising=False)
    seed(journal, "a", "single")
    seed(journal, "b", "cross")
    journal.finish("b", "done")
    monkeypatch.setattr(main, "JOBS", {"a": main.Job(id="a", mode="single", journal=journal)})
    client = TestClient(main.app)
    assert client.post("/api/history/cleanup", json={"mode": "single"}).json() == {
        "detail": "journal_in_use",
    }
    assert client.post("/api/history/cleanup", json={"mode": "cross"}).json()["removed_jobs"] == 1
    assert journal.get("a")["status"] == "running"


def test_start_and_cleanup_are_serialized(journal, monkeypatch):
    monkeypatch.setattr(main.app.state, "run_journal", journal, raising=False)
    monkeypatch.setattr(main, "JOBS", {})
    entered, release = threading.Event(), threading.Event()
    original = journal.create

    def create(*args):
        value = original(*args)
        entered.set()
        assert release.wait(5)
        return value
    monkeypatch.setattr(journal, "create", create)

    class DeferredThread:
        def __init__(self, *a, **kw):
            pass

        def start(self):
            pass
    # Patch only during _start_job; test concurrency uses executor's already-started worker.
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(lambda: release.wait(5))
        second = executor.submit(lambda: release.wait(5))
        release.set()
        first.result()
        second.result()
        release.clear()
        monkeypatch.setattr(main.threading, "Thread", DeferredThread)
        starting = executor.submit(main._start_job, make_cfg())
        assert entered.wait(5)

        def cleanup():
            with main.JOBS_LOCK:
                return journal.cleanup("single")
        deleting = executor.submit(cleanup)
        assert not deleting.done()
        release.set()
        starting.result()
        with pytest.raises(JournalError, match="journal_in_use"):
            deleting.result()
    assert journal.history("single")["running_count"] == 1


@pytest.mark.parametrize("batch", [False, True])
def test_unconfirmed_file_outcomes_are_not_reported_as_clean_success(
    journal, monkeypatch, batch,
):
    seed(journal)

    class Copier:
        def __init__(self, cfg, log):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def run(self):
            return {"saved": [], "fails": [], "unconfirmed": [{"path": "file"}]}

    monkeypatch.setattr(main, "DiskCopier", Copier)
    job = main.Job(id="run-1", journal=journal,
                   pairs=[main.PairState(1, "src@example.org", "dst@example.org")])
    if batch:
        main._run_batch(job, [make_cfg()])
    else:
        main._run_job(job, make_cfg())
    assert job.status == "done"
    assert "не подтверждён" in job.error
    assert journal.get(job.id)["summary"]["unconfirmed"] == 1
    assert journal.get(job.id)["summary"]["saved"] == 0
