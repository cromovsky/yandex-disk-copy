"""Live staged rates must not change durable transfer outcomes or HTTP framing."""

import threading

import pytest
import requests

from app.migration.errors import MigrationError
from app.migration.worker import MigrationService
from tests import test_migration_runner as fixtures
from tests.test_migration_clients import Response, destination
from tests.test_migration_runner import BODY, CONFIG, finish, run_until_attempt

migration = fixtures.migration


def test_staged_download_reports_written_bytes_and_clears_after_success(
    migration, monkeypatch,
):
    store, _, runner, job_id, source, _ = migration
    runner.default_strategy = "staged"
    now = [0.0]
    monkeypatch.setattr("app.migration.runner.time.monotonic", lambda: now[0])
    observed = []

    def chunks(path):
        now[0] = 1.0
        yield BODY[:4]
        now[0] = 2.0
        observed.append(runner.transfer_rates(job_id))
        assert store.get(job_id)["verified_bytes"] == 0
        yield BODY[4:]

    source.chunks = chunks
    assert finish(store, runner, job_id)["state"] == "completed"
    assert observed[0]["download_active"] is True
    assert observed[0]["download_bytes_per_second"] == 2.0
    assert observed[0]["upload_bytes_per_second"] is None
    assert runner.transfer_rates(job_id)["download_bytes_per_second"] is None


def test_rate_window_stall_reset_and_exception_cleanup(migration, monkeypatch):
    _, _, runner, job_id, _, _ = migration
    now = [0.0]
    monkeypatch.setattr("app.migration.runner.time.monotonic", lambda: now[0])
    with pytest.raises(MigrationError):
        with runner._measure_transfer(job_id, "upload") as progress:
            progress(0)
            now[0] = 1.0
            progress(100)
            now[0] = 2.0
            progress(100)
            assert runner.transfer_rates(job_id)["upload_bytes_per_second"] == 100
            now[0] = 8.0
            assert runner.transfer_rates(job_id)["upload_bytes_per_second"] == 0
            now[0] = 9.0
            progress(100)
            assert runner.transfer_rates(job_id)["upload_bytes_per_second"] == 20
            now[0] = 1009.0
            progress(100)
            assert runner.transfer_rates(job_id)["upload_bytes_per_second"] == 20
            raise MigrationError("destination_timeout", uncertain=True)
    assert runner.transfer_rates(job_id)["upload_active"] is False
    with runner._measure_transfer(job_id, "upload") as progress:
        progress(0)
        assert runner.transfer_rates(job_id)["upload_bytes_per_second"] == 0


def test_stop_clears_live_download(migration):
    store, _, runner, job_id, source, _ = migration
    runner.default_strategy = "staged"

    def chunks(path):
        yield BODY[:4]
        assert runner.transfer_rates(job_id)["download_active"]
        store.request_stop(job_id)
        yield BODY[4:]

    source.chunks = chunks
    run_until_attempt(store, runner, job_id)
    assert store.get(job_id)["stop_requested"]
    assert not runner.transfer_rates(job_id)["download_active"]
    runner.stop_job(job_id)
    assert store.get(job_id)["state"] == "stopped"


def test_failed_download_does_not_leak_rate_into_next_attempt(migration):
    store, _, runner, job_id, source, _ = migration
    runner.default_strategy = "staged"
    observed = []

    def chunks(path):
        observed.append(runner.transfer_rates(job_id)["download_bytes_per_second"])
        yield BODY[:4]
        raise MigrationError("source_download_timeout")

    source.chunks = chunks
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    assert observed == [0, 0, 0, 0, 0]
    assert runner.transfer_rates(job_id)["download_bytes_per_second"] is None


def test_runner_upload_callback_is_live_before_verification(migration, monkeypatch):
    store, _, runner, job_id, _, target = migration
    runner.default_strategy = "staged"
    now = [0.0]
    monkeypatch.setattr("app.migration.runner.time.monotonic", lambda: now[0])
    original = target.upload
    observed = []

    def upload(path, local_path, *, before_upload=None, on_progress=None):
        on_progress(0)
        now[0] = 2.0
        on_progress(10)
        observed.append(runner.transfer_rates(job_id))
        assert store.get(job_id)["verified_bytes"] == 0
        return original(path, local_path, before_upload=before_upload)

    target.upload = upload
    assert finish(store, runner, job_id)["state"] == "completed"
    assert observed[0]["upload_bytes_per_second"] == 5.0
    assert observed[0]["download_bytes_per_second"] is None
    assert runner.transfer_rates(job_id)["upload_bytes_per_second"] is None


@pytest.mark.parametrize("body", [b"abcdef", b""])
def test_upload_progress_preserves_requests_body_length(monkeypatch, tmp_path, body):
    client, session, _ = destination(monkeypatch, [Response(payload={
        "href": "https://upload.example.test/file", "method": "PUT",
    }), Response(201)])
    local = tmp_path / "file"
    local.write_bytes(body)
    original = session.request
    observed = []
    actual = []

    def request(method, url, **kwargs):
        if method == "PUT":
            prepared = requests.Request(
                method, url, data=kwargs["data"], headers=kwargs["headers"],
            ).prepare()
            assert prepared.headers["Content-Length"] == str(len(body))
            assert "Transfer-Encoding" not in prepared.headers
            if body:
                assert prepared.body.tell() == 0
                prepared.body.seek(0)
                actual.append(prepared.body.read(2) + prepared.body.read())
            else:
                actual.append(prepared.body or b"")
        return original(method, url, **kwargs)

    session.request = request
    assert client.upload("disk:/file", local, on_progress=observed.append) is None
    assert actual == [body]
    assert observed[0] == 0
    assert sum(observed) == len(body)


def test_rates_read_during_worker_guard_without_writing_history(tmp_path, monkeypatch):
    service = MigrationService(tmp_path, start_worker=False)
    try:
        job = service.store.create(CONFIG, "rates")
        service.store.update_job(job["id"], state="running")
        now = [0.0]
        monkeypatch.setattr("app.migration.runner.time.monotonic", lambda: now[0])
        before = service.store.last_sequence(job["id"])
        result = []
        completed = threading.Event()

        def read_job():
            result.append(service.describe_job(service.store.get(job["id"]), None))
            completed.set()

        with service.runner._measure_transfer(job["id"], "download") as progress:
            progress(0)
            now[0] = 2.0
            progress(1024)
            with service.guard:
                thread = threading.Thread(target=read_job)
                thread.start()
                readable = completed.wait(1)
            thread.join(1)
            assert readable
            assert result[0]["transfer_rates"]["download_bytes_per_second"] == 512
        assert service.store.last_sequence(job["id"]) == before
    finally:
        service.close()
