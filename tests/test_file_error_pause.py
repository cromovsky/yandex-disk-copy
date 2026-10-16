"""A skipped error still spaces the next object without retrying the failed one."""

import pytest

from app.disk_copy import CopyError
from app.file_cooldown import FileCooldown
from app.migration.errors import MigrationError
from tests import test_source_batches as batch_fixtures
from tests.fake_clock import FakeClock
from tests.fake_session import FakeSession
from tests.test_disk_copy import make_copier
from tests.test_source_batches import BODY, MD5, FakeSource, create, finish_plan

setup_service = batch_fixtures.setup_service


@pytest.fixture(autouse=True)
def file_pause_enabled(monkeypatch):
    monkeypatch.setattr("app.file_cooldown.FILE_PAUSE_SECONDS", 20)


class Log:
    def __init__(self):
        self.events = []

    def __call__(self, message):
        pass

    def event(self, event):
        self.events.append(event)


@pytest.mark.parametrize("destination", ["personal", "shared"])
@pytest.mark.parametrize("retry_enabled", [False, True])
def test_legacy_consecutive_errors_space_next_transfer_only_when_retries_disabled(
    destination, retry_enabled,
):
    clock, log = FakeClock(), Log()
    copier = make_copier(
        FakeSession({}), clock=clock, dest_type=destination, vd_hash="shared-test",
    )
    copier._log = log
    copier.retry_policy.set_enabled(retry_enabled)
    nodes = [{"path": "disk:/" + name, "name": name, "type": "file",
              "public_key": "synthetic-key"}
             for name in ("one.txt", "two.txt", "last.txt")]
    calls = []

    def copy(*args):
        path = args[-1] if destination == "shared" else args[0]["path"]
        name = path.rsplit("/", 1)[-1]
        calls.append((name, clock.monotonic()))
        if name != "last.txt":
            raise CopyError("HTTP 503", http_status=503,
                            diagnostics={"error_side": "source", "api_error": "Unavailable"})

    if destination == "personal":
        copier.links = list(nodes)
        copier._disk_ensure_folder = lambda *args: None
        copier._save_one_link = copy
        copier.save_links()
    else:
        copier._vd_ensure_folder_tree = lambda *args: None
        copier._collect_files_recursive = lambda path: list(nodes)
        copier._disk_publish_resource = lambda *args: None
        copier._disk_resource = lambda *args: {"public_key": "synthetic-key"}
        copier._public_download_href = lambda *args: "https://example.invalid/file"
        copier._vd_upload_from_url = copy
        copier.save_to_shared()
    assert [when for _, when in calls] == (
        [1000, 1020, 1040] if not retry_enabled else [1000, 1000, 1000]
    )
    assert len(copier.fails) == 2 and len(copier.links) == 1
    if not retry_enabled:
        errors = [e for e in log.events if e.get("retry_enabled") is False]
        assert len(errors) == 2
        assert all(e["attempt"] == 1 and e["http_status"] == 503
                   and e["error_side"] == "source" for e in errors)
        assert clock.sleeps == [20, 20]


@pytest.mark.parametrize("shared", [False, True])
def test_single_local_errors_wait_before_next_download(tmp_path, shared):
    clock = FakeClock()
    copier = make_copier(FakeSession({}), clock=clock)
    copier.cfg.local_save, copier.cfg.local_root = True, str(tmp_path)
    copier.cfg.source_type = "shared" if shared else "personal"
    copier.cfg.source_vd_hash = "shared-source" if shared else ""
    copier.retry_policy.set_enabled(False)
    nodes = [{"path": "disk:/" + name, "type": "file", "name": name, "size": 1}
             for name in ("one.txt", "two.txt", "last.txt")]
    copier._walk = lambda path, visit: [visit(node) for node in nodes]
    calls = []

    def download(path, target):
        calls.append(clock.monotonic())
        if path != "disk:/last.txt":
            raise CopyError("source failed", http_status=500)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"x")

    copier._download_to_path = download
    copier.save_local()
    assert calls == [1000, 1020, 1040]
    assert len(copier.fails) == 2 and len(copier.links) == 1
    assert clock.sleeps == [20, 20]


def local_errors(service, clock):
    class Source(FakeSource):
        nodes = [{"path": "disk:/", "type": "dir"}] + [
            {"path": "disk:/" + name, "type": "file", "size": len(BODY), "md5": MD5}
            for name in ("one.txt", "two.txt", "last.txt")
        ]
        calls = []

        def chunks(self, path):
            self.calls.append(clock.monotonic())
            if path != "disk:/last.txt":
                raise MigrationError("source_download_failed", http_status=503,
                                     diagnostics={"api_error": "Unavailable"})
            yield BODY

    service.source_factory = Source
    service._file_cooldown = FileCooldown(seconds=20, monotonic=clock.monotonic)
    detail = finish_plan(service, create(service))
    batch_id, source_id = detail["batch"]["id"], detail["sources"][0]["id"]
    service.set_retry_enabled(batch_id, False)
    service.start_transfer(batch_id)

    def delay(seconds, checkpoint):
        checkpoint()
        clock.sleep(seconds)
        checkpoint()

    service._delay = delay
    return batch_id, source_id, Source.calls


def test_local_csv_errors_pause_then_continue_and_preserve_diagnostics(setup_service):
    service, _ = setup_service
    clock = FakeClock()
    batch_id, source_id, calls = local_errors(service, clock)
    service._transfer_one(source_id)
    assert calls == [1000, 1020, 1040]
    entries = service.entries(source_id)["entries"]
    assert [e["state"] for e in entries] == ["failed", "failed", "done"]
    assert [e["attempts"] for e in entries] == [1, 1, 1]
    assert service.store.source(source_id)["state"] == "completed_with_issues"
    skipped = [e for e in service.store.events(batch_id)
               if e["status"] == "skipped" and e.get("retry_enabled") is False]
    assert len(skipped) == 2
    assert all(e["http_status"] == 503 and e["error_side"] == "source" for e in skipped)
    assert clock.sleeps == [20, 20]


@pytest.mark.parametrize("action", ["pause", "stop"])
def test_local_csv_error_pause_is_interruptible(setup_service, action):
    service, _ = setup_service
    clock = FakeClock()
    batch_id, source_id, calls = local_errors(service, clock)

    def interrupt(seconds, checkpoint):
        getattr(service, action)(batch_id)
        checkpoint()

    service._delay = interrupt
    service._transfer_one(source_id)
    assert calls == [1000]
    assert service.entries(source_id)["entries"][0]["state"] == "failed"
    assert service.store.get(batch_id)["state"] == ("paused" if action == "pause" else "stopped")


def test_single_local_error_waits_before_next_existing_object(tmp_path):
    clock, log = FakeClock(), Log()
    copier = make_copier(FakeSession({}), clock=clock)
    copier.cfg.local_save, copier.cfg.local_root = True, str(tmp_path)
    copier.retry_policy.set_enabled(False)
    nodes = [{"path": "disk:/" + name, "type": "file", "name": name, "size": 1}
             for name in ("bad.txt", "existing.txt")]
    copier._walk = lambda path, visit: [visit(node) for node in nodes]
    copier._log = log
    events = []
    log.event = lambda event: events.append((event, clock.monotonic()))
    base = tmp_path / copier.cfg.source_disk_id
    base.mkdir()
    (base / "existing.txt").write_bytes(b"x")

    def download(path, target):
        raise CopyError("HTTP 503", http_status=503)

    copier._download_to_path = download
    copier.save_local()
    existing = [(event, when) for event, when in events
                if event.get("stage") == "check_local" and event.get("status") == "skipped"]
    assert existing and existing[0][1] == 1020
    assert clock.sleeps == [20]
    assert len(copier.fails) == 1 and len(copier.skipped) == 1
