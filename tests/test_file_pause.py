import pytest

from app.disk_copy import CopyError
from app.file_cooldown import FileCooldown
from tests import test_source_batches as batch_fixtures
from tests.fake_clock import FakeClock
from tests.fake_session import FakeSession
from tests.test_disk_copy import make_copier
from tests.test_source_batches import BODY, MD5, FakeSource, create, finish_plan


setup_service = batch_fixtures.setup_service


@pytest.fixture(autouse=True)
def _enable_file_pause(monkeypatch):
    monkeypatch.setattr("app.file_cooldown.FILE_PAUSE_SECONDS", 20)


@pytest.mark.parametrize("destination", ["personal", "shared"])
@pytest.mark.parametrize("retry_enabled", [True, False])
def test_legacy_queue_waits_twenty_seconds_after_success_and_continues(
    destination, retry_enabled,
):
    clock = FakeClock()
    copier = make_copier(
        FakeSession({}), clock=clock, dest_type=destination, vd_hash="shared-test",
    )
    copier.retry_policy.set_enabled(retry_enabled)
    nodes = [
        {
            "path": "disk:/" + name,
            "name": name,
            "type": kind,
            "public_key": "test-key",
        }
        for name, kind in [
            ("one.txt", "file"),
            ("bad.txt", "file"),
            ("folder", "dir"),
            ("last.txt", "file"),
        ]
    ]
    if destination == "shared":
        nodes = [node for node in nodes if node["type"] == "file"]
    calls = []

    def copy(*args):
        path = args[-1] if destination == "shared" else args[0]["path"]
        name = path.rsplit("/", 1)[-1]
        calls.append((name, clock.monotonic()))
        if name == "bad.txt":
            raise CopyError("synthetic failure")

    if destination == "personal":
        copier.links = list(nodes)
        copier._disk_ensure_folder = lambda *args: None
        copier._save_one_link = copy
        copier.save_links()
        assert calls == [
            ("one.txt", 1000), ("bad.txt", 1020),
            ("folder", 1020 if retry_enabled else 1040),
            ("last.txt", 1040 if retry_enabled else 1060),
        ]
        assert clock.sleeps == ([20, 20] if retry_enabled else [20, 20, 20])
    else:
        copier._vd_ensure_folder_tree = lambda *args: None
        copier._collect_files_recursive = lambda path: list(nodes)
        copier._disk_publish_resource = lambda *args: None
        copier._disk_resource = lambda *args: {"public_key": "test-key"}
        copier._public_download_href = lambda *args: "https://example.invalid/file"
        copier._vd_upload_from_url = copy
        copier.save_to_shared()
        assert calls == [
            ("one.txt", 1000), ("bad.txt", 1020),
            ("last.txt", 1020 if retry_enabled else 1040),
        ]
        assert clock.sleeps == ([20] if retry_enabled else [20, 20])
    assert len(copier.fails) == 1
    assert len(copier.links) == len(nodes) - 1


@pytest.mark.parametrize("shared", [False, True])
def test_single_local_save_waits_between_downloads_without_final_delay(
    tmp_path, shared,
):
    clock = FakeClock()
    copier = make_copier(FakeSession({}), clock=clock)
    copier.cfg.local_save = True
    copier.cfg.local_root = str(tmp_path)
    copier.cfg.source_type = "shared" if shared else "personal"
    copier.cfg.source_vd_hash = "shared-source" if shared else ""
    copier.retry_policy.set_enabled(False)
    nodes = [
        {"path": "disk:/" + name, "name": name, "type": "file", "size": 1}
        for name in ("one.txt", "two.txt", "last.txt")
    ]
    copier._walk = lambda path, visit: [visit(node) for node in nodes]
    calls = []

    def download(path, target):
        calls.append((path, clock.monotonic()))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"x")

    copier._download_to_path = download
    copier.save_local()
    assert [when for _, when in calls] == [1000, 1020, 1040]
    assert clock.sleeps == [20, 20]
    assert len(copier.links) == 3
    assert not copier.fails


def _local_two_files(service, clock):
    class Source(FakeSource):
        nodes = [{"path": "disk:/", "type": "dir"}] + [
            {"path": "disk:/" + name, "type": "file", "size": len(BODY), "md5": MD5}
            for name in ("one.txt", "two.txt")
        ]
        calls = []

        def chunks(self, path):
            self.calls.append((path, clock.monotonic()))
            yield BODY

    service.source_factory = Source
    service._file_cooldown = FileCooldown(seconds=20, monotonic=clock.monotonic)
    detail = finish_plan(service, create(service))
    service.set_retry_enabled(detail["batch"]["id"], False)
    service.start_transfer(detail["batch"]["id"])

    def delay(seconds, checkpoint):
        checkpoint()
        clock.sleep(seconds)
        checkpoint()

    service._delay = delay
    return detail["batch"]["id"], detail["sources"][0]["id"], Source.calls


def test_local_csv_continues_after_cooldown_with_retries_disabled(setup_service):
    service, _ = setup_service
    clock = FakeClock()
    batch_id, source_id, calls = _local_two_files(service, clock)
    service._transfer_one(source_id)
    assert [when for _, when in calls] == [1000, 1020]
    assert clock.sleeps == [20]
    assert service.store.source(source_id)["state"] == "completed"
    assert all(e["state"] == "done" for e in service.entries(source_id)["entries"])


@pytest.mark.parametrize("action", ["pause", "stop"])
def test_local_csv_control_during_cooldown_keeps_downloaded_file_done(
    setup_service, action,
):
    service, _ = setup_service
    clock = FakeClock()
    batch_id, source_id, calls = _local_two_files(service, clock)

    def interrupt(seconds, checkpoint):
        getattr(service, action)(batch_id)
        checkpoint()

    service._delay = interrupt
    service._transfer_one(source_id)
    entries = service.entries(source_id)["entries"]
    assert next(
        e for e in entries if e["source_path"] == "disk:/one.txt"
    )["state"] == "done"
    assert len(calls) == 1
    assert service.store.get(batch_id)["state"] == (
        "paused" if action == "pause" else "stopped"
    )
    if action == "pause":
        def delay(seconds, checkpoint):
            checkpoint()
            clock.sleep(seconds)

        service._delay = delay
        service.resume(batch_id)
        service._transfer_one(source_id)
        assert [when for _, when in calls] == [1000, 1020]
        assert service.store.source(source_id)["state"] == "completed"
