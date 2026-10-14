import pytest

from app.migration.credentials import CredentialRegistry
from app.migration.runner import MigrationRunner
from app.migration.store import MigrationStore
from tests.test_migration_runner import CONFIG, Destination, META, Source


def test_full_plan_compares_content_and_keeps_scan_read_only(tmp_path):
    store = MigrationStore(tmp_path)
    runner = MigrationRunner(store, CredentialRegistry())
    job = store.create(CONFIG, "complete-plan", initial_state="planning")
    source, destination = Source(), Destination()
    destination.nodes["disk:/owner/Docs/a.txt"] = dict(META)
    runner.plan(job, source, destination, full_compare=True)
    current = store.get(job["id"])
    assert current["state"] == "ready"
    assert current["counts"] == {"already_verified": 1}
    assert current["verified_bytes"] == META["size"]
    assert source.downloads == destination.imports == 0
    assert set(destination.nodes) == {"disk:/owner/Docs/a.txt"}
    store.close()


def test_interrupted_full_plan_persists_partial_inventory_but_never_ready(tmp_path):
    store = MigrationStore(tmp_path)
    runner = MigrationRunner(store, CredentialRegistry())
    job = store.create(CONFIG, "interrupted-plan", initial_state="planning")
    source = Source()

    def broken_walk(path):
        yield dict(META)
        raise RuntimeError("scan interrupted")

    source.walk = broken_walk
    with pytest.raises(RuntimeError, match="scan interrupted"):
        runner.plan(job, source, Destination(), full_compare=True)
    assert store.get(job["id"])["state"] == "planning"
    assert len(store.entries(job["id"])) == 1
    store.reset_incomplete_plan(job["id"])
    assert store.entries(job["id"]) == []
    store.close()


@pytest.mark.parametrize("node,expected", [
    ({"type": "dir"}, "conflict"),
    ({"type": "file", "size": META["size"], "md5": "a" * 32}, "conflict"),
    ({"type": "file", "size": META["size"]}, "verification_unavailable"),
])
def test_full_plan_reports_target_conflicts_without_overwriting(tmp_path, node, expected):
    store = MigrationStore(tmp_path)
    runner = MigrationRunner(store, CredentialRegistry())
    job = store.create(CONFIG, "conflicting-plan", initial_state="planning")
    destination = Destination()
    destination.nodes["disk:/owner/Docs/a.txt"] = node
    runner.plan(job, Source(), destination, full_compare=True)
    assert store.get(job["id"])["state"] == "needs_attention"
    assert store.get(job["id"])["counts"] == {expected: 1}
    assert destination.imports == 0
    store.close()


def test_verified_dry_run_match_rechecks_changed_source_before_transfer(tmp_path):
    store = MigrationStore(tmp_path)
    registry = CredentialRegistry()
    source, destination = Source(), Destination()
    runner = MigrationRunner(store, registry, source_factory=lambda *args: source,
                             destination_factory=lambda *args: destination)
    job = store.create(CONFIG, "source-drift", initial_state="planning")
    destination.nodes["disk:/owner/Docs/a.txt"] = dict(META)
    registry.put(job["id"], CONFIG, {"source_client_secret": "s", "destination_token": "d"})
    runner.plan(job, source, destination, full_compare=True)
    store.revalidate_verified_files(job["id"])
    source.metadata["md5"] = "f" * 32
    store.update_job(job["id"], state="running")
    for _ in range(12):
        runner.step(job["id"])
    assert store.get(job["id"])["counts"] == {"source_changed": 1}
    assert source.downloads == destination.imports == 0
    store.close()
