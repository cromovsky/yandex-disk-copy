"""A disabled retry never turns one file failure into a paused queue."""
import csv
import io

import pytest

from app.migration.errors import MigrationError
from tests.test_migration_runner import BODY, DIR, META, finish, stage_events
from tests.test_migration_download_retries import unplanned_migration
from tests.test_migration_public_runner import public_migration
from tests.test_source_batches_review import paired, ready_onprem
from tests.test_source_batches import FakeSource


def two_files(selected):
    store, _, runner, job_id, source, destination = selected
    second = dict(META, path="disk:/Docs/b.txt", name="b.txt", resource_id="second")
    source.walk = lambda path: [dict(DIR), dict(META), second]
    source.stat = lambda path: dict(DIR if path == DIR["path"] else META if path == META["path"] else second)
    runner.set_retry_enabled(job_id, False)
    runner.step(job_id)
    return second


@pytest.mark.parametrize("stage,code,side,http,api", [
    ("upload", "destination_unavailable", "onprem", 503, "InternalServerError"),
    ("verify", "destination_unavailable", "onprem", 500, "InternalServerError"),
    ("source", "source_auth_failed", "saas", 403, "ForbiddenError"),
    ("cache", "staging_io_failed", "local", None, None),
    ("uncertain", "invalid_operation_reference", "onprem", 202, None),
])
def test_file_failure_skipped_once_then_next_file_succeeds(
    unplanned_migration, stage, code, side, http, api,
):
    store, registry, runner, job_id, source, destination = unplanned_migration
    second = two_files(unplanned_migration)
    failure = MigrationError(code, http_status=http,
        auth=stage == "source", uncertain=stage == "uncertain",
        diagnostics={"error_side": side, **({"api_error": api} if api else {})})
    failed, uploaded = [], []
    original_upload, original_stat = destination.upload, destination.stat
    original_stage, source_stat = runner._stage, source.stat
    def upload(path, *args, **kwargs):
        uploaded.append(path)
        if path.endswith("/a.txt") and stage in {"upload", "uncertain"}:
            failed.append(path)
            if kwargs.get("before_upload"):
                kwargs["before_upload"](None)
            raise failure
        return original_upload(path, *args, **kwargs)
    def stat(path):
        if path.endswith("/a.txt") and stage == "verify" and path in destination.nodes:
            failed.append(path)
            raise failure
        return original_stat(path)
    def cache(entry, *args):
        if entry["source_path"] == META["path"] and stage == "cache":
            failed.append(entry["source_path"])
            raise failure
        return original_stage(entry, *args)
    def read(path):
        if path == META["path"] and stage == "source":
            failed.append(path)
            raise failure
        return source_stat(path)
    destination.upload, destination.stat, runner._stage, source.stat = upload, stat, cache, read
    result = finish(store, runner, job_id)
    entries = {entry["source_path"]: entry for entry in store.entries(job_id) if entry["kind"] == "file"}
    assert len(failed) == 1
    assert entries[second["path"]]["state"] == "done"
    assert entries[META["path"]]["state"] in {"failed", "outcome_unknown"}
    assert entries[META["path"]]["error"] == code
    assert result["state"] == "completed_with_issues" and not result["pause_requested"]
    assert registry.get(job_id)
    skips = [event["payload"] for event in stage_events(store, job_id)
             if event["payload"]["status"] == "skipped" and event["payload"].get("entry_id") == entries[META["path"]]["id"]]
    assert skips and skips[-1]["error"] == code and skips[-1]["error_side"] == side
    assert skips[-1].get("http_status") == http
    if api:
        assert skips[-1]["api_error"] == api


def test_failed_directory_only_skips_its_own_subtree(unplanned_migration):
    store, _, runner, job_id, source, destination = unplanned_migration
    bad_dir = dict(DIR, path="disk:/Docs/bad", name="bad")
    first = dict(META, path="disk:/Docs/bad/a.txt")
    second = dict(META, path="disk:/Docs/b.txt", name="b.txt", resource_id="second")
    nodes = [DIR, bad_dir, first, second]
    source.walk = lambda path: nodes
    source.stat = lambda path: next(dict(node) for node in nodes if node["path"] == path)
    runner.set_retry_enabled(job_id, False)
    runner.step(job_id)
    original = destination.ensure_dir
    calls = []
    def mkdir(path):
        if path.endswith("/bad"):
            calls.append(path)
            raise MigrationError("destination_unavailable", http_status=500,
                                 diagnostics={"error_side": "onprem", "api_error": "InternalServerError"})
        return original(path)
    destination.ensure_dir = mkdir
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    entries = {entry["source_path"]: entry for entry in store.entries(job_id)}
    assert calls == ["disk:/owner/Docs/bad"]
    assert entries[first["path"]]["state"] == "failed"
    assert entries[second["path"]]["state"] == "done"


def test_publication_error_skips_dependent_file_but_publishes_other_root(public_migration):
    store, registry, runner, old_id, source, destination = public_migration
    config = dict(store.get(old_id)["config"], source_path="disk:/", retry_enabled=False)
    job_id = store.create(config, "skip-publication")['id']
    registry.put(job_id, config, registry.get(old_id))
    first = dict(META, path="disk:/a.txt")
    second = dict(META, path="disk:/b.txt", resource_id="second")
    source.walk = lambda path: [first, second]
    source.stat = lambda path: {"path": "disk:/", "type": "dir"} if path == "disk:/" else dict(first if path == first['path'] else second)
    original = source.ensure_publication
    calls = []
    def publish(node, **kwargs):
        calls.append(node['source_path'])
        if node['source_path'] == first['path']:
            raise MigrationError("source_public_access_denied", http_status=403,
                                 diagnostics={"error_side": "saas", "api_error": "ForbiddenError"})
        return original(node, **kwargs)
    source.ensure_publication = publish
    runner.step(job_id)
    assert finish(store, runner, job_id)["state"] == "completed_with_issues"
    entries = {entry["source_path"]: entry for entry in store.entries(job_id)}
    assert calls == [first['path'], second['path']]
    assert entries[first['path']]['state'] == 'failed' and entries[second['path']]['state'] == 'done'
    skipped = [event['payload'] for event in stage_events(store, job_id, 'publish_source') if event['payload']['status'] == 'skipped']
    assert skipped and skipped[0]['http_status'] == 403 and skipped[0]['api_error'] == 'ForbiddenError'


def test_bad_credentials_remain_global_when_retries_disabled(unplanned_migration):
    store, registry, runner, job_id, source, _ = unplanned_migration
    two_files(unplanned_migration)
    original = source.stat
    def read(path):
        if path == META['path']:
            raise MigrationError('source_auth_failed', auth=True, http_status=401,
                                 diagnostics={'error_side': 'saas', 'api_error': 'UnauthorizedError'})
        return original(path)
    source.stat = read
    assert finish(store, runner, job_id)['state'] == 'awaiting_credentials'
    assert not registry.get(job_id)
    assert not any(entry['state'] == 'done' for entry in store.entries(job_id) if entry['kind'] == 'file')


def test_onprem_batch_file_outage_does_not_pause_list(paired, monkeypatch):
    batches, onprem, destination = paired
    second = dict(FakeSource.nodes[-1], path='disk:/folder/second.txt', resource_id='second')
    monkeypatch.setattr(FakeSource, 'nodes', [*FakeSource.nodes, second])
    detail = ready_onprem(batches)
    batch_id, source = detail['batch']['id'], detail['sources'][0]
    batches.set_retry_enabled(batch_id, False)
    original = destination.upload
    calls = []
    def upload(path, *args, **kwargs):
        calls.append(path)
        if path.endswith('/file.txt'):
            raise MigrationError('destination_unavailable', http_status=503,
                                 diagnostics={'error_side': 'onprem', 'api_error': 'InternalServerError'})
        return original(path, *args, **kwargs)
    destination.upload = upload
    batches.start_transfer(batch_id)
    batches._transfer_one(source['id'])
    assert len(calls) == 2
    assert batches.store.get(batch_id)['state'] != 'paused'
    child = onprem.store.get(source['child_job_id'])
    assert child['state'] == 'completed_with_issues' and not child['pause_requested']
    with batches.export(batch_id, journal=True) as export:
        rows = list(csv.DictReader(io.StringIO(export.read().decode('utf-8-sig'))))
    assert any(row['status'] == 'skipped' and row['error_side'] == 'onprem' and row['http_status'] == '503' and row['api_error'] == 'InternalServerError' for row in rows)


from tests.test_source_batches import setup_service, create, finish_plan
from tests.test_migration_point_retry import direct_migration


@pytest.mark.parametrize('failure_kind', ['source403', 'cache'])
def test_local_list_skips_first_file_and_keeps_full_reason(setup_service, monkeypatch, failure_kind):
    service, _ = setup_service
    second = dict(FakeSource.nodes[-1], path='disk:/folder/second.txt', resource_id='second')
    monkeypatch.setattr(FakeSource, 'nodes', [*FakeSource.nodes, second])
    detail = finish_plan(service, create(service))
    batch_id, source_id = detail['batch']['id'], detail['sources'][0]['id']
    service.set_retry_enabled(batch_id, False)
    original_stat, original_download = FakeSource.stat, service._download_local
    failures = []
    def stat(self, path):
        if path.endswith('/file.txt') and failure_kind == 'source403':
            failures.append(path)
            raise MigrationError('source_auth_failed', auth=True, http_status=403,
                                 diagnostics={'error_side': 'saas', 'api_error': 'ForbiddenError'})
        return original_stat(self, path)
    def download(batch, source, entry, *args):
        if entry['source_path'].endswith('/file.txt') and failure_kind == 'cache':
            failures.append(entry['source_path'])
            raise PermissionError(13, 'cache write denied')
        return original_download(batch, source, entry, *args)
    monkeypatch.setattr(FakeSource, 'stat', stat)
    monkeypatch.setattr(service, '_download_local', download)
    service.start_transfer(batch_id)
    service._transfer_one(source_id)
    entries = {entry['source_path']: entry for entry in service.store.entries(source_id)['entries']}
    assert len(failures) == 1 and entries[second['path']]['state'] == 'done'
    assert service.store.source(source_id)['state'] == 'completed_with_issues'
    skipped = [event for event in service.store.events(batch_id) if event.get('status') == 'skipped']
    assert skipped and skipped[-1]['error_side'] == ('source' if failure_kind == 'source403' else 'local')
    if failure_kind == 'source403':
        assert skipped[-1]['http_status'] == 403 and skipped[-1]['api_error'] == 'ForbiddenError'


def test_other_enabled_batch_outage_does_not_pause_disabled_sibling(paired):
    batches, onprem, _ = paired
    first, second = ready_onprem(batches), ready_onprem(batches)
    first_id, second_id = first['batch']['id'], second['batch']['id']
    batches.set_retry_enabled(second_id, False)
    batches.store.update(first_id, state='running')
    batches.store.update(second_id, state='running')
    batches._pause_destination(batches.store.get(first_id), 'destination_unavailable')
    assert batches.store.get(first_id)['state'] == 'paused'
    assert batches.store.get(second_id)['state'] == 'running'
    second_child = second['sources'][0]['child_job_id']
    assert not onprem.store.get(second_child)['pause_requested']
    batches.pause(second_id)
    assert batches.store.get(second_id)['state'] == 'paused'
    assert onprem.store.get(second_child)['pause_requested']


def test_cached_outage_does_not_short_circuit_disabled_job(direct_migration):
    store, _, runner, job_id, _, destination = direct_migration
    runner.set_retry_enabled(job_id, False)
    key = runner._destination_key(store.get(job_id)['config'])
    runner._destination_outages[key] = ('destination_unavailable', 503, {'error_side': 'onprem'})
    assert finish(store, runner, job_id)['state'] == 'completed'
    assert destination.uploads == 1
