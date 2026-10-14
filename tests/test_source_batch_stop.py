"""Stopped list scans release lanes and cannot publish late scan results."""

import threading
import time

import pytest
import requests

from app.migration.destination import OnPremDiskDestination
from app.migration.source import API, SaasDiskSource
from app.source_batches.service import Interrupted
from app.throttle import RateLimiter, SplitRateLimiter
from tests.fake_clock import FakeClock
from tests.test_migration_clients import CREDENTIALS as HTTP_CREDS
from tests.test_migration_clients import DEST_CONFIG, SOURCE_CONFIG, Response, Session
from tests.test_source_batches import CONFIG, CREDENTIALS, CSV, FakeSource
from tests.test_source_batches_review import (
    ONPREM_CONFIG, ONPREM_CREDS, paired, wait_for,
)


def new_list(batches, mode, sources=CSV):
    return batches.create(mode, sources, ONPREM_CONFIG if mode == "onprem" else CONFIG,
                          ONPREM_CREDS if mode == "onprem" else CREDENTIALS,
                          "stop-regression-" + str(time.monotonic_ns()))


@pytest.mark.parametrize("mode", ["local", "onprem"])
def test_stop_cancels_scan_before_next_stage_and_new_dry_run_can_finish(paired, mode):
    batches, onprem, _ = paired
    entered, release = threading.Event(), threading.Event()
    stats = []

    class SlowDescription(FakeSource):
        def describe(self):
            if self.config["source_email"] == "user@company.ru":
                entered.set()
                assert release.wait(2)
            return super().describe()

        def stat(self, path):
            stats.append(self.config["source_email"])
            return super().stat(path)

    batches.source_factory = SlowDescription
    old = new_list(batches, mode, CSV + "personal,two@company.ru,,/\n")
    old_id = old["batch"]["id"]
    batches.plan(old_id)
    batches.start()
    assert entered.wait(2)
    try:
        stopped = batches.stop(old_id)
        assert stopped["batch"]["state"] == "stopped"
        assert stopped["batch"]["stop_pending"]
        fresh = new_list(batches, mode, "type,email,vd_hash,path\npersonal,new@company.ru,,/\n")
        batches.plan(fresh["batch"]["id"])
    finally:
        release.set()
    wait_for(lambda: batches.store.get(fresh["batch"]["id"])["state"] == "ready")
    wait_for(lambda: not batches._active)
    assert not batches.detail(old_id)["batch"]["stop_pending"]
    assert "user@company.ru" not in stats
    assert all(source["state"] == "stopped" for source in batches.store.sources(old_id))
    assert not batches.slots.active_owners
    assert all(job["state"] in {"stopped", "ready"} for job in onprem.store.jobs())


def test_stop_during_local_stat_does_not_enter_walk(paired):
    batches, _, _ = paired
    entered, release = threading.Event(), threading.Event()
    walks = []

    class SlowStat(FakeSource):
        def stat(self, path):
            entered.set()
            assert release.wait(2)
            return super().stat(path)

        def walk(self, path):
            walks.append(path)
            yield from super().walk(path)

    batches.source_factory = SlowStat
    old = new_list(batches, "local")
    batches.plan(old["batch"]["id"])
    batches.start()
    assert entered.wait(2)
    batches.stop(old["batch"]["id"])
    release.set()
    wait_for(lambda: not batches._active)
    assert walks == []


@pytest.mark.parametrize("split", [False, True])
def test_cancelled_rate_limit_wait_ends_without_waiting_for_cooldown(split):
    clock = FakeClock(start=0)
    limiter = (SplitRateLimiter(10, 3, total_rps=10, monotonic=clock.monotonic,
                               sleep=clock.sleep) if split else
               RateLimiter(10, monotonic=clock.monotonic, sleep=clock.sleep))
    limiter.penalize(120)

    def check():
        if clock.monotonic() >= .2:
            raise Interrupted()

    with pytest.raises(Interrupted):
        limiter.acquire("GET", checkpoint=check)
    assert clock.monotonic() <= .2
    # The shared cooldown still protects other transfers.
    limiter.acquire("GET")
    assert clock.monotonic() >= 120


def test_saas_cancelled_retry_delay_closes_response_and_sends_no_retry():
    clock = FakeClock(start=0)
    client = SaasDiskSource(SOURCE_CONFIG, HTTP_CREDS)
    response = Response(503)
    client.session = Session([response, Response(200, {})])
    client.limiter = RateLimiter(10, monotonic=clock.monotonic, sleep=clock.sleep)
    def check():
        if clock.monotonic():
            raise Interrupted()

    client.checkpoint = check
    try:
        with pytest.raises(Interrupted):
            client._request("GET", API + "/resources")
        assert len(client.session.calls) == 1
        assert response.closed
        assert clock.monotonic() <= .2
    finally:
        client.close()


@pytest.mark.parametrize("directory", [False, True])
def test_stop_during_credentials_lookup_prevents_next_api_request(directory):
    stopped = threading.Event()
    creds = dict(HTTP_CREDS, **({"source_admin_token": "admin"} if directory else {}))
    client = SaasDiskSource(dict(SOURCE_CONFIG, source_org_id="123"), creds)
    response = (Response(200, {"users": [{"email": "source@example.com", "isEnabled": True}]})
                if directory else Response(200, {"access_token": "owner", "expires_in": 3600}))

    class StopOnResponse(Session):
        def request(self, method, url, **kwargs):
            result = super().request(method, url, **kwargs)
            stopped.set()
            return result

    def check():
        if stopped.is_set():
            raise Interrupted()

    client.checkpoint = check
    client.session = StopOnResponse([response])
    client.limiter = RateLimiter(1000)
    try:
        with pytest.raises(Interrupted):
            client.stat("disk:/")
        assert len(client.session.calls) == 1
        assert response.closed
    finally:
        client.close()


@pytest.mark.parametrize("result", [Response(200, {}), requests.ConnectionError("stopped")])
def test_destination_stop_discards_inflight_read_result(result):
    stopped = threading.Event()
    client = OnPremDiskDestination(DEST_CONFIG, HTTP_CREDS)

    class StopOnResponse(Session):
        def request(self, method, url, **kwargs):
            stopped.set()
            return super().request(method, url, **kwargs)

    def check():
        if stopped.is_set():
            raise Interrupted()

    client.checkpoint = check
    client.session = StopOnResponse([result])
    client.limiter = RateLimiter(1000)
    try:
        with pytest.raises(Interrupted):
            client._request("GET", "/resources")
        assert len(client.session.calls) == 1
        if isinstance(result, Response):
            assert result.closed
    finally:
        client.close()


@pytest.mark.parametrize("side", ["saas", "onprem"])
def test_stopped_429_keeps_shared_retry_after_barrier(side):
    stopped = threading.Event()
    clock = FakeClock(start=0)
    response = Response(429, headers={"Retry-After": "30"})
    client = (SaasDiskSource(SOURCE_CONFIG, HTTP_CREDS) if side == "saas" else
              OnPremDiskDestination(DEST_CONFIG, HTTP_CREDS))

    class StopOnResponse(Session):
        def request(self, method, url, **kwargs):
            result = super().request(method, url, **kwargs)
            stopped.set()
            return result

    def check():
        if stopped.is_set():
            raise Interrupted()

    client.checkpoint = check
    client.session = StopOnResponse([response])
    client.limiter = RateLimiter(10, monotonic=clock.monotonic, sleep=clock.sleep)
    try:
        with pytest.raises(Interrupted):
            client._request("GET", API + "/resources" if side == "saas" else "/resources")
        assert response.closed
        assert len(client.session.calls) == 1
        client.limiter.acquire("GET")
        assert clock.monotonic() >= 30
    finally:
        client.close()


def test_stop_not_blocked_by_final_destination_check(paired):
    batches, _, destination = paired
    detail = new_list(batches, "onprem")
    batch_id = detail["batch"]["id"]
    batches.plan(batch_id)
    batches._plan_one(detail["sources"][0]["id"])
    entered, release, stopped = threading.Event(), threading.Event(), threading.Event()
    original = destination.info

    def blocked_info():
        entered.set()
        assert release.wait(2)
        return original()

    destination.info = blocked_info
    planner = threading.Thread(target=batches._finish_planning, args=(batch_id,))
    stopper = threading.Thread(target=lambda: (batches.stop(batch_id), stopped.set()))
    planner.start()
    assert entered.wait(2)
    stopper.start()
    was_prompt = stopped.wait(.3)
    release.set()
    planner.join(2)
    stopper.join(2)
    assert was_prompt, "stop waited behind the destination HTTP check"
    assert batches.store.get(batch_id)["state"] == "stopped"
    assert all(s["state"] == "stopped" for s in batches.store.sources(batch_id))


def test_selection_change_fences_old_capacity_check_and_checks_new_selection(paired):
    batches, _, destination = paired
    detail = new_list(batches, "onprem", CSV + "personal,two@company.ru,,/\n")
    batch_id = detail["batch"]["id"]
    batches.plan(batch_id)
    for source in detail["sources"]:
        batches._plan_one(source["id"])
    entered, release = threading.Event(), threading.Event()
    original = destination.info

    def blocked_info():
        entered.set()
        assert release.wait(2)
        return dict(original(), total_space=5)

    destination.info = blocked_info
    planner = threading.Thread(target=batches._finish_planning, args=(batch_id,))
    planner.start()
    assert entered.wait(2)
    result = batches.selection(batch_id, [detail["sources"][0]["id"]])
    assert result["batch"]["state"] == "planning"
    release.set()
    planner.join(2)
    assert batches.store.get(batch_id)["state"] == "planning"
    batches.start()
    wait_for(lambda: batches.store.get(batch_id)["state"] == "ready")
    assert batches.detail(batch_id)["batch"]["summary"]["required_bytes"] == 5
