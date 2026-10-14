import threading

from app.source_slots import SourceSlots


def test_four_source_cap_and_owner_release():
    slots = SourceSlots()
    assert slots.try_acquire("local:one")
    assert slots.try_acquire("onprem:two")
    assert slots.try_acquire("onprem:two")
    assert slots.try_acquire("batch:three")
    assert slots.try_acquire("batch:four")
    assert not slots.try_acquire("batch:five")
    slots.release("local:one")
    assert slots.try_acquire("batch:five")
    assert slots.active_owners == {
        "onprem:two", "batch:three", "batch:four", "batch:five",
    }


def test_waiting_lease_checks_stop_and_releases_on_error():
    slots = SourceSlots(limit=2)
    slots.try_acquire("one")
    slots.try_acquire("two")
    stopped = threading.Event()

    def checkpoint():
        stopped.set()
        raise RuntimeError("stopped")

    try:
        with slots.lease("three", checkpoint=checkpoint):
            raise AssertionError("must not enter full source lane")
    except RuntimeError as exc:
        assert str(exc) == "stopped"
    assert stopped.is_set()
    assert slots.active_owners == {"one", "two"}
    slots.release("one")
    try:
        with slots.lease("three"):
            raise RuntimeError("failure")
    except RuntimeError:
        pass
    assert slots.active_owners == {"two"}


def test_nested_source_lease_keeps_outer_slot_reserved():
    slots = SourceSlots()
    with slots.lease("one"):
        with slots.lease("one"):
            assert slots.active_owners == {"one"}
        assert slots.active_owners == {"one"}
    assert slots.active_owners == set()
