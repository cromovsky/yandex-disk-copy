import pytest

from app.migration.credentials import CredentialRegistry
from app.migration.errors import MigrationError
from app.migration.runner import MigrationRunner
from app.migration.store import MigrationStore


def test_atomic_cache_reservation_protects_second_download(tmp_path):
    store = MigrationStore(tmp_path)
    runner = MigrationRunner(store, CredentialRegistry(), staging_budget=100,
                             reserve_bytes=0)
    with runner.reserve_cache("a" * 32 + ".part", 70):
        with pytest.raises(MigrationError) as raised:
            with runner.reserve_cache("b" * 32 + ".part", 40):
                pytest.fail("two reservations would exceed the cache")
        assert raised.value.code == "staging_space_insufficient"
    with runner.reserve_cache("b" * 32 + ".part", 40):
        pass
    store.close()


def test_written_reserved_bytes_are_counted_once(tmp_path):
    store = MigrationStore(tmp_path)
    runner = MigrationRunner(store, CredentialRegistry(), staging_budget=100,
                             reserve_bytes=0)
    name = "a" * 32 + ".part"
    with runner.reserve_cache(name, 70):
        runner._local(name).write_bytes(b"x" * 50)
        assert runner._reserved_cache_bytes() == 20
        with runner.reserve_cache("b" * 32 + ".part", 30):
            pass
    assert runner._cache_reservations == {}
    store.close()


def test_cache_cleanup_rejects_open_download_even_after_full_stop(tmp_path):
    store = MigrationStore(tmp_path)
    runner = MigrationRunner(store, CredentialRegistry(), reserve_bytes=0)
    with runner.reserve_cache("a" * 32 + ".part", 70):
        with pytest.raises(MigrationError) as raised:
            runner.cleanup_cache()
        assert raised.value.code == "cache_in_use"
    store.close()
