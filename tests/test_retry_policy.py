import threading
import time

import pytest


def test_disabled_policy_never_waits():
    from app.retry_policy import RetryPolicy
    policy = RetryPolicy(False)
    started = time.monotonic()
    assert policy.wait(10) is False
    assert time.monotonic() - started < 0.1


def test_disabling_policy_wakes_existing_wait_and_can_be_enabled_again():
    from app.retry_policy import RetryPolicy
    policy = RetryPolicy()
    entered, completed = threading.Event(), threading.Event()
    result = []
    def run():
        result.append(policy.wait(30, checkpoint=entered.set))
        completed.set()
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    assert entered.wait(1)
    policy.set_enabled(False)
    assert completed.wait(1)
    thread.join()
    assert result == [False]
    policy.set_enabled(True)
    assert policy.wait(0) is True


def test_wait_obeys_stop_checkpoint_and_never_swallows_it():
    from app.retry_policy import RetryPolicy
    def stop():
        raise RuntimeError('stopped')
    with pytest.raises(RuntimeError, match='stopped'):
        RetryPolicy().wait(30, checkpoint=stop)


def test_policy_rejects_coerced_non_boolean_values():
    from app.retry_policy import RetryPolicy
    for value in ('false', 0, None):
        with pytest.raises((TypeError, ValueError)):
            RetryPolicy(value)
        with pytest.raises((TypeError, ValueError)):
            RetryPolicy().set_enabled(value)


def test_disabling_then_enabling_does_not_revive_a_cancelled_pending_wait():
    from app.retry_policy import RetryPolicy
    policy = RetryPolicy()
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    outcomes = []
    def checkpoint():
        entered.set()
        assert release.wait(1)
    def run():
        outcomes.append(policy.wait(30, checkpoint))
        finished.set()
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    assert entered.wait(1)
    policy.set_enabled(False)
    policy.set_enabled(True)
    release.set()
    assert finished.wait(1)
    thread.join(1)
    assert outcomes == [False]
    assert policy.wait(0) is True


def test_cancelled_generation_cannot_start_wait_after_toggle_back_on():
    from app.retry_policy import RetryPolicy
    policy = RetryPolicy()
    generation = policy.cancellation_generation
    policy.set_enabled(False)
    policy.set_enabled(True)
    assert policy.wait(30, generation=generation) is False
    assert policy.wait(0) is True
