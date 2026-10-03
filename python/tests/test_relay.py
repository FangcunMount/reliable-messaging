import asyncio

import pytest

from reliable_messaging import PeriodicRelay


async def test_constructor_start_stop_and_lost_notifications():
    scanned = asyncio.Event()
    calls = 0

    async def attempt():
        nonlocal calls
        calls += 1
        if calls >= 2:
            scanned.set()
        return 0

    relay = PeriodicRelay(attempt, poll_seconds=0.01)
    assert calls == 0
    await relay.start()
    with pytest.raises(RuntimeError, match="stopped"):
        await relay.start()
    # No notify: the second scan must still happen.
    async with asyncio.timeout(1):
        await scanned.wait()
    await relay.stop()
    stopped = calls
    await asyncio.sleep(0.03)
    assert calls == stopped
    await relay.start()
    await relay.stop()


async def test_stop_drains_admitted_work():
    admitted, finish = asyncio.Event(), asyncio.Event()
    completed = []

    async def attempt():
        admitted.set()
        await finish.wait()
        completed.append(1)
        return 1

    relay = PeriodicRelay(attempt)
    await relay.start()
    await admitted.wait()
    stop = asyncio.create_task(relay.stop())
    await asyncio.sleep(0)
    assert not stop.done()
    finish.set()
    await stop
    assert completed == [1]


async def test_stop_deadline_leaves_unknown_attempt_for_recovery():
    admitted = asyncio.Event()
    cancelled = []

    async def attempt():
        admitted.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.append(1)
        return 0

    relay = PeriodicRelay(attempt, shutdown_seconds=0.01)
    await relay.start()
    await admitted.wait()
    with pytest.raises(TimeoutError):
        await relay.stop()
    assert cancelled == [1]


async def test_scan_failure_reaches_host_supervisor():
    async def failed():
        raise ConnectionError("scan unavailable")

    relay = PeriodicRelay(failed)
    await relay.start()
    with pytest.raises(ConnectionError):
        await relay.wait()
    with pytest.raises(ConnectionError):
        await relay.stop()
