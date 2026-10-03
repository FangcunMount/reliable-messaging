import asyncio
from types import SimpleNamespace

import pytest

from reliable_messaging.delivery import Confirmation, DeliveryResult, Outcome
from reliable_messaging.nsq import NSQPublisher, NSQSubscriber
from reliable_messaging.wire import Envelope, encode


class Raw:
    def __init__(self, body, attempts=1):
        self.body, self.attempts = body, attempts
        self.id, self.timestamp, self.source_address = b"0" * 16, 100, "node:4150"
        self.finished, self.requeued = False, False

    def finish(self):
        assert not self.has_responded()
        self.finished = True

    def requeue(self, **kwargs):
        assert not self.has_responded()
        self.requeued = True

    def has_responded(self):
        return self.finished or self.requeued


async def committed(*args):
    pass


def subscriber(handler=committed, publisher=None, failed=committed, invalid=committed):
    if publisher is None:
        publisher = SimpleNamespace(publish=committed)
    return NSQSubscriber(
        "test.commands",
        "test.commands",
        publishers={"node:4150": publisher},
        handler=handler,
        failed_handler=failed,
        invalid_handler=invalid,
        failure_ready=committed,
    )


async def test_finish_requires_callback_commit_and_cancellation_does_not_finish():
    entered, permit = asyncio.Event(), asyncio.Event()

    async def persist(received):
        entered.set()
        await permit.wait()

    client = subscriber(persist)
    raw = Raw(encode(Envelope("original", b"body")))
    task = asyncio.create_task(client._handle(raw, False))
    await entered.wait()
    assert not raw.has_responded()
    task.cancel()
    await task
    assert not raw.has_responded()
    permit.set()
    await client._handle(raw, False)
    assert raw.finished


async def test_storage_failure_requeues_without_fin_and_failed_handoff_requires_confirmation():
    async def fails(*args):
        raise OSError("PRIVATE DRIVER PAYLOAD")

    client = subscriber(fails)
    raw = Raw(encode(Envelope("original", b"body")))
    await client._handle(raw, False)
    assert raw.requeued and not raw.finished
    seen = []

    async def unknown(topic, body):
        seen.append(body)
        return DeliveryResult(Outcome.UNKNOWN, Confirmation.BROKER)

    client = subscriber(fails, SimpleNamespace(publish=unknown))
    exhausted = Raw(raw.body, attempts=8)
    await client._handle(exhausted, False)
    assert exhausted.requeued and not exhausted.finished
    assert seen and b"PRIVATE" not in seen[0]


async def test_untrusted_wire_does_not_fin_when_quarantine_is_unavailable():
    async def fails(*args):
        raise OSError("unavailable")

    raw = Raw(b"PRIVATE malformed JSON")
    await subscriber(invalid=fails)._handle(raw, False)
    assert raw.requeued and not raw.finished


async def test_publish_timeout_retains_slot_until_real_callback_finishes():
    callbacks = []
    publisher = NSQPublisher("node:4150", timeout=0.02)
    publisher._client = SimpleNamespace(pub=lambda *args, callback: callbacks.append(callback))
    publisher._gate = asyncio.Semaphore(1)
    result = await publisher.publish("test.commands", b"wire")
    assert result.outcome == Outcome.UNKNOWN and publisher._gate.locked()
    callbacks[0](None, b"OK")
    assert not publisher._gate.locked() and not publisher._pending


async def test_constructors_are_inert():
    before = set(asyncio.all_tasks())
    publisher = NSQPublisher("node:4150")
    client = subscriber(publisher=publisher)
    assert publisher._client is None and not client._readers
    assert set(asyncio.all_tasks()) == before


async def test_stop_budget_retains_unknown_handler_that_suppresses_cancellation():
    entered, release = asyncio.Event(), asyncio.Event()

    async def host_commit():
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()

    client = subscriber()
    task = asyncio.create_task(host_commit())
    client._tasks.add(task)
    task.add_done_callback(client._tasks.discard)
    await entered.wait()
    try:
        async with asyncio.timeout(1):
            with pytest.raises(TimeoutError):
                await client.stop(grace_seconds=0)
        assert not task.done() and task in client._tasks
    finally:
        release.set()
        await task
        await client.stop()
