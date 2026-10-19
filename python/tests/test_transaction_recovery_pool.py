"""Real thread/executor bounds, independent from real Broker acceptance."""

import asyncio
import threading

import pytest

from reliable_messaging._rocketmq_transaction_driver import RecoveryPool


async def test_admit_before_submit_and_keep_real_running_capacity() -> None:
    pool = RecoveryPool(1, 1)
    started, release = threading.Event(), threading.Event()

    def blocking() -> None:
        started.set()
        release.wait()

    assert pool.submit(blocking)
    async with asyncio.timeout(2):
        assert await asyncio.to_thread(started.wait, 1)
    assert pool.submit(lambda: None)
    for _ in range(1000):
        assert not pool.submit(lambda: None)
    assert pool.snapshot()["pending"] == 2
    assert pool.snapshot()["running"] == 1
    drain = asyncio.create_task(pool.drain())
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.02):
            await asyncio.shield(drain)
    assert not drain.done()
    assert pool.snapshot()["pending"] == 2
    assert not pool.submit(lambda: None)
    release.set()
    await drain
    assert pool.snapshot()["pending"] == 0


async def test_instances_and_worker_errors_are_isolated() -> None:
    first, second = RecoveryPool(1, 0), RecoveryPool(1, 0)
    first.stop_admission()
    assert not first.submit(lambda: None)

    def fail() -> None:
        raise RuntimeError("synthetic callback failure")

    assert second.submit(fail)
    await second.drain()
    assert second.snapshot()["failed"] == 1
    assert first.snapshot()["failed"] == 0
    await first.drain()


async def test_real_pinned_producer_dispatches_to_host_loop_without_global_factory() -> None:
    from rocketmq.grpc_protocol import DigestType, Encoding, TransactionResolution
    from rocketmq.grpc_protocol.definition_pb2 import Message
    from rocketmq.v5.producer import Producer

    from reliable_messaging._rocketmq_transaction_driver import create_producer

    loop = asyncio.get_running_loop()
    checked = asyncio.Event()
    pool = RecoveryPool(1, 0)

    async def checker(message: object) -> int:
        assert asyncio.get_running_loop() is loop
        checked.set()
        return int(TransactionResolution.TRANSACTION_RESOLUTION_UNSPECIFIED)

    producer = create_producer(
        endpoint="127.0.0.1:1",
        access_key="fixture",
        secret_key="fixture",
        topics={"fixture"},
        timeout=0.25,
        host_loop=loop,
        pool=pool,
        checker=checker,
    )
    # Exercise the actual pinned telemetry dispatch hook without opening a
    # network connection; real Broker delivery is a separate required gate.
    producer._Client__is_running = True
    original = Producer.on_recover_orphaned_transaction_command
    message = Message(body=b"a")
    message.system_properties.body_encoding = Encoding.IDENTITY
    message.system_properties.body_digest.type = DigestType.MD5
    message.system_properties.body_digest.checksum = "0CC175B9C0F1B6A831C399E269772661"
    producer.on_recover_orphaned_transaction_command(None, message, "synthetic-tx")
    async with asyncio.timeout(2):
        await checked.wait()
    await pool.drain()
    assert pool.snapshot()["admitted"] == 1
    assert pool.snapshot()["failed"] == 0
    assert producer._Producer__transaction_check_executor._work_queue.qsize() == 0
    assert Producer.on_recover_orphaned_transaction_command is original
    failure = TimeoutError("synthetic ambiguous half result")
    no_retry = producer._Producer__check_send_retry_condition(None, None, 2, failure)
    assert no_retry.exception() is failure
    producer._Client__is_running = False
    await asyncio.to_thread(producer.shutdown)
