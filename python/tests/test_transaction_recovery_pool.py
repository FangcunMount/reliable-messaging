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


def test_route_refusal_keeps_original_error_on_only_one_pinned_manager():
    from concurrent.futures import Future
    from types import SimpleNamespace

    from rocketmq.v5.client import ClientConfiguration, Credentials
    from rocketmq.v5.client.client_route_manager import ClientRouteManager
    from rocketmq.v5.exception.client_exception import ForbiddenException

    from reliable_messaging._rocketmq_transaction_driver import preserve_route_errors

    refusal = ForbiddenException("synthetic refusal", 40300)

    def query(*args, **kwargs):
        future = Future()
        future.set_exception(refusal)
        return future

    def instance():
        producer = SimpleNamespace(
            client_configuration=ClientConfiguration(
                "127.0.0.1:1", Credentials("fixture", "fixture"), request_timeout=1
            ),
            rpc_client=SimpleNamespace(query_topic_route_async=query),
            sign=lambda: [],
        )
        producer._Client__route_manager = ClientRouteManager(producer)
        return producer

    first, second = instance(), instance()
    original = ClientRouteManager.update_topic_route
    preserve_route_errors(first)
    with pytest.raises(ForbiddenException) as error:
        first._Client__route_manager.update_topic_route("logical")
    assert error.value is refusal
    # Actual upstream still swallows it on the untouched peer. No network,
    # global method replacement, event loop or extra executor is involved.
    assert second._Client__route_manager.update_topic_route("logical") is None
    assert ClientRouteManager.update_topic_route is original


def test_half_rejection_requires_structured_status_not_exception_text():
    import grpc
    from rocketmq.v5.exception.client_exception import ForbiddenException

    from reliable_messaging._rocketmq_transaction_driver import rejection_code

    assert rejection_code(ForbiddenException("synthetic refusal", 40300)) == "half_rejected_40300"
    for code, expected in (
        (grpc.StatusCode.PERMISSION_DENIED, "half_rejected_grpc_permission_denied"),
        (grpc.StatusCode.UNAUTHENTICATED, "half_rejected_grpc_unauthenticated"),
        (grpc.StatusCode.UNAVAILABLE, None),
    ):
        assert rejection_code(grpc.aio.AioRpcError(code, (), (), "synthetic")) == expected
    assert rejection_code(TimeoutError()) is None
    assert rejection_code(RuntimeError("PERMISSION_DENIED text is not proof")) is None
