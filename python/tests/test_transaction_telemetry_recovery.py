"""Pinned heartbeat dispatch, instance isolation and shutdown; no Broker claim."""

import asyncio
import threading
from concurrent.futures import Future
from types import SimpleNamespace

import pytest
from rocketmq.grpc_protocol import Code
from rocketmq.v5.producer import Producer

from reliable_messaging._rocketmq_transaction_driver import RecoveryPool, create_producer


async def test_closed_telemetry_recovers_on_healthy_heartbeat_only_on_own_instance() -> None:
    loop = asyncio.get_running_loop()
    original = Producer._Client__heartbeat_callback
    clients = []
    pools = []
    try:
        for closed in (True, False):
            pool = RecoveryPool(1, 0)
            pools.append(pool)

            async def checker(message: object) -> int:
                raise AssertionError("heartbeat must never invoke the transaction Checker")

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
            client = SimpleNamespace(
                producer=producer, original_rpc=producer.rpc_client, closed=closed, registrations=0
            )
            clients.append(client)
            call = SimpleNamespace(done=lambda current=client: current.closed)
            channel = SimpleNamespace(
                telemetry_stream_stream_call=SimpleNamespace(
                    _RpcStreamStreamCall__stream_stream_call=call,
                    _rm_settings_enqueued=True,
                )
            )

            def settings_write(endpoints: object, request: object) -> Future[None]:
                done: Future[None] = Future()
                done.set_result(None)
                return done

            producer._Client__rpc_client = SimpleNamespace(
                channels=SimpleNamespace(get=lambda endpoints, current=channel: current),
                retrieve_or_create_channel=lambda endpoints, current=channel: current,
                get_channel_io_loop=client.original_rpc.get_channel_io_loop,
                telemetry_write_async=settings_write,
            )
            producer.sync_setting_req = lambda endpoints: None
            producer._Client__is_running = True

            def register(endpoints: object, current: SimpleNamespace = client) -> None:
                assert asyncio.get_running_loop() is current.producer._rpc_channel_io_loop()
                current.registrations += 1
                current.closed = False

            producer.on_new_endpoints = register

        async def heartbeat(client: SimpleNamespace, code: int = Code.OK) -> None:
            async def invoke() -> None:
                future: Future[object] = Future()
                future.set_result(
                    SimpleNamespace(
                        status=SimpleNamespace(code=code, message="synthetic heartbeat")
                    )
                )
                endpoint = SimpleNamespace(facade="fixture")
                client.producer.rpc_client.heartbeat_async = lambda *args, **kwargs: future
                client.producer._Client__heartbeat_async(endpoint)

            await asyncio.wrap_future(
                asyncio.run_coroutine_threadsafe(invoke(), client.producer._rpc_channel_io_loop())
            )

        first, second = clients
        threads = {thread.ident for thread in threading.enumerate()}
        await heartbeat(first)
        await heartbeat(first)
        await heartbeat(second)
        assert first.registrations == 1 and second.registrations == 0
        assert {thread.ident for thread in threading.enumerate()} == threads
        assert Producer._Client__heartbeat_callback is original
        assert all(pool.snapshot()["admitted"] == 0 for pool in pools)
        # A replacement stream may exist without any queued settings even
        # when the server still has a cached client record.
        stream = second.producer.rpc_client.channels.get(None).telemetry_stream_stream_call
        del stream._rm_settings_enqueued
        await heartbeat(second)
        assert second.producer._rm_settings.snapshot()["settings_writes"] == 1
        await heartbeat(second, Code.UNRECOGNIZED_CLIENT_TYPE)
        assert second.producer._rm_settings.snapshot()["settings_writes"] == 2
        assert second.registrations == 0  # Reuse the active stream; resend settings.

        first.closed = True
        await heartbeat(first, Code.UNRECOGNIZED_CLIENT_TYPE)
        assert first.registrations == 2
        first.closed = True
        await heartbeat(first, Code.FORBIDDEN)
        await heartbeat(first, Code.UNAUTHORIZED)
        assert first.registrations == 2
        first.producer._Client__is_running = False
        await heartbeat(first)
        assert first.registrations == 2
        failure: Future[object] = Future()
        failure.set_exception(ConnectionError("synthetic unavailable heartbeat"))
        second.producer._Client__heartbeat_callback(failure, object())
        assert second.registrations == 0
    finally:
        for client in clients:
            client.producer._Client__rpc_client = client.original_rpc
            client.producer._Client__is_running = False
            await asyncio.to_thread(client.producer.shutdown)
        for pool in pools:
            await pool.drain()


async def test_settings_sync_coalesces_until_actual_completion_and_close_waits() -> None:
    from reliable_messaging._rocketmq_transaction_driver import _SettingsSync

    writes = []
    callbacks = []
    manager = SimpleNamespace(
        _ClientTelemetryManager__setting_write_callback=lambda done, **kwargs: callbacks.append(
            done.result()
        )
    )

    def write(endpoints: object, request: object) -> Future[None]:
        future: Future[None] = Future()
        writes.append(future)
        return future

    stream = SimpleNamespace()
    client = SimpleNamespace(
        _Client__telemetry_manager=manager,
        rpc_client=SimpleNamespace(
            telemetry_write_async=write,
            channels=SimpleNamespace(
                get=lambda endpoints: SimpleNamespace(telemetry_stream_stream_call=stream)
            ),
        ),
        sync_setting_req=lambda endpoints: None,
    )
    state = _SettingsSync(client)
    endpoint = SimpleNamespace(facade="fixture")
    manager.setting_write(endpoint)
    for _ in range(1000):
        manager.setting_write(endpoint)
    assert len(writes) == 1 and state.snapshot()["pending_settings"] == 1
    cancelled = []
    state.started[endpoint.facade] -= 2
    assert state.busy(endpoint, 1, SimpleNamespace(cancel=lambda: cancelled.append(True)))
    assert cancelled == [True] and state.snapshot()["pending_settings"] == 1
    state.stop()
    manager.setting_write(endpoint)
    waiter = asyncio.create_task(asyncio.to_thread(state.wait))
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.02):
            await asyncio.shield(waiter)
    assert not waiter.done() and state.snapshot()["pending_settings"] == 1
    writes[0].set_result(None)
    await waiter
    assert state.snapshot()["pending_settings"] == 0 and not callbacks
    assert len(writes) == 1  # No restart or write after metadata admission closes.
