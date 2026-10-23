"""Pinned heartbeat dispatch, instance isolation and shutdown; no Broker claim."""

import asyncio
import threading
from concurrent.futures import Future
from types import SimpleNamespace

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
                    _RpcStreamStreamCall__stream_stream_call=call
                )
            )
            producer._Client__rpc_client = SimpleNamespace(
                retrieve_or_create_channel=lambda endpoints, current=channel: current,
                get_channel_io_loop=client.original_rpc.get_channel_io_loop,
            )
            producer._Client__is_running = True

            def register(endpoints: object, current: SimpleNamespace = client) -> None:
                assert asyncio.get_running_loop() is current.producer._rpc_channel_io_loop()
                current.registrations += 1
                current.closed = False

            producer.on_new_endpoints = register

        async def heartbeat(client: SimpleNamespace, code: int = Code.OK) -> None:
            async def invoke() -> None:
                future: Future[object] = Future()
                future.set_result(SimpleNamespace(status=SimpleNamespace(code=code)))
                client.producer._Client__heartbeat_callback(future, object())

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

        first.closed = True
        await heartbeat(first, Code.FORBIDDEN)
        assert first.registrations == 1
        first.producer._Client__is_running = False
        await heartbeat(first)
        assert first.registrations == 1
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
