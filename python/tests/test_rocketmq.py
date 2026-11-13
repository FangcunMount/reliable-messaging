import asyncio
import threading
from dataclasses import replace
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from grpc import RpcError, StatusCode  # type: ignore[import-untyped]
from rocketmq.v5.model import Message as DriverMessage  # type: ignore[import-untyped]

from reliable_messaging.delivery import Confirmation, Outcome
from reliable_messaging.rocketmq import (
    Config,
    RocketMQPublisher,
    RocketMQSubscriber,
    _consumer_session_lost,
    _ConsumerRouteUnavailable,
    _require_server_settings,
)
from reliable_messaging.transport import RouteBinding, consumer_group, physical_topic
from reliable_messaging.wire import Envelope, encode


class Unavailable(RpcError):  # type: ignore[misc]
    def code(self) -> StatusCode:
        return StatusCode.UNAVAILABLE


class DeadlineExceeded(RpcError):  # type: ignore[misc]
    def code(self) -> StatusCode:
        return StatusCode.DEADLINE_EXCEEDED


def configuration() -> Config:
    topic, subscription = "qs.ai.events.v1", "qs-server.ai-events.v1"
    route = RouteBinding(
        topic, subscription, physical_topic(topic), consumer_group(topic, subscription)
    )
    return Config("localhost:8081", "test", "test", (route,), timeout=0.02, max_in_flight=1)


def test_routes_fail_closed() -> None:
    config = configuration()
    route = config.routes[0]
    with pytest.raises(ValueError):
        RouteBinding(route.topic, route.subscription, route.topic, route.consumer_group)
    with pytest.raises(ValueError):
        Config("localhost:8081", "test", "test", (route, route))
    publisher = RocketMQPublisher(config)
    assert publisher._driver.executor is None
    assert publisher._driver.client is None


def test_startup_requires_real_server_settings_before_ready() -> None:
    handshake = threading.Event()
    client = SimpleNamespace(_init_settings_event=handshake)
    with pytest.raises(TimeoutError):
        _require_server_settings(client, 0.001)
    handshake.set()
    _require_server_settings(client, 0.001)


async def test_publish_keeps_wire_and_requires_receipt() -> None:
    DriverMessage()  # import/initialize the pinned driver outside the call budget
    publisher = RocketMQPublisher(configuration())
    publisher._driver.begin()
    publisher._publishing = asyncio.Semaphore(1)
    bodies: list[bytes] = []

    def send(message: object) -> object:
        assert message.topic == physical_topic("qs.ai.events.v1")  # type: ignore[attr-defined]
        bodies.append(message.body)  # type: ignore[attr-defined]
        return SimpleNamespace(message_id="broker-physical-id")

    publisher._driver.client = SimpleNamespace(send=send, shutdown=lambda: None)
    wire = encode(Envelope("original-id", b"encrypted bytes"))
    result = await publisher.publish("qs.ai.events.v1", wire)
    assert result.outcome == Outcome.CONFIRMED
    assert result.confirmation == Confirmation.BROKER
    assert bodies == [wire]
    assert (await publisher.publish("untrusted", wire)).outcome == Outcome.REJECTED
    publisher._driver.client.send = lambda message: None
    assert (await publisher.publish("qs.ai.events.v1", wire)).outcome == Outcome.UNKNOWN
    await publisher.stop()


async def test_timeout_retains_actual_driver_capacity_and_shutdown() -> None:
    DriverMessage()  # import/initialize the pinned driver outside the call budget
    publisher = RocketMQPublisher(configuration())
    publisher._driver.begin()
    publisher._publishing = asyncio.Semaphore(1)
    entered, finish = threading.Event(), threading.Event()

    def send(message: object) -> object:
        entered.set()
        finish.wait(5)
        return SimpleNamespace(message_id="late-confirmation")

    publisher._driver.client = SimpleNamespace(send=send, shutdown=lambda: None)
    try:
        result = await publisher.publish("qs.ai.events.v1", b"wire")
        assert entered.is_set()
        assert result.outcome == Outcome.UNKNOWN
        assert len(publisher._driver.pending) == 1
        assert publisher._driver.gate is not None and publisher._driver.gate.locked()
        with pytest.raises(TimeoutError):
            await publisher.stop(grace_seconds=0.01)
        assert publisher._driver.client is not None
    finally:
        finish.set()
        await publisher.stop()


async def test_durable_failure_precedes_ack_and_retains_provider() -> None:
    config = configuration()
    order: list[str] = []

    async def failed(body: bytes) -> None:
        import json

        value = json.loads(body)
        assert value["provider"] == "rocketmq"
        assert value["uuid"] == "original-id"
        assert value["topic"] == config.routes[0].topic
        order.append("durable_failure")

    def ack(raw: object) -> None:
        order.append("ack")

    subscriber = RocketMQSubscriber(
        config,
        config.routes[0].topic,
        config.routes[0].subscription,
        handler=AsyncMock(side_effect=ValueError("business rejected")),
        failed_handler=failed,
        invalid_handler=AsyncMock(),
    )
    subscriber._driver.begin()
    subscriber._driver.client = SimpleNamespace(
        ack=ack, change_invisible_duration=lambda *args: None, shutdown=lambda: None
    )
    raw = SimpleNamespace(
        topic=config.routes[0].physical_topic,
        corrupted=False,
        body=encode(Envelope("original-id", b"protected payload")),
        delivery_attempt=8,
        born_timestamp=123,
        message_id="physical-id",
    )
    await subscriber._handle(raw)
    assert order == ["durable_failure", "ack"]
    await subscriber._driver.close(1)


async def test_failed_audit_cannot_ack() -> None:
    config = configuration()
    acknowledged: list[object] = []
    subscriber = RocketMQSubscriber(
        config,
        config.routes[0].topic,
        config.routes[0].subscription,
        handler=AsyncMock(side_effect=ValueError()),
        failed_handler=AsyncMock(side_effect=OSError("database unavailable")),
        invalid_handler=AsyncMock(),
    )
    subscriber._driver.begin()
    subscriber._driver.client = SimpleNamespace(
        ack=acknowledged.append,
        change_invisible_duration=lambda *args: None,
        shutdown=lambda: None,
    )
    raw = SimpleNamespace(
        topic=config.routes[0].physical_topic,
        corrupted=False,
        body=encode(Envelope("original-id", b"body")),
        delivery_attempt=9,
        born_timestamp=123,
        message_id="physical-id",
    )
    await subscriber._handle(raw)
    assert not acknowledged
    await subscriber._driver.close(1)


@pytest.mark.parametrize("rpc_error", [ConnectionError, DeadlineExceeded])
async def test_receive_failure_reaches_host_supervision_with_cause(
    rpc_error: type[Exception],
) -> None:
    config = configuration()
    subscriber = RocketMQSubscriber(
        config,
        config.routes[0].topic,
        config.routes[0].subscription,
        handler=AsyncMock(),
        failed_handler=AsyncMock(),
        invalid_handler=AsyncMock(),
    )
    subscriber._driver.begin()
    subscriber._slots = asyncio.Semaphore(1)
    subscriber._failure = asyncio.get_running_loop().create_future()

    def receive(*args: object) -> None:
        raise rpc_error("disposable broker disconnected")

    subscriber._driver.client = SimpleNamespace(receive=receive, shutdown=lambda: None)
    await subscriber._poll()
    with pytest.raises(RuntimeError) as failure:
        await subscriber.wait()
    assert isinstance(failure.value.__cause__, rpc_error)
    await subscriber._driver.close(1)


async def test_receive_reregisters_after_proxy_session_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rocketmq.grpc_protocol import Code  # type: ignore[import-untyped]
    from rocketmq.v5.exception import InternalErrorException  # type: ignore[import-untyped]

    lost_settings = InternalErrorException(
        'Cannot invoke "apache.rocketmq.v2.Settings.getClientType()" because "settings" is null',
        Code.INTERNAL_SERVER_ERROR,
    )
    assert _consumer_session_lost(lost_settings)
    other_error = InternalErrorException("storage fault", Code.INTERNAL_SERVER_ERROR)
    assert not _consumer_session_lost(other_error)
    config = configuration()
    route = config.routes[0]
    acknowledged = threading.Event()
    calls = []

    def disconnect(*args: object) -> None:
        raise Unavailable()

    raw = SimpleNamespace(
        topic=route.physical_topic,
        corrupted=False,
        body=encode(Envelope("preserved-id", b"body")),
        delivery_attempt=2,
        born_timestamp=123,
        message_id="original-physical-id",
    )
    replacement = SimpleNamespace(
        receive=lambda *_: [raw] if not acknowledged.is_set() else [],
        ack=lambda message: (calls.append(("ack", message)), acknowledged.set()),
        shutdown=lambda: calls.append("new_shutdown"),
    )
    first = SimpleNamespace(receive=disconnect, shutdown=lambda: calls.append("old_shutdown"))
    handler = AsyncMock()
    subscriber = RocketMQSubscriber(
        config,
        route.topic,
        route.subscription,
        handler=handler,
        failed_handler=AsyncMock(),
        invalid_handler=AsyncMock(),
    )
    clients = iter((first, None, replacement))

    def register() -> None:
        client = next(clients)
        if client is None:
            calls.append("route_unavailable")
            raise _ConsumerRouteUnavailable("route RPC unavailable during restart")
        subscriber._driver.client = client
        calls.append("registered")

    monkeypatch.setattr(subscriber, "_start_consumer", register)
    await subscriber.start()
    supervision = asyncio.create_task(subscriber.wait())
    try:
        assert await asyncio.to_thread(acknowledged.wait, 2)
        assert not supervision.done()
        assert calls == [
            "registered",
            "old_shutdown",
            "route_unavailable",
            "registered",
            ("ack", raw),
        ]
        assert handler.await_count == 1
        assert handler.await_args.args[0].transport_id == "original-physical-id"
    finally:
        await subscriber.stop()
        with pytest.raises(RuntimeError):
            await supervision
    assert calls[-1] == "new_shutdown"


@pytest.mark.parametrize("rpc_error", [Unavailable, DeadlineExceeded])
@pytest.mark.parametrize(
    "disconnect_during,concurrent_poll",
    [("ack", False), ("renew", False), ("retry", False), ("ack", True)],
)
async def test_in_flight_session_loss_redelivers_with_current_receipt(
    monkeypatch: pytest.MonkeyPatch,
    disconnect_during: str,
    concurrent_poll: bool,
    rpc_error: type[RpcError],
) -> None:
    config = replace(configuration(), max_in_flight=2) if concurrent_poll else configuration()
    route = config.routes[0]
    replacement_ack = threading.Event()
    pending_receive = threading.Event()
    lost_session = threading.Event()
    order: list[tuple[str, object]] = []
    old, current = (
        SimpleNamespace(
            topic=route.physical_topic,
            corrupted=False,
            body=encode(Envelope("preserved-id", b"original")),
            delivery_attempt=attempt,
            born_timestamp=123,
            message_id="same-broker-message",
        )
        for attempt in (1, 2)
    )
    committed: set[bytes] = set()
    attempts: list[int] = []

    async def handle(delivery: Any) -> None:
        attempts.append(delivery.attempts)
        if delivery.attempts == 1:
            if concurrent_poll:
                assert await asyncio.to_thread(pending_receive.wait, 2)
            if disconnect_during == "renew":
                await asyncio.Event().wait()
            if disconnect_during == "retry":
                raise ValueError("host requested redelivery")
        committed.add(delivery.wire)

    def old_ack(raw: object) -> None:
        assert disconnect_during == "ack"
        order.append(("old-ack-lost", raw))
        raise rpc_error()

    def old_visibility(raw: object, duration: int) -> None:
        assert disconnect_during in ("renew", "retry")
        order.append(("old-visibility-lost", raw))
        raise rpc_error()

    def new_ack(raw: object) -> None:
        order.append(("new-ack", raw))
        replacement_ack.set()

    def old_poll_timeout() -> list[Any]:
        pending_receive.set()
        assert lost_session.wait(2)
        raise DeadlineExceeded()

    first_batches = iter(([old], old_poll_timeout))

    def old_receive(*args: object) -> list[Any]:
        batch = next(first_batches)
        return batch() if callable(batch) else batch

    replacement_batches = iter(([current],))
    first = SimpleNamespace(
        receive=old_receive,
        ack=old_ack,
        change_invisible_duration=old_visibility,
        shutdown=lambda: order.append(("old-shutdown", None)),
    )
    second = SimpleNamespace(
        receive=lambda *_: next(replacement_batches, []),
        ack=new_ack,
        shutdown=lambda: order.append(("new-shutdown", None)),
    )
    subscriber = RocketMQSubscriber(
        config,
        route.topic,
        route.subscription,
        handler=handle,
        failed_handler=AsyncMock(),
        invalid_handler=AsyncMock(),
    )
    # Fast virtual lease for the focused renew test. Public validation is unchanged.
    subscriber.invisible_duration = 0.03
    clients = iter((first, second))

    def register() -> None:
        subscriber._driver.client = next(clients)

    monkeypatch.setattr(subscriber, "_start_consumer", register)
    if concurrent_poll:
        record_loss = subscriber._record_session_loss

        def record_and_release(error: Exception, *, receipt_operation: bool = False) -> bool:
            result = record_loss(error, receipt_operation=receipt_operation)
            if result and receipt_operation:
                lost_session.set()
            return result

        monkeypatch.setattr(subscriber, "_record_session_loss", record_and_release)
    await subscriber.start()
    supervision = asyncio.create_task(subscriber.wait())
    try:
        assert await asyncio.to_thread(replacement_ack.wait, 3)
        assert not supervision.done()
        expected = "old-ack-lost" if disconnect_during == "ack" else "old-visibility-lost"
        assert order == [(expected, old), ("old-shutdown", None), ("new-ack", current)]
        assert attempts == [1, 2]
        assert committed == {old.body}
    finally:
        await subscriber.stop()
        with pytest.raises(RuntimeError):
            await supervision


async def test_repeated_receive_failure_spends_one_recovery_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = configuration()
    route = config.routes[0]
    starts = 0
    shutdowns = 0

    def receive(*args: object) -> None:
        raise Unavailable()

    def shutdown() -> None:
        nonlocal shutdowns
        shutdowns += 1

    subscriber = RocketMQSubscriber(
        config,
        route.topic,
        route.subscription,
        handler=AsyncMock(),
        failed_handler=AsyncMock(),
        invalid_handler=AsyncMock(),
    )

    def register() -> None:
        nonlocal starts
        starts += 1
        subscriber._driver.client = SimpleNamespace(receive=receive, shutdown=shutdown)

    monkeypatch.setattr("reliable_messaging.rocketmq._RECOVERY_DELAYS", (0, 0))
    monkeypatch.setattr(subscriber, "_start_consumer", register)
    await subscriber.start()
    try:
        with pytest.raises(RuntimeError) as reported:
            await asyncio.wait_for(subscriber.wait(), 2)
        exhausted = reported.value.__cause__
        assert exhausted is not None and "session recovery exhausted" in str(exhausted)
        assert isinstance(exhausted.__cause__, Unavailable)
        assert starts == shutdowns == 3
    finally:
        await subscriber.stop()


async def test_repeated_ack_failure_spends_budget_even_with_successful_receives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = configuration()
    route = config.routes[0]
    starts = 0
    attempts: list[int] = []

    def register() -> None:
        nonlocal starts
        starts += 1
        attempt = starts
        raw = SimpleNamespace(
            topic=route.physical_topic,
            corrupted=False,
            body=encode(Envelope("preserved-id", b"original")),
            delivery_attempt=attempt,
            born_timestamp=123,
            message_id="same-broker-message",
        )
        batches = iter(([], [raw]))

        def ack(message: Any) -> None:
            assert message is raw
            attempts.append(attempt)
            raise Unavailable()

        subscriber._driver.client = SimpleNamespace(
            receive=lambda *_: next(batches), ack=ack, shutdown=lambda: None
        )

    subscriber = RocketMQSubscriber(
        config,
        route.topic,
        route.subscription,
        handler=AsyncMock(),
        failed_handler=AsyncMock(),
        invalid_handler=AsyncMock(),
    )
    monkeypatch.setattr("reliable_messaging.rocketmq._RECOVERY_DELAYS", (0, 0))
    monkeypatch.setattr(subscriber, "_start_consumer", register)
    await subscriber.start()
    try:
        with pytest.raises(RuntimeError) as reported:
            await asyncio.wait_for(subscriber.wait(), 0.2)
        exhausted = reported.value.__cause__
        assert exhausted is not None and "session recovery exhausted" in str(exhausted)
        assert isinstance(exhausted.__cause__, Unavailable)
        assert starts == 3 and attempts == [1, 2, 3]
    finally:
        await subscriber.stop()


async def test_handler_budget_cannot_overlap_ack_with_visibility_change() -> None:
    config = configuration()
    route = config.routes[0]
    entered, release = threading.Event(), threading.Event()
    visibility_calls: list[object] = []

    def ack(message: object) -> None:
        entered.set()
        assert release.wait(5)

    subscriber = RocketMQSubscriber(
        config,
        route.topic,
        route.subscription,
        handler=AsyncMock(),
        failed_handler=AsyncMock(),
        invalid_handler=AsyncMock(),
        handler_timeout=0.15,
    )
    subscriber._driver.begin()
    subscriber._driver.client = SimpleNamespace(
        ack=ack,
        change_invisible_duration=lambda raw, duration: visibility_calls.append(raw),
        shutdown=lambda: None,
    )
    raw = SimpleNamespace(
        topic=route.physical_topic,
        corrupted=False,
        body=encode(Envelope("preserved-id", b"original")),
        delivery_attempt=1,
        born_timestamp=123,
        message_id="same-broker-message",
    )
    task = asyncio.create_task(subscriber._handle(raw))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(task), 0.2)
        assert not visibility_calls
    finally:
        release.set()
        await task
        await subscriber.stop()
    assert not visibility_calls


async def test_stop_retry_does_not_shutdown_same_client_twice() -> None:
    config = configuration()
    route = config.routes[0]
    entered, release = threading.Event(), threading.Event()
    calls = 0

    def shutdown() -> None:
        nonlocal calls
        calls += 1
        entered.set()
        assert release.wait(5)

    subscriber = RocketMQSubscriber(
        config,
        route.topic,
        route.subscription,
        handler=AsyncMock(),
        failed_handler=AsyncMock(),
        invalid_handler=AsyncMock(),
    )
    subscriber._driver.begin()
    subscriber._driver.client = SimpleNamespace(shutdown=shutdown)
    try:
        with pytest.raises(TimeoutError):
            await subscriber.stop(grace_seconds=0.05)
        assert entered.is_set()
    finally:
        release.set()
        await subscriber.stop()
    assert calls == 1


async def test_receiver_cannot_create_more_handlers_than_reserved_slots() -> None:
    called = 0

    async def handler(*args: Any) -> None:
        nonlocal called
        called += 1

    subscriber = RocketMQSubscriber(
        configuration(),
        "qs.ai.events.v1",
        "qs-server.ai-events.v1",
        handler=handler,
        failed_handler=handler,
        invalid_handler=handler,
    )
    subscriber._slots = asyncio.Semaphore(1)
    subscriber._failure = asyncio.get_running_loop().create_future()
    subscriber._driver.client = SimpleNamespace(receive=lambda *args: None)

    async def too_many(*args: Any) -> list[Any]:
        return [SimpleNamespace(), SimpleNamespace()]

    subscriber._driver.call = too_many  # type: ignore[method-assign]
    async with asyncio.timeout(0.2):
        await subscriber._poll()
    assert isinstance(subscriber._failure_error, RuntimeError)
    assert called == 0 and not subscriber._tasks
    assert not subscriber._slots.locked()
