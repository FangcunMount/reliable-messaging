import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from rocketmq.v5.model import Message as DriverMessage  # type: ignore[import-untyped]

from reliable_messaging.delivery import Confirmation, Outcome
from reliable_messaging.rocketmq import (
    Config,
    RocketMQPublisher,
    RocketMQSubscriber,
    _require_server_settings,
)
from reliable_messaging.transport import RouteBinding, consumer_group, physical_topic
from reliable_messaging.wire import Envelope, encode


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


async def test_receive_failure_reaches_host_supervision_with_cause() -> None:
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
        raise ConnectionError("disposable broker disconnected")

    subscriber._driver.client = SimpleNamespace(receive=receive, shutdown=lambda: None)
    await subscriber._poll()
    with pytest.raises(RuntimeError) as failure:
        await subscriber.wait()
    assert isinstance(failure.value.__cause__, ConnectionError)
    await subscriber._driver.close(1)
