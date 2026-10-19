"""Broker-neutral protocols and trusted logical/physical route bindings."""

import hashlib
import struct
from dataclasses import dataclass
from typing import Protocol

from reliable_messaging.delivery import DeliveryResult
from reliable_messaging.wire import Envelope


def physical_topic(topic: str) -> str:
    return "rmq_t_" + hashlib.sha256(topic.encode()).hexdigest()[:32]


def consumer_group(topic: str, subscription: str) -> str:
    return "rmq_g_" + hashlib.sha256(f"{topic}\0{subscription}".encode()).hexdigest()[:32]


NORMAL_MESSAGE_TYPE = "NORMAL"
TRANSACTION_MESSAGE_TYPE = "TRANSACTION"


def _field_v2(value: str, limit: int) -> bytes:
    if not isinstance(value, str) or not value or "\0" in value:
        raise ValueError("explicit v2 route field required")
    encoded = value.encode()
    if len(encoded) > limit:
        raise ValueError("v2 route field exceeds UTF-8 byte limit")
    return encoded


def _digest_v2(domain: bytes, fields: tuple[bytes, ...]) -> str:
    return hashlib.sha256(
        domain + b"".join(struct.pack(">Q", len(v)) + v for v in fields)
    ).hexdigest()[:32]


def physical_topic_v2(topic: str, message_type: str, owner: str, revision: str) -> str:
    if message_type not in (NORMAL_MESSAGE_TYPE, TRANSACTION_MESSAGE_TYPE):
        raise ValueError("NORMAL or TRANSACTION route type required")
    fields = (
        _field_v2(topic, 255),
        message_type.encode(),
        _field_v2(owner, 128),
        _field_v2(revision, 128),
    )
    return "rmq2_t_" + _digest_v2(b"rmq-topic-v2\0", fields)


def consumer_group_v2(topic: str, subscription: str) -> str:
    return "rmq2_g_" + _digest_v2(
        b"rmq-group-v2\0", (_field_v2(topic, 255), _field_v2(subscription, 255))
    )


@dataclass(frozen=True)
class RouteBinding:
    topic: str
    subscription: str = ""
    physical_topic: str = ""
    consumer_group: str = ""
    message_type: str = ""
    owner: str = ""
    route_revision: str = ""

    def __post_init__(self) -> None:
        if not self.topic or "\0" in self.topic or "\0" in self.subscription:
            raise ValueError("logical topic/subscription required")
        if self.message_type:
            expected_topic = physical_topic_v2(
                self.topic, self.message_type, self.owner, self.route_revision
            )
            expected = consumer_group_v2(self.topic, self.subscription) if self.subscription else ""
        else:
            if self.owner or self.route_revision:
                raise ValueError("v2 type, owner and revision must be explicit together")
            expected_topic = physical_topic(self.topic)
            expected = consumer_group(self.topic, self.subscription) if self.subscription else ""
        if self.physical_topic != expected_topic:
            raise ValueError("noncanonical physical topic")
        if self.consumer_group != expected:
            raise ValueError("noncanonical consumer group")


def validate_routes(routes: tuple[RouteBinding, ...]) -> None:
    if not routes or len(
        {(r.topic, r.subscription, r.message_type, r.owner, r.route_revision) for r in routes}
    ) != len(routes):
        raise ValueError("explicit unique route bindings required")
    topics: dict[str, tuple[str, str, str, str]] = {}
    groups: dict[str, tuple[str, str]] = {}
    for route in routes:
        identity = (route.topic, route.message_type, route.owner, route.route_revision)
        if topics.setdefault(route.physical_topic, identity) != identity:
            raise ValueError("physical topic collision")
        if route.subscription:
            logical = (route.topic, route.subscription)
            if groups.setdefault(route.consumer_group, logical) != logical:
                raise ValueError("consumer group collision")


@dataclass(frozen=True)
class Received:
    envelope: Envelope
    wire: bytes
    transport_id: str
    source_address: str
    attempts: int
    timestamp: int
    provider: str = "nsq"
    physical_topic: str = ""
    logical_topic: str = ""


class Publisher(Protocol):
    async def start(self) -> None: ...
    async def publish(self, topic: str, body: bytes) -> DeliveryResult: ...
    async def wait(self) -> None: ...
    async def stop(self, *, grace_seconds: float = 10.0) -> None: ...


class Subscriber(Protocol):
    async def start(self) -> None: ...
    async def wait(self) -> None: ...
    async def stop(self, *, grace_seconds: float = 10.0) -> None: ...
