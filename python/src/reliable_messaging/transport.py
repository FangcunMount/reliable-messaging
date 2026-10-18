"""Broker-neutral protocols and trusted logical/physical route bindings."""

import hashlib
from dataclasses import dataclass
from typing import Protocol

from reliable_messaging.delivery import DeliveryResult
from reliable_messaging.wire import Envelope


def physical_topic(topic: str) -> str:
    return "rmq_t_" + hashlib.sha256(topic.encode()).hexdigest()[:32]


def consumer_group(topic: str, subscription: str) -> str:
    return "rmq_g_" + hashlib.sha256(f"{topic}\0{subscription}".encode()).hexdigest()[:32]


@dataclass(frozen=True)
class RouteBinding:
    topic: str
    subscription: str = ""
    physical_topic: str = ""
    consumer_group: str = ""

    def __post_init__(self) -> None:
        if not self.topic or "\0" in self.topic or "\0" in self.subscription:
            raise ValueError("logical topic/subscription required")
        if self.physical_topic != physical_topic(self.topic):
            raise ValueError("noncanonical physical topic")
        expected = consumer_group(self.topic, self.subscription) if self.subscription else ""
        if self.consumer_group != expected:
            raise ValueError("noncanonical consumer group")


def validate_routes(routes: tuple[RouteBinding, ...]) -> None:
    if not routes or len({(r.topic, r.subscription) for r in routes}) != len(routes):
        raise ValueError("explicit unique route bindings required")
    topics: dict[str, str] = {}
    groups: dict[str, tuple[str, str]] = {}
    for route in routes:
        if topics.setdefault(route.physical_topic, route.topic) != route.topic:
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
