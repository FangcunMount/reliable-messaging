"""Broker-neutral protocols and trusted logical/physical route bindings."""

import hashlib
import json
import re
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
READABLE_NAMING_V1 = "readable-v1"


def _readable_name(*fields: str) -> str:
    if any(
        not isinstance(v, str) or not re.fullmatch(r"[a-z0-9]+(?:[.-][a-z0-9]+)*", v)
        for v in fields
    ):
        raise ValueError("readable fields require lowercase single-separated tokens")
    name = "--".join(v.replace(".", "-") for v in fields)
    if len(name.encode()) > 64:
        raise ValueError("readable resource exceeds 64 bytes")
    return name


def physical_topic_v3(topic: str, message_type: str, owner: str, revision: str) -> str:
    if message_type not in (NORMAL_MESSAGE_TYPE, TRANSACTION_MESSAGE_TYPE):
        raise ValueError("NORMAL or TRANSACTION required")
    return _readable_name(
        topic, "normal" if message_type == NORMAL_MESSAGE_TYPE else "tx", owner, revision
    )


def consumer_group_v3(topic: str, subscription: str) -> str:
    return _readable_name(topic, subscription)


def expected_physical_topic(
    topic: str, message_type: str, owner: str, revision: str, scheme: str = ""
) -> str:
    if scheme == READABLE_NAMING_V1:
        return physical_topic_v3(topic, message_type, owner, revision)
    if scheme:
        raise ValueError("unknown route naming scheme")
    if message_type:
        return physical_topic_v2(topic, message_type, owner, revision)
    if owner or revision:
        raise ValueError("incomplete typed route")
    return physical_topic(topic)


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
    naming_scheme: str = ""

    def __post_init__(self) -> None:
        if not self.topic or "\0" in self.topic or "\0" in self.subscription:
            raise ValueError("logical topic/subscription required")
        expected_topic = expected_physical_topic(
            self.topic, self.message_type, self.owner, self.route_revision, self.naming_scheme
        )
        if self.naming_scheme == READABLE_NAMING_V1:
            expected = consumer_group_v3(self.topic, self.subscription) if self.subscription else ""
        elif self.message_type:
            expected = consumer_group_v2(self.topic, self.subscription) if self.subscription else ""
        else:
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
class RouteKey:
    topic: str
    message_type: str
    owner: str
    route_revision: str


def _key(route: RouteBinding) -> RouteKey:
    return RouteKey(route.topic, route.message_type, route.owner, route.route_revision)


class RouteResolver:
    """Historical evidence never grants publishing or subscription capabilities."""

    def __init__(
        self, active: tuple[RouteBinding, ...], historical: tuple[RouteBinding, ...] = ()
    ) -> None:
        validate_routes(active)
        if historical:
            validate_routes(historical)
        if {r.physical_topic for r in active} & {r.physical_topic for r in historical}:
            raise ValueError("active/historical resource overlap")
        groups: dict[str, tuple[str, str]] = {}
        logical_groups: dict[tuple[str, str], str] = {}
        for route in active:
            if route.consumer_group:
                identity = (route.topic, route.subscription)
                if (
                    logical_groups.setdefault(identity, route.consumer_group)
                    != route.consumer_group
                ):
                    raise ValueError("logical subscription mixes consumer group schemes")
        self._active = tuple(active)
        self._recorded: dict[str, RouteBinding] = {}
        for route in (*active, *historical):
            if route.physical_topic in self._recorded and _key(
                self._recorded[route.physical_topic]
            ) != _key(route):
                raise ValueError("physical route collision")
            if route.consumer_group:
                identity = (route.topic, route.subscription)
                if groups.setdefault(route.consumer_group, identity) != identity:
                    raise ValueError("consumer group collision")
            self._recorded[route.physical_topic] = route

    def publish_route(self, key: RouteKey) -> RouteBinding:
        for route in self._active:
            if _key(route) == key:
                return route
        raise ValueError("publishing route not in active manifest")

    def recorded_route(self, key: RouteKey, physical: str) -> RouteBinding:
        route = self._recorded.get(physical)
        if route is None or _key(route) != key:
            raise ValueError("recorded route not in trusted manifest")
        return route

    def subscription_routes(self, topic: str, subscription: str) -> tuple[RouteBinding, ...]:
        result = tuple(
            r
            for r in self._active
            if (r.topic, r.subscription) == (topic, subscription) and r.consumer_group
        )
        if not result:
            raise ValueError("subscription not in active manifest")
        return result


@dataclass(frozen=True)
class RouteManifest:
    version: int
    routes: tuple[RouteBinding, ...]
    historical_routes: tuple[RouteBinding, ...] = ()


def parse_route_manifest(raw: str | bytes) -> RouteManifest:
    if not raw or len(raw) > 1 << 20:
        raise ValueError("bounded route manifest required")
    value = json.loads(raw)
    version = value.get("version")
    if type(version) is not int or version not in (1, 2, 3):
        raise ValueError("unsupported route manifest")
    fields = set(RouteBinding.__dataclass_fields__)
    routes = tuple(
        RouteBinding(**{k: v for k, v in r.items() if k in fields}) for r in value.get("routes", [])
    )
    historical = tuple(
        RouteBinding(**{k: v for k, v in r.items() if k in fields})
        for r in value.get("historical_routes", [])
    )
    for route in routes:
        if (
            (version == 1 and (route.message_type or route.naming_scheme))
            or (version == 2 and (not route.message_type or route.naming_scheme))
            or (
                version == 3
                and (not route.message_type or route.naming_scheme != READABLE_NAMING_V1)
            )
        ):
            raise ValueError("route codec differs from manifest version")
    if (version != 3 and historical) or any(
        not r.message_type or r.naming_scheme for r in historical
    ):
        raise ValueError("historical typed hash routes require manifest v3")
    RouteResolver(routes, historical)
    return RouteManifest(version, routes, historical)


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
