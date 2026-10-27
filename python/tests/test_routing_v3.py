import json
from dataclasses import replace
from pathlib import Path

import pytest

from reliable_messaging.transport import (
    RouteBinding,
    RouteKey,
    RouteResolver,
    consumer_group_v2,
    consumer_group_v3,
    parse_route_manifest,
    physical_topic_v2,
    physical_topic_v3,
    validate_routes,
)


def vectors():
    value = json.loads(
        (Path(__file__).resolve().parents[2] / "contracts/rocketmq-routing-v3.json").read_text()
    )
    assert value["version"] == 3 and len(value["vectors"]) == 12
    return tuple(RouteBinding(**r) for r in value["vectors"])


def test_readable_shared_vectors_and_readonly_history():
    routes = vectors()
    validate_routes(routes)
    old = replace(
        routes[0],
        naming_scheme="",
        physical_topic=physical_topic_v2(routes[0].topic, "TRANSACTION", "iam.mysql", "m3-1"),
        consumer_group=consumer_group_v2(routes[0].topic, routes[0].subscription),
    )
    resolver = RouteResolver(routes, (old,))
    key = RouteKey(old.topic, old.message_type, old.owner, old.route_revision)
    assert resolver.publish_route(key).physical_topic != old.physical_topic
    assert resolver.recorded_route(key, old.physical_topic) == old
    assert len(resolver.subscription_routes("qs.evaluation.lifecycle", "qs-worker")) == 2
    with pytest.raises(ValueError):
        resolver.recorded_route(key, "unreviewed")
    with pytest.raises(ValueError):
        RouteResolver(routes, (routes[0],))
    raw = {
        "version": 3,
        "routes": [r.__dict__ for r in routes],
        "historical_routes": [old.__dict__],
    }
    assert parse_route_manifest(json.dumps(raw)).routes == routes
    raw["version"] = 2
    with pytest.raises(ValueError):
        parse_route_manifest(json.dumps(raw))


@pytest.mark.parametrize(
    "field", ["", "Upper", "a--b", "a..b", "a.-b", "a_b", "a b", "中文", "a\0b"]
)
def test_readable_invalid_inputs(field):
    with pytest.raises(ValueError):
        physical_topic_v3(field, "NORMAL", "qs.mysql", "m3-1")


def test_normalization_collisions_and_limits():
    routes = vectors()
    for alias in (
        replace(routes[0], topic=routes[0].topic.replace(".", "-")),
        replace(routes[0], subscription=routes[0].subscription.replace(".", "-")),
    ):
        with pytest.raises(ValueError):
            validate_routes((*routes, alias))
    with pytest.raises(ValueError):
        consumer_group_v3("a" * 40, "b" * 24)
    with pytest.raises(ValueError):
        physical_topic_v3("topic", "FIFO", "qs.mysql", "m3-1")
