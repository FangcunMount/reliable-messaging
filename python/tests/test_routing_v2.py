import json
from dataclasses import replace
from pathlib import Path

import pytest

from reliable_messaging.rocketmq import Config, RocketMQPublisher, RocketMQSubscriber
from reliable_messaging.transport import (
    RouteBinding,
    consumer_group_v2,
    physical_topic_v2,
    validate_routes,
)


def routes():
    value = json.loads(
        (Path(__file__).resolve().parents[2] / "contracts/rocketmq-routing-v2.json").read_text()
    )
    assert value["version"] == 2 and len(value["vectors"]) == 6
    return tuple(RouteBinding(**v) for v in value["vectors"])


def test_shared_go_python_canonical_vectors():
    values = routes()
    validate_routes(values)
    assert values[0].physical_topic != values[1].physical_topic != values[2].physical_topic
    assert values[0].consumer_group == values[1].consumer_group == values[2].consumer_group
    with pytest.raises(ValueError):
        replace(values[1], owner="iam.mysql")
    with pytest.raises(ValueError):
        physical_topic_v2("topic", "FIFO", "qs.mysql", "r1")
    with pytest.raises(ValueError):
        physical_topic_v2("topic\0suffix", "TRANSACTION", "qs.mysql", "r1")
    with pytest.raises(ValueError):
        consumer_group_v2("topic", "")


def test_typed_publisher_and_multiple_physical_inputs_share_one_subscriber():
    values = routes()[:3]
    config = Config("127.0.0.1:1", "fixture", "fixture", values, max_in_flight=2)
    publisher = RocketMQPublisher(config)
    assert publisher.routes == {"qs.events": values[0].physical_topic}
    with pytest.raises(ValueError):
        RocketMQPublisher(Config("127.0.0.1:1", "fixture", "fixture", (values[1],)))

    async def handler(*args):
        pass

    subscriber = RocketMQSubscriber(
        config,
        "qs.events",
        "worker",
        handler=handler,
        failed_handler=handler,
        invalid_handler=handler,
    )
    assert len(subscriber._physical_routes) == 3
    assert subscriber._slots is None and subscriber._driver.executor is None
