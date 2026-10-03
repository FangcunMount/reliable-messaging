import asyncio
import json
import os
import urllib.request
import uuid

import pytest

from reliable_messaging.delivery import Confirmation, Outcome
from reliable_messaging.nsq import NSQPublisher, NSQSubscriber
from reliable_messaging.wire import Envelope, encode

pytestmark = pytest.mark.integration


@pytest.fixture
def endpoints():
    tcp, http = os.environ.get("RM_MQ_NSQ_TCP"), os.environ.get("RM_MQ_NSQ_HTTP")
    if not tcp or not http:
        pytest.fail("Required disposable NSQ TCP/HTTP endpoints are missing")
    return tcp, http


async def http_json(base, path, post=False):
    def request():
        req = urllib.request.Request(base + path, method="POST" if post else "GET")
        with urllib.request.urlopen(req, timeout=3) as reply:
            body = reply.read()
            return json.loads(body) if body else {}

    return await asyncio.to_thread(request)


async def prepare(base, topic, channel):
    await http_json(base, f"/topic/create?topic={topic}", True)
    await http_json(base, f"/channel/create?topic={topic}&channel={channel}", True)


async def test_real_nsq_commit_barrier_single_loop_and_shutdown(endpoints):
    tcp, http = endpoints
    topic = "rm.mq." + uuid.uuid4().hex[:12]
    channel = "durable"
    await prepare(http, topic, channel)
    entered, permit, completed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    observed = []
    loop = asyncio.get_running_loop()
    publisher = NSQPublisher(tcp)

    async def handle(received):
        assert asyncio.get_running_loop() is loop
        observed.append(received)
        entered.set()
        await permit.wait()  # the host commit barrier
        completed.set()

    async def durable_failure(body):
        raise AssertionError("unexpected failure handoff")

    async def quarantine(body, code):
        raise AssertionError("unexpected invalid wire")

    async def ready(address, failure_topic, failure_channel):
        assert address == tcp
        stats = await http_json(http, "/stats?format=json")
        matches = [
            c
            for t in stats["topics"]
            if t["topic_name"] == failure_topic
            for c in t["channels"]
            if c["channel_name"] == failure_channel
        ]
        assert matches and matches[0]["clients"]

    subscriber = NSQSubscriber(
        topic,
        channel,
        publishers={tcp: publisher},
        handler=handle,
        failed_handler=durable_failure,
        invalid_handler=quarantine,
        failure_ready=ready,
    )
    await prepare(http, subscriber.failure_topic, "cb-failed-handler")
    try:
        await publisher.start()
        await subscriber.start()
        result = await publisher.publish(topic, encode(Envelope("immutable-original", b"payload")))
        assert result.outcome == Outcome.CONFIRMED and result.confirmation == Confirmation.BROKER
        await asyncio.wait_for(entered.wait(), 5)
        stats = await http_json(http, "/stats?format=json")
        actual = next(
            c
            for t in stats["topics"]
            if t["topic_name"] == topic
            for c in t["channels"]
            if c["channel_name"] == channel
        )
        assert actual["in_flight_count"] == 1
        assert actual["clients"][0]["finish_count"] == 0
        permit.set()
        await asyncio.wait_for(completed.wait(), 5)
        readers = list(subscriber._readers)
        writer = publisher._client
        await subscriber.stop()
        await publisher.stop()
        await subscriber.wait()
        await publisher.wait()
        assert observed[0].envelope.message_id == "immutable-original"
        assert observed[0].source_address == tcp
        assert all(r.io_loop.closed and not r.io_loop.handles for r in readers)
        assert writer.io_loop.closed and not writer.io_loop.handles
        assert all(not r.owned_heartbeat.is_running() and not r.conns for r in readers)
        # Host loop and its shared HTTP client are still usable after stopping both adapters.
        assert asyncio.get_running_loop() is loop
        assert (await http_json(http, "/stats?format=json"))["health"] == "OK"
    finally:
        permit.set()
        await subscriber.stop(grace_seconds=0)
        await publisher.stop(grace_seconds=0)
