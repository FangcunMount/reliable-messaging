"""Disposable interoperability probe; requires preprovisioned infra test routes."""

import argparse
import asyncio
import json
import os
from pathlib import Path

from reliable_messaging.delivery import Outcome
from reliable_messaging.rocketmq import Config, RocketMQPublisher, RocketMQSubscriber
from reliable_messaging.transport import Received, RouteBinding, consumer_group, physical_topic
from reliable_messaging.wire import Envelope, encode


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("publish", "consume"))
    parser.add_argument("--id", default="python-probe")
    args = parser.parse_args()
    credentials = json.loads(
        await asyncio.to_thread(Path(os.environ["ROCKETMQ_CREDENTIALS_FILE"]).read_text)
    )
    account = credentials[os.environ["ROCKETMQ_ACCOUNT"]]
    topic, subscription = "m1.interop", "m1-python"
    route = RouteBinding(
        topic, subscription, physical_topic(topic), consumer_group(topic, subscription)
    )
    config = Config("127.0.0.1:8081", account["username"], account["password"], (route,))
    if args.mode == "publish":
        publisher = RocketMQPublisher(config)
        await publisher.start()
        try:
            wire = encode(Envelope(args.id, b"immutable interop payload", {"from": "python"}))
            result = await publisher.publish(topic, wire)
            if result.outcome != Outcome.CONFIRMED:
                raise RuntimeError(f"publish outcome {result.outcome}") from result.error
            print("broker_confirmed", args.id)
        finally:
            await publisher.stop(grace_seconds=15)
        return

    done = asyncio.Event()

    async def handler(received: Received) -> None:
        assert received.logical_topic == topic
        assert received.physical_topic == route.physical_topic
        assert received.envelope.payload == b"immutable interop payload"
        print("received", received.envelope.message_id)
        if received.envelope.message_id == args.id:
            done.set()

    async def failed(body: bytes) -> None:
        raise RuntimeError("probe cannot durably accept failures")

    async def invalid(body: bytes, cause: str) -> None:
        raise RuntimeError("invalid interop wire")

    subscriber = RocketMQSubscriber(
        config,
        topic,
        subscription,
        handler=handler,
        failed_handler=failed,
        invalid_handler=invalid,
    )
    await subscriber.start()
    waiter = asyncio.create_task(subscriber.wait())
    try:
        ready = asyncio.create_task(done.wait())
        completed, _ = await asyncio.wait(
            (ready, waiter), timeout=30, return_when=asyncio.FIRST_COMPLETED
        )
        if ready not in completed:
            if waiter in completed:
                await waiter
            raise TimeoutError("no interop delivery")
    finally:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        await subscriber.stop(grace_seconds=15)
    print("acknowledged_after_handler_commit")


if __name__ == "__main__":
    asyncio.run(main())
