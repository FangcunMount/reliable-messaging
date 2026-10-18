"""RocketMQ ordinary-message adapter. All I/O begins at explicit start().

The pinned upstream client owns one process-wide daemon gRPC I/O loop plus
per-client schedulers. The adapter creates no event loops, uses a bounded executor
for blocking driver calls, closes its clients and never stops another client's loop.
"""

import asyncio
import base64
import functools
import math
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from reliable_messaging.delivery import Confirmation, DeliveryResult, Outcome
from reliable_messaging.transport import Received, RouteBinding, validate_routes
from reliable_messaging.wire import MAX_METADATA_BYTES, decode, go_json


@dataclass(frozen=True)
class Config:
    endpoint: str
    access_key: str
    secret_key: str = field(repr=False)
    routes: tuple[RouteBinding, ...]
    timeout: float = 5.0
    max_in_flight: int = 16

    def __post_init__(self) -> None:
        host, separator, port = self.endpoint.rpartition(":")
        if not host or not separator or not port.isdigit() or not 1 <= int(port) <= 65535:
            raise ValueError("explicit host:port endpoint required")
        if not self.access_key or not self.secret_key or self.timeout <= 0:
            raise ValueError("credentials and bounded timeout required")
        if not 1 <= self.max_in_flight <= 1000:
            raise ValueError("in-flight limit must be1..1000")
        object.__setattr__(self, "routes", tuple(self.routes))
        validate_routes(self.routes)


class _Driver:
    def __init__(self, config: Config, lifecycle_slots: int = 0) -> None:
        self.config = config
        self.lifecycle_slots = lifecycle_slots
        self.client: Any = None
        self.executor: ThreadPoolExecutor | None = None
        self.gate: asyncio.Semaphore | None = None
        self.pending: set[asyncio.Future[Any]] = set()
        self.closing = False
        self.stopped: asyncio.Event | None = None

    def begin(self) -> None:
        if self.executor is not None or self.closing:
            raise RuntimeError("client already started or stopping")
        self.executor = ThreadPoolExecutor(
            max_workers=self.config.max_in_flight + self.lifecycle_slots,
            thread_name_prefix="rm-rocketmq",
        )
        self.gate = asyncio.Semaphore(self.config.max_in_flight + self.lifecycle_slots)
        self.stopped = asyncio.Event()

    async def call(self, function: Callable[..., Any], *args: Any) -> Any:
        if self.executor is None or self.gate is None:
            raise RuntimeError("client not started")
        await self.gate.acquire()
        try:
            future = asyncio.get_running_loop().run_in_executor(
                self.executor, functools.partial(function, *args)
            )
        except BaseException:
            self.gate.release()
            raise
        self.pending.add(future)
        gate = self.gate

        def complete(done: asyncio.Future[Any]) -> None:
            self.pending.discard(done)
            gate.release()
            if not done.cancelled():
                done.exception()  # consume an error even after caller cancellation

        future.add_done_callback(complete)
        # Cancellation must not free capacity before a blocking driver call exits.
        return await asyncio.shield(future)

    async def close(self, grace_seconds: float) -> None:
        self.closing = True
        if grace_seconds < 0:
            raise ValueError("nonnegative shutdown budget required")
        async with asyncio.timeout(grace_seconds):
            if self.pending:
                await asyncio.shield(asyncio.gather(*self.pending, return_exceptions=True))
            if self.client is not None:
                await self.call(self.client.shutdown)
                self.client = None
            if self.executor is not None:
                self.executor.shutdown(wait=False)
                self.executor = None
            if self.stopped is not None:
                self.stopped.set()

    async def wait(self) -> None:
        if self.stopped is None:
            raise RuntimeError("client not started")
        await self.stopped.wait()


class RocketMQPublisher:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.routes = {r.topic: r.physical_topic for r in config.routes}
        self._driver = _Driver(config)
        self._publishing: asyncio.Semaphore | None = None

    async def start(self) -> None:
        self._driver.begin()
        self._publishing = asyncio.Semaphore(self.config.max_in_flight)

        def create() -> None:
            from rocketmq.v5.client import (  # type: ignore[import-untyped]
                ClientConfiguration,
                Credentials,
            )
            from rocketmq.v5.producer import Producer  # type: ignore[import-untyped]

            self._driver.client = Producer(
                ClientConfiguration(
                    self.config.endpoint,
                    Credentials(self.config.access_key, self.config.secret_key),
                    request_timeout=math.ceil(self.config.timeout),
                ),
                topics=set(self.routes.values()),
                tls_enable=True,
            )
            self._driver.client.startup()
            # Upstream startup logs route failures without raising. Explicitly
            # require every route before host readiness can become true.
            for topic in self.routes.values():
                self._driver.client._retrieve_topic_route_data(topic)

        try:
            await self._driver.call(create)
        except BaseException:
            await self._driver.close(self.config.timeout + 5)
            raise

    async def publish(self, topic: str, body: bytes) -> DeliveryResult:
        physical = self.routes.get(topic)
        if physical is None or not body or len(body) > 262144:
            return DeliveryResult(Outcome.REJECTED, Confirmation.BROKER)
        if self._driver.client is None or self._publishing is None or self._driver.closing:
            raise RuntimeError("publisher not running")
        owned = bytes(body)

        def send() -> Any:
            from rocketmq.v5.model import Message  # type: ignore[import-untyped]

            message = Message()
            message.topic, message.body = physical, owned
            return self._driver.client.send(message)

        try:
            async with asyncio.timeout(self.config.timeout):
                # The driver gate retains the real slot if this caller times out.
                async with self._publishing:
                    receipt = await self._driver.call(send)
            outcome = Outcome.CONFIRMED if receipt and receipt.message_id else Outcome.UNKNOWN
            return DeliveryResult(outcome, Confirmation.BROKER)
        except TimeoutError:
            return DeliveryResult(Outcome.UNKNOWN, Confirmation.BROKER)
        except Exception as error:
            code = getattr(error, "code", None)
            rejected = isinstance(code, int) and (40000 <= code < 40600 or 41300 <= code < 41400)
            return DeliveryResult(
                Outcome.REJECTED if rejected else Outcome.UNKNOWN, Confirmation.BROKER, error
            )

    async def wait(self) -> None:
        await self._driver.wait()

    async def stop(self, *, grace_seconds: float = 10.0) -> None:
        await self._driver.close(grace_seconds)


class RocketMQSubscriber:
    def __init__(
        self,
        config: Config,
        topic: str,
        channel: str,
        *,
        handler: Callable[[Received], Awaitable[None]],
        failed_handler: Callable[[bytes], Awaitable[None]],
        invalid_handler: Callable[[bytes, str], Awaitable[None]],
        max_attempts: int = 8,
        invisible_duration: float = 60,
        handler_timeout: float = 540,
        retry_cap: float = 60,
    ) -> None:
        bindings = [r for r in config.routes if r.topic == topic and r.subscription == channel]
        if len(bindings) != 1 or not bindings[0].consumer_group:
            raise ValueError("subscription not in trusted manifest")
        if not 1 <= max_attempts <= 65535 or not 10 <= invisible_duration <= 43200:
            raise ValueError("bounded attempts and visibility required")
        if handler_timeout <= 0 or not 0 < retry_cap <= 43200:
            raise ValueError("bounded handler/retry durations required")
        self.config, self.route = config, bindings[0]
        self.handler, self.failed_handler = handler, failed_handler
        self.invalid_handler = invalid_handler
        self.max_attempts, self.invisible_duration = max_attempts, invisible_duration
        self.handler_timeout, self.retry_cap = handler_timeout, retry_cap
        self._driver = _Driver(config, lifecycle_slots=2)
        self._poller: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._slots: asyncio.Semaphore | None = None
        self._failure: asyncio.Future[None] | None = None

    async def start(self) -> None:
        self._driver.begin()
        self._slots = asyncio.Semaphore(self.config.max_in_flight)
        self._failure = asyncio.get_running_loop().create_future()

        def create() -> None:
            from rocketmq.v5.client import (  # type: ignore[import-untyped]
                ClientConfiguration,
                Credentials,
            )
            from rocketmq.v5.consumer import SimpleConsumer  # type: ignore[import-untyped]
            from rocketmq.v5.model import FilterExpression  # type: ignore[import-untyped]

            self._driver.client = SimpleConsumer(
                ClientConfiguration(
                    self.config.endpoint,
                    Credentials(self.config.access_key, self.config.secret_key),
                    request_timeout=math.ceil(self.config.timeout),
                ),
                self.route.consumer_group,
                {self.route.physical_topic: FilterExpression()},
                await_duration=5,
                tls_enable=True,
            )
            self._driver.client.startup()
            self._driver.client._retrieve_topic_route_data(self.route.physical_topic)

        try:
            await self._driver.call(create)
            self._poller = asyncio.create_task(self._poll())
        except BaseException:
            await self._driver.close(self.config.timeout + 5)
            raise

    async def _poll(self) -> None:
        assert self._slots is not None
        try:
            while not self._driver.closing:
                await self._slots.acquire()
                try:
                    messages = await self._driver.call(
                        self._driver.client.receive, 1, math.ceil(self.invisible_duration)
                    )
                except BaseException:
                    self._slots.release()
                    raise
                if not messages:
                    self._slots.release()
                    continue
                for message in messages:
                    task = asyncio.create_task(self._handle(message))
                    self._tasks.add(task)
                    task.add_done_callback(self._completed)
        except asyncio.CancelledError:
            raise
        except Exception:
            if self._failure is not None and not self._failure.done():
                self._failure.set_result(None)

    def _completed(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        assert self._slots is not None
        self._slots.release()
        if not task.cancelled() and task.exception() is not None:
            if self._failure is not None and not self._failure.done():
                self._failure.set_result(None)

    async def _handle(self, raw: Any) -> None:
        lock = asyncio.Lock()
        done = asyncio.Event()
        task = asyncio.current_task()

        async def renew() -> None:
            while not done.is_set():
                await asyncio.sleep(self.invisible_duration / 3)
                async with lock:
                    if done.is_set():
                        return
                    try:
                        await self._driver.call(
                            self._driver.client.change_invisible_duration,
                            raw,
                            math.ceil(self.invisible_duration),
                        )
                    except Exception:
                        if task is not None:
                            task.cancel()
                        raise

        renewer = asyncio.create_task(renew())
        try:
            async with asyncio.timeout(self.handler_timeout):
                wire = bytes(raw.body)
                if raw.topic != self.route.physical_topic or raw.corrupted:
                    raise ValueError("physical source or broker checksum mismatch")
                try:
                    value = decode(wire)
                    if not value.message_id or len(go_json(value.metadata)) > MAX_METADATA_BYTES:
                        raise ValueError("invalid message identity/metadata")
                except ValueError:
                    await self.invalid_handler(wire, "invalid_wire")
                else:
                    attempts = max(1, int(raw.delivery_attempt or 1))
                    received = Received(
                        value,
                        wire,
                        raw.message_id,
                        self.config.endpoint,
                        attempts,
                        int(raw.born_timestamp or 0) * 1_000_000,
                        "rocketmq",
                        raw.topic,
                        self.route.topic,
                    )
                    if attempts > self.max_attempts:
                        await self._failed(received)
                    else:
                        try:
                            await self.handler(received)
                        except Exception:
                            if attempts >= self.max_attempts:
                                await self._failed(received)
                            else:
                                raise
                async with lock:
                    await self._driver.call(self._driver.client.ack, raw)
                    done.set()
        except asyncio.CancelledError:
            raise  # unknown host outcome: no acknowledgement or business replay
        except Exception:
            async with lock:
                await self._driver.call(
                    self._driver.client.change_invisible_duration, raw, math.ceil(self.retry_cap)
                )
        finally:
            done.set()
            renewer.cancel()
            await asyncio.gather(renewer, return_exceptions=True)

    async def _failed(self, received: Received) -> None:
        # Separate new provider-aware record; historical NSQ bytes stay unchanged.
        document: dict[str, object] = {
            "type": "reliable-messaging.failed.v1",
            "provider": "rocketmq",
            "topic": self.route.topic,
            "channel": self.route.subscription,
            "physical_topic": self.route.physical_topic,
            "uuid": received.envelope.message_id,
            "transport_message_id": received.transport_id,
            "metadata": received.envelope.metadata,
            "payload": base64.b64encode(received.envelope.payload).decode("ascii"),
            "attempts": received.attempts,
            "timestamp": received.timestamp,
            "cause": "delivery attempts exhausted",
        }
        await self.failed_handler(go_json(document))

    async def wait(self) -> None:
        if self._failure is None:
            raise RuntimeError("subscriber not started")
        await asyncio.shield(self._failure)
        raise RuntimeError("RocketMQ subscriber stopped or transport supervision failed")

    async def stop(self, *, grace_seconds: float = 10.0) -> None:
        self._driver.closing = True
        async with asyncio.timeout(grace_seconds):
            if self._poller is not None:
                self._poller.cancel()
                await asyncio.gather(self._poller, return_exceptions=True)
                self._poller = None
            if self._tasks:
                await asyncio.shield(asyncio.gather(*self._tasks, return_exceptions=True))
            await self._driver.close(grace_seconds)
            if self._failure is not None and not self._failure.done():
                self._failure.set_result(None)
