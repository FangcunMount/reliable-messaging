"""Explicit, single-asyncio-loop NSQ adapter using the official pinned client.

Handlers return only after host durable commit. Publisher confirmation is Broker
confirmation. No callback grants business retry permission or creates a transaction.
"""

import asyncio
import base64
import functools
import json
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

import nsq  # type: ignore[import-untyped]
from nsq.client import Client  # type: ignore[import-untyped]
from tornado.ioloop import IOLoop, PeriodicCallback

from reliable_messaging.delivery import Confirmation, DeliveryResult, Outcome
from reliable_messaging.wire import (
    FAILED_CHANNEL,
    FAILED_TYPE,
    MAX_METADATA_BYTES,
    NSQ_MAX_BYTES,
    Envelope,
    decode,
    encode_failure,
    failed_topic,
    go_json,
)


def _name(value: str) -> bool:
    return bool(re.fullmatch(r"[.a-zA-Z0-9_-]{1,64}", value))


class _Callbacks:
    """Track only this client's callbacks; never close the borrowed Tornado loop."""

    def __init__(self) -> None:
        self.loop = IOLoop.current()
        self.closed = False
        self.handles: set[Any] = set()

    def add_callback(self, callback: Any, *args: Any, **kwargs: Any) -> None:
        def guarded() -> None:
            if not self.closed:
                callback(*args, **kwargs)

        self.loop.add_callback(guarded)

    def call_later(self, delay: float, callback: Any, *args: Any, **kwargs: Any) -> Any:
        handle: Any = None

        def guarded() -> None:
            self.handles.discard(handle)
            if not self.closed:
                callback(*args, **kwargs)

        handle = self.loop.call_later(delay, guarded)
        if self.closed:
            self.loop.remove_timeout(handle)
        else:
            self.handles.add(handle)
        return handle

    def remove_timeout(self, handle: Any) -> None:
        self.handles.discard(handle)
        self.loop.remove_timeout(handle)

    def close(self) -> None:
        self.closed = True
        for handle in list(self.handles):
            self.remove_timeout(handle)


class _OwnedClient(Client):  # type: ignore[misc]
    def __init__(self, **kwargs: Any) -> None:
        # pynsq Client otherwise creates an untracked 60-second PeriodicCallback.
        self.io_loop = _Callbacks()
        self.ready_event = asyncio.Event()
        self.closed_event = asyncio.Event()
        self.connecting: dict[str, Any] = {}
        self.owned_heartbeat = PeriodicCallback(self._check_last_recv_timestamps, 60_000)
        self.owned_heartbeat.start()

    def close_owned(self) -> None:
        self.io_loop.close()
        self.owned_heartbeat.stop()
        for name in ("redist_periodic", "query_periodic"):
            periodic = getattr(self, name, None)
            if periodic is not None:
                periodic.stop()
        # Closing callbacks can remove entries; never iterate the live dictionary.
        for conn in [*self.conns.values(), *self.connecting.values()]:
            conn.close()
        if not self.conns and not self.connecting:
            self.closed_event.set()

    def _on_connection_error(self, conn: Any, error: Any, **kwargs: Any) -> None:
        # Protocol errors can contain an attacker-controlled body. Do not log it.
        conn.close()


class _Reader(nsq.Reader, _OwnedClient):  # type: ignore[misc]
    def connect_to_nsqd(self, host: str, port: int) -> None:
        # Pinned client's connection setup, retaining pending sockets for startup cancellation.
        identity = f"{host}:{port}"
        if self.io_loop.closed or identity in self.conns or identity in self.connecting:
            return
        now = time.time()
        if self.connection_attempts.get(identity, 0) > now - 10:
            return
        self.connection_attempts[identity] = now
        conn = nsq.AsyncConn(host, port, **self.conn_kwargs)
        for event, callback in (
            ("identify", self._on_connection_identify),
            ("identify_response", self._on_connection_identify_response),
            ("auth", self._on_connection_auth),
            ("auth_response", self._on_connection_auth_response),
            ("error", self._on_connection_error),
            ("close", self._on_connection_close),
            ("ready", self._on_connection_ready),
            ("message", self._on_message),
            ("heartbeat", self._on_heartbeat),
            ("backoff", functools.partial(self._on_backoff_resume, success=False)),
            ("resume", functools.partial(self._on_backoff_resume, success=True)),
            ("continue", functools.partial(self._on_backoff_resume, success=None)),
        ):
            conn.on(event, callback)
        self.connecting[conn.id] = conn
        conn.connect()

    def _on_connection_close(self, conn: Any, **kwargs: Any) -> None:
        self.connecting.pop(conn.id, None)
        super()._on_connection_close(conn, **kwargs)
        if self.io_loop.closed and not self.conns and not self.connecting:
            self.closed_event.set()

    def _on_connection_ready(self, conn: Any, **kwargs: Any) -> None:
        self.connecting.pop(conn.id, None)
        if self.io_loop.closed:
            conn.close()
            return
        super()._on_connection_ready(conn, **kwargs)
        self.ready_event.set()

    def _handle_message(self, conn: Any, message: Any) -> None:
        message.source_address = conn.id
        super()._handle_message(conn, message)

    def close(self) -> None:
        self.max_in_flight = 0
        for conn in list(self.conns.values()):
            conn.send_rdy(0)
        self.close_owned()


class _Writer(nsq.Writer, _OwnedClient):  # type: ignore[misc]
    def __init__(self, addresses: list[str], **kwargs: Any) -> None:
        nsq.Writer.__init__(self, addresses, **kwargs)

    def connect_to_nsqd(self, host: str, port: int) -> None:
        identity = f"{host}:{port}"
        if self.io_loop.closed or identity in self.conns or identity in self.connecting:
            return
        conn = nsq.AsyncConn(host, port, **self.conn_kwargs)
        for event, callback in (
            ("identify", self._on_connection_identify),
            ("identify_response", self._on_connection_identify_response),
            ("auth", self._on_connection_auth),
            ("auth_response", self._on_connection_auth_response),
            ("error", self._on_connection_error),
            ("response", self._on_connection_response),
            ("close", self._on_connection_close),
            ("ready", self._on_connection_ready),
            ("heartbeat", self.heartbeat),
        ):
            conn.on(event, callback)
        conn.callback_queue = []
        self.connecting[conn.id] = conn
        conn.connect()

    def _on_connection_ready(self, conn: Any, **kwargs: Any) -> None:
        self.connecting.pop(conn.id, None)
        if self.io_loop.closed:
            conn.close()
            return
        super()._on_connection_ready(conn, **kwargs)
        self.ready_event.set()

    def _on_connection_close(self, conn: Any, **kwargs: Any) -> None:
        self.connecting.pop(conn.id, None)
        super()._on_connection_close(conn, **kwargs)
        if self.io_loop.closed and not self.conns and not self.connecting:
            self.closed_event.set()

    def close(self) -> None:
        self.close_owned()


async def _ready(client: Any, budget_seconds: float) -> None:
    async with asyncio.timeout(budget_seconds):
        await client.ready_event.wait()


class NSQPublisher:
    def __init__(self, address: str, *, timeout: float = 5.0, max_in_flight: int = 1) -> None:
        if not address or timeout <= 0 or not 1 <= max_in_flight <= 100:
            raise ValueError("explicit NSQD address and bounded publishing required")
        self.address = address
        self.timeout = timeout
        self.max_in_flight = max_in_flight
        self._client: Any = None
        self._pending: set[asyncio.Future[DeliveryResult]] = set()
        self._gate: asyncio.Semaphore | None = None
        self._stopped: asyncio.Event | None = None
        self._closing = False

    async def start(self) -> None:
        if self._client is not None:
            raise RuntimeError("publisher already started")
        self._closing = False
        self._gate = asyncio.Semaphore(self.max_in_flight)
        self._stopped = asyncio.Event()
        self._client = _Writer([self.address], reconnect_interval=2, name="rm-python-publisher")
        try:
            await _ready(self._client, self.timeout)
        except BaseException:
            self._client.close()
            self._client = None
            self._stopped.set()
            raise

    async def publish(self, topic: str, body: bytes) -> DeliveryResult:
        if not _name(topic) or not body or len(body) > NSQ_MAX_BYTES:
            return DeliveryResult(Outcome.REJECTED, Confirmation.BROKER)
        if self._client is None or self._gate is None or self._closing:
            raise RuntimeError("publisher is not running")
        await self._gate.acquire()
        if self._closing:
            self._gate.release()
            raise RuntimeError("publisher is stopping")
        future: asyncio.Future[DeliveryResult] = asyncio.get_running_loop().create_future()
        self._pending.add(future)
        gate = self._gate

        def complete(conn: Any, result: Any) -> None:
            if future.done():
                return
            outcome = Outcome.CONFIRMED if result == b"OK" else Outcome.UNKNOWN
            future.set_result(DeliveryResult(outcome, Confirmation.BROKER))
            self._pending.discard(future)
            gate.release()

        try:
            # Retain the in-flight slot even if the caller times out or is canceled.
            self._client.pub(topic, bytes(body), callback=complete)
        except Exception:
            complete(None, None)
        try:
            return await asyncio.wait_for(asyncio.shield(future), self.timeout)
        except TimeoutError:
            return DeliveryResult(Outcome.UNKNOWN, Confirmation.BROKER)

    async def wait(self) -> None:
        if self._stopped is None:
            raise RuntimeError("publisher has not started")
        await self._stopped.wait()

    async def stop(self, *, grace_seconds: float = 10.0) -> None:
        if grace_seconds < 0:
            raise ValueError("nonnegative stop budget required")
        self._closing = True
        deadline = asyncio.get_running_loop().time() + grace_seconds
        if self._pending:
            await asyncio.wait(list(self._pending), timeout=grace_seconds)
        if self._client is not None:
            self._client.close()
            await asyncio.sleep(0)  # run the stream close callbacks, never a new event loop
            if not self._client.closed_event.is_set():
                await asyncio.wait_for(
                    self._client.closed_event.wait(),
                    max(0, deadline - asyncio.get_running_loop().time()),
                )
            self._client = None
        if self._stopped is not None:
            self._stopped.set()


@dataclass(frozen=True)
class Received:
    envelope: Envelope
    wire: bytes
    transport_id: str
    source_address: str
    attempts: int
    timestamp: int


class NSQSubscriber:
    def __init__(
        self,
        topic: str,
        channel: str,
        *,
        publishers: Mapping[str, NSQPublisher],
        handler: Callable[[Received], Awaitable[None]],
        failed_handler: Callable[[bytes], Awaitable[None]],
        invalid_handler: Callable[[bytes, str], Awaitable[None]],
        failure_ready: Callable[[str, str, str], Awaitable[None]],
        max_attempts: int = 8,
        max_in_flight: int = 1,
        startup_timeout: float = 5,
        retry_cap: float = 60,
    ) -> None:
        if not _name(topic) or not _name(channel) or not publishers:
            raise ValueError("durable topic, channel and explicit source publishers required")
        if not 1 <= max_attempts <= 65535 or not 1 <= max_in_flight <= 100:
            raise ValueError("bounded attempts and in-flight required")
        if startup_timeout <= 0 or retry_cap <= 0:
            raise ValueError("positive timeout and retry cap required")
        self.topic, self.channel = topic, channel
        self.publishers = dict(publishers)  # borrowed; host starts and stops them
        self.handler, self.failed_handler = handler, failed_handler
        self.invalid_handler, self.failure_ready = invalid_handler, failure_ready
        self.max_attempts, self.max_in_flight = max_attempts, max_in_flight
        self.startup_timeout, self.retry_cap = startup_timeout, retry_cap
        self.failure_topic = failed_topic(topic, channel)
        self._readers: list[Any] = []
        self._tasks: set[asyncio.Task[None]] = set()
        self._stopped: asyncio.Event | None = None
        self._closing = False

    async def start(self) -> None:
        if self._readers:
            raise RuntimeError("subscriber already started")
        self._closing = False
        self._stopped = asyncio.Event()
        try:
            # Connect durable failure consumers before admitting business messages.
            failure = self._reader(self.failure_topic, FAILED_CHANNEL, True)
            self._readers.append(failure)
            await _ready(failure, self.startup_timeout)
            for address in self.publishers:
                await self.failure_ready(address, self.failure_topic, FAILED_CHANNEL)
            business = self._reader(self.topic, self.channel, False)
            self._readers.append(business)
            await _ready(business, self.startup_timeout)
        except BaseException:
            await self.stop(grace_seconds=0)
            raise

    def _reader(self, topic: str, channel: str, failure: bool) -> Any:
        def dispatch(raw: Any) -> None:
            raw.enable_async()
            if self._closing:
                return  # leave unsettled; shutdown closes the connection
            task = asyncio.create_task(self._handle(raw, failure))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        return _Reader(
            topic=topic,
            channel=channel,
            nsqd_tcp_addresses=list(self.publishers),
            message_handler=dispatch,
            max_tries=0,
            max_in_flight=self.max_in_flight,
            name="rm-python-subscriber",
        )

    def _delay(self, attempts: int) -> float:
        return min(self.retry_cap, float(2 ** min(max(attempts, 1), 16)))

    async def _handle(self, raw: Any, failure: bool) -> None:
        try:
            if failure:
                try:
                    document = json.loads(raw.body)
                    if document.get("type") != FAILED_TYPE or document.get("provider") != "nsq":
                        raise ValueError("invalid handoff")
                    base64.b64decode(document["payload"], validate=True)
                except (ValueError, TypeError, KeyError, AttributeError):
                    await self.invalid_handler(bytes(raw.body), "invalid_failure_wire")
                else:
                    await self.failed_handler(bytes(raw.body))
                raw.finish()  # callback contract requires host commit first
                return
            try:
                value = decode(raw.body)
                if not value.message_id or len(go_json(value.metadata)) > MAX_METADATA_BYTES:
                    raise ValueError("invalid metadata or identity")
            except ValueError:
                await self.invalid_handler(bytes(raw.body), "invalid_wire")
                raw.finish()
                return
            received = Received(
                value,
                bytes(raw.body),
                raw.id.decode("ascii"),
                raw.source_address,
                raw.attempts,
                raw.timestamp,
            )
            if raw.attempts > self.max_attempts:
                await self._handoff(raw, value)
                return
            try:
                await self.handler(received)
            except asyncio.CancelledError:
                raise
            except Exception:
                if raw.attempts >= self.max_attempts:
                    await self._handoff(raw, value)
                else:
                    raw.requeue(delay=self._delay(raw.attempts), backoff=False)
                return
            raw.finish()
        except asyncio.CancelledError:
            return  # unknown host outcome; do not FIN or settle cancellation
        except Exception:
            if not raw.has_responded():
                raw.requeue(delay=self._delay(raw.attempts), backoff=False)

    async def _handoff(self, raw: Any, value: Envelope) -> None:
        publisher = self.publishers.get(raw.source_address)
        if publisher is None:
            raise ValueError("source NSQD is not configured")
        await self.failure_ready(raw.source_address, self.failure_topic, FAILED_CHANNEL)
        body = encode_failure(
            value,
            topic=self.topic,
            channel=self.channel,
            transport_id=raw.id.decode("ascii"),
            attempts=self.max_attempts,
            timestamp=raw.timestamp,
            cause="handler_failed",
        )
        if len(body) > NSQ_MAX_BYTES:
            await self.invalid_handler(bytes(raw.body), "failure_wire_oversize")
            raw.finish()
            return
        result = await publisher.publish(self.failure_topic, body)
        if result.outcome != Outcome.CONFIRMED:
            raise RuntimeError("failure handoff not confirmed")
        raw.finish()

    async def wait(self) -> None:
        if self._stopped is None:
            raise RuntimeError("subscriber has not started")
        await self._stopped.wait()

    async def stop(self, *, grace_seconds: float = 10.0) -> None:
        if grace_seconds < 0:
            raise ValueError("nonnegative stop budget required")
        self._closing = True
        deadline = asyncio.get_running_loop().time() + grace_seconds
        for reader in self._readers:
            reader.max_in_flight = 0
            for conn in list(reader.conns.values()):
                conn.send_rdy(0)
        if self._tasks:
            _, pending = await asyncio.wait(list(self._tasks), timeout=grace_seconds)
            for task in pending:
                task.cancel()
            if pending:
                # A host callback may suppress cancellation. Never wait past the
                # stop budget or close resources beneath an unknown active commit.
                await asyncio.sleep(0)
                remaining = [task for task in pending if not task.done()]
                if remaining:
                    raise TimeoutError("host handlers did not stop within the stop budget")
        for reader in self._readers:
            reader.close()
        await asyncio.sleep(0)
        for reader in self._readers:
            if not reader.closed_event.is_set():
                await asyncio.wait_for(
                    reader.closed_event.wait(),
                    max(0, deadline - asyncio.get_running_loop().time()),
                )
        self._readers.clear()
        if self._stopped is not None:
            self._stopped.set()
