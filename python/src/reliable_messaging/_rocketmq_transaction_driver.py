"""Instance-only adaptation of pinned rocketmq-python-client 5.1.2.

No upstream factory, process I/O loop or global executor is replaced. The SDK
imports the optional driver and creates its resources only at explicit Start.
"""

import asyncio
import math
import threading
import time
from collections.abc import Callable, Coroutine
from concurrent.futures import Future, ThreadPoolExecutor
from importlib.metadata import version
from typing import Any


class RecoveryPool:
    """Admit BEFORE submit. Capacity remains held through DB AND final RPC."""

    def __init__(self, workers: int, waiting: int) -> None:
        self._lock = threading.Lock()
        self._capacity = threading.BoundedSemaphore(workers + waiting)
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="rm-tx-check")
        self._futures: set[Future[Any]] = set()
        self._closing = False
        self.rejected = 0
        self.failed = 0
        self.running = 0
        self.admitted = 0

    def submit(self, work: Callable[[], None]) -> bool:
        with self._lock:
            if self._closing or not self._capacity.acquire(blocking=False):
                self.rejected += 1
                return False
            try:
                future = self._pool.submit(self._run, work)
                self._futures.add(future)
                self.admitted += 1
            except BaseException:
                self._capacity.release()
                self.failed += 1
                return False
        # A completed Future invokes callbacks inline: never register under lock.
        future.add_done_callback(self._complete)
        return True

    def _run(self, work: Callable[[], None]) -> None:
        with self._lock:
            self.running += 1
        try:
            work()
        finally:
            with self._lock:
                self.running -= 1

    def _complete(self, future: Future[Any]) -> None:
        with self._lock:
            self._futures.discard(future)
            if not future.cancelled() and future.exception() is not None:
                self.failed += 1
            self._capacity.release()

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "running": self.running,
                "pending": len(self._futures),
                "rejected": self.rejected,
                "failed": self.failed,
                "admitted": self.admitted,
            }

    def stop_admission(self) -> None:
        with self._lock:
            self._closing = True

    async def drain(self) -> None:
        self.stop_admission()
        with self._lock:
            current = tuple(self._futures)
        if current:
            await asyncio.shield(
                asyncio.gather(*(asyncio.wrap_future(f) for f in current), return_exceptions=True)
            )
        self._pool.shutdown(wait=False)


class _SettingsSync:
    """One actual settings write per endpoint; close retains running capacity."""

    def __init__(self, client: Any) -> None:
        self.client = client
        self.manager = client._Client__telemetry_manager
        self.condition = threading.Condition()
        self.pending: dict[str, Future[Any] | None] = {}
        self.started: dict[str, float] = {}
        self.closing = False
        self.writes = self.failures = self.skipped = 0
        self.heartbeat_ok = self.heartbeat_missing = self.heartbeat_errors = 0
        self.last_code = 0
        self.manager.setting_write = self.write

    def write(self, endpoints: Any) -> None:
        key = endpoints.facade
        with self.condition:
            if self.closing or key in self.pending:
                self.skipped += 1
                return
            self.pending[key] = None
            self.started[key] = time.monotonic()
            self.writes += 1
        try:
            channel = self.client.rpc_client.channels.get(endpoints)
            stream = None if channel is None else channel.telemetry_stream_stream_call
            future = self.client.rpc_client.telemetry_write_async(
                endpoints, self.client.sync_setting_req(endpoints)
            )
        except BaseException:
            with self.condition:
                self.pending.pop(key, None)
                self.started.pop(key, None)
                self.failures += 1
                self.condition.notify_all()
            raise
        if stream is not None:
            # A replacement created by upstream's error callback starts empty.
            # Keep this marker on the stream, not in an ever-growing route map.
            stream._rm_settings_enqueued = True
        with self.condition:
            self.pending[key] = future

        def completed(done: Future[Any]) -> None:
            # Serialize metadata re-creation with shutdown's admission seal.
            # The pinned callback only reads an already-completed Future and
            # may construct a stream; it never waits for a database or RPC.
            with self.condition:
                try:
                    if not self.closing:
                        self.manager._ClientTelemetryManager__setting_write_callback(
                            done, endpoints=endpoints
                        )
                finally:
                    if not done.cancelled() and done.exception() is not None:
                        self.failures += 1
                    self.pending.pop(key, None)
                    self.started.pop(key, None)
                    self.condition.notify_all()

        # A completed Future invokes its callback inline; never hold our lock.
        future.add_done_callback(completed)

    def busy(self, endpoints: Any, budget: float, call: Any) -> bool:
        with self.condition:
            pending = endpoints.facade in self.pending
            expired = pending and time.monotonic() - self.started[endpoints.facade] >= budget
        if expired and call is not None:
            # Cancellation asks the real gRPC write to finish. Its slot remains
            # owned until the actual Future and original callback have ended.
            call.cancel()
        return pending

    def stop(self) -> None:
        with self.condition:
            self.closing = True

    def wait(self) -> None:
        with self.condition:
            self.condition.wait_for(lambda: not self.pending)

    def snapshot(self) -> dict[str, int | bool]:
        with self.condition:
            return dict(
                pending_settings=len(self.pending),
                settings_closing=self.closing,
                settings_writes=self.writes,
                settings_failures=self.failures,
                settings_skipped=self.skipped,
                heartbeat_ok=self.heartbeat_ok,
                heartbeat_missing=self.heartbeat_missing,
                heartbeat_errors=self.heartbeat_errors,
                last_heartbeat_code=self.last_code,
            )


def create_producer(
    *,
    endpoint: str,
    access_key: str,
    secret_key: str,
    topics: set[str],
    timeout: float,
    host_loop: asyncio.AbstractEventLoop,
    pool: RecoveryPool,
    checker: Callable[[Any], Coroutine[Any, Any, int]],
) -> Any:
    if version("rocketmq-python-client") != "5.1.2":
        raise RuntimeError("transaction adaptation requires pinned rocketmq-python-client 5.1.2")
    from rocketmq.grpc_protocol import (  # type: ignore[import-untyped]
        Code,
        TransactionResolution,
        TransactionSource,
    )
    from rocketmq.v5.client import ClientConfiguration, Credentials  # type: ignore[import-untyped]
    from rocketmq.v5.model import Message  # type: ignore[import-untyped]
    from rocketmq.v5.model.retry_policy import (  # type: ignore[import-untyped]
        ExponentialBackoffRetryPolicy,
    )
    from rocketmq.v5.producer import Producer  # type: ignore[import-untyped]

    class BoundedProducer(Producer):  # type: ignore[misc]
        def shutdown(self) -> None:
            settings = getattr(self, "_rm_settings", None)
            if settings is not None:
                settings.stop()
            try:
                super().shutdown()
            finally:
                # Upstream startup failure sets is_running=False; its shutdown
                # then returns early. Still close THIS instance's channels.
                self.rpc_client.stop()
                if settings is not None:
                    settings.wait()

        def _Client__heartbeat_callback(self, future: Any, endpoints: Any) -> None:
            # Pinned 5.1.2 leaves a failed telemetry read closed until its
            # 300-second settings sync. Reuse THIS client's existing heartbeat
            # and I/O-loop callback: no timer, thread, factory or global patch.
            try:
                super()._Client__heartbeat_callback(future, endpoints)
            except Exception:
                self._rm_settings.heartbeat_errors += 1
                return  # A failed RPC is not authority to reopen a stream.
            response = future.result()
            settings = self._rm_settings
            if response is not None:
                code = response.status.code
                settings.last_code = code if 0 <= code <= 99999 else 0
                settings.heartbeat_ok += int(code == Code.OK)
                settings.heartbeat_missing += int(code == Code.UNRECOGNIZED_CLIENT_TYPE)
            # Proxy 5.5.1 loses settings on restart and answers 40015 until
            # telemetry registers them again. Waiting for OK would deadlock
            # recovery. Other refusal codes never authorize a rebuild.
            if (
                not self.is_running
                or settings.closing
                or response is None
                or response.status.code not in (Code.OK, Code.UNRECOGNIZED_CLIENT_TYPE)
            ):
                return
            try:
                channel = self.rpc_client.channels.get(endpoints)
                telemetry = None if channel is None else channel.telemetry_stream_stream_call
                call = getattr(telemetry, "_RpcStreamStreamCall__stream_stream_call", None)
                with settings.condition:
                    if settings.closing or settings.busy(endpoints, timeout, call):
                        return
                    if call is None or call.done():
                        self.on_new_endpoints(endpoints)
                    elif response.status.code == Code.UNRECOGNIZED_CLIENT_TYPE or not getattr(
                        telemetry, "_rm_settings_enqueued", False
                    ):
                        # Upstream can open a replacement stream without
                        # resending settings. Active alone is not registration.
                        settings.write(endpoints)
            except Exception:
                from rocketmq.v5.log import logger  # type: ignore[import-untyped]

                logger.warning("Transaction telemetry recovery failed; awaiting next heartbeat")

        def _Producer__check_send_retry_condition(
            self,
            message: Any,
            topic_queue: Any,
            attempt: int,
            error: Exception,
        ) -> Future[Any]:
            # The pinned synchronous send loop resolves this INSTANCE hook.
            # Telemetry cannot authorize a second physical half-message RPC.
            done: Future[Any] = Future()
            done.set_exception(error)
            return done

        def reset_setting(self, settings: Any) -> None:
            super().reset_setting(settings)
            # One physical send attempt per frozen branch. Backcheck resolves
            # an ambiguous half; the SDK never retries it on the user's behalf.
            self._Producer__backoff_policy = ExponentialBackoffRetryPolicy(None, 1)

        def on_recover_orphaned_transaction_command(
            self,
            endpoints: Any,
            msg: Any,
            transaction_id: str,
        ) -> None:
            if not self.is_running:
                return

            def recover() -> None:
                message = Message().fromProtobuf(msg)
                # Only this coroutine reads the database, on the HOST loop.
                # No timeout on Future.result(): the slot is held until the
                # coroutine has really finished, including delayed cancellation.
                result = asyncio.run_coroutine_threadsafe(checker(message), host_loop).result()
                if result not in (TransactionResolution.COMMIT, TransactionResolution.ROLLBACK):
                    return  # Unknown never emits an EndTransaction confirmation.
                response = self.end_transaction(
                    endpoints,
                    message,
                    transaction_id,
                    result,
                    TransactionSource.SOURCE_SERVER_CHECK,
                )
                if response.status.code != Code.OK:
                    raise RuntimeError("transaction backcheck final RPC was not acknowledged")

            # Rejection creates no task, coroutine, callback RPC or new thread.
            pool.submit(recover)

    producer = BoundedProducer(
        ClientConfiguration(
            endpoint, Credentials(access_key, secret_key), request_timeout=math.ceil(timeout)
        ),
        topics=topics,
        checker=object(),
        tls_enable=True,
    )

    producer._rm_settings = _SettingsSync(producer)

    # Keep telemetry's integer Duration configuration while imposing the actual
    # configured (possibly fractional) deadline on these instance RPC methods.
    def bounded(method: Any) -> Any:
        def invoke(endpoints: Any, req: Any, metadata: Any, timeout: float = 3) -> Any:
            return method(endpoints, req, metadata, timeout=min(timeout, rpc_budget))

        return invoke

    rpc_budget = timeout
    for name in ("send_message_async", "end_transaction_async", "query_topic_route_async"):
        setattr(producer.rpc_client, name, bounded(getattr(producer.rpc_client, name)))
    preserve_route_errors(producer)
    return producer


def rejection_code(error: Exception) -> str | None:
    """Only positive protocol refusals; transport failures remain Unknown."""
    code = getattr(error, "code", None)
    if isinstance(code, int) and (40000 <= code < 40600 or 41300 <= code < 41400):
        return f"half_rejected_{code}"
    import grpc  # type: ignore[import-untyped]

    if isinstance(error, grpc.RpcError):
        status = error.code()
        if status == grpc.StatusCode.PERMISSION_DENIED:
            return "half_rejected_grpc_permission_denied"
        if status == grpc.StatusCode.UNAUTHENTICATED:
            return "half_rejected_grpc_unauthenticated"
    return None


def preserve_route_errors(producer: Any) -> None:
    """Adapt this 5.1.2 manager only; keep callbacks on its existing I/O loop.

    Upstream update_topic_route waits for an event but swallows the RPC/status
    error in its callback. Preserve it through that same event/callback boundary
    so startup can distinguish authorization from unavailable transport.
    """
    manager = producer._Client__route_manager

    def update(topic: str) -> Any:
        completed = threading.Event()
        failures: list[Exception] = []

        def received(future: Future[Any]) -> None:
            try:
                manager._ClientRouteManager__handle_topic_route_res(future.result(), topic)
            except Exception as error:
                failures.append(error)
            finally:
                completed.set()

        future = producer.rpc_client.query_topic_route_async(
            producer.client_configuration.rpc_endpoints,
            manager._ClientRouteManager__topic_route_req(topic),
            metadata=producer.sign(),
            timeout=producer.client_configuration.request_timeout,
        )
        future.add_done_callback(received)
        # Capacity remains owned until the real RPC callback finishes, including
        # cancellation cleanup. The actual upstream RPC has its own deadline.
        completed.wait()
        if failures:
            raise failures[0]
        return manager._ClientRouteManager__topic_route_cache.get(topic)

    manager.update_topic_route = update
