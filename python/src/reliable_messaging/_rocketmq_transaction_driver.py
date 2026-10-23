"""Instance-only adaptation of pinned rocketmq-python-client 5.1.2.

No upstream factory, process I/O loop or global executor is replaced. The SDK
imports the optional driver and creates its resources only at explicit Start.
"""

import asyncio
import math
import threading
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
            try:
                super().shutdown()
            finally:
                # Upstream startup failure sets is_running=False; its shutdown
                # then returns early. Still close THIS instance's channels.
                self.rpc_client.stop()

        def _Client__heartbeat_callback(self, future: Any, endpoints: Any) -> None:
            # Pinned 5.1.2 leaves a failed telemetry read closed until its
            # 300-second settings sync. Reuse THIS client's existing heartbeat
            # and I/O-loop callback: no timer, thread, factory or global patch.
            try:
                super()._Client__heartbeat_callback(future, endpoints)
            except Exception:
                return  # A failed heartbeat is not authority to reopen a stream.
            response = future.result()
            if not self.is_running or response is None or response.status.code != Code.OK:
                return
            try:
                channel = self.rpc_client.retrieve_or_create_channel(endpoints)
                telemetry = channel.telemetry_stream_stream_call
                call = getattr(telemetry, "_RpcStreamStreamCall__stream_stream_call", None)
                if call is not None and call.done() and self.is_running:
                    # on_new_endpoints also sends settings; merely opening a
                    # channel cannot register this producer for Broker checks.
                    self.on_new_endpoints(endpoints)
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
