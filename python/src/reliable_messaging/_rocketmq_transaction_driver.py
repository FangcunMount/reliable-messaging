"""Instance-only adaptation of pinned rocketmq-python-client 5.1.2.

No upstream factory, process I/O loop or global executor is replaced. The SDK
imports the optional driver and creates its resources only at explicit Start.
"""

import asyncio
import math
import threading
from collections.abc import Callable, Coroutine
from concurrent.futures import Future, ThreadPoolExecutor
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
    for name in ("send_message_async", "end_transaction_async"):
        setattr(producer.rpc_client, name, bounded(getattr(producer.rpc_client, name)))
    return producer
