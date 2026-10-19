"""Embedded transaction sender; host owns its UOW, database pool and asyncio loop."""

import asyncio
import functools
import hashlib
from collections.abc import Awaitable, Callable, Coroutine
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any, Protocol, TypeVar

from reliable_messaging._rocketmq_transaction_driver import RecoveryPool, create_producer
from reliable_messaging.rocketmq import _require_server_settings
from reliable_messaging.transaction import (
    Binding,
    BindingError,
    BranchResult,
    Decision,
    Evidence,
    Execution,
    FinalOutcome,
    FrozenBranch,
    FrozenPlan,
    HalfOutcome,
    HalfReceipt,
    Limits,
    LocalExecutor,
    LocalOutcome,
    Reference,
    Request,
    Reservation,
    Result,
    ShutdownIncompleteError,
    StaleExecutionError,
    Store,
    decide_local,
    decode_reference,
    encode_reference,
    resolve,
)

REFERENCE_PROPERTY = "rm_tx_ref"
_T = TypeVar("_T")


class PreparedStore(Store, Protocol):
    @property
    def binding(self) -> Binding: ...
    async def validate(self) -> None: ...
    async def record_half(self, e: Execution, ref: Reference, receipt: HalfReceipt) -> None: ...
    async def record_final(
        self, e: Execution, ref: Reference, outcome: FinalOutcome, code: str
    ) -> None: ...


@dataclass(frozen=True)
class TransactionRoute:
    logical_topic: str
    physical_topic: str
    revision: str


@dataclass(frozen=True)
class TransactionConfig:
    endpoint: str
    access_key: str
    secret_key: str = field(repr=False)
    owner: str
    routes: tuple[TransactionRoute, ...]
    limits: Limits = field(default_factory=Limits)

    def __post_init__(self) -> None:
        host, sep, port = self.endpoint.rpartition(":")
        if not host or not sep or not port.isdigit() or not 1 <= int(port) <= 65535:
            raise ValueError("explicit host:port endpoint required")
        object.__setattr__(self, "routes", tuple(self.routes))
        if (
            not self.access_key
            or not self.secret_key
            or not self.owner
            or not 0 < len(self.routes) <= 256
            or any(not (r.logical_topic and r.physical_topic and r.revision) for r in self.routes)
            or len({r.physical_topic for r in self.routes}) != len(self.routes)
            or len({(r.logical_topic, r.revision) for r in self.routes}) != len(self.routes)
        ):
            raise ValueError("explicit unique owner routes required")


class TransactionSendError(RuntimeError):
    def __init__(self, code: str, result: Result) -> None:
        super().__init__(code)
        self.code, self.result = code, result


class TransactionalSender:
    def __init__(self, config: TransactionConfig, store: PreparedStore) -> None:
        if config.owner != store.binding.owner:
            raise BindingError("store owner differs")
        self.config, self.store = config, store
        self._create_producer = create_producer
        self._routes = {r.physical_topic: r for r in config.routes}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._client: Any = None
        self._executor: ThreadPoolExecutor | None = None
        self._pool: RecoveryPool | None = None
        self._operations: set[asyncio.Task[Any]] = set()
        self._calls: set[asyncio.Future[Any]] = set()
        self._active = 0
        self._available: asyncio.Event | None = None
        self._started = False
        self._stopped = False
        self._ever_started = False
        self._stopping = False
        self._db_healthy = False
        self._start_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._maintenance: asyncio.Task[None] | None = None
        self._maintenance_stop: asyncio.Event | None = None
        self._check_commit = self._check_rollback = self._check_unknown = 0
        self._invalid_checks = self._database_errors = self._reconciled = 0
        self._local_errors = 0

    def _assert_loop(self) -> None:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("transaction runtime belongs to its original host loop")

    async def _database(self, operation: Awaitable[_T]) -> _T:
        try:
            async with asyncio.timeout(self.config.limits.check_timeout):
                return await operation
        except StaleExecutionError:
            raise
        except Exception:
            self._db_healthy = False
            self._database_errors += 1
            raise

    async def _call(self, function: Callable[..., _T], *args: Any) -> _T:
        if self._executor is None or self._loop is None:
            raise RuntimeError("runtime not initialized")
        future = self._loop.run_in_executor(self._executor, functools.partial(function, *args))
        self._calls.add(future)

        def completed(done: asyncio.Future[Any]) -> None:
            self._calls.discard(done)
            if not done.cancelled():
                done.exception()

        future.add_done_callback(completed)
        # Operation/Close cancellation never releases a real blocking call.
        return await asyncio.shield(future)

    async def start(self) -> None:
        if self._ever_started or self._stopping:
            raise RuntimeError("transaction runtime already started or stopping")
        self._ever_started = True
        self._loop = asyncio.get_running_loop()
        self._available, self._maintenance_stop = asyncio.Event(), asyncio.Event()
        self._available.set()
        self._start_task = asyncio.create_task(self._start())
        try:
            await asyncio.shield(self._start_task)
        except BaseException:
            self.stop_admission()
            self._ensure_close()
            raise

    async def _start(self) -> None:
        await self._database(self.store.validate())
        limits = self.config.limits
        self._executor = ThreadPoolExecutor(
            max_workers=limits.sends + 1, thread_name_prefix="rm-tx-rpc"
        )
        self._pool = RecoveryPool(limits.check_workers, limits.check_queue)

        def create() -> None:
            assert self._loop is not None and self._pool is not None
            self._client = self._create_producer(
                endpoint=self.config.endpoint,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                topics=set(self._routes),
                timeout=limits.rpc_timeout,
                host_loop=self._loop,
                pool=self._pool,
                checker=self._check,
            )
            self._client.startup()
            for topic in self._routes:
                self._client._retrieve_topic_route_data(topic)
            _require_server_settings(self._client, limits.rpc_timeout)

        try:
            await self._call(create)
        except BaseException:
            self.stop_admission()
            raise
        self._started, self._db_healthy = True, True
        if not self._stopping:
            self._maintenance = asyncio.create_task(self._maintain())

    async def _maintain(self) -> None:
        assert self._maintenance_stop is not None
        while True:
            try:
                await asyncio.wait_for(
                    self._maintenance_stop.wait(), self.config.limits.reconcile_interval
                )
                return
            except TimeoutError:
                pass
            try:
                await self._database(self.store.validate())
                self._reconciled += await self._database(
                    self.store.reconcile_once(self.config.limits.reconcile_batch)
                )
                self._db_healthy = True
            except Exception:
                pass

    def _validate_plan(self, plan: FrozenPlan) -> None:
        if plan.owner != self.config.owner or len(plan.branches) > self.config.limits.branches:
            raise BindingError("owner or branch budget differs")
        for branch in plan.branches:
            route = self._routes.get(branch.physical_topic)
            if (
                route is None
                or route.logical_topic != branch.message.destination
                or route.revision != branch.route_revision
                or len(branch.wire) > 262144
            ):
                raise BindingError("frozen route or body budget differs")

    async def _admit(self) -> None:
        self._assert_loop()
        assert self._available is not None
        while self._active >= self.config.limits.sends and not self._stopping:
            await self._available.wait()
        stats = self.stats()
        if not stats["ready"]:
            raise RuntimeError("transaction admission unavailable")
        self._active += 1
        if self._active >= self.config.limits.sends:
            self._available.clear()

    def _own(self, operation: Coroutine[Any, Any, _T]) -> asyncio.Task[_T]:
        task = asyncio.create_task(operation)
        self._operations.add(task)

        def complete(done: asyncio.Task[Any]) -> None:
            self._operations.discard(done)
            self._active -= 1
            assert self._available is not None
            self._available.set()
            if not done.cancelled():
                done.exception()

        task.add_done_callback(complete)
        return task

    async def reserve(self, request: Request, candidate: FrozenPlan) -> Reservation:
        self._validate_plan(candidate)
        request.match(candidate)
        await self._admit()
        return await asyncio.shield(
            self._own(
                self._database(
                    self.store.reserve(request, candidate, lease_seconds=self.config.limits.lease)
                )
            )
        )

    async def execute(self, execution: Execution, local_executor: LocalExecutor) -> Result:
        self._validate_plan(execution.plan)
        if not execution.owned or execution.writer_epoch != self.store.binding.writer_epoch:
            raise BindingError("exact owned execution required")
        await self._admit()
        cancelled = asyncio.Event()
        try:
            return await asyncio.shield(
                self._own(self._execute(execution, local_executor, cancelled))
            )
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def _proof(self, e: Execution) -> LocalOutcome:
        async def read() -> tuple[Evidence | None, ...]:
            return tuple([await self.store.evidence(r) for r in e.references])

        try:
            evidence = await self._database(read())
            return decide_local(e.references, evidence)
        except Exception:
            return LocalOutcome.UNKNOWN

    async def _execute(
        self, e: Execution, local: LocalExecutor, cancelled: asyncio.Event
    ) -> Result:
        branches = [
            BranchResult(r, HalfOutcome.NOT_ATTEMPTED, FinalOutcome.NOT_ATTEMPTED)
            for r in e.references
        ]
        local_result = LocalOutcome.NOT_STARTED
        handles: list[Any] = [None] * len(branches)

        def result() -> Result:
            return Result(local_result, tuple(branches))

        async def finalize(decision: Decision) -> bool:
            failed = False
            for i, handle in enumerate(handles):
                if handle is None:
                    continue
                outcome, code = FinalOutcome.UNKNOWN, "final_rpc_unknown"
                try:
                    await self._call(
                        handle.commit if decision == Decision.COMMIT else handle.rollback
                    )
                    outcome = (
                        FinalOutcome.COMMIT_CONFIRMED
                        if decision == Decision.COMMIT
                        else FinalOutcome.ROLLBACK_CONFIRMED
                    )
                    code = ""
                except Exception:
                    failed = True
                branches[i] = replace(
                    branches[i], final=outcome, error_code=code or branches[i].error_code
                )
                try:
                    await self._database(
                        self.store.record_final(e, branches[i].reference, outcome, code)
                    )
                except Exception:
                    branches[i] = replace(branches[i], error_code="final_audit_failed")
                    failed = True
            return failed

        try:
            await self._database(self.store.begin_prepare(e))
        except StaleExecutionError:
            local_result = await self._proof(e)
            raise TransactionSendError("execution_already_consumed", result()) from None
        except Exception:
            raise TransactionSendError("prepare_admission_failed", result()) from None

        async def abort_unstarted() -> None:
            try:
                aborted = await self._database(self.store.abort(e))
                if aborted or await self._proof(e) == LocalOutcome.ABORTED:
                    await finalize(Decision.ROLLBACK)
            except Exception:
                pass

        for i, branch in enumerate(e.plan.branches):
            if cancelled.is_set():
                await abort_unstarted()
                raise TransactionSendError("caller_cancelled_before_local", result())
            try:
                receipt, handles[i] = await self._call(self._prepare, branches[i].reference, branch)
            except Exception as error:
                code = getattr(error, "code", None)
                rejected = isinstance(code, int) and (
                    40000 <= code < 40600 or 41300 <= code < 41400
                )
                receipt = HalfReceipt(
                    HalfOutcome.REJECTED if rejected else HalfOutcome.UNKNOWN,
                    error_code="half_rejected" if rejected else "half_rpc_unknown",
                )
            branches[i] = replace(
                branches[i],
                half=receipt.outcome,
                broker_message_id=receipt.broker_message_id,
                broker_transaction_id=receipt.broker_transaction_id,
                error_code=receipt.error_code,
            )
            failed = receipt.outcome != HalfOutcome.PREPARED
            try:
                await self._database(self.store.record_half(e, branches[i].reference, receipt))
            except Exception:
                failed = True
                branches[i] = replace(branches[i], error_code="half_audit_failed")
            if failed:
                await abort_unstarted()
                raise TransactionSendError("all_halves_not_confirmed", result())
        if cancelled.is_set():
            await abort_unstarted()
            raise TransactionSendError("caller_cancelled_before_local", result())
        # Invoke once. Cooperative cancellation/lease expiration asks the host
        # UOW to stop, then STILL awaits its actual completion before proof/RPC.
        reported = LocalOutcome.UNKNOWN
        callback_failed = False

        async def run_local() -> LocalOutcome:
            return await local(e)

        local_task = asyncio.create_task(run_local())
        cancel_wait = asyncio.create_task(cancelled.wait())
        try:
            remaining = max(0, (e.lease_until - datetime.now(UTC)).total_seconds())
            await asyncio.wait(
                (local_task, cancel_wait), timeout=remaining, return_when=asyncio.FIRST_COMPLETED
            )
            if not local_task.done():
                local_task.cancel()
            try:
                reported = await local_task
            except (Exception, asyncio.CancelledError):
                callback_failed = True
                self._local_errors += 1
        finally:
            cancel_wait.cancel()
            await asyncio.gather(cancel_wait, return_exceptions=True)
        local_result = await self._proof(e)
        if local_result == LocalOutcome.UNKNOWN and reported == LocalOutcome.ABORTED:
            try:
                if await self._database(self.store.abort(e)):
                    local_result = LocalOutcome.ABORTED
            except Exception:
                pass
        if local_result == LocalOutcome.UNKNOWN:
            raise TransactionSendError("local_outcome_unknown_do_not_repeat_business", result())
        if await finalize(
            Decision.COMMIT if local_result == LocalOutcome.COMMITTED else Decision.ROLLBACK
        ):
            raise TransactionSendError("final_outcome_unknown", result())
        if callback_failed:
            raise TransactionSendError("local_callback_error_with_terminal_proof", result())
        return result()

    def _prepare(self, reference: Reference, branch: FrozenBranch) -> tuple[HalfReceipt, Any]:
        from rocketmq.v5.model import Message  # type: ignore[import-untyped]

        message = Message()
        message.topic, message.body = branch.physical_topic, branch.wire
        message.keys = branch.message.message_id
        message.add_property(REFERENCE_PROPERTY, encode_reference(reference).decode())
        handle = self._client.begin_transaction()
        receipt = self._client.send(message, handle)
        return HalfReceipt(HalfOutcome.PREPARED, receipt.message_id, receipt.transaction_id), handle

    async def _check(self, message: Any) -> int:
        from rocketmq.grpc_protocol import TransactionResolution  # type: ignore[import-untyped]

        try:
            route = self._routes.get(message.topic)
            ref = decode_reference(message.properties.get(REFERENCE_PROPERTY, "").encode())
            if (
                route is None
                or ref.owner != self.config.owner
                or ref.physical_topic != message.topic
                or ref.logical_topic != route.logical_topic
                or ref.route_revision != route.revision
                or len(message.body) > 262144
                or hashlib.sha256(message.body).digest() != ref.wire_hash
            ):
                raise BindingError("invalid actual route or wire")
        except Exception:
            self._invalid_checks += 1
            self._check_unknown += 1
            return int(TransactionResolution.TRANSACTION_RESOLUTION_UNSPECIFIED)
        try:
            decision = resolve(ref, await self._database(self.store.evidence(ref)))
        except Exception:
            decision = Decision.UNKNOWN
        if decision == Decision.COMMIT:
            self._check_commit += 1
            return int(TransactionResolution.COMMIT)
        if decision == Decision.ROLLBACK:
            self._check_rollback += 1
            return int(TransactionResolution.ROLLBACK)
        self._check_unknown += 1
        return int(TransactionResolution.TRANSACTION_RESOLUTION_UNSPECIFIED)

    def stop_admission(self) -> None:
        self._stopping = True
        if self._available is not None:
            self._available.set()

    def _ensure_close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())

    async def _close(self) -> None:
        if self._start_task is not None:
            await asyncio.gather(self._start_task, return_exceptions=True)
        if self._operations:
            await asyncio.gather(*tuple(self._operations), return_exceptions=True)
        if self._maintenance_stop is not None:
            self._maintenance_stop.set()
        if self._maintenance is not None:
            await self._maintenance
        if self._pool is not None:
            await self._pool.drain()
        if self._calls:
            await asyncio.gather(*tuple(self._calls), return_exceptions=True)
        try:
            if self._client is not None:
                await self._call(self._client.shutdown)
        finally:
            if self._executor is not None:
                self._executor.shutdown(wait=False)
        self._started = False
        self._stopped = True

    async def close(self, *, grace_seconds: float = 10) -> None:
        if grace_seconds < 0:
            raise ValueError("nonnegative shutdown budget required")
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        self._assert_loop()
        self.stop_admission()
        self._ensure_close()
        assert self._close_task is not None
        budget = asyncio.timeout(grace_seconds)
        try:
            async with budget:
                await asyncio.shield(self._close_task)
        except TimeoutError:
            if not budget.expired():
                raise
            error = ShutdownIncompleteError("transaction resources still running", self.stats())
            raise error from None

    async def wait(self) -> None:
        self._assert_loop()
        if self._close_task is None:
            raise RuntimeError("Close has not started")
        await asyncio.shield(self._close_task)

    def stats(self) -> dict[str, int | bool]:
        pool = self._pool.snapshot() if self._pool is not None else {}
        pending = pool.get("pending", 0)
        return {
            "pending_calls": len(self._calls),
            "starting": self._start_task is not None and not self._start_task.done(),
            "active_sends": self._active,
            "pending_checks": pending,
            "running_checks": pool.get("running", 0),
            "check_rejected": pool.get("rejected", 0),
            "callback_errors": pool.get("failed", 0),
            "local_errors": self._local_errors,
            "check_commit": self._check_commit,
            "check_rollback": self._check_rollback,
            "check_unknown": self._check_unknown,
            "invalid_checks": self._invalid_checks,
            "database_errors": self._database_errors,
            "reconciled": self._reconciled,
            "stopping": self._stopping,
            "stopped": self._stopped,
            "ready": self._started
            and not self._stopping
            and self._db_healthy
            and pending < self.config.limits.check_workers + self.config.limits.check_queue,
        }
