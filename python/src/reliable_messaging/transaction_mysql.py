"""Original MySQL transaction witnesses, separate from Outbox.

The host supplies primary-only session factories and explicitly installs SCHEMA.
Constructors perform no I/O. No engine, pool, schema or business transaction is owned.
"""

import secrets
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from typing import Any, cast

from sqlalchemy import text
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession, async_sessionmaker

from reliable_messaging.transaction import (
    Binding,
    BindingError,
    Evidence,
    Execution,
    FinalOutcome,
    FrozenPlan,
    HalfReceipt,
    Reference,
    ReplanAuthorization,
    Request,
    Reservation,
    RetryAuthorization,
    SchemaError,
    StaleExecutionError,
    State,
    branch_key,
    decode_plan,
    decode_reference,
    encode_plan,
    encode_reference,
    execution_key,
    validate_advance,
)

SCHEMA = files("reliable_messaging").joinpath("transaction_mysql.sql").read_text()
SCHEMA_VERSION = 1
_CLOCK = "%Y-%m-%d %H:%M:%S.%f"
_DB = AsyncSession | AsyncConnection


def _str(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _key(e: Execution) -> bytes:
    return execution_key(e.plan.owner, e.plan.operation_id, e.writer_epoch, e.fence)


def _lease(seconds: float) -> None:
    if not 0.001 <= seconds <= 86400:
        raise ValueError("lease must be 1ms..24h")


class MySQLTransactionStore:
    def __init__(self, sessions: async_sessionmaker[AsyncSession], binding: Binding) -> None:
        if (
            max(len(v.encode()) for v in (binding.owner, binding.store_id, binding.writer_epoch))
            > 128
        ):
            raise BindingError("binding exceeds binary schema limits")
        self.sessions, self.binding = sessions, binding

    async def validate(self) -> None:
        """Validate explicitly installed schema/binding; never execute DDL."""
        async with self.sessions() as db, db.begin():
            await self._binding(db, writer=True)
            result = await db.execute(
                text(
                    "SELECT r.request_key,e.execution_key,w.reference,w.broker_message_id,"
                    "w.half_outcome,w.final_outcome FROM rm_tx_request r,rm_tx_execution e,"
                    "rm_tx_witness w WHERE FALSE"
                )
            )
            result.close()

    async def _receipt(
        self, e: Execution, ref: Reference, values: dict[str, Any], *, final: bool
    ) -> None:
        if not e.owned or ref not in e.references or ref.owner != self.binding.owner:
            raise BindingError("exact owned execution required")
        async with self.sessions() as db, db.begin():
            await self._binding(db, writer=False)
            row = (
                (
                    await db.execute(
                        text(
                            "SELECT state,token FROM rm_tx_execution "
                            "WHERE execution_key=:key FOR UPDATE"
                        ),
                        {"key": _key(e)},
                    )
                )
                .mappings()
                .one()
            )
            if bytes(row["token"]) != e.token:
                raise StaleExecutionError("execution capability differs")
            branch = (
                (
                    await db.execute(
                        text(
                            "SELECT state,reference,broker_message_id,broker_transaction_id,"
                            "half_outcome,final_outcome FROM rm_tx_witness "
                            "WHERE execution_key=:key "
                            "AND branch_key=:branch FOR UPDATE"
                        ),
                        {"key": _key(e), "branch": branch_key(ref.branch_id)},
                    )
                )
                .mappings()
                .one()
            )
            state = State(_str(row["state"]))
            if decode_reference(branch["reference"]) != ref or _str(branch["state"]) != state:
                raise StaleExecutionError("branch proof differs")
            if final:
                outcome = values["outcome"]
                previous = _str(branch["final_outcome"])
                if previous not in ("not_attempted", "unknown", outcome):
                    raise StaleExecutionError("positive final receipt cannot be downgraded")
                if (
                    state not in (State.COMMITTED, State.ABORTED)
                    or (outcome == FinalOutcome.COMMIT_CONFIRMED and state != State.COMMITTED)
                    or (outcome == FinalOutcome.ROLLBACK_CONFIRMED and state != State.ABORTED)
                ):
                    raise StaleExecutionError("final receipt contradicts local evidence")
                sql = (
                    "UPDATE rm_tx_witness SET final_outcome=:outcome,final_error_code=:code,"
                    "final_recorded_at=UTC_TIMESTAMP(6) "
                    "WHERE execution_key=:key AND branch_key=:branch"
                )
            else:
                if state != State.PREPARED:
                    raise StaleExecutionError("half audit requires prepared witness")
                if (
                    _str(branch["half_outcome"])
                    not in ("not_attempted", "unknown", values["outcome"])
                    or (
                        branch["broker_message_id"]
                        and bytes(branch["broker_message_id"]) != values["message"]
                    )
                    or (
                        branch["broker_transaction_id"]
                        and bytes(branch["broker_transaction_id"]) != values["transaction"]
                    )
                ):
                    raise StaleExecutionError("original Broker receipt cannot be replaced")
                sql = (
                    "UPDATE rm_tx_witness SET half_outcome=:outcome,broker_message_id=:message,"
                    "broker_transaction_id=:transaction,half_error_code=:code,"
                    "half_recorded_at=UTC_TIMESTAMP(6) "
                    "WHERE execution_key=:key AND branch_key=:branch"
                )
            await db.execute(
                text(sql), {**values, "key": _key(e), "branch": branch_key(ref.branch_id)}
            )

    async def record_recovery_final(self, ref: Reference, outcome: FinalOutcome) -> None:
        """Positive recovery RPC audit, without an execution capability.

        This fresh protocol transaction cannot commit business or change witness
        state. The caller must have observed EndTransaction OK for this exact
        Reference; a Checker decision alone is insufficient.
        """
        if outcome == FinalOutcome.COMMIT_CONFIRMED:
            expected = State.COMMITTED
        elif outcome == FinalOutcome.ROLLBACK_CONFIRMED:
            expected = State.ABORTED
        else:
            raise ValueError("positive recovery confirmation required")
        if not isinstance(ref, Reference) or ref.owner != self.binding.owner:
            raise BindingError("configured recovery owner required")
        key = execution_key(ref.owner, ref.operation_id, ref.writer_epoch, ref.fence)
        branch_id = branch_key(ref.branch_id)
        async with self.sessions() as db, db.begin():
            await self._binding(db, writer=False)
            row = (
                (
                    await db.execute(
                        text(
                            "SELECT owner,operation_id,writer_epoch,fence,state "
                            "FROM rm_tx_execution "
                            "WHERE execution_key=:key FOR UPDATE"
                        ),
                        {"key": key},
                    )
                )
                .mappings()
                .one()
            )
            if (
                _str(row["owner"]) != ref.owner
                or _str(row["operation_id"]) != ref.operation_id
                or _str(row["writer_epoch"]) != ref.writer_epoch
                or row["fence"] != ref.fence
                or _str(row["state"]) != expected
            ):
                raise StaleExecutionError("recovery execution differs or is not terminal")
            branch = (
                (
                    await db.execute(
                        text(
                            "SELECT reference,state,final_outcome FROM rm_tx_witness "
                            "WHERE execution_key=:key AND branch_key=:branch FOR UPDATE"
                        ),
                        {"key": key, "branch": branch_id},
                    )
                )
                .mappings()
                .one()
            )
            previous = _str(branch["final_outcome"])
            if (
                decode_reference(branch["reference"]) != ref
                or _str(branch["state"]) != expected
                or previous not in ("not_attempted", "unknown", outcome)
            ):
                raise StaleExecutionError("recovery branch proof differs")
            if previous == outcome:
                return  # Preserve the first positive confirmation timestamp.
            await db.execute(
                text(
                    "UPDATE rm_tx_witness SET final_outcome=:outcome,final_error_code='',"
                    "final_recorded_at=UTC_TIMESTAMP(6) "
                    "WHERE execution_key=:key AND branch_key=:branch"
                ),
                {"outcome": outcome, "key": key, "branch": branch_id},
            )

    async def record_half(self, e: Execution, ref: Reference, receipt: HalfReceipt) -> None:
        await self._receipt(
            e,
            ref,
            {
                "outcome": receipt.outcome,
                "message": receipt.broker_message_id.encode(),
                "transaction": receipt.broker_transaction_id.encode(),
                "code": receipt.error_code.encode(),
            },
            final=False,
        )

    async def record_final(
        self, e: Execution, ref: Reference, outcome: FinalOutcome, code: str
    ) -> None:
        if (
            outcome
            not in (
                FinalOutcome.COMMIT_CONFIRMED,
                FinalOutcome.ROLLBACK_CONFIRMED,
                FinalOutcome.UNKNOWN,
            )
            or len(code.encode()) > 128
        ):
            raise ValueError("invalid final receipt")
        await self._receipt(e, ref, {"outcome": outcome, "code": code.encode()}, final=True)

    async def _binding(self, db: _DB, *, writer: bool) -> None:
        await _validate_root(db)
        result = await db.execute(
            text(
                "SELECT store_id,writer_epoch,schema_version FROM rm_tx_binding "
                "WHERE owner=:owner FOR SHARE"
            ),
            {"owner": self.binding.owner.encode()},
        )
        row = result.mappings().one_or_none()
        if row is None or _str(row["store_id"]) != self.binding.store_id:
            raise BindingError("original store identity differs")
        if writer and _str(row["writer_epoch"]) != self.binding.writer_epoch:
            raise BindingError("writer epoch differs")
        if row["schema_version"] != SCHEMA_VERSION:
            raise SchemaError("transaction schema version differs")

    async def _load(self, db: _DB, active: bytes, *, capability: bool = False) -> Reservation:
        result = await db.execute(
            text(
                "SELECT plan,fence,writer_epoch,token,"
                "DATE_FORMAT(lease_until,'%Y-%m-%d %H:%i:%s.%f') AS lease_until,state "
                "FROM rm_tx_execution WHERE execution_key=:key"
            ),
            {"key": active},
        )
        row = result.mappings().one()
        plan = decode_plan(row["plan"])
        e = Execution(
            plan,
            row["fence"],
            _str(row["writer_epoch"]),
            bytes(row["token"]),
            datetime.strptime(_str(row["lease_until"]), _CLOCK).replace(tzinfo=UTC),
        )
        if _key(e) != active:
            raise BindingError("stored execution identity differs")
        if not capability:
            e = replace(e, token=bytes(32))
        return Reservation(e, capability, State(_str(row["state"])))

    async def _lookup(self, db: _DB, request: Request, *, lock: bool = False) -> Reservation | None:
        sql = (
            "SELECT owner,scope,idempotency_key,input_hash,active_execution "
            "FROM rm_tx_request WHERE request_key=:key"
        )
        result = await db.execute(
            text(sql + (" FOR UPDATE" if lock else "")), {"key": request.key_digest}
        )
        row = result.mappings().one_or_none()
        if row is None:
            return None
        if (_str(row["owner"]), _str(row["scope"]), _str(row["idempotency_key"])) != (
            request.owner,
            request.scope,
            request.key,
        ):
            raise BindingError("request identity differs")
        if bytes(row["input_hash"]) != request.input_hash:
            raise ValueError("same idempotency key has different business input")
        reserved = await self._load(db, bytes(row["active_execution"]))
        request.match(reserved.execution.plan)
        return reserved

    async def lookup(self, request: Request) -> Reservation | None:
        if request.owner != self.binding.owner:
            raise BindingError("owner differs")
        async with self.sessions() as db, db.begin():
            await self._binding(db, writer=False)
            return await self._lookup(db, request)

    async def _new(self, db: _DB, plan: FrozenPlan, fence: int, lease: float) -> Execution:
        now = (
            await db.execute(text("SELECT DATE_FORMAT(UTC_TIMESTAMP(6),'%Y-%m-%d %H:%i:%s.%f')"))
        ).scalar_one()
        until = datetime.strptime(_str(now), _CLOCK).replace(tzinfo=UTC) + timedelta(seconds=lease)
        return Execution(plan, fence, self.binding.writer_epoch, secrets.token_bytes(32), until)

    async def _insert(self, db: _DB, request: Request, e: Execution) -> None:
        await db.execute(
            text(
                "INSERT INTO rm_tx_execution(execution_key,request_key,owner,operation_id,"
                "writer_epoch,fence,token,plan,state,lease_until) "
                "VALUES(:key,:request,:owner,:operation,:epoch,:fence,:token,:plan,'prepared',:lease)"
            ),
            {
                "key": _key(e),
                "request": request.key_digest,
                "owner": e.plan.owner.encode(),
                "operation": e.plan.operation_id.encode(),
                "epoch": e.writer_epoch.encode(),
                "fence": e.fence,
                "token": e.token,
                "plan": encode_plan(e.plan),
                "lease": e.lease_until.astimezone(UTC).strftime(_CLOCK),
            },
        )
        for ref in e.references:
            await db.execute(
                text(
                    "INSERT INTO rm_tx_witness(execution_key,branch_key,reference,state) "
                    "VALUES(:key,:branch,:reference,'prepared')"
                ),
                {
                    "key": _key(e),
                    "branch": branch_key(ref.branch_id),
                    "reference": encode_reference(ref),
                },
            )

    async def reserve(
        self, request: Request, candidate: FrozenPlan, *, lease_seconds: float = 60
    ) -> Reservation:
        _lease(lease_seconds)
        request.match(candidate)
        if request.owner != self.binding.owner:
            raise BindingError("owner differs")
        try:
            async with self.sessions() as db, db.begin():
                await self._binding(db, writer=True)
                e = await self._new(db, candidate, 1, lease_seconds)
                await db.execute(
                    text(
                        "INSERT INTO rm_tx_request(request_key,owner,scope,idempotency_key,"
                        "input_hash,active_execution) "
                        "VALUES(:key,:owner,:scope,:idem,:hash,:active)"
                    ),
                    {
                        "key": request.key_digest,
                        "owner": request.owner.encode(),
                        "scope": request.scope.encode(),
                        "idem": request.key.encode(),
                        "hash": request.input_hash,
                        "active": _key(e),
                    },
                )
                await self._insert(db, request, e)
                result = Reservation(e, True, State.PREPARED)
            return result
        except IntegrityError as error:
            if getattr(error.orig, "args", (None,))[0] != 1062:
                raise
            observed = await self.lookup(request)
            if observed is None:
                raise StaleExecutionError(
                    "duplicate operation belongs to another request"
                ) from error
            return observed

    async def begin_prepare(self, e: Execution) -> None:
        if (
            not e.owned
            or e.plan.owner != self.binding.owner
            or e.writer_epoch != self.binding.writer_epoch
        ):
            raise BindingError("owned execution with active writer epoch required")
        async with self.sessions() as db, db.begin():
            await self._binding(db, writer=True)
            result = await db.execute(
                text(
                    "UPDATE rm_tx_execution SET prepare_started=TRUE WHERE execution_key=:key "
                    "AND token=:token AND state='prepared' AND prepare_started=FALSE "
                    "AND lease_until>UTC_TIMESTAMP(6)"
                ),
                {"key": _key(e), "token": e.token},
            )
            if cast(CursorResult[Any], result).rowcount != 1:
                raise StaleExecutionError("prepare capability already consumed or expired")
            stored = await self._load(db, _key(e), capability=True)
            if stored.execution.plan.digest != e.plan.digest:
                raise StaleExecutionError("frozen plan differs")

    async def evidence(self, reference: Reference) -> Evidence | None:
        if reference.owner != self.binding.owner:
            raise BindingError("owner differs")
        async with self.sessions() as db, db.begin():
            await self._binding(db, writer=False)
            return await self._evidence(db, reference)

    async def _evidence(self, db: _DB, ref: Reference) -> Evidence | None:
        result = await db.execute(
            text(
                "SELECT w.reference,e.state AS execution_state,w.state AS branch_state "
                "FROM rm_tx_execution e JOIN rm_tx_witness w ON w.execution_key=e.execution_key "
                "WHERE e.execution_key=:key AND w.branch_key=:branch"
            ),
            {
                "key": execution_key(ref.owner, ref.operation_id, ref.writer_epoch, ref.fence),
                "branch": branch_key(ref.branch_id),
            },
        )
        row = result.mappings().one_or_none()
        if row is None:
            return None
        return Evidence(
            decode_reference(row["reference"]),
            State(_str(row["execution_state"])),
            State(_str(row["branch_state"])),
        )

    async def abort(self, execution: Execution) -> bool:
        return await self._abort(execution, expired=False)

    async def _abort(self, e: Execution, *, expired: bool) -> bool:
        if e.plan.owner != self.binding.owner or not e.owned:
            raise BindingError("owner or execution capability differs")
        async with self.sessions() as db, db.begin():
            await self._binding(db, writer=False)
            result = await db.execute(
                text(
                    "UPDATE rm_tx_execution SET state='aborted',guard_version=guard_version+1 "
                    "WHERE execution_key=:key AND state='prepared' AND token=:token"
                    + (" AND lease_until<=UTC_TIMESTAMP(6)" if expired else "")
                ),
                {"key": _key(e), "token": e.token},
            )
            if cast(CursorResult[Any], result).rowcount != 1:
                return False
            result = await db.execute(
                text(
                    "UPDATE rm_tx_witness SET state='aborted' "
                    "WHERE execution_key=:key AND state='prepared'"
                ),
                {"key": _key(e)},
            )
            if cast(CursorResult[Any], result).rowcount != len(e.references):
                raise StaleExecutionError("incomplete prepared witness bundle")
            return True

    async def reconcile_once(self, limit: int) -> int:
        if not 1 <= limit <= 1000:
            raise ValueError("bounded scan limit required")
        async with self.sessions() as db, db.begin():
            await self._binding(db, writer=False)
            result = await db.execute(
                text(
                    "SELECT execution_key FROM rm_tx_execution WHERE owner=:owner "
                    "AND state='prepared' AND lease_until<=UTC_TIMESTAMP(6) "
                    "ORDER BY lease_until LIMIT :limit"
                ),
                {"owner": self.binding.owner.encode(), "limit": limit},
            )
            executions = [
                (await self._load(db, bytes(key), capability=True)).execution
                for key in result.scalars()
            ]
        count = 0
        for e in executions:
            count += await self._abort(e, expired=True)
        return count

    async def retry(
        self, authorization: RetryAuthorization, *, lease_seconds: float = 60
    ) -> Reservation:
        return await self._advance(
            authorization.previous, authorization.previous.plan, lease_seconds, replan=False
        )

    async def replan(
        self, authorization: ReplanAuthorization, *, lease_seconds: float = 60
    ) -> Reservation:
        return await self._advance(
            authorization.previous, authorization.candidate, lease_seconds, replan=True
        )

    async def _advance(
        self, previous: Execution, candidate: FrozenPlan, lease: float, *, replan: bool
    ) -> Reservation:
        _lease(lease)
        request = Request(
            previous.plan.owner,
            previous.plan.scope,
            previous.plan.idempotency_key,
            previous.plan.input_hash,
        )
        if request.owner != self.binding.owner:
            raise BindingError("owner differs")
        async with self.sessions() as db, db.begin():
            await self._binding(db, writer=True)
            current = await self._lookup(db, request, lock=True)
            if (
                current is None
                or _key(current.execution) != _key(previous)
                or current.execution.plan.digest != previous.plan.digest
            ):
                raise StaleExecutionError("root request advanced")
            evidence = tuple([await self._evidence(db, ref) for ref in previous.references])
            validate_advance(previous, candidate, evidence, replan=replan)
            e = await self._new(db, candidate, previous.fence + 1, lease)
            await self._insert(db, request, e)
            await db.execute(
                text("UPDATE rm_tx_request SET active_execution=:active WHERE request_key=:key"),
                {"active": _key(e), "key": request.key_digest},
            )
            return Reservation(e, True, State.PREPARED)

    async def bind(self, db: _DB, e: Execution) -> "MySQLTransactionGuard":
        if not e.owned:
            raise BindingError("execution capability required")
        guard = MySQLTransactionGuard(db, e)
        await guard.validate()
        if e.plan.owner != self.binding.owner or e.writer_epoch != self.binding.writer_epoch:
            raise BindingError("owner or epoch differs")
        await self._binding(db, writer=True)
        result = await db.execute(
            text(
                "UPDATE rm_tx_execution SET guard_version=guard_version+1 WHERE execution_key=:key "
                "AND token=:token AND state='prepared' AND lease_until>UTC_TIMESTAMP(6)"
            ),
            {"key": _key(e), "token": e.token},
        )
        if cast(CursorResult[Any], result).rowcount != 1:
            raise StaleExecutionError("execution no longer owned/prepared")
        stored = await self._load(db, _key(e), capability=True)
        if stored.execution.plan.digest != e.plan.digest:
            raise StaleExecutionError("stored frozen plan differs")
        for ref in e.references:
            evidence = await self._evidence(db, ref)
            if (
                evidence is None
                or evidence.reference != ref
                or evidence.branch_state != State.PREPARED
            ):
                raise StaleExecutionError("missing or mismatched prepared witness")
        return guard


class MySQLTransactionGuard:
    def __init__(self, db: _DB, execution: Execution) -> None:
        self.db, self.execution = db, execution
        self.root = db.get_transaction()
        self.finished = False

    async def validate(self) -> None:
        if (
            self.finished
            or self.root is None
            or not self.root.is_active
            or self.db.get_transaction() is not self.root
            or self.db.get_nested_transaction() is not None
        ):
            raise BindingError("active original root transaction required")
        await _validate_root(self.db)

    async def commit_witness(self) -> None:
        await self.validate()
        result = await self.db.execute(
            text(
                "UPDATE rm_tx_witness SET state='committed' "
                "WHERE execution_key=:key AND state='prepared'"
            ),
            {"key": _key(self.execution)},
        )
        if cast(CursorResult[Any], result).rowcount != len(self.execution.references):
            raise StaleExecutionError("incomplete prepared witness bundle")
        result = await self.db.execute(
            text(
                "UPDATE rm_tx_execution SET state='committed' WHERE execution_key=:key "
                "AND token=:token AND state='prepared'"
            ),
            {"key": _key(self.execution), "token": self.execution.token},
        )
        if cast(CursorResult[Any], result).rowcount != 1:
            raise StaleExecutionError("execution changed")
        self.finished = True


async def _validate_root(db: _DB) -> None:
    root = db.get_transaction()
    if root is None or not root.is_active or db.get_nested_transaction() is not None:
        raise BindingError("active original root transaction required")
    if isinstance(db, AsyncSession):
        if not isinstance(db.bind, AsyncEngine):
            raise BindingError("session joined to external connection/transaction is unsupported")
        connection = await db.connection()
    else:
        connection = db
    if connection.get_nested_transaction() is not None or connection.dialect.name != "mysql":
        raise BindingError("original MySQL root transaction required")
    raw = await connection.get_raw_connection()
    autocommit = getattr(raw.driver_connection, "get_autocommit", None)
    if autocommit is None or autocommit():
        raise BindingError("physical MySQL AUTOCOMMIT is unsupported")
