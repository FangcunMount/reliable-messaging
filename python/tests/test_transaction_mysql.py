import asyncio
import dataclasses
import hashlib
import os
import secrets
from datetime import UTC, datetime

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from reliable_messaging.message import Message
from reliable_messaging.transaction import (
    Binding,
    BindingError,
    Decision,
    FrozenBranch,
    FrozenPlan,
    ReplanAuthorization,
    Request,
    RetryAuthorization,
    StaleExecutionError,
    StoreChecker,
)
from reliable_messaging.transaction_mysql import SCHEMA, MySQLTransactionStore

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="module")]


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def fixture():
    url = os.environ.get("RM_M7_MYSQL_URL")
    if not url or not os.environ.get("RM_TX_DISPOSABLE"):
        pytest.fail("required owned disposable transaction MySQL fixture missing")
    engine = create_async_engine(url, pool_size=10, max_overflow=15)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store_id = secrets.token_hex(16)
    store = MySQLTransactionStore(sessions, Binding("test.mysql", store_id, "epoch"))
    tables = []
    try:
        async with engine.begin() as db:
            for statement in SCHEMA.split(";"):
                if not statement.strip():
                    continue
                await db.execute(text(statement))
                # Record exactly the tables this fixture successfully CREATED.
                tables.append(statement.split("CREATE TABLE ")[1].split()[0])
            await db.execute(
                text("INSERT INTO rm_tx_binding VALUES(:owner,:store,'epoch',1)"),
                {"owner": b"test.mysql", "store": store_id.encode()},
            )
            await db.execute(
                text("CREATE TABLE rm_tx_business_proof(id VARBINARY(128) PRIMARY KEY, value INT)")
            )
            tables.append("rm_tx_business_proof")
        yield engine, sessions, store
    finally:
        async with engine.begin() as db:
            for table in reversed(tables):
                # Names originate only from the SDK's fixed schema, not inputs.
                await db.execute(text("DROP TABLE " + table))
        await engine.dispose()


def plan(identity, key=None):
    message = Message(
        "fixture",
        identity,
        "logical",
        "created",
        "1",
        "scope",
        "application/octet-stream",
        "2026-10-19T00:00:00.000000123Z",
        b"\0\xff",
    )
    return FrozenPlan(
        "test.mysql",
        "scope",
        "operation-" + identity,
        key or identity,
        hashlib.sha256(b"same business input").digest(),
        (FrozenBranch("branch", "physical", "v2", message, b"\0\xff\x01"),),
    )


async def reserve(store, identity):
    p = plan(identity)
    result = await store.reserve(Request(p.owner, p.scope, p.idempotency_key, p.input_hash), p)
    assert result.acquired and result.execution.owned
    return result.execution


async def decision(store, execution):
    return await StoreChecker(store).check(execution.references[0])


async def test_business_and_witness_share_original_root(fixture):
    _, sessions, store = fixture
    execution = await reserve(store, "commit")
    async with sessions() as db:
        async with db.begin():
            guard = await store.bind(db, execution)
            await db.execute(text("INSERT INTO rm_tx_business_proof VALUES('commit',1)"))
            await guard.commit_witness()
            assert await decision(store, execution) == Decision.UNKNOWN
        assert await decision(store, execution) == Decision.COMMIT
        with pytest.raises(BindingError):
            await guard.commit_witness()
    assert not await store.abort(execution)


async def test_rollback_never_leaves_committed_witness(fixture):
    _, sessions, store = fixture
    execution = await reserve(store, "rollback")
    async with sessions() as db:
        await db.begin()
        guard = await store.bind(db, execution)
        await db.execute(text("INSERT INTO rm_tx_business_proof VALUES('rollback',1)"))
        await guard.commit_witness()
        await db.rollback()
    assert await decision(store, execution) == Decision.UNKNOWN
    async with sessions() as db:
        assert (
            await db.execute(text("SELECT COUNT(*) FROM rm_tx_business_proof WHERE id='rollback'"))
        ).scalar_one() == 0
    assert await store.abort(execution)
    assert await decision(store, execution) == Decision.ROLLBACK


async def test_concurrent_reservation_hides_winner_capability(fixture):
    _, _, store = fixture

    async def attempt(i):
        p = plan("parallel-" + str(i), "parallel")
        return await store.reserve(Request(p.owner, p.scope, p.idempotency_key, p.input_hash), p)

    results = await asyncio.gather(*(attempt(i) for i in range(20)))
    assert sum(r.acquired for r in results) == 1
    assert len({r.execution.plan.digest for r in results}) == 1
    for r in results:
        assert r.execution.owned == r.acquired
    loser = next(r.execution for r in results if not r.acquired)
    with pytest.raises(BindingError):
        await store.abort(loser)
    p = dataclasses.replace(plan("conflict", "parallel"), input_hash=b"z" * 32)
    with pytest.raises(ValueError):
        await store.reserve(Request(p.owner, p.scope, p.idempotency_key, p.input_hash), p)


async def test_abort_fences_late_original_transaction_and_retry_is_explicit(fixture):
    _, sessions, store = fixture
    old = await reserve(store, "abort-first")
    assert await store.abort(old)
    async with sessions() as db, db.begin():
        with pytest.raises(StaleExecutionError):
            await store.bind(db, old)
    retry = await store.retry(RetryAuthorization(old))
    assert retry.execution.fence == old.fence + 1 and retry.execution.plan == old.plan
    assert await decision(store, old) == Decision.ROLLBACK
    with pytest.raises(StaleExecutionError):
        await store.retry(RetryAuthorization(old))
    assert await store.abort(retry.execution)
    new = await store.replan(
        ReplanAuthorization(retry.execution, plan("new-generation", "abort-first"))
    )
    assert new.execution.plan.operation_id != old.plan.operation_id
    assert await decision(store, old) == Decision.ROLLBACK


async def test_abort_cannot_cross_original_transaction_lock(fixture):
    _, sessions, store = fixture
    e = await reserve(store, "active")
    async with sessions() as db, db.begin():
        guard = await store.bind(db, e)
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.15):
                await store.abort(e)
        assert await decision(store, e) == Decision.UNKNOWN
        await guard.commit_witness()
    assert await decision(store, e) == Decision.COMMIT


async def test_root_savepoint_and_ended_binding_rejected(fixture):
    engine, sessions, store = fixture
    e = await reserve(store, "binding")
    async with sessions() as db:
        with pytest.raises(BindingError):
            await store.bind(db, e)
        async with db.begin():
            async with db.begin_nested():
                with pytest.raises(BindingError):
                    await store.bind(db, e)
            guard = await store.bind(db, e)
            async with db.begin_nested():
                with pytest.raises(BindingError):
                    await guard.commit_witness()
        with pytest.raises(BindingError):
            await guard.commit_witness()
    async with engine.connect() as connection, connection.begin():
        async with connection.begin_nested():
            with pytest.raises(BindingError):
                await store.bind(connection, e)


async def test_physical_autocommit_cannot_fake_original_transaction(fixture):
    engine, _, store = fixture
    e = await reserve(store, "autocommit")
    async with engine.connect() as connection:
        connection = await connection.execution_options(isolation_level="AUTOCOMMIT")
        async with connection.begin():
            with pytest.raises(BindingError):
                await store.bind(connection, e)


async def test_async_connection_root_supported_but_joined_session_rejected(fixture):
    engine, _, store = fixture
    e = await reserve(store, "connection")
    async with engine.connect() as connection, connection.begin():
        guard = await store.bind(connection, e)
        await connection.execute(text("INSERT INTO rm_tx_business_proof VALUES('connection',1)"))
        await guard.commit_witness()
    assert await decision(store, e) == Decision.COMMIT
    e = await reserve(store, "joined")
    async with engine.connect() as connection, connection.begin():
        joined = async_sessionmaker(connection)
        async with joined() as db, db.begin():
            with pytest.raises(BindingError):
                await store.bind(db, e)


async def test_missing_expired_and_wrong_identity_remain_unknown(fixture):
    _, sessions, store = fixture
    e = await reserve(store, "expiry")
    async with sessions() as db, db.begin():
        await db.execute(
            text("UPDATE rm_tx_execution SET lease_until=:expired WHERE operation_id=:operation"),
            {
                "expired": datetime(2000, 1, 1, tzinfo=UTC).strftime("%Y-%m-%d %H:%M:%S.%f"),
                "operation": e.plan.operation_id.encode(),
            },
        )
    assert await decision(store, e) == Decision.UNKNOWN
    assert await store.reconcile_once(100) == 1
    assert await decision(store, e) == Decision.ROLLBACK
    wrong = dataclasses.replace(e.references[0], writer_epoch="wrong")
    assert await StoreChecker(store).check(wrong) == Decision.UNKNOWN


async def test_prepare_admission_is_durable_and_one_shot(fixture):
    _, _, store = fixture
    e = await reserve(store, "prepare-once")
    await store.begin_prepare(e)
    with pytest.raises(StaleExecutionError):
        await store.begin_prepare(e)
    observed = await store.lookup(
        Request(e.plan.owner, e.plan.scope, e.plan.idempotency_key, e.plan.input_hash)
    )
    assert observed is not None and not observed.execution.owned
    with pytest.raises(BindingError):
        await store.begin_prepare(observed.execution)


async def test_broker_receipt_audit_cannot_replace_original_transaction_proof(fixture):
    from reliable_messaging.transaction import FinalOutcome, HalfOutcome, HalfReceipt

    _, sessions, store = fixture
    await store.validate()
    e = await reserve(store, "receipt")
    ref = e.references[0]
    await store.record_half(e, ref, HalfReceipt(HalfOutcome.PREPARED, "broker-id", "broker-tx"))
    with pytest.raises(StaleExecutionError):
        await store.record_half(e, ref, HalfReceipt(HalfOutcome.PREPARED, "different", "broker-tx"))
    with pytest.raises(StaleExecutionError):
        await store.record_half(e, ref, HalfReceipt(HalfOutcome.UNKNOWN))
    assert await decision(store, e) == Decision.UNKNOWN
    with pytest.raises(StaleExecutionError):
        await store.record_final(e, ref, FinalOutcome.COMMIT_CONFIRMED, "")
    async with sessions() as db, db.begin():
        guard = await store.bind(db, e)
        await db.execute(text("INSERT INTO rm_tx_business_proof VALUES('receipt',1)"))
        await guard.commit_witness()
    await store.record_final(e, ref, FinalOutcome.UNKNOWN, "rpc_unknown")
    assert await decision(store, e) == Decision.COMMIT
    async with sessions() as db:
        row = (
            await db.execute(
                text(
                    "SELECT w.broker_message_id,w.broker_transaction_id,w.final_outcome "
                    "FROM rm_tx_witness w JOIN rm_tx_execution e "
                    "ON w.execution_key=e.execution_key "
                    "WHERE e.operation_id=:operation"
                ),
                {"operation": e.plan.operation_id.encode()},
            )
        ).one()
        assert tuple(row) == (b"broker-id", b"broker-tx", b"unknown")

    await store.record_final(e, ref, FinalOutcome.COMMIT_CONFIRMED, "")
    with pytest.raises(StaleExecutionError):
        await store.record_final(e, ref, FinalOutcome.UNKNOWN, "late_timeout")


@pytest.mark.parametrize("committed", [True, False])
async def test_recovery_audit_exact_terminal_reference_without_winner_capability(
    fixture, committed
):
    from reliable_messaging.transaction import FinalOutcome, State, branch_key, execution_key

    _, sessions, store = fixture
    identity = f"recovery-audit-{committed}"
    execution = await reserve(store, identity)
    ref = execution.references[0]
    with pytest.raises(StaleExecutionError):
        await store.record_recovery_final(ref, FinalOutcome.COMMIT_CONFIRMED)
    outcome = FinalOutcome.COMMIT_CONFIRMED if committed else FinalOutcome.ROLLBACK_CONFIRMED
    if committed:
        async with sessions() as db, db.begin():
            guard = await store.bind(db, execution)
            await db.execute(
                text("INSERT INTO rm_tx_business_proof VALUES(:id,1)"), {"id": identity}
            )
            await guard.commit_witness()
    else:
        assert await store.abort(execution)
    await store.record_final(execution, ref, FinalOutcome.UNKNOWN, "lost_ack")
    engine = fixture[0]
    autocommit_store = MySQLTransactionStore(
        async_sessionmaker(engine.execution_options(isolation_level="AUTOCOMMIT")), store.binding
    )
    with pytest.raises(BindingError, match="AUTOCOMMIT"):
        await autocommit_store.record_recovery_final(ref, outcome)
    async with engine.connect() as external:
        externally_bound = MySQLTransactionStore(async_sessionmaker(external), store.binding)
        with pytest.raises(BindingError, match="external"):
            await externally_bound.record_recovery_final(ref, outcome)
    observer = MySQLTransactionStore(sessions, store.binding)
    observed = await observer.lookup(
        Request(
            execution.plan.owner,
            execution.plan.scope,
            execution.plan.idempotency_key,
            execution.plan.input_hash,
        )
    )
    assert not observed.execution.owned
    await observer.record_recovery_final(ref, outcome)
    params = {
        "key": execution_key(ref.owner, ref.operation_id, ref.writer_epoch, ref.fence),
        "branch": branch_key(ref.branch_id),
    }

    async def audit():
        async with sessions() as db:
            return (
                await db.execute(
                    text(
                        "SELECT final_outcome,final_recorded_at FROM rm_tx_witness "
                        "WHERE execution_key=:key AND branch_key=:branch"
                    ),
                    params,
                )
            ).one()

    first = await audit()
    assert bytes(first[0]).decode() == outcome
    await observer.record_recovery_final(ref, outcome)
    assert await audit() == first
    for bad in (
        dataclasses.replace(ref, owner="wrong"),
        dataclasses.replace(ref, fence=ref.fence + 1),
        dataclasses.replace(ref, wire_hash=b"x" * 32),
    ):
        with pytest.raises((BindingError, StaleExecutionError, NoResultFound)):
            await observer.record_recovery_final(bad, outcome)
    with pytest.raises(ValueError):
        await observer.record_recovery_final(ref, FinalOutcome.UNKNOWN)
    opposite = FinalOutcome.ROLLBACK_CONFIRMED if committed else FinalOutcome.COMMIT_CONFIRMED
    with pytest.raises(StaleExecutionError):
        await observer.record_recovery_final(ref, opposite)
    wrong = MySQLTransactionStore(sessions, Binding(ref.owner, "wrong-store", ref.writer_epoch))
    with pytest.raises(BindingError):
        await wrong.record_recovery_final(ref, outcome)
    with pytest.raises(StaleExecutionError):
        await store.record_final(execution, ref, FinalOutcome.UNKNOWN, "late_timeout")
    assert await decision(store, execution) == (Decision.COMMIT if committed else Decision.ROLLBACK)
    async with sessions() as db:
        assert (
            await db.execute(
                text("SELECT COUNT(*) FROM rm_tx_business_proof WHERE id=:id"), {"id": identity}
            )
        ).scalar_one() == int(committed)
        assert bytes(
            (
                await db.execute(
                    text(
                        "SELECT state FROM rm_tx_witness "
                        "WHERE execution_key=:key AND branch_key=:branch"
                    ),
                    params,
                )
            ).scalar_one()
        ).decode() == (State.COMMITTED if committed else State.ABORTED)


async def test_read_only_owner_protocol_observation(fixture):
    from reliable_messaging.transaction import FinalOutcome, HalfOutcome, HalfReceipt, SchemaError

    engine, sessions, _ = fixture
    binding = Binding("observe.mysql", "observation-store", "epoch")
    async with engine.begin() as db:
        await db.execute(
            text("INSERT INTO rm_tx_binding VALUES(:owner,:store,'epoch',1)"),
            {"owner": binding.owner.encode(), "store": binding.store_id.encode()},
        )
    store = MySQLTransactionStore(sessions, binding)
    empty = await store.observe_protocol()
    assert empty.as_of.tzinfo == UTC and empty.confirmation_gap_branches == 0

    async def make_execution(identity):
        p = plan("observe-" + identity)
        p = dataclasses.replace(
            p,
            owner=binding.owner,
            branches=p.branches + (dataclasses.replace(p.branches[0], branch_id="second"),),
        )
        reservation = await store.reserve(
            Request(p.owner, p.scope, p.idempotency_key, p.input_hash), p
        )
        assert reservation.acquired
        return reservation.execution

    await make_execution("reserved")
    preparing = await make_execution("preparing")
    await store.begin_prepare(preparing)
    async with engine.begin() as db:
        await db.execute(
            text(
                "UPDATE rm_tx_execution SET lease_until=UTC_TIMESTAMP(6)-INTERVAL 2 MINUTE "
                "WHERE owner=:owner AND operation_id=:operation"
            ),
            {"owner": binding.owner.encode(), "operation": preparing.plan.operation_id.encode()},
        )
    gap = await make_execution("gap")
    await store.begin_prepare(gap)
    for ref in gap.references:
        await store.record_half(gap, ref, HalfReceipt(HalfOutcome.UNKNOWN))
    assert await store.abort(gap)
    async with engine.begin() as db:
        await db.execute(
            text(
                "UPDATE rm_tx_witness w JOIN rm_tx_execution e USING(execution_key) "
                "SET w.half_recorded_at=UTC_TIMESTAMP(6)-INTERVAL 10 MINUTE "
                "WHERE e.owner=:owner AND e.operation_id=:operation"
            ),
            {"owner": binding.owner.encode(), "operation": gap.plan.operation_id.encode()},
        )
    untracked = await make_execution("untracked")
    await store.begin_prepare(untracked)
    assert await store.abort(untracked)
    committed = await make_execution("committed")
    async with sessions() as db, db.begin():
        guard = await store.bind(db, committed)
        await guard.commit_witness()
        before = await store.observe_protocol()
        assert before.unobserved_committed_branches == 0
    snapshot = await store.observe_protocol()
    assert snapshot.reserved_executions == snapshot.preparing_executions == 1
    assert snapshot.prepared_expiry_age_seconds >= 120
    assert snapshot.confirmation_gap_branches == 2
    assert snapshot.oldest_confirmation_observation_age_seconds >= 600
    assert snapshot.untracked_aborted_branches == snapshot.unobserved_committed_branches == 2
    assert snapshot.metadata_conflicts == snapshot.unaged_confirmation_gap_branches == 0
    for ref in gap.references:
        await store.record_recovery_final(ref, FinalOutcome.ROLLBACK_CONFIRMED)
    snapshot = await store.observe_protocol()
    assert snapshot.confirmation_gap_branches == 0 and snapshot.untracked_aborted_branches == 2
    wrong = MySQLTransactionStore(sessions, dataclasses.replace(binding, writer_epoch="wrong"))
    with pytest.raises(BindingError):
        await wrong.observe_protocol()
    for budget in (0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            await store.observe_protocol(budget_seconds=budget)
    async with engine.begin() as db:
        await db.execute(
            text("UPDATE rm_tx_binding SET schema_version=2 WHERE owner=:owner"),
            {"owner": binding.owner.encode()},
        )
    with pytest.raises(SchemaError):
        await store.observe_protocol()
    async with engine.begin() as db:
        await db.execute(
            text("UPDATE rm_tx_binding SET schema_version=1 WHERE owner=:owner"),
            {"owner": binding.owner.encode()},
        )
        await db.execute(
            text(
                "UPDATE rm_tx_witness w JOIN rm_tx_execution e USING(execution_key) "
                "SET w.half_outcome='invented' WHERE e.owner=:owner"
            ),
            {"owner": binding.owner.encode()},
        )
    with pytest.raises(ValueError):
        await store.observe_protocol()
