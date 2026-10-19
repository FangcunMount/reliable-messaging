import asyncio
import dataclasses
import hashlib
import os
import secrets
from datetime import UTC, datetime

import pytest
import pytest_asyncio
from sqlalchemy import text
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
