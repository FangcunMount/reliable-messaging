import asyncio
import os
import sys
from pathlib import Path

import pytest
from mysql_tables import business, metadata, queue, receipts
from sqlalchemy import func, select, text
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from reliable_messaging.sqlalchemy import MySQLPendingOutbox, TransactionBindingError, bind

pytestmark = pytest.mark.integration
EVENT = {
    "event_id": "original",
    "id": 18446744073709551615,
    "text": "青岛😀",
    "default": False,
    "unknown": None,
    "at": "2026-10-03T12:00:00.123456789+08:00",
}


@pytest.fixture
async def storage():
    url = os.getenv("RM_M7_MYSQL_URL")
    if not url:
        pytest.fail("Required disposable RM_M7_MYSQL_URL is missing")
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    try:
        yield engine, async_sessionmaker(engine, expire_on_commit=False)
    finally:
        async with engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
        await engine.dispose()


async def append(db, key="original", event=EVENT):
    statement = insert(queue).values(
        event_id=key, payload=event, available_at=func.utc_timestamp(6)
    )
    await bind(db).append(statement.on_duplicate_key_update(event_id=queue.c.event_id))


async def count(sessions, table):
    async with sessions() as db:
        return await db.scalar(select(func.count()).select_from(table))


async def test_original_transaction_commit_rollback_and_failed_append(storage):
    engine, sessions = storage
    async with sessions.begin() as db:
        await db.execute(insert(business).values(id="committed", wire=b"same bytes"))
        await append(db)
    assert await count(sessions, business) == await count(sessions, queue) == 1
    with pytest.raises(RuntimeError, match="host abort"):
        async with sessions.begin() as db:
            await db.execute(insert(business).values(id="aborted"))
            await append(db, "aborted")
            raise RuntimeError("host abort")
    with pytest.raises(IntegrityError) as failure:
        async with sessions.begin() as db:
            await db.execute(insert(business).values(id="bad-append"))
            await bind(db).append(insert(queue).values(event_id=None, payload={}))
    assert failure.value.orig.args[0] == 1048
    assert await count(sessions, business) == await count(sessions, queue) == 1
    # The SDK did not dispose the borrowed engine.
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT 1")) == 1


async def test_binding_refuses_autobegin_replacement_and_savepoint_change(storage):
    engine, sessions = storage
    async with sessions() as db:
        with pytest.raises(TransactionBindingError):
            bind(db)
        await db.begin()
        original = bind(db)
        await db.commit()
        with pytest.raises(TransactionBindingError):
            await original.append(insert(business).values(id="no-autobegin"))
        assert not db.in_transaction()
        await db.begin()
        with pytest.raises(TransactionBindingError):
            await original.append(insert(business).values(id="replacement"))
        current = bind(db)
        async with db.begin_nested():
            with pytest.raises(TransactionBindingError):
                await current.append(insert(business).values(id="nested-mismatch"))
            await bind(db).append(insert(business).values(id="nested-ok"))
        await db.rollback()
    async with engine.connect() as conn:
        async with conn.begin():
            await bind(conn).append(insert(business).values(id="connection-ok"))
    assert await count(sessions, business) == 1


async def test_cancelled_host_transaction_rolls_back_both(storage):
    _, sessions = storage
    ready = asyncio.Event()

    async def host():
        async with sessions.begin() as db:
            await db.execute(insert(business).values(id="cancelled"))
            await append(db, "cancelled")
            ready.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(host())
    await ready.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await count(sessions, business) == await count(sessions, queue) == 0


async def test_json_identity_duplicate_settlement_and_timezone(storage):
    _, sessions = storage
    adapter = MySQLPendingOutbox(queue, max_retry_seconds=3)
    for zone in ("+00:00", "+08:00", "-05:00"):
        async with sessions.begin() as db:
            await db.execute(text("SET SESSION time_zone=:zone"), {"zone": zone})
            await append(db)
            rows = await adapter.pending(db, 20)
            assert rows == [EVENT]
    async with sessions.begin() as db:
        await append(db, event={"changed": True})
        assert await db.scalar(select(queue.c.payload)) == EVENT
        await adapter.retry(db, "original")
        row = (await db.execute(select(queue))).mappings().one()
        assert row["attempts"] == 1 and row["payload"] == EVENT
        delay = await db.scalar(
            select(
                func.timestampdiff(text("MICROSECOND"), func.utc_timestamp(6), queue.c.available_at)
            )
        )
        assert 0 < delay <= 3_000_000
        await adapter.delivered(db, "original")
        delivered_at = await db.scalar(select(queue.c.delivered_at))
        await adapter.retry(db, "original")
        await adapter.delivered(db, "original")
        assert await db.scalar(select(queue.c.attempts)) == 1
        assert await db.scalar(select(queue.c.delivered_at)) == delivered_at
    async with sessions() as db:
        with pytest.raises(TransactionBindingError):
            await adapter.delivered(db, "original")
        assert await adapter.pending(db, 20) == []


async def spawn(phase):
    return await asyncio.create_subprocess_exec(
        sys.executable,
        str(Path(__file__).with_name("probe_process.py")),
        phase,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )


async def until(process, marker):
    async with asyncio.timeout(10):
        while True:
            line = await process.stdout.readline()
            if line.strip() == marker.encode():
                return
            if not line:
                pytest.fail((await process.stderr.read()).decode())


async def terminate(process):
    if process.returncode is None:
        process.kill()
    await process.communicate()


async def test_process_kill_before_commit_then_restart_rolls_back(storage):
    _, sessions = storage
    process = await spawn("before-commit")
    try:
        await until(process, "appended")
    finally:
        await terminate(process)
    assert process.returncode < 0
    assert await count(sessions, business) == await count(sessions, queue) == 0
    async with sessions.begin() as db:
        await append(db)
    restart = await spawn("deliver")
    try:
        async with asyncio.timeout(10):
            output, error = await restart.communicate()
        assert restart.returncode == 0, error.decode()
        assert b"complete" in output
    finally:
        if restart.returncode is None:
            await terminate(restart)
    assert await count(sessions, receipts) == 1


async def test_durable_acceptance_receipt_lost_then_process_restart(storage):
    _, sessions = storage
    async with sessions.begin() as db:
        await append(db)
    process = await spawn("ack-lost")
    try:
        await until(process, "accepted")
    finally:
        await terminate(process)
    async with sessions() as db:
        assert await db.scalar(select(queue.c.delivered)) is False
        assert await db.scalar(select(receipts.c.payload)) == EVENT
    restart = await spawn("deliver")
    try:
        async with asyncio.timeout(10):
            _, error = await restart.communicate()
        assert restart.returncode == 0, error.decode()
    finally:
        if restart.returncode is None:
            await terminate(restart)
    async with sessions() as db:
        assert await db.scalar(select(queue.c.delivered)) is True
        assert await db.scalar(select(queue.c.payload)) == EVENT
    assert await count(sessions, receipts) == 1


async def test_lost_wake_after_startup_scan_recovers_committed_intent(storage):
    _, sessions = storage
    process = await spawn("poll")
    try:
        await until(process, "scanned")
        async with sessions.begin() as db:
            await append(db)  # deliberately no wake signal to the child process
        async with asyncio.timeout(10):
            _, error = await process.communicate()
        assert process.returncode == 0, error.decode()
    finally:
        if process.returncode is None:
            await terminate(process)
    assert await count(sessions, receipts) == 1
    async with sessions() as db:
        assert await db.scalar(select(queue.c.delivered)) is True
