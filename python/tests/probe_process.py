"""Subprocess fault probe: no models, brokers or qs-ai imports."""

import asyncio
import os
import sys
from functools import partial

from mysql_tables import business, queue, receipts
from sqlalchemy import func, select
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from reliable_messaging import PeriodicRelay, deliver_durable
from reliable_messaging.sqlalchemy import MySQLPendingOutbox, bind


async def run():
    engine = create_async_engine(os.environ["RM_M7_MYSQL_URL"])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    adapter = MySQLPendingOutbox(queue)
    done = asyncio.Event()

    class Store:
        async def delivered(self, key):
            async with sessions.begin() as db:
                await adapter.delivered(db, key)

        async def retry(self, key):
            async with sessions.begin() as db:
                await adapter.retry(db, key)

    async def accept(event):
        async with sessions.begin() as db:
            statement = insert(receipts).values(event_id=event["event_id"], payload=event)
            await db.execute(statement.on_duplicate_key_update(event_id=receipts.c.event_id))
            previous = await db.scalar(
                select(receipts.c.payload).where(receipts.c.event_id == event["event_id"])
            )
            assert previous == event
        print("accepted", flush=True)
        if sys.argv[1] == "ack-lost":
            await asyncio.Event().wait()  # killed after durable acceptance, before local settlement

    async def attempt():
        async with sessions() as db:
            rows = await adapter.pending(db, 20)
        for event in rows:
            await deliver_durable(event["event_id"], Store(), partial(accept, event))
            done.set()
        print("scanned", flush=True)
        return len(rows)

    try:
        if sys.argv[1] == "before-commit":
            async with sessions.begin() as db:
                await db.execute(insert(business).values(id="uncommitted", wire=b"original"))
                await bind(db).append(
                    insert(queue).values(
                        event_id="uncommitted",
                        payload={"event_id": "uncommitted"},
                        available_at=func.utc_timestamp(6),
                    )
                )
                print("appended", flush=True)
                await asyncio.Event().wait()
        elif sys.argv[1] == "poll":
            relay = PeriodicRelay(attempt, poll_seconds=0.02)
            await relay.start()
            async with asyncio.timeout(10):
                await done.wait()
            await relay.stop()
        else:
            await attempt()
        print("complete", flush=True)
    finally:
        await engine.dispose()  # host-owned resource, deliberately not closed by the SDK


if __name__ == "__main__":
    asyncio.run(run())
