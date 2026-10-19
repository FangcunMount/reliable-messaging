"""Host-owned ORIGINAL SQLAlchemy root transaction; no import-time I/O or DDL.

Host separately installs schema and host_business_events(id), constructs a store
with its own session factory, configures owner/revision routes and starts sender.
"""
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from reliable_messaging.rocketmq_transaction import TransactionalSender
from reliable_messaging.transaction import FrozenPlan, LocalOutcome, Request, Result
from reliable_messaging.transaction_mysql import MySQLTransactionStore


async def send_original(
    sender: TransactionalSender,
    store: MySQLTransactionStore,
    sessions: async_sessionmaker[AsyncSession],
    plan: FrozenPlan,
) -> Result:
    reservation = await sender.reserve(
        Request(plan.owner, plan.scope, plan.idempotency_key, plan.input_hash), plan
    )
    if not reservation.acquired:
        raise RuntimeError("observe original request; duplicate does not authorize execution")

    async def local(execution):
        try:
            async with sessions() as session, session.begin():
                guard = await store.bind(session, execution)  # first, original root UOW
                await session.execute(
                    text("INSERT INTO host_business_events(id) VALUES(:id)"),
                    {"id": execution.plan.idempotency_key.encode()},
                )
                await guard.commit_witness()
            return LocalOutcome.COMMITTED
        except Exception:
            # A failed Commit reply cannot prove Rollback. SDK reads exact
            # authoritative witness through a fresh session before deciding.
            return LocalOutcome.UNKNOWN

    return await sender.execute(reservation.execution, local)
