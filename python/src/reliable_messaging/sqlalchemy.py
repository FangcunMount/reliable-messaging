"""Borrow original transactions and an existing host table; no engine or migrations."""

from typing import Any

from sqlalchemy import Table, func, literal_column, select, update
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession
from sqlalchemy.sql.dml import Insert


class TransactionBindingError(ValueError):
    pass


class TransactionAppender:
    def __init__(self, db: AsyncSession | AsyncConnection) -> None:
        self._db = db
        self._transaction = db.get_transaction()
        self._nested = db.get_nested_transaction()
        self._check()

    def _check(self) -> None:
        if (
            self._transaction is None
            or not self._transaction.is_active
            or self._db.get_transaction() is not self._transaction
            or self._db.get_nested_transaction() is not self._nested
            or (self._nested is not None and not self._nested.is_active)
        ):
            raise TransactionBindingError("active original SQLAlchemy transaction required")

    async def append(self, statement: Insert) -> None:
        self._check()
        if not isinstance(statement, Insert):
            raise TypeError("append requires a host-defined INSERT")
        # The host supplies the schema, values and duplicate identity policy.
        # Never begin, commit, roll back, close or dispose a borrowed resource.
        await self._db.execute(statement)


def bind(db: AsyncSession | AsyncConnection) -> TransactionAppender:
    return TransactionAppender(db)


class MySQLPendingOutbox:
    """Existing pending/delivered JSON outbox, not the Go leased-store schema.

    Supports a single host process and duplicate-safe durable receiver. No claim,
    fencing, quarantine or broker adapter is promised by this minimal adapter.
    Callers own every read/write transaction and commit after settlement.
    """

    def __init__(self, table: Table, *, max_retry_seconds: int = 60) -> None:
        required = {"event_id", "payload", "delivered", "attempts", "available_at", "delivered_at"}
        if not required.issubset(table.c.keys()) or max_retry_seconds < 1:
            raise ValueError("existing outbox columns and positive retry cap required")
        self._table = table
        self._max_retry_seconds = max_retry_seconds

    async def pending(self, db: AsyncSession, limit: int) -> list[Any]:
        if not 1 <= limit <= 100:
            raise ValueError("Batch limit must be 1..100")
        table = self._table
        rows = await db.execute(
            select(table.c.payload)
            .where(table.c.delivered.is_(False), table.c.available_at <= func.utc_timestamp(6))
            .order_by(table.c.available_at, table.c.event_id)
            .limit(limit)
        )
        return list(rows.scalars())

    async def delivered(self, db: AsyncSession, event_id: str) -> None:
        self._require_transaction(db)
        table = self._table
        await db.execute(
            update(table)
            .where(table.c.event_id == event_id, table.c.delivered.is_(False))
            .values(delivered=True, delivered_at=func.utc_timestamp(6))
        )

    async def retry(self, db: AsyncSession, event_id: str) -> None:
        self._require_transaction(db)
        table = self._table
        await db.execute(
            update(table)
            .where(table.c.event_id == event_id, table.c.delivered.is_(False))
            .values(
                attempts=table.c.attempts + 1,
                available_at=func.timestampadd(
                    literal_column("SECOND"),
                    func.least(
                        self._max_retry_seconds,
                        func.pow(2, func.least(table.c.attempts, 17)),
                    ),
                    func.utc_timestamp(6),
                ),
            )
        )

    @staticmethod
    def _require_transaction(db: AsyncSession) -> None:
        bind(db)  # Fail before execute can autobegin a settlement transaction.
