"""MySQL adapter for broker publishing followed by separate durable acceptance.

Hosts declare/migrate tables, append with bind(db), commit every operation, validate
authenticated receipts and own retry authorization. This adapter never starts a
transaction, scheduler or connection, and never closes a borrowed resource.
"""

from dataclasses import dataclass
from typing import Any

from sqlalchemy import Table, exists, func, literal_column, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from reliable_messaging.sqlalchemy import bind

STAGED = "staged"
AWAITING_RECEIPT = "awaiting_receipt"
CONFIRMED = "confirmed"
HELD = "held"


class MessageConflict(ValueError):
    """Unknown identity or mismatched original body; cannot authorize confirmation."""


@dataclass(frozen=True)
class Identity:
    producer: str
    destination: str
    message_id: str


class MySQLDurableOutbox:
    def __init__(self, table: Table) -> None:
        required = {
            "producer",
            "destination",
            "message_id",
            "body_sha256",
            "body",
            "wire",
            "topic",
            "aggregate_key",
            "aggregate_sequence",
            "ordered",
            "requires_receipt",
            "stage",
            "attempts",
            "available_at",
            "published_at",
            "confirmed_at",
            "error_code",
        }
        if not required.issubset(table.c.keys()):
            raise ValueError("durable acceptance outbox columns required")
        self.table = table

    def _identity(self, identity: Identity) -> Any:
        if not all((identity.producer, identity.destination, identity.message_id)):
            raise MessageConflict("complete message identity required")
        c = self.table.c
        return (
            (c.producer == identity.producer)
            & (c.destination == identity.destination)
            & (c.message_id == identity.message_id)
        )

    async def _locked(self, db: AsyncSession, identity: Identity, body_sha256: str) -> Any:
        await bind(db).validate()
        result = await db.execute(
            select(self.table).where(self._identity(identity)).with_for_update()
        )
        row = result.mappings().one_or_none()
        if row is None or row["body_sha256"] != body_sha256:
            raise MessageConflict("original message body mismatch or identity missing")
        return row

    async def pending(self, db: AsyncSession, limit: int = 20) -> list[Any]:
        await bind(db).validate()
        if not 1 <= limit <= 100:
            raise ValueError("bounded pending batch required")
        table, older = self.table, self.table.alias("older")
        blocked = exists(
            select(older.c.message_id).where(
                older.c.producer == table.c.producer,
                older.c.destination == table.c.destination,
                older.c.aggregate_key == table.c.aggregate_key,
                older.c.ordered.is_(True),
                older.c.stage != CONFIRMED,
                older.c.aggregate_sequence < table.c.aggregate_sequence,
            )
        )
        query = (
            select(table)
            .where(
                table.c.stage.in_((STAGED, AWAITING_RECEIPT)),
                table.c.available_at <= func.utc_timestamp(6),
                (~table.c.ordered) | (~blocked),
            )
            .order_by(table.c.available_at, table.c.message_id)
            .limit(limit)
        )
        return list((await db.execute(query)).mappings())

    @staticmethod
    def _later(seconds: int) -> Any:
        if not 1 <= seconds <= 60:
            raise ValueError("delay must be bounded to 1..60 seconds")
        return func.timestampadd(literal_column("SECOND"), seconds, func.utc_timestamp(6))

    async def published(
        self,
        db: AsyncSession,
        identity: Identity,
        body_sha256: str,
        *,
        receipt_wait_seconds: int = 30,
    ) -> None:
        later = self._later(receipt_wait_seconds)
        row = await self._locked(db, identity, body_sha256)
        if row["stage"] in (CONFIRMED, HELD):
            return  # a late publisher callback cannot undo a durable acknowledgement or hold
        receipt = bool(row["requires_receipt"])
        await db.execute(
            update(self.table)
            .where(self._identity(identity))
            .values(
                stage=AWAITING_RECEIPT if receipt else CONFIRMED,
                # Receipt-free ACKs finish at PUB OK. Only failed/uncertain PUB
                # spends their budget; duplicate successful PUB is notification.
                attempts=self.table.c.attempts + 1 if receipt else self.table.c.attempts,
                published_at=func.utc_timestamp(6),
                confirmed_at=None if receipt else func.utc_timestamp(6),
                available_at=later,
                error_code="",
            )
        )

    async def retry(
        self,
        db: AsyncSession,
        identity: Identity,
        body_sha256: str,
        *,
        delay_seconds: int,
        error_code: str = "publish_unknown",
    ) -> None:
        later = self._later(delay_seconds)
        row = await self._locked(db, identity, body_sha256)
        if row["stage"] in (CONFIRMED, HELD):
            return
        await db.execute(
            update(self.table)
            .where(self._identity(identity))
            .values(
                attempts=self.table.c.attempts + 1,
                available_at=later,
                error_code=error_code,
            )
        )

    async def confirm(self, db: AsyncSession, identity: Identity, body_sha256: str) -> None:
        """Call only after host verification of the authenticated business receipt."""
        row = await self._locked(db, identity, body_sha256)
        if not row["requires_receipt"]:
            raise MessageConflict(
                "receipt-free acknowledgements cannot accept another acknowledgement"
            )
        await db.execute(
            update(self.table)
            .where(self._identity(identity))
            .values(
                stage=CONFIRMED,
                confirmed_at=func.utc_timestamp(6),
                error_code="",
            )
        )

    async def hold(
        self,
        db: AsyncSession,
        identity: Identity,
        body_sha256: str,
        *,
        error_code: str,
    ) -> None:
        row = await self._locked(db, identity, body_sha256)
        if row["stage"] == CONFIRMED:
            return
        await db.execute(
            update(self.table)
            .where(self._identity(identity))
            .values(
                stage=HELD,
                error_code=error_code,
            )
        )

    async def rearm_ack(self, db: AsyncSession, identity: Identity, body_sha256: str) -> None:
        """Duplicate committed events regenerate notification for the same immutable ack."""
        row = await self._locked(db, identity, body_sha256)
        if row["requires_receipt"]:
            raise MessageConflict("only receipt-free acknowledgements can be automatically rearmed")
        if row["stage"] == HELD:
            return  # duplicates cannot revoke a persistent hold or renew its budget
        await db.execute(
            update(self.table)
            .where(self._identity(identity))
            .values(
                stage=STAGED,
                available_at=func.utc_timestamp(6),
                confirmed_at=None,
                error_code="",
            )
        )
