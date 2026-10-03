from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    text,
)
from sqlalchemy.dialects.mysql import DATETIME

metadata = MetaData()
business = Table(
    "m7_business",
    metadata,
    Column("id", String(128), primary_key=True),
    Column("wire", LargeBinary),
)
queue = Table(
    "m7_events",
    metadata,
    Column("event_id", String(128), primary_key=True),
    Column("payload", JSON, nullable=False),
    Column("delivered", Boolean, nullable=False, server_default=text("0")),
    Column("attempts", Integer, nullable=False, server_default=text("0")),
    Column("available_at", DATETIME(fsp=6), nullable=False),
    Column("delivered_at", DATETIME(fsp=6)),
)
receipts = Table(
    "m7_receipts",
    metadata,
    Column("event_id", String(128), primary_key=True),
    Column("payload", JSON),
)
