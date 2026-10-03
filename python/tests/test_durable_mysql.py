import asyncio
import base64
import hashlib
import json
import os
import subprocess
import uuid
from pathlib import Path

import pytest
from sqlalchemy import MetaData, Table, func, select, text
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from reliable_messaging.durable import (
    AWAITING_RECEIPT,
    CONFIRMED,
    HELD,
    STAGED,
    Identity,
    MessageConflict,
    MySQLDurableOutbox,
)
from reliable_messaging.sqlalchemy import TransactionBindingError, bind

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]
BODY = '{"id":18446744073709551615,"text":"原始正文🙂"}'.encode()
HASH = hashlib.sha256(BODY).hexdigest()
WIRE = b"immutable-ciphertext-saved-once"
ID = Identity("qs-server", "qs-ai", "original-command")


@pytest.fixture
async def storage():
    url = os.environ.get("RM_M7_MYSQL_URL")
    if not url:
        pytest.fail("Required disposable RM_M7_MYSQL_URL is missing")
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.execute(text((ROOT / "delivery/mysql/schema.sql").read_text()))
        table = await conn.run_sync(
            lambda c: Table("rm_durable_outbox", MetaData(), autoload_with=c)
        )
    try:
        yield engine, async_sessionmaker(engine, expire_on_commit=False), table
    finally:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE rm_durable_outbox"))
        await engine.dispose()


async def append(db, table, identity=ID, *, sequence=1, receipt=True):
    await bind(db).append(
        insert(table).values(
            producer=identity.producer,
            destination=identity.destination,
            message_id=identity.message_id,
            body_sha256=HASH,
            body=BODY,
            wire=WIRE,
            topic="qs.ai.commands.v1",
            aggregate_key="original",
            aggregate_sequence=sequence,
            ordered=receipt,
            requires_receipt=receipt,
            stage=STAGED,
            available_at=func.utc_timestamp(6),
            created_at=func.utc_timestamp(6),
            attempts=0,
            error_code="",
        )
    )


async def row(sessions, table, identity=ID):
    async with sessions.begin() as db:
        return (
            (await db.execute(select(table).where(table.c.message_id == identity.message_id)))
            .mappings()
            .one()
        )


async def test_atomic_rollback_and_refuse_autobegin(storage):
    engine, sessions, table = storage
    store = MySQLDurableOutbox(table)
    with pytest.raises(RuntimeError):
        async with sessions.begin() as db:
            await append(db, table)
            raise RuntimeError("host transaction failed")
    async with sessions() as db:
        with pytest.raises(TransactionBindingError):
            await store.pending(db)
        assert not db.in_transaction()
    async with sessions.begin() as db:
        assert await store.pending(db) == []
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT 1"))).scalar() == 1


async def test_broker_confirmation_not_acceptance_and_late_callbacks_do_not_regress(storage):
    _, sessions, table = storage
    store = MySQLDurableOutbox(table)
    async with sessions.begin() as db:
        await append(db, table)
    async with sessions.begin() as db:
        await store.published(db, ID, HASH)
    original = await row(sessions, table)
    assert original["stage"] == AWAITING_RECEIPT and original["confirmed_at"] is None
    with pytest.raises(MessageConflict):
        async with sessions.begin() as db:
            await store.confirm(db, ID, "0" * 64)
    with pytest.raises(MessageConflict):
        async with sessions.begin() as db:
            await store.confirm(db, Identity(ID.producer, "wrong", ID.message_id), HASH)
    async with sessions.begin() as db:
        await store.confirm(db, ID, HASH)
    async with sessions.begin() as db:
        await store.published(db, ID, HASH)
        await store.retry(db, ID, HASH, delay_seconds=1)
        await store.hold(db, ID, HASH, error_code="late_failure")
    final = await row(sessions, table)
    assert final["stage"] == CONFIRMED and final["attempts"] == original["attempts"]
    assert final["wire"] == WIRE and final["body"] == BODY


async def test_per_aggregate_order_and_held_work_not_automatically_replayed(storage):
    _, sessions, table = storage
    store = MySQLDurableOutbox(table)
    second = Identity(ID.producer, ID.destination, "second-command")
    async with sessions.begin() as db:
        await append(db, table)
        await append(db, table, second, sequence=18446744073709551615)
    async with sessions.begin() as db:
        assert [r["message_id"] for r in await store.pending(db)] == [ID.message_id]
        await store.hold(db, ID, HASH, error_code="storage_failure")
    async with sessions.begin() as db:
        assert await store.pending(db) == []
        with pytest.raises(MessageConflict):
            await store.rearm_ack(db, ID, HASH)
    assert (await row(sessions, table))["stage"] == HELD
    async with sessions.begin() as db:
        await store.confirm(db, ID, HASH)  # authenticated late durable acceptance remains valid
    async with sessions.begin() as db:
        pending = await store.pending(db)
        assert pending[0]["message_id"] == second.message_id
        assert pending[0]["aggregate_sequence"] == 18446744073709551615


async def test_lost_final_ack_rearms_original_wire_without_ack_of_ack(storage):
    _, sessions, table = storage
    store = MySQLDurableOutbox(table)
    async with sessions.begin() as db:
        await append(db, table, receipt=False)
        await store.published(db, ID, HASH)
    original = await row(sessions, table)
    assert original["stage"] == CONFIRMED
    with pytest.raises(MessageConflict):
        async with sessions.begin() as db:
            await store.confirm(db, ID, HASH)
    async with sessions.begin() as db:
        await store.rearm_ack(db, ID, HASH)
    restored = await row(sessions, table)
    assert restored["wire"] == original["wire"] and restored["created_at"] == original["created_at"]
    async with sessions.begin() as db:
        assert len(await store.pending(db)) == 1


async def test_successful_final_ack_replays_do_not_spend_failure_budget(storage):
    _, sessions, table = storage
    store = MySQLDurableOutbox(table)
    async with sessions.begin() as db:
        await append(db, table, receipt=False)
        await store.retry(db, ID, HASH, delay_seconds=1)
        await store.retry(db, ID, HASH, delay_seconds=1)
    original = await row(sessions, table)
    assert original["attempts"] == 2
    for _ in range(16):
        async with sessions.begin() as db:
            await store.published(db, ID, HASH)
        published = await row(sessions, table)
        assert published["stage"] == CONFIRMED and published["attempts"] == 2
        assert published["wire"] == WIRE and published["body"] == BODY
        assert published["created_at"] == original["created_at"]
        async with sessions.begin() as db:
            await store.rearm_ack(db, ID, HASH)
    async with sessions.begin() as db:
        await store.retry(db, ID, HASH, delay_seconds=1)
    assert (await row(sessions, table))["attempts"] == 3


async def test_final_ack_hold_survives_duplicate_without_new_budget(storage):
    _, sessions, table = storage
    store = MySQLDurableOutbox(table)
    async with sessions.begin() as db:
        await append(db, table, receipt=False)
        for _ in range(8):
            await store.retry(db, ID, HASH, delay_seconds=1)
        await store.hold(db, ID, HASH, error_code="delivery_budget_exhausted")
    before = await row(sessions, table)
    async with sessions.begin() as db:
        await store.rearm_ack(db, ID, HASH)
        await store.published(db, ID, HASH)
    after = await row(sessions, table)
    assert after["stage"] == HELD and after["attempts"] == 8
    assert after["error_code"] == before["error_code"]
    assert after["wire"] == before["wire"] and after["body"] == before["body"]
    async with sessions.begin() as db:
        assert await store.pending(db) == []


async def test_go_and_python_share_durable_states_and_restart_scan(storage, tmp_path):
    engine, sessions, table = storage
    async with sessions.begin() as db:
        await append(db, table)
    binary = tmp_path / "probe"
    await asyncio.to_thread(
        subprocess.run,
        ["go", "build", "-o", str(binary), "./tests/integration/protectedprobe"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    url = make_url(os.environ["RM_M7_MYSQL_URL"])
    dsn = f"{url.username}:{url.password}@tcp({url.host}:{url.port})/{url.database}?parseTime=true"
    result = await asyncio.to_thread(
        subprocess.run,
        [str(binary)],
        input=json.dumps({"mode": "outbox-publish", "dsn": dsn}).encode(),
        check=True,
        capture_output=True,
    )
    assert base64.b64decode(result.stdout) == WIRE
    assert (await row(sessions, table))["stage"] == AWAITING_RECEIPT
    # A publisher process exited; its durable wire and receipt wait survive.
    await engine.dispose()
    async with sessions.begin() as db:
        await db.execute(text("UPDATE rm_durable_outbox SET available_at=UTC_TIMESTAMP(6)"))
        pending = await MySQLDurableOutbox(table).pending(db)
        assert pending[0]["wire"] == WIRE and pending[0]["body"] == BODY
        await MySQLDurableOutbox(table).confirm(db, ID, HASH)


async def test_broker_crash_loss_and_lost_business_receipt_recover_original_wire(storage):
    """Fault only a disposable broker created here; never the configured shared endpoint."""
    from test_nsq_integration import http_json, prepare

    from reliable_messaging.delivery import Outcome
    from reliable_messaging.nsq import NSQPublisher, NSQSubscriber
    from reliable_messaging.wire import Envelope, encode

    engine, sessions, table = storage
    store = MySQLDurableOutbox(table)
    token = uuid.uuid4().hex
    name = "rm-mq-fault-" + token[:12]
    label = "rm.mq.disposable=" + token

    async def docker(*arguments):
        result = await asyncio.to_thread(
            subprocess.run, ["docker", *arguments], check=True, capture_output=True, timeout=30
        )
        return result.stdout.decode().strip()

    async def verify_owned():
        data = json.loads(await docker("inspect", name))[0]
        assert data["Config"]["Labels"]["rm.mq.disposable"] == token
        assert data["Config"]["Image"] == "nsqio/nsq:v1.3.0"

    async def wait_http(base):
        async with asyncio.timeout(15):
            while True:
                try:
                    if (await http_json(base, "/stats?format=json"))["health"] == "OK":
                        return
                except OSError:
                    pass
                await asyncio.sleep(0.1)

    publisher, subscriber = None, None
    try:
        await docker(
            "run",
            "-d",
            "--name",
            name,
            "--label",
            label,
            "--memory",
            "128m",
            "--cpus",
            "0.5",
            "--tmpfs",
            "/data:rw,size=64m",
            "-p",
            "127.0.0.1::4150",
            "-p",
            "127.0.0.1::4151",
            "nsqio/nsq:v1.3.0",
            "/nsqd",
            "--data-path=/data",
            "--max-msg-size=262144",
            "--mem-queue-size=3000",
            "--sync-every=2500",
        )
        data = json.loads(await docker("inspect", name))[0]
        ports = data["NetworkSettings"]["Ports"]
        tcp = "127.0.0.1:" + ports["4150/tcp"][0]["HostPort"]
        http = "http://127.0.0.1:" + ports["4151/tcp"][0]["HostPort"]
        await wait_http(http)
        topic, channel = "rm.crash." + token[:12], "durable"
        await prepare(http, topic, channel)
        wire = encode(Envelope(ID.message_id, BODY))
        async with engine.begin() as db:
            await db.execute(
                text(
                    "CREATE TABLE rm_mq_fault_effects (id VARCHAR(128) PRIMARY KEY, "
                    "body_sha256 CHAR(64) NOT NULL, effect_count INT NOT NULL) ENGINE=InnoDB"
                )
            )
            effects = await db.run_sync(
                lambda c: Table("rm_mq_fault_effects", MetaData(), autoload_with=c)
            )
        async with sessions.begin() as db:
            await append(db, table)
            await db.execute(table.update().values(wire=wire, topic=topic))
        publisher = NSQPublisher(tcp)
        await publisher.start()
        assert (await publisher.publish(topic, wire)).outcome == Outcome.CONFIRMED
        async with sessions.begin() as db:
            await store.published(db, ID, HASH)
        stats = await http_json(http, "/stats?format=json")
        original_topic = next(t for t in stats["topics"] if t["topic_name"] == topic)
        assert original_topic["depth"] + sum(c["depth"] for c in original_topic["channels"]) == 1
        await publisher.stop()
        await verify_owned()
        await docker("kill", "--signal=KILL", name)
        await docker("start", name)
        # Docker may allocate fresh host ports on restart of random bindings.
        restarted = json.loads(await docker("inspect", name))[0]["NetworkSettings"]["Ports"]
        tcp = "127.0.0.1:" + restarted["4150/tcp"][0]["HostPort"]
        http = "http://127.0.0.1:" + restarted["4151/tcp"][0]["HostPort"]
        await wait_http(http)
        stats = await http_json(http, "/stats?format=json")
        assert (
            sum(
                t["depth"] + sum(c["depth"] for c in t["channels"])
                for t in stats["topics"]
                if t["topic_name"] == topic
            )
            == 0
        )
        assert (await row(sessions, table))["stage"] == AWAITING_RECEIPT
        await prepare(http, topic, channel)
        publisher = NSQPublisher(tcp)
        await publisher.start()
        committed = asyncio.Event()
        deliveries = []

        async def handle(received):
            assert received.envelope.message_id == ID.message_id
            assert received.wire == wire
            async with sessions.begin() as db:
                statement = insert(effects).values(
                    id=ID.message_id, body_sha256=HASH, effect_count=1
                )
                await bind(db).append(statement.on_duplicate_key_update(id=effects.c.id))
                actual = (
                    await db.execute(
                        text(
                            "SELECT body_sha256,effect_count FROM rm_mq_fault_effects WHERE id=:id"
                        ),
                        {"id": ID.message_id},
                    )
                ).one()
                assert actual == (HASH, 1)
            deliveries.append(received.envelope.message_id)
            committed.set()  # deliberately lose the business receipt after this commit

        async def unexpected(*args):
            raise AssertionError("unexpected failed/quarantined message")

        async def ready(address, failure_topic, failure_channel):
            assert address == tcp
            stats = await http_json(http, "/stats?format=json")
            assert any(
                c["clients"]
                for t in stats["topics"]
                if t["topic_name"] == failure_topic
                for c in t["channels"]
                if c["channel_name"] == failure_channel
            )

        for _ in range(2):
            subscriber = NSQSubscriber(
                topic,
                channel,
                publishers={tcp: publisher},
                handler=handle,
                failed_handler=unexpected,
                invalid_handler=unexpected,
                failure_ready=ready,
            )
            await prepare(http, subscriber.failure_topic, "cb-failed-handler")
            await subscriber.start()
            async with sessions.begin() as db:
                await db.execute(table.update().values(available_at=func.utc_timestamp(6)))
                pending = await store.pending(db)
                assert len(pending) == 1 and pending[0]["wire"] == wire
            assert (await publisher.publish(topic, pending[0]["wire"])).outcome == Outcome.CONFIRMED
            async with sessions.begin() as db:
                await store.published(db, ID, HASH)
            await asyncio.wait_for(committed.wait(), 10)
            await subscriber.stop()
            committed.clear()
        assert deliveries == [ID.message_id, ID.message_id]
        assert (await row(sessions, table))["stage"] == AWAITING_RECEIPT
        async with sessions.begin() as db:
            await store.confirm(db, ID, HASH)
            assert (
                await db.execute(text("SELECT SUM(effect_count) FROM rm_mq_fault_effects"))
            ).scalar() == 1
        assert (await row(sessions, table))["stage"] == CONFIRMED
    finally:
        if subscriber is not None:
            await subscriber.stop(grace_seconds=0)
        if publisher is not None:
            await publisher.stop(grace_seconds=0)
        async with engine.begin() as db:
            await db.execute(text("DROP TABLE IF EXISTS rm_mq_fault_effects"))
        # Exact ownership check is required even on cleanup after a failed assertion.
        inspect = await asyncio.to_thread(
            subprocess.run, ["docker", "inspect", name], capture_output=True, timeout=10
        )
        if inspect.returncode == 0:
            await verify_owned()
            await docker("rm", "-f", name)
