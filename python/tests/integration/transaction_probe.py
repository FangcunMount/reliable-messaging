"""Owned-fixture PUBLIC Python SDK acceptance; never production endpoints."""

import argparse
import asyncio
import hashlib
import json
import os
import sys
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from reliable_messaging.message import Message
from reliable_messaging.rocketmq_transaction import (
    TransactionalSender,
    TransactionConfig,
    TransactionRoute,
)
from reliable_messaging.transaction import (
    Binding,
    FrozenBranch,
    FrozenPlan,
    Limits,
    LocalOutcome,
    Request,
)
from reliable_messaging.transaction_mysql import MySQLTransactionStore


def emit(event, identity="", decision=""):
    print(
        "TXPROBE " + json.dumps({"event": event, "id": identity, "decision": decision}), flush=True
    )


async def run(args):
    config = json.loads(args.config.read_text())
    if not config["endpoint"].startswith("127.0.0.1:") or "@127.0.0.1:" not in config["mysql_url"]:
        raise ValueError("owned loopback fixture required")
    engine = create_async_engine(
        config["mysql_url"], pool_size=8, max_overflow=4, connect_args={"connect_timeout": 2}
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = MySQLTransactionStore(
        sessions, Binding(config["owner"], "sdk-disposable", "sdk-epoch-1")
    )
    sender = TransactionalSender(
        TransactionConfig(
            config["endpoint"],
            config["accesskey"],
            config["secretkey"],
            config["owner"],
            (TransactionRoute("sdk.logical", config["topic"], "sdk-v2"),),
            Limits(lease=15, reconcile_interval=0.2),
        ),
        store,
    )
    try:
        await sender.start()
        if args.mode == "isolation":
            other = TransactionalSender(sender.config, store)
            await other.start()
            await sender.close(grace_seconds=15)
            sender = other
            emit("isolated", args.id, "first_closed_second_ready")
        emit("ready", args.id, "public_python_sdk")
        if args.mode == "recover":

            async def monitor():
                last = {"check_commit": 0, "check_rollback": 0, "check_unknown": 0}
                while True:
                    await asyncio.sleep(0.1)
                    current = sender.stats()
                    for key, decision in (
                        ("check_commit", "commit"),
                        ("check_rollback", "rollback"),
                        ("check_unknown", "unknown"),
                    ):
                        if current[key] > last[key]:
                            emit("checked", decision=decision)
                            last[key] = current[key]

            monitor_task = asyncio.create_task(monitor())
            try:
                while (await asyncio.to_thread(sys.stdin.readline)).strip() != "stop":
                    pass
            finally:
                monitor_task.cancel()
                await asyncio.gather(monitor_task, return_exceptions=True)
        else:
            identity = args.id
            message = Message(
                "sdkprobe",
                identity,
                "sdk.logical",
                "created",
                "1",
                "fixture",
                "application/octet-stream",
                "2026-10-19T00:00:00.000000123Z",
                ("sdk:" + identity).encode(),
            )
            plan = FrozenPlan(
                config["owner"],
                "fixture",
                "op-" + identity,
                identity,
                hashlib.sha256(identity.encode()).digest(),
                (
                    FrozenBranch(
                        "branch", config["topic"], "sdk-v2", message, ("sdk:" + identity).encode()
                    ),
                ),
            )
            request = Request(plan.owner, plan.scope, plan.idempotency_key, plan.input_hash)
            reservation = await sender.reserve(request, plan)
            if not reservation.acquired:
                raise RuntimeError("fixture request not new")

            async def local(e):
                emit("half", identity, "all_prepared")
                action = (await asyncio.to_thread(sys.stdin.readline)).strip()
                if args.mode == "crash-prepared":
                    os._exit(22)
                if action != "commit":
                    return LocalOutcome.ABORTED
                # ORIGINAL host session/root transaction. SDK guard does not
                # open/commit a replacement transaction for the business write.
                try:
                    async with sessions() as db, db.begin():
                        guard = await store.bind(db, e)
                        await db.execute(
                            text("INSERT INTO sdk_business VALUES(:id,1)"),
                            {"id": identity.encode()},
                        )
                        await guard.commit_witness()
                except Exception:
                    return LocalOutcome.UNKNOWN
                if args.mode == "crash-committed":
                    os._exit(23)
                return LocalOutcome.COMMITTED

            result = await sender.execute(reservation.execution, local)
            emit("finalized", identity, result.local)
        await sender.close(grace_seconds=15)
        emit("closed", args.id, "drained")
    finally:
        # Never dispose a host pool while SDK resources are still running.
        if not sender.stats()["stopped"]:
            await sender.close(grace_seconds=20)
            emit("closed", args.id, "failed_start_drained")
        await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--mode",
        choices=("send", "recover", "isolation", "crash-committed", "crash-prepared"),
        required=True,
    )
    parser.add_argument("--id", default="")
    args = parser.parse_args()
    try:
        asyncio.run(run(args))
    except Exception:
        emit("failed", args.id, "python_sdk_operation_failed")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
