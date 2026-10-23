"""Owned-fixture PUBLIC Python SDK acceptance; never production endpoints."""

import argparse
import asyncio
import hashlib
import json
import os
import sys
import threading
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from reliable_messaging.message import Message
from reliable_messaging.rocketmq import Config, RocketMQSubscriber
from reliable_messaging.rocketmq_transaction import (
    TransactionalSender,
    TransactionConfig,
    TransactionRoute,
    TransactionSendError,
)
from reliable_messaging.transaction import (
    Binding,
    FinalOutcome,
    FrozenBranch,
    FrozenPlan,
    HalfOutcome,
    Limits,
    LocalOutcome,
    Request,
    resolve,
)
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from reliable_messaging.transport import RouteBinding
from reliable_messaging.wire import Envelope, decode, encode

_output = threading.Lock()


def emit(event, identity="", decision=""):
    with _output:
        print(
            "TXPROBE " + json.dumps({"event": event, "id": identity, "decision": decision}),
            flush=True,
        )


class ObservedStore:
    """Transparent observation of actual public Checker reads, never fake evidence."""

    def __init__(self, store, *, enabled=True):
        self._store = store
        self.enabled = enabled

    def __getattr__(self, name):
        return getattr(self._store, name)

    async def evidence(self, reference):
        try:
            evidence = await self._store.evidence(reference)
        except BaseException:
            if self.enabled:
                emit("checked_reference", reference.message_id, "unknown")
            raise
        if self.enabled:
            emit("checked_reference", reference.message_id, resolve(reference, evidence).value)
        return evidence


async def run(args):
    config = json.loads(args.config.read_text())
    if not config["endpoint"].startswith("127.0.0.1:") or "@127.0.0.1:" not in config["mysql_url"]:
        raise ValueError("owned loopback fixture required")
    if args.mode in ("consume", "shared-consume"):
        await consume(config, args.mode == "shared-consume")
        return
    engine = create_async_engine(
        config["mysql_url"], pool_size=8, max_overflow=4, connect_args={"connect_timeout": 2}
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = MySQLTransactionStore(
        sessions, Binding(config["owner"], "sdk-disposable", "sdk-epoch-1")
    )
    observed_store = ObservedStore(store, enabled=args.mode == "recover")
    sender = TransactionalSender(
        TransactionConfig(
            config["endpoint"],
            config["accesskey"],
            config["secretkey"],
            config["owner"],
            tuple(
                TransactionRoute(r["topic"], r["physical_topic"], r["route_revision"])
                for r in config["owner_routes"]
            ),
            Limits(lease=config.get("lease_millis", 15000) / 1000, reconcile_interval=0.2),
        ),
        observed_store if args.mode in ("recover", "fault-live-broker") else store,
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
                while True:
                    command = (await asyncio.to_thread(sys.stdin.readline)).strip()
                    if command == "stop" or not command:
                        break
                    if command == "seal":
                        sender.stop_admission()
                        request = Request(
                            config["owner"],
                            "fixture",
                            args.id,
                            hashlib.sha256(args.id.encode()).digest(),
                        )
                        prior = await store.lookup(request)
                        if prior is None:
                            raise RuntimeError("seal fixture request missing")
                        try:
                            await sender.reserve(request, prior.execution.plan)
                        except RuntimeError:
                            pass
                        else:
                            raise RuntimeError("sealed sender admitted request")
                        if sender.stats()["ready"] or not sender.stats()["stopping"]:
                            raise RuntimeError("seal failed")
                        emit("sealed", decision="admission_stopped_checker_alive")
            finally:
                monitor_task.cancel()
                await asyncio.gather(monitor_task, return_exceptions=True)
        else:
            identity = args.id
            count = config.get("branches", 1)
            if not 1 <= count <= 16:
                raise ValueError("bounded fixture branches required")
            branches = []
            for index in range(count):
                message_id = identity if count == 1 else f"{identity}-b{index}"
                message = Message(
                    "sdkprobe",
                    message_id,
                    "sdk.logical",
                    "created",
                    "1",
                    "fixture",
                    "application/octet-stream",
                    "2026-10-19T00:00:00.000000123Z",
                    ("sdk:" + message_id).encode(),
                )
                branches.append(
                    FrozenBranch(
                        f"branch-{index}",
                        config["topic"],
                        config["route_revision"],
                        message,
                        fixture_wire(message_id),
                    )
                )
            plan = FrozenPlan(
                config["owner"],
                "fixture",
                "op-" + identity,
                identity,
                hashlib.sha256(identity.encode()).digest(),
                tuple(branches),
            )
            request = Request(plan.owner, plan.scope, plan.idempotency_key, plan.input_hash)
            reservation = await sender.reserve(request, plan)
            if not reservation.acquired:
                raise RuntimeError("fixture request not new")

            local_calls = 0

            async def local(e):
                nonlocal local_calls
                local_calls += 1
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

            try:
                result = await sender.execute(reservation.execution, local)
            except TransactionSendError as error:
                if not args.mode.startswith("fault-"):
                    raise
                result = error.result
            if args.mode.startswith("fault-"):
                valid = {
                    "fault-half": result.local == LocalOutcome.NOT_STARTED
                    and local_calls == 0
                    and any(b.half == HalfOutcome.UNKNOWN for b in result.branches),
                    "fault-final": result.local == LocalOutcome.COMMITTED
                    and local_calls == 1
                    and any(b.final == FinalOutcome.UNKNOWN for b in result.branches),
                    "fault-live-broker": result.local == LocalOutcome.COMMITTED
                    and local_calls == 1
                    and all(b.half == HalfOutcome.PREPARED for b in result.branches)
                    and all(b.final == FinalOutcome.UNKNOWN for b in result.branches),
                    "fault-local-committed": result.local == LocalOutcome.COMMITTED
                    and local_calls == 1,
                    "fault-local-uncommitted": local_calls == 1
                    and (
                        result.local == LocalOutcome.UNKNOWN
                        and all(b.final == FinalOutcome.NOT_ATTEMPTED for b in result.branches)
                        or result.local == LocalOutcome.ABORTED
                        and all(b.final == FinalOutcome.ROLLBACK_CONFIRMED for b in result.branches)
                    ),
                }.get(args.mode, False)
                emit(
                    "result",
                    identity,
                    json.dumps(
                        {
                            "local": result.local,
                            "local_calls": local_calls,
                            "branches": [
                                {
                                    "message_id": b.reference.message_id,
                                    "half": b.half,
                                    "final": b.final,
                                    "error_code": b.error_code,
                                    "broker_message_id": b.broker_message_id,
                                    "broker_transaction_id": b.broker_transaction_id,
                                }
                                for b in result.branches
                            ],
                        }
                    ),
                )
                if not valid:
                    raise RuntimeError("expected real fault outcome not established")
                emit("fault_observed", identity, args.mode)
                if args.mode == "fault-live-broker":
                    sender.stop_admission()
                    async with asyncio.timeout(10):
                        # No completion event is exposed by the public stats API.
                        while any(  # noqa: ASYNC110
                            sender.stats()[key]
                            for key in (
                                "pending_calls",
                                "pending_checks",
                                "running_checks",
                                "active_sends",
                            )
                        ):
                            await asyncio.sleep(0.05)
                    observed_store.enabled = True
                    emit("live_waiting", identity, "admission_stopped_original_checks_drained")
                    if (await asyncio.to_thread(sys.stdin.readline)).strip() != "stop":
                        raise RuntimeError("explicit owned live-recovery stop required")
            else:
                emit("finalized", identity, result.local)
        await sender.close(grace_seconds=15)
        stats = sender.stats()
        if (
            not stats["stopped"]
            or stats["active_sends"]
            or stats["pending_calls"]
            or stats["pending_checks"]
        ):
            raise RuntimeError("actual SDK resources remain after close")
        emit("metrics", args.id, json.dumps(stats))
        emit("closed", args.id, "drained")
    finally:
        # Never dispose a host pool while SDK resources are still running.
        if not sender.stats()["stopped"]:
            await sender.close(grace_seconds=20)
            emit("metrics", args.id, json.dumps(sender.stats()))
            emit("closed", args.id, "failed_start_drained")
        await engine.dispose()


def fixture_wire(identity):
    return (
        b"\n"
        + encode(
            Envelope(
                identity, ("sdk:" + identity).encode(), {"fixture": "m2-sdk", "unicode": "测评 <&>"}
            )
        )
        + b"\n"
    )


async def consume(config, shared):
    routes = tuple(RouteBinding(**r) for r in config["inputs"])
    release = asyncio.Event()
    if not shared:
        release.set()
    active, maximum = 0, 0

    async def handler(received):
        nonlocal active, maximum
        identity = received.envelope.message_id
        if (
            received.wire != fixture_wire(identity)
            or received.envelope.payload != ("sdk:" + identity).encode()
            or received.logical_topic != "sdk.logical"
        ):
            raise RuntimeError("public SDK original wire or ID mismatch")
        active += 1
        maximum = max(maximum, active)
        if shared:
            emit("handler_enter", identity, str(active))
        try:
            await release.wait()
        finally:
            active -= 1

    async def failed(*args):
        raise RuntimeError("unexpected public SDK durable failure path")

    subscriber = RocketMQSubscriber(
        Config(
            config["endpoint"],
            config["accesskey"],
            config["secretkey"],
            routes,
            timeout=5,
            max_in_flight=2,
        ),
        "sdk.logical",
        config["subscription"],
        handler=handler,
        failed_handler=failed,
        invalid_handler=failed,
        invisible_duration=15,
        handler_timeout=60,
    )
    await subscriber.start()
    # Fixture-only instance observation AFTER the actual upstream Ack succeeds.
    # The production SDK settlement path is unchanged; no global factory changes.
    original_ack = subscriber._driver.client.ack

    def acknowledged(raw):
        result = original_ack(raw)
        emit("received", decode(raw.body).message_id, "exact_wire_public_sdk_acknowledged")
        return result

    subscriber._driver.client.ack = acknowledged
    emit("ready", decision="public_python_sdk_consumer")
    try:
        while True:
            command = (await asyncio.to_thread(sys.stdin.readline)).strip()
            if command == "release":
                release.set()
            if command == "stop" or not command:
                break
    finally:
        await subscriber.stop(grace_seconds=15)
    emit("max_inflight", decision=str(maximum))
    emit("closed", decision="consumer_drained")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--mode",
        choices=(
            "send",
            "recover",
            "consume",
            "shared-consume",
            "isolation",
            "crash-committed",
            "crash-prepared",
            "fault-half",
            "fault-final",
            "fault-local-committed",
            "fault-local-uncommitted",
            "fault-live-broker",
        ),
        required=True,
    )
    parser.add_argument("--id", default="")
    args = parser.parse_args()
    try:
        asyncio.run(run(args))
    except Exception as error:
        from reliable_messaging._rocketmq_transaction_driver import rejection_code

        code = rejection_code(error)
        emit("failure_code", decision=code or type(error).__name__)
        if code in (
            "half_rejected_40100",
            "half_rejected_40300",
            "half_rejected_grpc_permission_denied",
            "half_rejected_grpc_unauthenticated",
        ):
            emit("broker_denied", decision=code)
        elif isinstance(error, TransactionSendError) and any(
            b.error_code
            in (
                "half_rejected_40100",
                "half_rejected_40300",
                "half_rejected_grpc_permission_denied",
                "half_rejected_grpc_unauthenticated",
            )
            for b in error.result.branches
        ):
            emit("broker_denied", decision="half_rejected")
        emit("failed", args.id, "python_sdk_operation_failed")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
