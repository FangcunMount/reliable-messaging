"""Orchestration-only tests; real storage and Broker proofs have separate gates."""

import asyncio
import hashlib
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from reliable_messaging.message import Message
from reliable_messaging.rocketmq_transaction import (
    TransactionalSender,
    TransactionConfig,
    TransactionRoute,
    TransactionSendError,
)
from reliable_messaging.transaction import (
    Binding,
    Evidence,
    Execution,
    FinalOutcome,
    FrozenBranch,
    FrozenPlan,
    HalfOutcome,
    LocalOutcome,
    Reservation,
    ShutdownIncompleteError,
    StaleExecutionError,
    State,
    encode_reference,
)
from reliable_messaging.transport import physical_topic_v2


class MemoryStore:
    def __init__(self, execution):
        self.binding = Binding("test.mysql", "fixture", "epoch")
        self.e, self.state, self.begun = execution, State.PREPARED, False
        self.halves, self.finals = {}, {}
        self.read_error = None
        self.validations = 0
        self.loop = None

    async def validate(self):
        self.loop = asyncio.get_running_loop()
        self.validations += 1

    async def reserve(self, request, candidate, *, lease_seconds=60):
        request.match(self.e.plan)
        return Reservation(self.e, True, self.state)

    async def lookup(self, request):
        return Reservation(replace(self.e, token=bytes(32)), False, self.state)

    async def begin_prepare(self, execution):
        if self.begun:
            raise StaleExecutionError("already prepared")
        self.begun = True

    async def evidence(self, reference):
        assert asyncio.get_running_loop() is self.loop
        if self.read_error:
            raise self.read_error
        return Evidence(reference, self.state, self.state)

    async def abort(self, execution):
        if self.state != State.PREPARED:
            return False
        self.state = State.ABORTED
        return True

    async def reconcile_once(self, limit):
        return 0

    async def record_half(self, e, reference, receipt):
        self.halves[reference.branch_id] = receipt

    async def record_final(self, e, reference, outcome, code):
        self.finals[reference.branch_id] = outcome


class Client:
    def __init__(self):
        self._init_settings_event = threading.Event()
        self._init_settings_event.set()
        self.started = self.stopped = self.sent = self.committed = self.aborted = 0
        self.fail_send_at = None
        self.send_error = None
        self.fail_final = False
        self.send_started = threading.Event()
        self.send_release = None
        self.final_started = threading.Event()
        self.final_release = None

    def startup(self):
        self.started += 1

    def _retrieve_topic_route_data(self, topic):
        pass

    def shutdown(self):
        self.stopped += 1

    def begin_transaction(self):
        return self

    def send(self, message, handle):
        self.sent += 1
        self.send_started.set()
        if self.send_release is not None:
            self.send_release.wait()
        if self.sent == self.fail_send_at:
            raise self.send_error or TimeoutError("synthetic half reply loss")
        return SimpleNamespace(
            message_id="broker-" + str(self.sent), transaction_id="transaction-" + str(self.sent)
        )

    def commit(self):
        self.final_started.set()
        if self.final_release is not None:
            self.final_release.wait()
        self.committed += 1
        if self.fail_final:
            raise TimeoutError("synthetic final reply loss")

    def rollback(self):
        self.aborted += 1


def fixture(branches=1):
    message = Message(
        "fixture",
        "original-id",
        "logical",
        "created",
        "1",
        "scope",
        "application/octet-stream",
        "2026-10-19T00:00:00Z",
        b"original",
    )
    plan = FrozenPlan(
        "test.mysql",
        "scope",
        "operation",
        "request",
        hashlib.sha256(b"input").digest(),
        tuple(
            FrozenBranch(
                "branch-" + str(i),
                physical_topic_v2("logical-" + str(i), "TRANSACTION", "test.mysql", "v2"),
                "v2",
                replace(message, message_id="original-" + str(i), destination="logical-" + str(i)),
                b"wire",
            )
            for i in range(branches)
        ),
    )
    execution = Execution(plan, 1, "epoch", b"a" * 32, datetime.now(UTC) + timedelta(minutes=1))
    store, client = MemoryStore(execution), Client()
    sender = TransactionalSender(
        TransactionConfig(
            "127.0.0.1:1",
            "fixture",
            "fixture",
            "test.mysql",
            tuple(
                TransactionRoute(b.message.destination, b.physical_topic, "v2")
                for b in plan.branches
            ),
        ),
        store,
    )
    sender._create_producer = lambda **kwargs: client
    return sender, store, client, execution


async def test_inert_constructor_and_positive_fresh_proof_after_commit_reply_loss():
    sender, store, client, execution = fixture()
    assert store.validations == client.started == 0
    await sender.start()
    client.fail_final = True
    calls = 0

    async def local(e):
        nonlocal calls
        calls += 1
        store.state = State.COMMITTED
        return LocalOutcome.COMMITTED

    with pytest.raises(TransactionSendError) as error:
        await sender.execute(execution, local)
    assert error.value.result.local == LocalOutcome.COMMITTED
    assert error.value.result.branches[0].final == FinalOutcome.UNKNOWN
    assert sender.stats()["completed_results"] == 1
    assert sender.stats()["local_committed_results"] == 1
    assert sender.stats()["final_unknown_results"] == 1
    with pytest.raises(TransactionSendError):
        await sender.execute(execution, local)
    assert calls == client.sent == 1
    await sender.close()
    assert client.stopped == 1


async def test_partial_preparation_cannot_start_local_executor():
    sender, store, client, execution = fixture(3)
    client.fail_send_at = 2
    await sender.start()
    calls = 0

    async def local(e):
        nonlocal calls
        calls += 1
        return LocalOutcome.COMMITTED

    with pytest.raises(TransactionSendError) as error:
        await sender.execute(execution, local)
    result = error.value.result
    assert calls == 0 and store.state == State.ABORTED
    assert result.local == LocalOutcome.NOT_STARTED
    assert result.branches[0].final == FinalOutcome.ROLLBACK_CONFIRMED
    assert result.branches[1].half == HalfOutcome.UNKNOWN
    assert result.branches[2].half == HalfOutcome.NOT_ATTEMPTED
    await sender.close()


async def test_callback_success_alone_is_not_local_commit():
    sender, store, client, execution = fixture()
    await sender.start()

    async def local(e):
        return LocalOutcome.COMMITTED

    with pytest.raises(TransactionSendError) as error:
        await sender.execute(execution, local)
    assert error.value.result.local == LocalOutcome.UNKNOWN
    assert client.committed == client.aborted == 0
    await sender.close()


async def test_caller_cancellation_and_timed_close_retain_live_business_and_rpc():
    sender, store, client, execution = fixture()
    await sender.start()
    entered, release = asyncio.Event(), asyncio.Event()

    async def local(e):
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()  # emulate a database UOW whose cancellation is delayed
        store.state = State.COMMITTED
        return LocalOutcome.COMMITTED

    task = asyncio.create_task(sender.execute(execution, local))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sender.stats()["active_sends"] == 1
    with pytest.raises(ShutdownIncompleteError) as error:
        await sender.close(grace_seconds=0.02)
    assert error.value.counts["active_sends"] == 1
    assert client.stopped == 0
    release.set()
    await sender.wait()
    assert client.committed == client.stopped == 1
    await sender.close()
    assert sender.stats()["active_sends"] == 0


async def test_checker_requires_actual_route_wire_and_host_loop():
    from rocketmq.grpc_protocol import TransactionResolution

    sender, store, client, execution = fixture()
    await sender.start()
    store.state = State.COMMITTED
    ref = execution.references[0]
    message = SimpleNamespace(
        topic=ref.physical_topic,
        body=b"wire",
        properties={"rm_tx_ref": encode_reference(ref).decode()},
    )
    assert await sender._check(message) == TransactionResolution.COMMIT
    message.body = b"wrong wire"
    assert await sender._check(message) == TransactionResolution.TRANSACTION_RESOLUTION_UNSPECIFIED
    message.body, message.topic = b"wire", "wrong-topic"
    assert await sender._check(message) == TransactionResolution.TRANSACTION_RESOLUTION_UNSPECIFIED
    message.topic = ref.physical_topic
    store.read_error = ConnectionError("synthetic database disconnect")
    assert await sender._check(message) == TransactionResolution.TRANSACTION_RESOLUTION_UNSPECIFIED
    assert not sender.stats()["ready"]
    await sender.close()


async def test_close_timeout_keeps_real_blocking_final_rpc_capacity():
    sender, store, client, execution = fixture()
    await sender.start()
    client.final_release = threading.Event()

    async def local(e):
        store.state = State.COMMITTED
        return LocalOutcome.COMMITTED

    operation = asyncio.create_task(sender.execute(execution, local))
    assert await asyncio.to_thread(client.final_started.wait, 2)
    with pytest.raises(ShutdownIncompleteError):
        await sender.close(grace_seconds=0.02)
    assert sender.stats()["active_sends"] == 1
    assert client.stopped == 0
    client.final_release.set()
    assert (await operation).local == LocalOutcome.COMMITTED
    await sender.wait()
    assert client.stopped == 1


async def test_failed_start_cleans_owned_resources_and_does_not_stop_other_instance():
    first, _, broken, _ = fixture()
    second, _, healthy, execution = fixture()

    def fail():
        raise ConnectionError("synthetic startup failure")

    broken.startup = fail
    await second.start()
    with pytest.raises(ConnectionError):
        await first.start()
    await first.wait()
    assert broken.stopped == 1 and healthy.stopped == 0
    assert second.stats()["ready"]
    await second.close()
    assert healthy.stopped == 1


async def test_cancellation_during_half_preparation_does_not_start_business():
    sender, store, client, execution = fixture()
    await sender.start()
    client.send_release = threading.Event()
    calls = 0

    async def local(e):
        nonlocal calls
        calls += 1
        return LocalOutcome.COMMITTED

    operation = asyncio.create_task(sender.execute(execution, local))
    assert await asyncio.to_thread(client.send_started.wait, 2)
    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation
    with pytest.raises(ShutdownIncompleteError):
        await sender.close(grace_seconds=0.02)
    assert calls == 0 and sender.stats()["pending_calls"] == 1
    client.send_release.set()
    await sender.wait()
    assert calls == 0 and store.state == State.ABORTED and client.aborted == 1


async def test_explicit_broker_rejection_is_distinct_from_unknown():
    class Rejected(Exception):
        code = 40300

    sender, store, client, execution = fixture()
    client.fail_send_at, client.send_error = 1, Rejected("synthetic")
    await sender.start()
    calls = 0

    async def local(e):
        nonlocal calls
        calls += 1
        return LocalOutcome.COMMITTED

    with pytest.raises(TransactionSendError) as error:
        await sender.execute(execution, local)
    assert error.value.result.branches[0].half == HalfOutcome.REJECTED
    assert calls == 0 and store.state == State.ABORTED
    await sender.close()


async def test_callback_error_after_commit_preserves_terminal_evidence_and_error():
    sender, store, client, execution = fixture()
    await sender.start()

    async def local(e):
        store.state = State.COMMITTED
        raise ConnectionError("synthetic root commit reply lost")

    with pytest.raises(TransactionSendError) as error:
        await sender.execute(execution, local)
    assert error.value.code == "local_callback_error_with_terminal_proof"
    assert error.value.result.local == LocalOutcome.COMMITTED
    assert error.value.result.branches[0].final == FinalOutcome.COMMIT_CONFIRMED
    assert sender.stats()["local_errors"] == 1
    await sender.close()
