"""Host-authorized sender advancement only; no database/Broker proof substitution."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from test_rocketmq_transaction import MemoryStore, fixture

from reliable_messaging.transaction import (
    MAX_FENCE,
    BindingError,
    Evidence,
    ReplanAuthorization,
    Request,
    Reservation,
    RetryAuthorization,
    ShutdownIncompleteError,
    StaleExecutionError,
    State,
    validate_advance,
)


class AuthorizedMemoryStore(MemoryStore):
    def __init__(self, execution):
        super().__init__(execution)
        self.state = State.ABORTED
        self.advances, self.reserves = 0, 0
        self.entered, self.release = asyncio.Event(), None
        self.last_lease = None

    async def reserve(self, request, candidate, *, lease_seconds=60):
        self.reserves += 1
        return await super().reserve(request, candidate, lease_seconds=lease_seconds)

    async def _advance(self, previous, candidate, lease, replan):
        self.advances += 1
        self.last_lease = lease
        self.entered.set()
        if self.release is not None:
            await self.release.wait()
        if previous.fence != self.e.fence or previous.plan.digest != self.e.plan.digest:
            raise StaleExecutionError("exact previous execution differs")
        validate_advance(
            previous,
            candidate,
            tuple(Evidence(ref, self.state, self.state) for ref in previous.references),
            replan=replan,
        )
        self.e = replace(
            previous,
            plan=candidate,
            fence=previous.fence + 1,
            token=b"b" * 32,
            lease_until=datetime.now(UTC) + timedelta(seconds=lease),
        )
        self.state, self.begun = State.PREPARED, False
        return Reservation(self.e, True, self.state)

    async def retry(self, authorization, *, lease_seconds=60):
        return await self._advance(
            authorization.previous, authorization.previous.plan, lease_seconds, False
        )

    async def replan(self, authorization, *, lease_seconds=60):
        return await self._advance(
            authorization.previous, authorization.candidate, lease_seconds, True
        )


def authorized():
    sender, _, client, previous = fixture()
    store = AuthorizedMemoryStore(previous)
    sender.store = store  # Per-instance test adapter, before explicit Start.
    return sender, store, client, previous


def candidate(previous):
    return replace(
        previous.plan,
        operation_id="explicit-new-operation",
        branches=(replace(previous.plan.branches[0], wire=b"explicit-new-first-wire"),),
    )


@pytest.mark.parametrize("replan", [False, True])
async def test_advancement_requires_host_call_and_does_not_prepare_or_execute_business(replan):
    sender, store, client, previous = authorized()
    assert store.advances == client.started == 0
    await sender.start()
    try:
        assert store.advances == 0
        observed = replace(previous, token=bytes(32))
        first = candidate(previous) if replan else previous.plan
        owned = (
            await sender.replan(ReplanAuthorization(observed, first))
            if replan
            else await sender.retry(RetryAuthorization(observed))
        )
        assert owned.acquired and owned.execution.owned
        assert owned.execution.fence == previous.fence + 1
        assert owned.execution.plan == first and store.last_lease == sender.config.limits.lease
        assert client.sent == client.committed == client.aborted == 0
        assert sender.stats()["active_sends"] == 0
    finally:
        await sender.close()


@pytest.mark.parametrize("state", [State.PREPARED, State.COMMITTED])
async def test_unresolved_or_committed_evidence_does_not_authorize_new_execution(state):
    sender, store, client, previous = authorized()
    store.state = state
    await sender.start()
    try:
        with pytest.raises(StaleExecutionError):
            await sender.retry(RetryAuthorization(previous))
        assert store.e == previous and client.sent == 0
        assert sender.stats()["database_errors"] == 0
    finally:
        await sender.close()


@pytest.mark.parametrize("damage", ["owner", "route", "same_operation", "input", "max_fence"])
async def test_invalid_plan_or_previous_fence_is_rejected_before_store_or_runtime(damage):
    sender, store, client, previous = authorized()
    first = candidate(previous)
    if damage == "owner":
        first = replace(first, owner="another-owner")
    elif damage == "route":
        first = replace(first, branches=(replace(first.branches[0], physical_topic="other-route"),))
    elif damage == "same_operation":
        first = replace(first, operation_id=previous.plan.operation_id)
    elif damage == "input":
        first = replace(first, input_hash=b"b" * 32)
    else:
        previous = replace(previous, fence=MAX_FENCE)
    with pytest.raises(ValueError):
        await sender.replan(ReplanAuthorization(previous, first))
    assert store.advances == client.started == client.sent == 0


async def test_store_without_authorized_capability_remains_usable_but_rejects_advancement():
    sender, _, client, previous = fixture()
    with pytest.raises(BindingError):
        await sender.retry(RetryAuthorization(previous))
    assert client.started == client.sent == 0


@pytest.mark.parametrize("replan", [False, True])
async def test_canceled_advance_keeps_real_shared_slot_and_shutdown_until_store_finishes(replan):
    sender, store, client, previous = authorized()
    sender.config = replace(sender.config, limits=replace(sender.config.limits, sends=1))
    store.release = asyncio.Event()
    await sender.start()
    call = (
        sender.replan(ReplanAuthorization(previous, candidate(previous)))
        if replan
        else sender.retry(RetryAuthorization(previous))
    )
    task = asyncio.create_task(call)
    await asyncio.wait_for(store.entered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sender.stats()["active_sends"] == 1
    plan = previous.plan
    queued = asyncio.create_task(
        sender.reserve(
            Request(plan.owner, plan.scope, plan.idempotency_key, plan.input_hash),
            plan,
        )
    )
    await asyncio.sleep(0)
    assert not queued.done() and store.reserves == 0
    sender.stop_admission()
    with pytest.raises(RuntimeError):
        await queued
    with pytest.raises(ShutdownIncompleteError):
        await sender.close(grace_seconds=0)
    assert sender.stats()["active_sends"] == 1
    store.release.set()
    await sender.wait()
    assert sender.stats()["active_sends"] == 0
    assert store.advances == 1 and client.sent == 0 and client.stopped == 1
    assert asyncio.get_running_loop().is_running()
