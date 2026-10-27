"""Adapter orchestration; actual Broker/storage acceptance is a separate gate."""

from dataclasses import replace

import pytest
from test_rocketmq_authorized import AuthorizedMemoryStore
from test_rocketmq_transaction import fixture

from reliable_messaging.rocketmq_transaction import TransactionalSender
from reliable_messaging.transaction import (
    BindingError,
    LocalOutcome,
    ReplanAuthorization,
    RetryAuthorization,
    StaleExecutionError,
    State,
)
from reliable_messaging.transport import physical_topic_v3


@pytest.mark.parametrize("state", [State.PREPARED, State.COMMITTED, State.ABORTED])
async def test_explicit_readable_handoff_cannot_send_or_register_old_routes(state):
    old_sender, _, client, previous = fixture()
    original = previous.plan
    old_routes = old_sender.config.routes
    active = tuple(
        replace(
            r,
            physical_topic=physical_topic_v3(
                r.logical_topic, "TRANSACTION", "test.mysql", r.revision
            ),
            naming_scheme="readable-v1",
        )
        for r in old_routes
    )
    store = AuthorizedMemoryStore(previous)
    store.state = state
    sender = TransactionalSender(
        replace(old_sender.config, routes=active, historical_routes=old_routes), store
    )
    registered = set()

    def create(**kwargs):
        registered.update(kwargs["topics"])
        return client

    sender._create_producer = create
    await sender.start()
    try:
        assert registered == {r.physical_topic for r in active}
        assert not registered & {r.physical_topic for r in old_routes}
        with pytest.raises(BindingError):
            await sender.retry(RetryAuthorization(previous))

        async def forbidden(execution):
            pytest.fail("retired business execution was called")

        with pytest.raises(BindingError):
            await sender.execute(previous, forbidden)
        candidate = replace(
            original,
            operation_id="explicit-route-handoff",
            branches=(replace(original.branches[0], physical_topic=active[0].physical_topic),),
        )
        assert candidate.branches[0].wire == original.branches[0].wire
        assert candidate.branches[0].message == original.branches[0].message
        assert candidate.digest != original.digest
        assert client.sent == 0
        if state != State.ABORTED:
            with pytest.raises(StaleExecutionError):
                await sender.replan(ReplanAuthorization(previous, candidate))
        else:
            result = await sender.replan(ReplanAuthorization(previous, candidate))
            assert result.acquired and result.execution.fence == previous.fence + 1
            assert client.sent == 0

            async def commit(execution):
                store.state = State.COMMITTED
                return LocalOutcome.COMMITTED

            outcome = await sender.execute(result.execution, commit)
            assert outcome.local == LocalOutcome.COMMITTED and client.sent == 1
        assert original == previous.plan
    finally:
        await sender.close()
