import asyncio
from unittest.mock import AsyncMock

import pytest

from reliable_messaging import Confirmation, DeliveryRejected, Outcome, deliver_durable


@pytest.mark.parametrize(
    "error,outcome",
    [
        (TimeoutError("receipt lost"), Outcome.UNKNOWN),
        (ConnectionError("unavailable"), Outcome.UNKNOWN),
        (DeliveryRejected("explicit refusal"), Outcome.REJECTED),
    ],
)
async def test_failure_requeues_original_notification(error, outcome):
    store, accept = AsyncMock(), AsyncMock(side_effect=error)
    result = await deliver_durable("original", store, accept)
    assert result.outcome == outcome and result.error is error
    assert result.confirmation == Confirmation.DURABLE_ACCEPTANCE
    store.retry.assert_awaited_once_with("original")
    store.delivered.assert_not_awaited()


async def test_cancelled_delivery_does_not_settle():
    store, accept = AsyncMock(), AsyncMock(side_effect=asyncio.CancelledError)
    with pytest.raises(asyncio.CancelledError):
        await deliver_durable("original", store, accept)
    store.retry.assert_not_awaited()
    store.delivered.assert_not_awaited()


async def test_persistence_failure_is_not_classified_as_receiver_failure():
    store, accept = AsyncMock(), AsyncMock()
    store.delivered.side_effect = ConnectionError("commit unknown")
    with pytest.raises(ConnectionError):
        await deliver_durable("original", store, accept)
    store.retry.assert_not_awaited()


async def test_durable_confirmation_follows_receipt_then_persistence():
    order = []
    store = AsyncMock()
    store.delivered.side_effect = lambda key: order.append(("persist", key))

    async def accept():
        order.append(("receipt", "original"))

    result = await deliver_durable("original", store, accept)
    assert order == [("receipt", "original"), ("persist", "original")]
    assert result.outcome == Outcome.CONFIRMED
    assert result.confirmation != Confirmation.BROKER
