"""Settlement of an existing durable intent, never authorization for business retries."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import IntEnum, StrEnum
from typing import Protocol


class Outcome(IntEnum):
    UNKNOWN = 0
    CONFIRMED = 1
    REJECTED = 2


class Confirmation(StrEnum):
    BROKER = "broker"
    DURABLE_ACCEPTANCE = "durable_acceptance"


class DeliveryRejected(Exception):
    """The receiver explicitly rejected this attempt; the host owns retry eligibility."""


@dataclass(frozen=True)
class DeliveryResult:
    outcome: Outcome
    confirmation: Confirmation
    error: Exception | None = None


class ResultStore(Protocol):
    async def delivered(self, event_id: str) -> None: ...
    async def retry(self, event_id: str) -> None: ...


async def deliver_durable(
    event_id: str,
    store: ResultStore,
    accept: Callable[[], Awaitable[None]],
) -> DeliveryResult:
    """accept must verify the receiver's durable acknowledgement of the original ID.

    A broker publisher is not an accept callback. Host adapters retain their wire and
    acknowledgement validation. Only committed result notification may be retried.
    Cancellation leaves the durable row available; persistence errors propagate.
    """
    try:
        await accept()
    except Exception as error:
        await store.retry(event_id)
        return DeliveryResult(
            Outcome.REJECTED if isinstance(error, DeliveryRejected) else Outcome.UNKNOWN,
            Confirmation.DURABLE_ACCEPTANCE,
            error,
        )
    await store.delivered(event_id)
    return DeliveryResult(Outcome.CONFIRMED, Confirmation.DURABLE_ACCEPTANCE)
