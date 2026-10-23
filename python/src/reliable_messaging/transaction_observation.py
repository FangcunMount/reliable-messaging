"""Read-only protocol metadata; never Broker backlog or consumer receipts."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from reliable_messaging.transaction import FinalOutcome, HalfOutcome, State


@dataclass
class ProtocolObservation:
    as_of: datetime
    reserved_executions: int = 0
    preparing_executions: int = 0
    # Lease expiry permits reconciliation, never proves rollback.
    prepared_expiry_age_seconds: float = 0.0
    confirmation_gap_branches: int = 0
    # Lower bound from the half audit, otherwise final audit; not actual half age.
    oldest_confirmation_observation_age_seconds: float = 0.0
    unaged_confirmation_gap_branches: int = 0
    untracked_aborted_branches: int = 0
    unobserved_committed_branches: int = 0
    metadata_conflicts: int = 0


class ObservationReader(Protocol):
    async def observe_protocol(self, *, budget_seconds: float = 2.0) -> ProtocolObservation: ...


def _age(now: datetime, at: datetime) -> float:
    return max(0.0, (now - at).total_seconds())


class _Accumulator:
    def __init__(self, as_of: datetime) -> None:
        self.snapshot = ProtocolObservation(as_of)

    def execution(self, state: State, started: bool, lease: datetime) -> None:
        s = self.snapshot
        if state == State.PREPARED:
            if not started:
                s.reserved_executions += 1
            else:
                s.preparing_executions += 1
                s.prepared_expiry_age_seconds = max(
                    s.prepared_expiry_age_seconds, _age(s.as_of, lease)
                )

    def branch(
        self,
        state: State,
        started: bool,
        branch_state: State,
        half: HalfOutcome,
        final: FinalOutcome,
        half_at: datetime | None,
        final_at: datetime | None,
    ) -> None:
        s = self.snapshot
        if (
            branch_state != state
            or (final == FinalOutcome.COMMIT_CONFIRMED and state != State.COMMITTED)
            or (final == FinalOutcome.ROLLBACK_CONFIRMED and state != State.ABORTED)
        ):
            s.metadata_conflicts += 1
        if state == State.COMMITTED and half != HalfOutcome.PREPARED:
            s.unobserved_committed_branches += 1
        positive = final in (FinalOutcome.COMMIT_CONFIRMED, FinalOutcome.ROLLBACK_CONFIRMED)
        if (
            state == State.ABORTED
            and started
            and half == HalfOutcome.NOT_ATTEMPTED
            and not positive
        ):
            s.untracked_aborted_branches += 1
        if (
            state == State.PREPARED
            or half not in (HalfOutcome.PREPARED, HalfOutcome.UNKNOWN)
            or positive
        ):
            return
        s.confirmation_gap_branches += 1
        at = half_at or final_at
        if at is None:
            s.unaged_confirmation_gap_branches += 1
        else:
            s.oldest_confirmation_observation_age_seconds = max(
                s.oldest_confirmation_observation_age_seconds, _age(s.as_of, at)
            )
