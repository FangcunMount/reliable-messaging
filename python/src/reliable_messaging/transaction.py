"""Provisional transaction-message contracts; no I/O or transaction ownership.

Public API stability awaits RocketMQ M2 original MySQL/Mongo transaction proofs.
These producer outcomes never describe consumer effects or business receipts.
"""

import hashlib
import struct
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol

from reliable_messaging.message import Message


def _required(*values: str) -> None:
    if any(
        not isinstance(value, str) or not value or not value.encode("utf-8") for value in values
    ):
        raise ValueError("required UTF-8 transaction identity")


def _hash(value: bytes) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise ValueError("SHA256 bytes required")
    result = bytes(value)
    if len(result) != 32:
        raise ValueError("SHA256 bytes required")
    return result


def _field(value: bytes) -> bytes:
    return struct.pack(">Q", len(value)) + value


@dataclass(frozen=True)
class FrozenBranch:
    branch_id: str
    physical_topic: str
    route_revision: str
    message: Message
    wire: bytes

    def __post_init__(self) -> None:
        _required(self.branch_id, self.physical_topic, self.route_revision)
        if not isinstance(self.message, Message):
            raise ValueError("original immutable message required")
        if not isinstance(self.wire, (bytes, bytearray, memoryview)) or not self.wire:
            raise ValueError("already frozen wire bytes required")
        object.__setattr__(self, "wire", bytes(self.wire))

    @property
    def wire_hash(self) -> bytes:
        return hashlib.sha256(self.wire).digest()


@dataclass(frozen=True)
class FrozenPlan:
    owner: str
    scope: str
    operation_id: str
    idempotency_key: str
    input_hash: bytes
    branches: tuple[FrozenBranch, ...]
    _digest: bytes = field(init=False, repr=False)

    def __post_init__(self) -> None:
        _required(self.owner, self.scope, self.operation_id, self.idempotency_key)
        object.__setattr__(self, "input_hash", _hash(self.input_hash))
        object.__setattr__(self, "branches", tuple(self.branches))
        if (
            not self.branches
            or any(not isinstance(b, FrozenBranch) for b in self.branches)
            or len({b.branch_id for b in self.branches}) != len(self.branches)
            or any(b.message.scope != self.scope for b in self.branches)
        ):
            raise ValueError("nonempty unique branches in original scope required")
        digest = hashlib.sha256(b"rm-tx-plan-draft-v1\x00")
        for value in (self.owner, self.scope, self.operation_id, self.idempotency_key):
            digest.update(_field(value.encode()))
        digest.update(_field(self.input_hash))
        digest.update(_field(struct.pack(">Q", len(self.branches))))
        for branch in self.branches:
            for value_bytes in (
                branch.branch_id.encode(),
                bytes.fromhex(branch.message.fingerprint),
                branch.physical_topic.encode(),
                branch.route_revision.encode(),
                branch.wire_hash,
            ):
                digest.update(_field(value_bytes))
        object.__setattr__(self, "_digest", digest.digest())

    @property
    def digest(self) -> bytes:
        return self._digest

    def match_request(self, owner: str, scope: str, key: str, input_hash: bytes) -> None:
        """Check durable lookup/unique-key winner before generating new IDs/wire."""
        if (owner, scope, key) != (self.owner, self.scope, self.idempotency_key):
            raise ValueError("transaction request identity differs")
        if _hash(input_hash) != self.input_hash:
            raise ValueError("transaction idempotency key has different business input")

    def reference(self, branch_id: str, fence: int, epoch: str) -> "Reference":
        for branch in self.branches:
            if branch.branch_id == branch_id:
                return Reference(
                    self.owner,
                    self.scope,
                    self.operation_id,
                    branch_id,
                    branch.message.message_id,
                    branch.message.destination,
                    branch.physical_topic,
                    branch.route_revision,
                    epoch,
                    self.digest,
                    branch.wire_hash,
                    bytes.fromhex(branch.message.fingerprint),
                    fence,
                )
        raise ValueError("branch not in frozen plan")


@dataclass(frozen=True)
class Reference:
    owner: str
    scope: str
    operation_id: str
    branch_id: str
    message_id: str
    logical_topic: str
    physical_topic: str
    route_revision: str
    writer_epoch: str
    plan_digest: bytes
    wire_hash: bytes
    message_fingerprint: bytes
    fence: int

    def __post_init__(self) -> None:
        _required(
            self.owner,
            self.scope,
            self.operation_id,
            self.branch_id,
            self.message_id,
            self.logical_topic,
            self.physical_topic,
            self.route_revision,
            self.writer_epoch,
        )
        if (
            not isinstance(self.fence, int)
            or isinstance(self.fence, bool)
            or not 0 < self.fence < 1 << 64
        ):
            raise ValueError("positive uint64 fence required")
        for name in ("plan_digest", "wire_hash", "message_fingerprint"):
            object.__setattr__(self, name, _hash(getattr(self, name)))


class State(StrEnum):
    PREPARED = "prepared"
    COMMITTED = "committed"
    ABORTED = "aborted"


class Decision(StrEnum):
    UNKNOWN = "unknown"
    COMMIT = "commit"
    ROLLBACK = "rollback"


@dataclass(frozen=True)
class Evidence:
    reference: Reference
    operation_state: State
    branch_state: State


def resolve(reference: Reference, evidence: Evidence | None) -> Decision:
    if not isinstance(reference, Reference) or evidence is None or evidence.reference != reference:
        return Decision.UNKNOWN
    if evidence.operation_state == State.COMMITTED and evidence.branch_state == State.COMMITTED:
        return Decision.COMMIT
    if evidence.operation_state == State.ABORTED and evidence.branch_state == State.ABORTED:
        return Decision.ROLLBACK
    # Expired leases/missing/in-progress/contradictory records are not abortion proofs.
    return Decision.UNKNOWN


class Checker(Protocol):
    """Read authoritative host evidence; return Unknown on DB errors/timeouts."""

    async def check(self, reference: Reference) -> Decision: ...


class HalfOutcome(StrEnum):
    NOT_ATTEMPTED = "not_attempted"
    PREPARED = "prepared"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


class LocalOutcome(StrEnum):
    NOT_STARTED = "not_started"
    COMMITTED = "committed"
    ABORTED = "aborted"
    UNKNOWN = "unknown"


class FinalOutcome(StrEnum):
    NOT_ATTEMPTED = "not_attempted"
    COMMIT_CONFIRMED = "commit_confirmed"
    ROLLBACK_CONFIRMED = "rollback_confirmed"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class BranchResult:
    reference: Reference
    half: HalfOutcome
    final: FinalOutcome


@dataclass(frozen=True)
class Result:
    local: LocalOutcome
    branches: tuple[BranchResult, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "branches", tuple(self.branches))
