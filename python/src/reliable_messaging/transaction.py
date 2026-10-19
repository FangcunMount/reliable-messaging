"""Provisional transaction-message contracts; no I/O or transaction ownership.

Public API stability awaits RocketMQ M2 original MySQL/Mongo transaction proofs.
These producer outcomes never describe consumer effects or business receipts.
"""

import base64
import hashlib
import json
import math
import struct
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol

from reliable_messaging.message import Message

MAX_FENCE = (1 << 63) - 1
MAX_BRANCHES = 16
MAX_PLAN_BYTES = 8 << 20
CODEC_VERSION = 1


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
        if any(
            len(v.encode()) > n
            for v, n in zip(
                (self.branch_id, self.physical_topic, self.route_revision),
                (128, 255, 128),
                strict=True,
            )
        ):
            raise ValueError("branch identity too long")
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
        if any(
            len(v.encode()) > n
            for v, n in zip(
                (self.owner, self.scope, self.operation_id, self.idempotency_key),
                (128, 255, 128, 1024),
                strict=True,
            )
        ):
            raise ValueError("transaction plan identity too long")
        object.__setattr__(self, "input_hash", _hash(self.input_hash))
        object.__setattr__(self, "branches", tuple(self.branches))
        if (
            not self.branches
            or len(self.branches) > MAX_BRANCHES
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
            or not 0 < self.fence <= MAX_FENCE
        ):
            raise ValueError("positive signed-int64-compatible fence required")
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
    execution_state: State
    branch_state: State


def resolve(reference: Reference, evidence: Evidence | None) -> Decision:
    if not isinstance(reference, Reference) or evidence is None or evidence.reference != reference:
        return Decision.UNKNOWN
    if evidence.execution_state == State.COMMITTED and evidence.branch_state == State.COMMITTED:
        return Decision.COMMIT
    if evidence.execution_state == State.ABORTED and evidence.branch_state == State.ABORTED:
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
    broker_message_id: str = ""
    broker_transaction_id: str = ""
    error_code: str = ""


@dataclass(frozen=True)
class HalfReceipt:
    outcome: HalfOutcome
    broker_message_id: str = ""
    broker_transaction_id: str = ""
    error_code: str = ""

    def __post_init__(self) -> None:
        if (
            self.outcome not in (HalfOutcome.PREPARED, HalfOutcome.REJECTED, HalfOutcome.UNKNOWN)
            or len(self.broker_message_id.encode()) > 512
            or len(self.broker_transaction_id.encode()) > 4096
            or len(self.error_code.encode()) > 128
            or (
                self.outcome == HalfOutcome.PREPARED
                and not (self.broker_message_id and self.broker_transaction_id)
            )
        ):
            raise ValueError("invalid half receipt")


@dataclass(frozen=True)
class Result:
    local: LocalOutcome
    branches: tuple[BranchResult, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "branches", tuple(self.branches))


class StaleExecutionError(ValueError):
    pass


class BindingError(ValueError):
    pass


class SchemaError(ValueError):
    pass


class ShutdownIncompleteError(RuntimeError):
    def __init__(self, message: str, counts: dict[str, int | bool] | None = None) -> None:
        super().__init__(message)
        self.counts = dict(counts or {})


@dataclass(frozen=True)
class Request:
    owner: str
    scope: str
    key: str
    input_hash: bytes

    def __post_init__(self) -> None:
        _required(self.owner, self.scope, self.key)
        object.__setattr__(self, "input_hash", _hash(self.input_hash))

    def match(self, plan: FrozenPlan) -> None:
        plan.match_request(self.owner, self.scope, self.key, self.input_hash)

    @property
    def key_digest(self) -> bytes:
        return hashlib.sha256(
            b"rm-tx-request-v1\0"
            + b"".join(_field(v.encode()) for v in (self.owner, self.scope, self.key))
        ).digest()


@dataclass(frozen=True)
class Binding:
    owner: str
    store_id: str
    writer_epoch: str

    def __post_init__(self) -> None:
        _required(self.owner, self.store_id, self.writer_epoch)


@dataclass(frozen=True)
class Execution:
    plan: FrozenPlan
    fence: int
    writer_epoch: str
    token: bytes = field(repr=False)
    lease_until: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.plan, FrozenPlan):
            raise ValueError("frozen plan required")
        if type(self.fence) is not int or not 0 < self.fence <= MAX_FENCE:
            raise ValueError("positive signed-int64-compatible fence required")
        _required(self.writer_epoch)
        object.__setattr__(self, "token", _hash(self.token))
        if self.lease_until.tzinfo is None:
            raise ValueError("timezone-aware lease required")

    @property
    def owned(self) -> bool:
        """Observed executions carry a zero capability and cannot execute/abort."""
        return any(self.token)

    @property
    def references(self) -> tuple[Reference, ...]:
        return tuple(
            self.plan.reference(b.branch_id, self.fence, self.writer_epoch)
            for b in self.plan.branches
        )


@dataclass(frozen=True)
class Reservation:
    execution: Execution
    acquired: bool
    state: State


class Store(Protocol):
    async def lookup(self, request: Request) -> Reservation | None: ...
    async def reserve(
        self, request: Request, candidate: FrozenPlan, *, lease_seconds: float = 60
    ) -> Reservation: ...
    async def begin_prepare(self, execution: Execution) -> None: ...
    async def evidence(self, reference: Reference) -> Evidence | None: ...
    async def abort(self, execution: Execution) -> bool: ...
    async def reconcile_once(self, limit: int) -> int: ...


@dataclass(frozen=True)
class RetryAuthorization:
    previous: Execution


@dataclass(frozen=True)
class ReplanAuthorization:
    previous: Execution
    candidate: FrozenPlan


class AuthorizedStore(Store, Protocol):
    async def retry(
        self, authorization: RetryAuthorization, *, lease_seconds: float = 60
    ) -> Reservation: ...
    async def replan(
        self, authorization: ReplanAuthorization, *, lease_seconds: float = 60
    ) -> Reservation: ...


class Guard(Protocol):
    async def commit_witness(self) -> None: ...


LocalExecutor = Callable[[Execution], Awaitable[LocalOutcome]]


class Sender(Protocol):
    async def execute(self, execution: Execution, local_executor: LocalExecutor) -> Result: ...


class StoreChecker:
    def __init__(self, store: Store) -> None:
        self.store = store

    async def check(self, reference: Reference) -> Decision:
        try:
            return resolve(reference, await self.store.evidence(reference))
        except Exception:
            return Decision.UNKNOWN


def classify_half(*, confirmed: bool, rejected: bool) -> HalfOutcome:
    if confirmed and not rejected:
        return HalfOutcome.PREPARED
    if rejected and not confirmed:
        return HalfOutcome.REJECTED
    return HalfOutcome.UNKNOWN


def decide_local(
    references: tuple[Reference, ...], evidence: tuple[Evidence | None, ...]
) -> LocalOutcome:
    if not references or len(references) != len(evidence):
        return LocalOutcome.UNKNOWN
    first = references[0]
    for ref in references[1:]:
        if (
            ref.owner,
            ref.scope,
            ref.operation_id,
            ref.fence,
            ref.writer_epoch,
            ref.plan_digest,
        ) != (
            first.owner,
            first.scope,
            first.operation_id,
            first.fence,
            first.writer_epoch,
            first.plan_digest,
        ):
            return LocalOutcome.UNKNOWN
    decisions = {resolve(r, e) for r, e in zip(references, evidence, strict=True)}
    if decisions == {Decision.COMMIT}:
        return LocalOutcome.COMMITTED
    if decisions == {Decision.ROLLBACK}:
        return LocalOutcome.ABORTED
    return LocalOutcome.UNKNOWN


def encode_plan(plan: FrozenPlan) -> bytes:
    branches = []
    for branch in plan.branches:
        m = branch.message
        branches.append(
            {
                "branch_id": branch.branch_id,
                "physical_topic": branch.physical_topic,
                "route_revision": branch.route_revision,
                "message": {
                    "producer": m.producer,
                    "message_id": m.message_id,
                    "destination": m.destination,
                    "event_type": m.event_type,
                    "schema_version": m.schema_version,
                    "scope": m.scope,
                    "content_type": m.content_type,
                    "occurred_at": m.occurred_at,
                    "payload": base64.b64encode(m.payload).decode("ascii"),
                },
                "wire": base64.b64encode(branch.wire).decode("ascii"),
            }
        )
    data = json.dumps(
        {
            "version": CODEC_VERSION,
            "owner": plan.owner,
            "scope": plan.scope,
            "operation_id": plan.operation_id,
            "idempotency_key": plan.idempotency_key,
            "input_hash": plan.input_hash.hex(),
            "plan_digest": plan.digest.hex(),
            "branches": branches,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    if len(data) > MAX_PLAN_BYTES:
        raise ValueError("plan exceeds codec limit")
    return data


def decode_plan(data: bytes) -> FrozenPlan:
    if not data or len(data) > MAX_PLAN_BYTES:
        raise ValueError("invalid plan size")

    def exact(value: Any, keys: str) -> dict[str, Any]:
        if not isinstance(value, dict) or set(value) != set(keys.split()):
            raise ValueError("invalid versioned plan fields")
        return value

    p = exact(
        json.loads(data),
        "version owner scope operation_id idempotency_key input_hash plan_digest branches",
    )
    if type(p["version"]) is not int or p["version"] != CODEC_VERSION:
        raise ValueError("unsupported plan codec")
    branches = []
    for value in p["branches"]:
        b = exact(value, "branch_id physical_topic route_revision message wire")
        m = exact(
            b["message"],
            "producer message_id destination event_type schema_version "
            "scope content_type occurred_at payload",
        )
        message = Message(
            m["producer"],
            m["message_id"],
            m["destination"],
            m["event_type"],
            m["schema_version"],
            m["scope"],
            m["content_type"],
            m["occurred_at"],
            base64.b64decode(m["payload"], validate=True),
        )
        branches.append(
            FrozenBranch(
                b["branch_id"],
                b["physical_topic"],
                b["route_revision"],
                message,
                base64.b64decode(b["wire"], validate=True),
            )
        )
    plan = FrozenPlan(
        p["owner"],
        p["scope"],
        p["operation_id"],
        p["idempotency_key"],
        bytes.fromhex(p["input_hash"]),
        tuple(branches),
    )
    if plan.digest.hex() != p["plan_digest"]:
        raise ValueError("frozen plan digest differs")
    return plan


class Runtime(Protocol):
    async def start(self) -> None: ...
    def stop_admission(self) -> None: ...
    async def close(self, *, grace_seconds: float) -> None: ...
    async def wait(self) -> None: ...


@dataclass(frozen=True)
class Limits:
    sends: int = 8
    branches: int = 16
    check_workers: int = 4
    check_queue: int = 32
    reconcile_batch: int = 100
    check_timeout: float = 2
    rpc_timeout: float = 5
    lease: float = 60
    reconcile_interval: float = 5

    def __post_init__(self) -> None:
        if not (
            all(
                type(v) is int
                for v in (
                    self.sends,
                    self.branches,
                    self.check_workers,
                    self.check_queue,
                    self.reconcile_batch,
                )
            )
            and 0 < self.sends <= 1024
            and 0 < self.branches <= MAX_BRANCHES
            and 0 < self.check_workers <= 128
            and 0 <= self.check_queue <= 10000
            and 0 < self.reconcile_batch <= 1000
            and self.check_timeout > 0
            and self.rpc_timeout > 0
            and self.lease > self.rpc_timeout
            and self.reconcile_interval > 0
            and 0.001 <= self.lease <= 86400
            and all(
                math.isfinite(v)
                for v in (self.check_timeout, self.rpc_timeout, self.lease, self.reconcile_interval)
            )
        ):
            raise ValueError("invalid bounded transaction limits")


def validate_advance(
    previous: Execution,
    candidate: FrozenPlan,
    evidence: tuple[Evidence | None, ...],
    *,
    replan: bool,
) -> None:
    if previous.fence >= MAX_FENCE:
        raise ValueError("fence exhausted")
    Request(
        previous.plan.owner,
        previous.plan.scope,
        previous.plan.idempotency_key,
        previous.plan.input_hash,
    ).match(candidate)
    if decide_local(previous.references, evidence) != LocalOutcome.ABORTED:
        raise StaleExecutionError("old execution is not durably aborted")
    if replan:
        if candidate.operation_id == previous.plan.operation_id:
            raise ValueError("replan requires a new immutable operation")
    elif candidate.digest != previous.plan.digest:
        raise ValueError("retry must preserve the frozen plan")


def encode_reference(reference: Reference) -> bytes:
    from dataclasses import asdict

    values = asdict(reference)
    for name in ("plan_digest", "wire_hash", "message_fingerprint"):
        values[name] = values[name].hex()
    data = json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode()
    if len(data) > 8192:
        raise ValueError("reference exceeds protocol limit")
    return data


def decode_reference(data: bytes) -> Reference:
    if not data or len(data) > 8192:
        raise ValueError("invalid reference size")
    values = json.loads(data)
    for name in ("plan_digest", "wire_hash", "message_fingerprint"):
        values[name] = bytes.fromhex(values[name])
    return Reference(**values)


def execution_key(owner: str, operation_id: str, epoch: str, fence: int) -> bytes:
    return hashlib.sha256(
        b"rm-tx-execution-v1\0"
        + b"".join(_field(value.encode()) for value in (owner, operation_id, epoch))
        + _field(struct.pack(">Q", fence))
    ).digest()


def branch_key(branch_id: str) -> bytes:
    return hashlib.sha256(branch_id.encode()).digest()
