import base64
import dataclasses
import json
from pathlib import Path

import pytest

from reliable_messaging.message import Message
from reliable_messaging.transaction import (
    Decision,
    Evidence,
    FrozenBranch,
    FrozenPlan,
    State,
    resolve,
)


def plans():
    vectors = json.loads(
        (
            Path(__file__).resolve().parents[2] / "contracts/transaction-plans-draft-v1.json"
        ).read_text()
    )
    assert vectors["version"] == 1 and len(vectors["plans"]) == 2
    result = []
    for vector in vectors["plans"]:
        branches = []
        for branch in vector["branches"]:
            m = branch["message"]
            message = Message(
                m["Producer"],
                m["ID"],
                m["Destination"],
                m["EventType"],
                m["SchemaVersion"],
                m["Scope"],
                m["ContentType"],
                m["OccurredAt"],
                base64.b64decode(m["Payload"]),
            )
            assert message.fingerprint == branch["message_fingerprint"]
            frozen = FrozenBranch(
                branch["id"],
                branch["physical_topic"],
                branch["route_revision"],
                message,
                base64.b64decode(branch["wire"]),
            )
            assert frozen.wire_hash.hex() == branch["wire_hash"]
            branches.append(frozen)
        plan = FrozenPlan(
            vector["owner"],
            vector["scope"],
            vector["operation_id"],
            vector["idempotency_key"],
            bytes.fromhex(vector["input_hash"]),
            tuple(branches),
        )
        assert plan.digest.hex() == vector["plan_digest"]
        for branch in plan.branches:
            ref = plan.reference(branch.branch_id, 1, "epoch-a")
            assert ref.wire_hash == branch.wire_hash
            assert ref.message_fingerprint.hex() == branch.message.fingerprint
        result.append(plan)
    return result


def test_shared_go_python_vectors():
    plans()


def test_first_wire_is_owned_and_lookup_uses_stable_business_input():
    plan = plans()[0]
    original = plan.branches[0]
    wire = bytearray(original.wire)
    branch = dataclasses.replace(original, wire=wire)
    reservation = dataclasses.replace(plan, branches=(branch,))
    wire[0] ^= 255
    assert reservation.branches[0].wire == original.wire
    assert reservation.digest == plan.digest
    reservation.match_request(plan.owner, plan.scope, plan.idempotency_key, plan.input_hash)
    for owner, scope, key in (
        ("other", plan.scope, plan.idempotency_key),
        (plan.owner, "other", plan.idempotency_key),
        (plan.owner, plan.scope, "other"),
    ):
        with pytest.raises(ValueError, match="identity differs"):
            reservation.match_request(owner, scope, key, plan.input_hash)
    with pytest.raises(ValueError, match="different business input"):
        reservation.match_request(plan.owner, plan.scope, plan.idempotency_key, b"x" * 32)
    rerouted = dataclasses.replace(
        plan, branches=(dataclasses.replace(branch, route_revision="next"),)
    )
    assert rerouted.digest != plan.digest
    assert rerouted.branches[0].message.fingerprint == original.message.fingerprint


@pytest.mark.parametrize("operation", list(State) + ["unexpected"])
@pytest.mark.parametrize("branch", list(State) + ["unexpected"])
def test_only_joint_terminal_states_resolve(operation, branch):
    ref = plans()[0].reference("branch-0", 7, "epoch-a")
    expected = Decision.UNKNOWN
    if operation == branch == State.COMMITTED:
        expected = Decision.COMMIT
    elif operation == branch == State.ABORTED:
        expected = Decision.ROLLBACK
    assert (
        resolve(ref, Evidence(reference=ref, execution_state=operation, branch_state=branch))
        == expected
    )
    assert resolve(ref, None) == Decision.UNKNOWN


def test_every_reference_field_is_bound():
    ref = plans()[0].reference("branch-0", 7, "epoch-a")
    proof = Evidence(reference=ref, execution_state=State.COMMITTED, branch_state=State.COMMITTED)
    for field in dataclasses.fields(ref):
        value = getattr(ref, field.name)
        if isinstance(value, str):
            changed = value + "-wrong"
        elif isinstance(value, bytes):
            changed = bytes([value[0] ^ 1]) + value[1:]
        else:
            changed = value + 1
        assert resolve(dataclasses.replace(ref, **{field.name: changed}), proof) == Decision.UNKNOWN


def test_reject_unreserved_invalid_or_cross_scope_identity():
    plan = plans()[0]
    for changes in (
        {"owner": ""},
        {"operation_id": ""},
        {"idempotency_key": ""},
        {"branches": ()},
        {"branches": plan.branches + plan.branches},
        {"scope": "another-org"},
    ):
        with pytest.raises(ValueError):
            dataclasses.replace(plan, **changes)
    with pytest.raises(ValueError):
        dataclasses.replace(plan.branches[0], wire=b"")
    for fence in (0, -1, 1 << 64, True):
        with pytest.raises(ValueError):
            plan.reference("branch-0", fence, "epoch")
    with pytest.raises(ValueError):
        plan.reference("missing-branch", 1, "epoch")


def test_new_fence_reuses_wire_without_reopening_old_terminal():
    plan = plans()[0]
    old = plan.reference("branch-0", 1, "epoch")
    new = plan.reference("branch-0", 2, "epoch")
    old_proof = Evidence(reference=old, execution_state=State.ABORTED, branch_state=State.ABORTED)
    new_proof = Evidence(
        reference=new, execution_state=State.COMMITTED, branch_state=State.COMMITTED
    )
    assert (old.plan_digest, old.wire_hash, old.message_id) == (
        new.plan_digest,
        new.wire_hash,
        new.message_id,
    )
    assert resolve(old, old_proof) == Decision.ROLLBACK
    assert resolve(new, new_proof) == Decision.COMMIT
    assert resolve(old, new_proof) == resolve(new, old_proof) == Decision.UNKNOWN
