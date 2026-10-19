import dataclasses
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from reliable_messaging.transaction import (
    MAX_FENCE,
    Evidence,
    Execution,
    HalfOutcome,
    Limits,
    LocalOutcome,
    Request,
    State,
    classify_half,
    decide_local,
    decode_plan,
    decode_reference,
    encode_plan,
    encode_reference,
    execution_key,
    validate_advance,
)
from reliable_messaging.transaction_mysql import SCHEMA

VECTORS = Path(__file__).resolve().parents[2] / "contracts/transaction-storage-v1.json"


def test_installed_schema_matches_go_original_transaction_template():
    assert SCHEMA == (VECTORS.parent.parent / "storage/transaction/mysql/schema.sql").read_text()


def test_shared_storage_vectors_and_first_wire_roundtrip():
    fixtures = json.loads(VECTORS.read_bytes())
    assert fixtures["version"] == 1 and len(fixtures["plans"]) == 2
    for keys in fixtures["keys"]:
        ref = decode_reference(json.dumps(keys["reference"]).encode())
        assert json.loads(encode_reference(ref)) == keys["reference"]
        assert (
            execution_key(ref.owner, ref.operation_id, ref.writer_epoch, ref.fence).hex()
            == keys["execution_key"]
        )
    for stored in fixtures["plans"]:
        plan = decode_plan(json.dumps(stored).encode())
        assert json.loads(encode_plan(plan)) == stored
        assert decode_plan(encode_plan(plan)) == plan
        for field, value in (("version", 2), ("plan_digest", "00" * 32)):
            bad = dict(stored, **{field: value})
            with pytest.raises(ValueError):
                decode_plan(json.dumps(bad).encode())
        with pytest.raises(ValueError):
            decode_plan(encode_plan(plan) + b"{}")


def test_portable_fence_and_exact_bundle_evidence():
    plan = decode_plan(json.dumps(json.loads(VECTORS.read_bytes())["plans"][1]).encode())
    execution = Execution(plan, MAX_FENCE, "epoch", b"x" * 32, datetime.now(UTC))
    with pytest.raises(ValueError):
        dataclasses.replace(execution, fence=MAX_FENCE + 1)
    refs = execution.references
    for state, wanted in (
        (State.PREPARED, LocalOutcome.UNKNOWN),
        (State.COMMITTED, LocalOutcome.COMMITTED),
        (State.ABORTED, LocalOutcome.ABORTED),
    ):
        evidence = tuple(Evidence(r, state, state) for r in refs)
        assert decide_local(refs, evidence) == wanted
        assert decide_local(refs, (None,) + evidence[1:]) == LocalOutcome.UNKNOWN
    request = Request(plan.owner, plan.scope, plan.idempotency_key, plan.input_hash)
    request.match(plan)
    assert len(request.key_digest) == 32
    with pytest.raises(ValueError):
        dataclasses.replace(request, input_hash=b"z" * 32).match(plan)
    assert classify_half(confirmed=False, rejected=False) == HalfOutcome.UNKNOWN
    assert classify_half(confirmed=True, rejected=True) == HalfOutcome.UNKNOWN


@pytest.mark.parametrize("execution_state", list(State))
@pytest.mark.parametrize("branch_state", list(State))
def test_advance_requires_every_old_branch_aborted(execution_state, branch_state):
    plan = decode_plan(json.dumps(json.loads(VECTORS.read_bytes())["plans"][1]).encode())
    previous = Execution(plan, 1, "epoch", b"x" * 32, datetime.now(UTC))
    evidence = tuple(Evidence(r, execution_state, branch_state) for r in previous.references)
    if execution_state == branch_state == State.ABORTED:
        validate_advance(previous, plan, evidence, replan=False)
        candidate = dataclasses.replace(plan, operation_id="generation-2")
        validate_advance(previous, candidate, evidence, replan=True)
        with pytest.raises(ValueError):
            validate_advance(previous, candidate, evidence, replan=False)
        with pytest.raises(ValueError):
            validate_advance(previous, plan, evidence, replan=True)
    else:
        with pytest.raises(ValueError):
            validate_advance(previous, plan, evidence, replan=False)


def test_limits_and_branch_cap_are_explicit():
    assert Limits().check_queue == 32
    with pytest.raises(ValueError):
        Limits(lease=5)
