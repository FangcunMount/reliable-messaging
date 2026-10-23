import dataclasses
import json
from datetime import datetime
from importlib.resources import files
from pathlib import Path

import pytest

from reliable_messaging.transaction import FinalOutcome, HalfOutcome, State
from reliable_messaging.transaction_observation import _Accumulator

VECTORS = Path(__file__).resolve().parents[2] / "contracts/transaction-observation-v1.json"


def test_packaged_observation_query_matches_go():
    assert (
        files("reliable_messaging").joinpath("transaction_observation.sql").read_text()
        == (VECTORS.parent.parent / "storage/transaction/mysql/observation.sql").read_text()
    )


def test_shared_observation_meanings():
    fixture = json.loads(VECTORS.read_bytes())
    assert fixture["version"] == 1 and len(fixture["cases"]) == 14
    now = datetime.fromisoformat(fixture["as_of"])
    for case in fixture["cases"]:
        a = _Accumulator(now)
        state = State(case["state"])
        a.execution(state, case["started"], datetime.fromisoformat(case["lease"]))

        def branch(case=case, a=a, state=state):
            b = case["branch"]
            a.branch(
                state,
                case["started"],
                State(b["state"]),
                HalfOutcome(b["half"]),
                FinalOutcome(b["final"]),
                datetime.fromisoformat(b["half_at"]) if b.get("half_at") else None,
                datetime.fromisoformat(b["final_at"]) if b.get("final_at") else None,
            )

        if case["error"]:
            with pytest.raises(ValueError):
                branch()
            continue
        if "branch" in case:
            branch()
        got = dataclasses.asdict(a.snapshot)
        got.pop("as_of")
        assert got == {key: case["expected"].get(key, 0) for key in got}, case["name"]
        assert set(case["expected"]) <= set(got)
