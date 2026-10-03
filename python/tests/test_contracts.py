import base64
import json
import shutil
import subprocess
from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest

from reliable_messaging import Message, Outcome

ROOT = Path(__file__).resolve().parents[2]


def message(value):
    return Message(
        value["producer"],
        value["message_id"],
        value["destination"],
        value["event_type"],
        value["schema_version"],
        value["tenant"],
        value["content_type"],
        value["occurred_at"],
        value["payload"].encode(),
    )


def test_shared_golden_vectors():
    cases = json.loads((ROOT / "contracts/fixtures/identity-vectors.json").read_text())["cases"]
    assert len(cases) == 10
    for case in cases:
        value = message(case["message"])
        # Generic delivery does not impose the reference checker's host-specific validity.
        assert value.fingerprint == case["expected_fingerprint"], case["name"]
        assert value.identity == tuple(
            case["message"][k] for k in ("producer", "message_id", "destination")
        )


def test_go_python_extended_contract(tmp_path):
    # This M7 oracle calls existing public primitives only; it does not run M0-M6 suites.
    if shutil.which("go") is None:
        pytest.fail("Go required for the M7 cross-language oracle")
    source = tmp_path / "main.go"
    source.write_text("""package main
import (
 "encoding/json"
 "fmt"
 "os"
 "github.com/FangcunMount/reliable-messaging/message"
 "github.com/FangcunMount/reliable-messaging/transport"
)
func main() {
 var values []message.Input
 if err := json.NewDecoder(os.Stdin).Decode(&values); err != nil { panic(err) }
 results := make([]map[string]any, 0, len(values))
 for _, value := range values {
  m, err := message.New(value)
  result := map[string]any{"valid": err == nil}
  if err == nil { result["hash"] = fmt.Sprintf("%x", m.Fingerprint()) }
  results = append(results, result)
 }
 json.NewEncoder(os.Stdout).Encode(map[string]any{
  "values": results, "outcomes": []int{
   int(transport.Unknown), int(transport.Confirmed), int(transport.Rejected)},
 })
}
""")
    original = dict(
        Producer="qs-ai",
        ID="原消息-1",
        Destination="qs.results",
        EventType="state.changed",
        SchemaVersion="v1",
        Scope="18446744073709551615",
        ContentType="application/json",
        OccurredAt="2026-10-03T12:34:56.123456789+08:00",
        Payload=base64.b64encode(
            '{"id":18446744073709551615,"text":"青岛😀","unknown":null}'.encode()
        ).decode(),
    )
    vectors = [original, {**original, "OccurredAt": "2026-10-03T04:34:56.123456789Z"}]
    vectors += [{**original, key: ""} for key in original]
    vectors += [
        {**original, "OccurredAt": at}
        for at in (
            "2026-02-30T00:00:00Z",
            "2026-10-03T24:00:00Z",
            "2026-10-03T12:00:00",
            "2026-10-03 12:00:00+08:00",
            "2026-10-03T12:00:00.000000001+08:00",
            "0000-02-29T00:00:00Z",
            "2026-10-03T1:00:00,123Z",
            "2026-10-03T12:00:00+24:60",
            "2026-10-03T12:00:00+25:00",
        )
    ]
    vectors += [{**original, "ID": "中" * 43}, {**original, "ID": "x" * 128}]
    completed = subprocess.run(
        ["go", "run", str(source)],
        input=json.dumps(vectors),
        text=True,
        cwd=ROOT,
        capture_output=True,
        check=True,
        timeout=60,
    )
    oracle = json.loads(completed.stdout)
    assert oracle["outcomes"] == [int(x) for x in Outcome]
    for value, expected in zip(vectors, oracle["values"], strict=True):
        try:
            item = Message(
                value["Producer"],
                value["ID"],
                value["Destination"],
                value["EventType"],
                value["SchemaVersion"],
                value["Scope"],
                value["ContentType"],
                value["OccurredAt"],
                base64.b64decode(value["Payload"]),
            )
        except ValueError:
            assert not expected["valid"], value
        else:
            assert expected["valid"], value
            assert item.fingerprint == expected["hash"]
            assert json.loads(item.payload)["id"] == 18446744073709551615
            assert json.loads(item.payload)["unknown"] is None


def test_immutable_bytes_and_format_are_preserved():
    source = bytearray(b'{ "big":18446744073709551615 }')
    value = Message(
        "qs-ai",
        "1",
        "results",
        "changed",
        "v1",
        "scope",
        "json",
        "2026-10-03T12:00:00+08:00",
        source,
    )
    original = value.fingerprint
    source[0] = 0
    assert value.fingerprint == original
    assert value.payload.startswith(b"{")
    assert replace(value, payload=b'{"big":18446744073709551615}').fingerprint != original
    assert replace(value, occurred_at="2026-10-03T04:00:00Z").fingerprint != original
    with pytest.raises(FrozenInstanceError):
        value.message_id = "other"
