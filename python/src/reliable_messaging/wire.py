"""The existing Revision 2 envelope, including Go JSON escaping and checksum.

The checksum detects corruption; it does not authenticate a publisher.
"""

import base64
import hashlib
import hmac
import json
import struct
from dataclasses import dataclass, field

ENVELOPE_TYPE = "component-base.messaging.message.v1"
FAILED_TYPE = "component-base.messaging.failed.v1"
FAILED_CHANNEL = "cb-failed-handler"
NSQ_MAX_BYTES = 262_144
MAX_CAUSE_BYTES = 1024
MAX_METADATA_BYTES = 4096


@dataclass(frozen=True)
class Envelope:
    message_id: str
    payload: bytes
    metadata: dict[str, str] = field(default_factory=dict)


def go_json(value: object) -> bytes:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    for original, escaped in (
        ("&", r"\u0026"),
        ("<", r"\u003c"),
        (">", r"\u003e"),
        ("\u2028", r"\u2028"),
        ("\u2029", r"\u2029"),
    ):
        encoded = encoded.replace(original, escaped)
    return encoded.encode("utf-8")


def checksum(value: Envelope) -> str:
    digest = hashlib.sha256()
    parts = [value.message_id]
    for key in sorted(value.metadata):
        parts.extend((key, value.metadata[key]))
    for part in [*(item.encode("utf-8") for item in parts), value.payload]:
        digest.update(struct.pack(">Q", len(part)))
        digest.update(part)
    return digest.hexdigest()


def encode(value: Envelope) -> bytes:
    document: dict[str, object] = {"type": ENVELOPE_TYPE, "schema_revision": 2}
    if value.message_id:
        document["uuid"] = value.message_id
    if value.metadata:
        document["metadata"] = dict(sorted(value.metadata.items()))
    document["payload"] = base64.b64encode(value.payload).decode("ascii") if value.payload else None
    document["checksum"] = checksum(value)
    return go_json(document)


def decode(body: bytes) -> Envelope:
    try:
        value = json.loads(body)
        if not isinstance(value, dict) or value.get("type") != ENVELOPE_TYPE:
            raise ValueError("marked Revision 2 envelope required")
        if value.get("schema_revision") != 2:
            raise ValueError("Revision 2 required")
        identity, metadata = value.get("uuid", ""), value.get("metadata") or {}
        if not isinstance(identity, str) or not isinstance(metadata, dict):
            raise ValueError("invalid envelope identity or metadata")
        if any(not isinstance(k, str) or not isinstance(v, str) for k, v in metadata.items()):
            raise ValueError("metadata must contain strings")
        raw = value.get("payload")
        if raw is not None and not isinstance(raw, str):
            raise ValueError("invalid envelope payload")
        payload = base64.b64decode(raw, validate=True) if raw else b""
        envelope = Envelope(identity, payload, metadata)
        supplied = value.get("checksum")
        if not isinstance(supplied, str) or not hmac.compare_digest(supplied, checksum(envelope)):
            raise ValueError("checksum mismatch")
        return envelope
    except (ValueError, TypeError, UnicodeError) as error:
        raise ValueError("invalid Revision 2 envelope") from error


def failed_topic(topic: str, channel: str) -> str:
    digest = hashlib.sha256(f"{topic}\0{channel}".encode()).hexdigest()
    return "cb.failed." + digest[:24]


def encode_failure(
    value: Envelope,
    *,
    topic: str,
    channel: str,
    transport_id: str,
    attempts: int,
    timestamp: int,
    cause: str,
) -> bytes:
    if not topic or not channel or not value.message_id or attempts < 1 or not cause:
        raise ValueError("invalid failure handoff")
    # Never include exception text: a driver or host exception can contain plaintext.
    document: dict[str, object] = {
        "type": FAILED_TYPE,
        "provider": "nsq",
        "topic": topic,
        "channel": channel,
        "uuid": value.message_id,
    }
    if transport_id:
        document["transport_message_id"] = transport_id
    if value.metadata:
        document["metadata"] = dict(sorted(value.metadata.items()))
    document.update(
        payload=base64.b64encode(value.payload).decode("ascii") if value.payload else None,
        attempts=attempts,
    )
    if timestamp:
        document["timestamp"] = timestamp
    document["cause"] = cause
    return go_json(document)


def fits_nsq(value: Envelope, topic: str, channel: str) -> bool:
    """Reserve even the malformed-envelope handoff (whose payload is the whole wire)."""
    if len(go_json(value.metadata)) > MAX_METADATA_BYTES:
        return False
    wire = encode(value)
    worst = encode_failure(
        Envelope(value.message_id, wire, value.metadata),
        topic=topic,
        channel=channel,
        transport_id="0" * 16,
        attempts=65535,
        timestamp=9223372036854775807,
        cause="x" * MAX_CAUSE_BYTES,
    )
    return max(len(wire), len(worst)) <= NSQ_MAX_BYTES
