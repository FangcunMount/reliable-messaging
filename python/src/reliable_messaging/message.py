"""Go message.Input identity/hash compatibility, without a new wire envelope."""

import hashlib
import re
import struct
from calendar import monthrange
from dataclasses import dataclass

_TIME = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{1,2}):([0-9]{2}):([0-9]{2})"
    r"(?:[.,][0-9]+)?(?:Z|[+-]([0-9]{2}):([0-9]{2}))\Z"
)


@dataclass(frozen=True)
class Message:
    producer: str
    message_id: str
    destination: str
    event_type: str
    schema_version: str
    scope: str
    content_type: str
    occurred_at: str
    payload: bytes

    def __post_init__(self) -> None:
        limits = (128, 128, 255, 255, 128, 255, 255, 64)
        for value, limit in zip(self._fields()[:-1], limits, strict=True):
            if not value or len(value) > limit:
                raise ValueError(f"required UTF-8 field exceeds {limit} bytes or is empty")
        parsed = _TIME.fullmatch(self.occurred_at)
        if parsed is None:
            raise ValueError("occurred_at requires RFC3339 with an explicit offset")
        year, month, day, hour, minute, second = map(int, parsed.groups()[:6])
        # Match Go time.Parse(RFC3339Nano), including its layout-parser fallbacks.
        # Year zero and offset 24:60 are accepted by Go; datetime.fromisoformat differs.
        if not 1 <= month <= 12 or not 1 <= day <= monthrange(year, month)[1]:
            raise ValueError("invalid occurred_at calendar date")
        if hour > 23 or minute > 59 or second > 59:
            raise ValueError("invalid occurred_at clock")
        if parsed[7] is not None and (int(parsed[7]) > 24 or int(parsed[8]) > 60):
            raise ValueError("invalid occurred_at offset")
        if not isinstance(self.payload, (bytes, bytearray, memoryview)) or not self.payload:
            raise ValueError("payload bytes required")
        object.__setattr__(self, "payload", bytes(self.payload))

    def _fields(self) -> tuple[bytes, ...]:
        return tuple(
            value.encode("utf-8")
            for value in (
                self.producer,
                self.message_id,
                self.destination,
                self.event_type,
                self.schema_version,
                self.scope,
                self.content_type,
                self.occurred_at,
            )
        ) + (self.payload,)

    @property
    def identity(self) -> tuple[str, str, str]:
        return self.producer, self.message_id, self.destination

    @property
    def fingerprint(self) -> str:
        digest = hashlib.sha256(b"rm-fingerprint-draft-v1\x00")
        for value in self._fields():
            digest.update(struct.pack(">Q", len(value)))
            digest.update(value)
        return digest.hexdigest()
