"""Optional JOSE adapter. Keys and immutable sealed-byte storage belong to the host."""

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass, field
from typing import Any

from jwcrypto import jwe, jwk, jws  # type: ignore[import-untyped]

from reliable_messaging.wire import go_json

PROFILE = "rm-secure-v1"
KEY_ALGORITHM = "ECDH-ES+A256KW"
CONTENT_ALGORITHM = "A256GCM"


class ProtectionError(ValueError):
    """Deliberately carries no untrusted wire, decrypted bytes or driver exception text."""


@dataclass(frozen=True)
class Context:
    producer: str
    destination: str
    topic: str
    message_id: str
    metadata: dict[str, str] = field(default_factory=dict)

    def document(self) -> dict[str, object]:
        if not all((self.producer, self.destination, self.topic, self.message_id)):
            raise ProtectionError("complete protected routing context required")
        if any(not isinstance(k, str) or not isinstance(v, str) for k, v in self.metadata.items()):
            raise ProtectionError("metadata must contain strings")
        return {
            "producer": self.producer,
            "destination": self.destination,
            "topic": self.topic,
            "message_id": self.message_id,
            "metadata": dict(sorted(self.metadata.items())),
        }


@dataclass(frozen=True)
class TrustedSigner:
    producer: str
    key: Any


def _key(key: Any, *, private: bool = False) -> str:
    if not isinstance(key, jwk.JWK) or key.get("kty") != "EC" or key.get("crv") != "P-256":
        raise ProtectionError("P-256 key required")
    kid = key.get("kid")
    if not isinstance(kid, str) or not kid or (private and not key.has_private):
        raise ProtectionError("identified key with required capability expected")
    return kid


def _header(segment: str, allowed: set[str]) -> dict[str, Any]:
    try:
        value = json.loads(base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4)))
        if not isinstance(value, dict) or not set(value).issubset(allowed):
            raise ProtectionError("unsupported protected header")
        return value
    except (ValueError, TypeError, UnicodeError):
        raise ProtectionError("invalid protected header") from None


def seal(context: Context, payload: bytes, signing_key: Any, recipient_key: Any) -> bytes:
    """Seal once, then persist and reuse the returned bytes; no key lookup or I/O."""
    signing_kid = _key(signing_key, private=True)
    recipient_kid = _key(recipient_key)
    if signing_key.thumbprint() == recipient_key.thumbprint():
        raise ProtectionError("signing and encryption keys must be separate")
    document = {
        "context": context.document(),
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "payload": base64.b64encode(payload).decode("ascii"),
    }
    try:
        signed = jws.JWS(go_json(document))
        signed.add_signature(
            signing_key,
            protected={
                "alg": "ES256",
                "typ": PROFILE,
                "kid": signing_kid,
            },
        )
        encrypted = jwe.JWE(
            signed.serialize(compact=True).encode("ascii"),
            protected={
                "alg": KEY_ALGORITHM,
                "enc": CONTENT_ALGORITHM,
                "typ": PROFILE,
                "cty": "JWS",
                "kid": recipient_kid,
            },
        )
        encrypted.add_recipient(recipient_key)
        return encrypted.serialize(compact=True).encode("ascii")
    except Exception:
        raise ProtectionError("message protection failed") from None


def open_message(
    body: bytes,
    expected: Context,
    *,
    decrypt_keys: dict[str, Any],
    trusted_signers: dict[str, TrustedSigner],
    max_payload_bytes: int = 16 * 1024 * 1024,
) -> bytes:
    """Authenticate before using identity, metadata, body references or business effects."""
    if max_payload_bytes < 1 or len(body) > 3 * max_payload_bytes + 8192:
        raise ProtectionError("protected message exceeds decoding limit")
    try:
        compact = body.decode("ascii")
        if len(compact.split(".")) != 5:
            raise ProtectionError("compact encrypted message required")
        header = _header(compact.split(".")[0], {"alg", "enc", "typ", "cty", "kid", "epk"})
        if (header.get("alg"), header.get("enc"), header.get("typ"), header.get("cty")) != (
            KEY_ALGORITHM,
            CONTENT_ALGORITHM,
            PROFILE,
            "JWS",
        ):
            raise ProtectionError("unsupported protection profile")
        key = decrypt_keys.get(str(header.get("kid", "")))
        if key is None or _key(key, private=True) != header["kid"]:
            raise ProtectionError("unknown decryption key")
        encrypted = jwe.JWE(algs=[KEY_ALGORITHM, CONTENT_ALGORITHM])
        encrypted.deserialize(compact, key=key)
        signed_body = encrypted.payload.decode("ascii")
        if len(signed_body.split(".")) != 3:
            raise ProtectionError("compact signature required")
        signature_header = _header(signed_body.split(".")[0], {"alg", "typ", "kid"})
        if (signature_header.get("alg"), signature_header.get("typ")) != ("ES256", PROFILE):
            raise ProtectionError("unsupported signature profile")
        signer = trusted_signers.get(str(signature_header.get("kid", "")))
        if signer is None or signer.producer != expected.producer:
            raise ProtectionError("untrusted signing identity")
        if _key(signer.key) != signature_header["kid"]:
            raise ProtectionError("signer key identity mismatch")
        signed = jws.JWS()
        signed.allowed_algs = ["ES256"]
        signed.deserialize(signed_body, key=signer.key)
        document = json.loads(signed.payload)
        if document.get("context") != expected.document():
            raise ProtectionError("protected routing context mismatch")
        payload = base64.b64decode(document["payload"], validate=True)
        if len(payload) > max_payload_bytes:
            raise ProtectionError("plaintext exceeds decoding limit")
        if not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), document["payload_sha256"]):
            raise ProtectionError("protected payload hash mismatch")
        return payload
    except ProtectionError:
        raise
    except Exception:
        raise ProtectionError("message authentication failed") from None
