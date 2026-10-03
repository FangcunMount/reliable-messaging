import base64
import json
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest
from jwcrypto import jwk

from reliable_messaging.protected import Context, ProtectionError, TrustedSigner, open_message, seal
from reliable_messaging.wire import Envelope, decode, encode, failed_topic, fits_nsq

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def go_probe(tmp_path_factory):
    binary = tmp_path_factory.mktemp("go-protected") / "probe"
    subprocess.run(
        ["go", "build", "-o", str(binary), "./tests/integration/protectedprobe"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )

    def probe(mode, context, payload, signing, encryption, wire=b""):
        request = {
            "mode": mode,
            "context": context.document(),
            "payload": base64.b64encode(payload).decode(),
            "signing": json.loads(signing.export(private_key=True)),
            "encryption": json.loads(encryption.export(private_key=True)),
            "wire": base64.b64encode(wire).decode(),
        }
        result = subprocess.run(
            [str(binary)], input=json.dumps(request).encode(), check=True, capture_output=True
        )
        return base64.b64decode(result.stdout)

    return probe


@pytest.fixture
def keys():
    signing = jwk.JWK.generate(kty="EC", crv="P-256", kid="qs-server.sign.1")
    encryption = jwk.JWK.generate(kty="EC", crv="P-256", kid="qs-ai.encrypt.1")
    return signing, encryption


@pytest.fixture
def context():
    return Context(
        "qs-server",
        "qs-ai",
        "qs.ai.commands.v1",
        "原身份-42",
        {"secure_profile": "rm-secure-v1", "source": "qs-server"},
    )


@pytest.mark.parametrize(
    "payload", [b"", b"\x00\xff", '{"id":18446744073709551615,"text":"中文<>&\u2028"}'.encode()]
)
def test_revision_two_exact_go_bytes(go_probe, keys, context, payload):
    context = replace(context, metadata={"<key>": "中文\u2028\u2029&", "z": "\\\n"})
    value = Envelope(context.message_id, payload, context.metadata)
    assert go_probe("legacy", context, payload, *keys) == encode(value)
    assert decode(encode(value)) == value


@pytest.mark.parametrize(
    "payload", [b"", '{"value":18446744073709551615,"text":"汉字🙂"}'.encode()]
)
def test_jose_both_languages(go_probe, keys, context, payload):
    signing, encryption = keys
    python_wire = seal(context, payload, signing, encryption)
    assert go_probe("open", context, b"", *keys, wire=python_wire) == payload
    go_wire = go_probe("seal", context, payload, *keys)
    assert (
        open_message(
            go_wire,
            context,
            decrypt_keys={encryption.get("kid"): encryption},
            trusted_signers={signing.get("kid"): TrustedSigner(context.producer, signing)},
        )
        == payload
    )


@pytest.mark.parametrize("field", ["producer", "destination", "topic", "message_id", "metadata"])
def test_routing_cannot_be_substituted(keys, context, field):
    signing, encryption = keys
    wire = seal(context, b"secret", *keys)
    changed = replace(context, **{field: {} if field == "metadata" else "wrong"})
    with pytest.raises(ProtectionError):
        open_message(
            wire,
            changed,
            decrypt_keys={encryption.get("kid"): encryption},
            trusted_signers={signing.get("kid"): TrustedSigner(context.producer, signing)},
        )


def test_key_rotation_and_sanitized_failure(keys, context):
    signing, encryption = keys
    wire = seal(context, b"PRIVATE PLAINTEXT", *keys)
    decryption = {
        encryption.get("kid"): encryption,
        "qs-ai.encrypt.2": jwk.JWK.generate(kty="EC", crv="P-256", kid="qs-ai.encrypt.2"),
    }
    signers = {signing.get("kid"): TrustedSigner(context.producer, signing)}
    assert (
        open_message(wire, context, decrypt_keys=decryption, trusted_signers=signers)
        == b"PRIVATE PLAINTEXT"
    )
    with pytest.raises(ProtectionError) as error:
        open_message(wire, context, decrypt_keys=decryption, trusted_signers={})
    assert "PRIVATE" not in str(error.value)
    with pytest.raises(ProtectionError):
        open_message(wire, context, decrypt_keys={}, trusted_signers=signers)
    with pytest.raises(ProtectionError):
        open_message(
            wire, context, decrypt_keys=decryption, trusted_signers=signers, max_payload_bytes=1
        )


def test_checksum_and_handoff_budget(context):
    value = Envelope(context.message_id, b"small", context.metadata)
    assert fits_nsq(value, context.topic, "qs-ai.commands.v1")
    assert not fits_nsq(replace(value, payload=b"x" * 180_000), context.topic, "qs-ai.commands.v1")
    changed = json.loads(encode(value))
    changed["uuid"] = "spoofed"
    with pytest.raises(ValueError):
        decode(json.dumps(changed).encode())
    assert failed_topic("qs.ai.commands.v1", "qs-ai.commands.v1").startswith("cb.failed.")
