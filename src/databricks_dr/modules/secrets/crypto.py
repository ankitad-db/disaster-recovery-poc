"""Envelope encryption for secret values (AWS KMS + AES-256-GCM).

Secret values can be up to 128 KB -- larger than the 4 KB limit of a direct
``kms:Encrypt`` call -- so we use envelope encryption: ask KMS for a one-time data
key, encrypt the value locally with AES-GCM, and store the KMS-wrapped data key
alongside the ciphertext. The plaintext value therefore never leaves the process
in the clear and never lands in S3.

The (scope, key) pair is bound into both the KMS EncryptionContext and the AES-GCM
associated data, so a ciphertext cannot be silently relocated to a different key.
"""

from __future__ import annotations

import base64
import json
import os
from typing import Dict

from ...common.logging import get_logger

_logger = get_logger(__name__)


def kms_client(region: str):
    import boto3  # lazy: only needed when encryption actually runs

    return boto3.client("kms", region_name=region)


def _aad(context: Dict[str, str]) -> bytes:
    return json.dumps(context, sort_keys=True).encode()


def encrypt_value(kms, key_id: str, plaintext: bytes, context: Dict[str, str]) -> Dict[str, str]:
    """Envelope-encrypt ``plaintext``. Returns a JSON-serialisable blob."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    dk = kms.generate_data_key(KeyId=key_id, KeySpec="AES_256", EncryptionContext=context)
    try:
        nonce = os.urandom(12)
        ct = AESGCM(dk["Plaintext"]).encrypt(nonce, plaintext, _aad(context))
    finally:
        # Best-effort scrub of the plaintext data key reference.
        del dk["Plaintext"]
    return {
        "alg": "AES256-GCM",
        "edk": base64.b64encode(dk["CiphertextBlob"]).decode(),
        "nonce": base64.b64encode(nonce).decode(),
        "ct": base64.b64encode(ct).decode(),
    }


def decrypt_value(kms, blob: Dict[str, str], context: Dict[str, str]) -> bytes:
    """Reverse :func:`encrypt_value`. ``context`` must match the encrypt call."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    edk = base64.b64decode(blob["edk"])
    dk = kms.decrypt(CiphertextBlob=edk, EncryptionContext=context)["Plaintext"]
    nonce = base64.b64decode(blob["nonce"])
    ct = base64.b64decode(blob["ct"])
    return AESGCM(dk).decrypt(nonce, ct, _aad(context))


# --- plaintext fallback (client_side_encryption: false) -----------------------
# Values still get SSE-KMS at rest in S3, but sit as base64 plaintext in the
# bundle. Only for envs where client-side crypto deps are unavailable.

def wrap_plain(plaintext: bytes) -> Dict[str, str]:
    return {"alg": "PLAIN", "ct": base64.b64encode(plaintext).decode()}


def unwrap_plain(blob: Dict[str, str]) -> bytes:
    return base64.b64decode(blob["ct"])
