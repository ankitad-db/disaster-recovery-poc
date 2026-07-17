"""S3 bundle store for secret DR bundles.

Writes/reads a single JSON bundle per export under
``<bucket>/<prefix>/<bundle_id>/bundle.json`` and maintains a ``_latest.txt``
pointer. Objects are written with SSE-KMS (belt-and-suspenders on top of the
client-side envelope encryption of the values inside the bundle).

The export writes to the PRIMARY-region bucket; AWS S3 CRR replicates it to the
secondary-region bucket. The import always reads from its own LOCAL-region bucket,
so failover has no cross-region dependency at read time.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Tuple
from urllib.parse import urlparse

from ...common.logging import get_logger

_logger = get_logger(__name__)


def s3_client(region: str):
    import boto3  # lazy

    return boto3.client("s3", region_name=region)


def _split(bucket_uri: str) -> Tuple[str, str]:
    p = urlparse(bucket_uri)
    return p.netloc, p.path.strip("/")


def _join(*parts: str) -> str:
    return "/".join(p.strip("/") for p in parts if p)


def put_bundle(
    s3, bucket_uri: str, prefix: str, bundle_id: str, bundle: Dict[str, Any],
    kms_key_id: str | None = None, latest_pointer: str = "_latest.txt",
) -> str:
    bkt, base = _split(bucket_uri)
    key = _join(base, prefix, bundle_id, "bundle.json")
    body = json.dumps(bundle, indent=2, sort_keys=True).encode()
    extra: Dict[str, Any] = {}
    if kms_key_id:
        extra = {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": kms_key_id}
    s3.put_object(Bucket=bkt, Key=key, Body=body, **extra)
    # Advance the latest pointer (relative path from the prefix root).
    ptr_key = _join(base, prefix, latest_pointer)
    s3.put_object(Bucket=bkt, Key=ptr_key, Body=f"{bundle_id}".encode(), **extra)
    uri = f"s3://{bkt}/{key}"
    _logger.info("Wrote secrets bundle %s (%d items)", uri, len(bundle.get("items", [])))
    return uri


def read_latest(s3, bucket_uri: str, prefix: str, latest_pointer: str = "_latest.txt") -> Dict[str, Any]:
    bkt, base = _split(bucket_uri)
    ptr_key = _join(base, prefix, latest_pointer)
    bundle_id = s3.get_object(Bucket=bkt, Key=ptr_key)["Body"].read().decode().strip()
    key = _join(base, prefix, bundle_id, "bundle.json")
    body = s3.get_object(Bucket=bkt, Key=key)["Body"].read()
    _logger.info("Read secrets bundle s3://%s/%s", bkt, key)
    return json.loads(body)
