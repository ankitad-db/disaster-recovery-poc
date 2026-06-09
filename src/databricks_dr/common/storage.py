"""Storage paths and the cross-region bridge.

Two responsibilities:
  1. Build *dynamic* (never hardcoded) export directory paths with a run-time
     timestamp, under ``dr/<folder>/exports/<ts>``.
  2. Bridge an export directory from the source region's DBFS root bucket to the
     destination region's bucket, either via ``aws s3 sync`` or by relying on
     S3 cross-region replication (CRR).
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timezone

from .config import Config, Direction
from .logging import get_logger

_logger = get_logger(__name__)


def new_timestamp() -> str:
    """UTC, filesystem-safe, lexically sortable run timestamp."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")


def rel_export_dir(cfg: Config, direction: Direction, ts: str, model_name: str | None = None) -> str:
    """Relative path under the bucket root, e.g. ``dr/primary/exports/<ts>``.

    For per-model CDC, pass ``model_name`` to scope under ``cdc/<model>/<ts>``.
    """
    base = cfg.storage["base_path"]
    if model_name:
        safe = model_name.replace(".", "_")
        return f"{base}/{direction.folder}/cdc/{safe}/{ts}"
    return f"{base}/{direction.folder}/exports/{ts}"


def dbfs_path(rel: str) -> str:
    """FUSE path usable from a Databricks notebook/job (``/dbfs/...``)."""
    return f"/dbfs/{rel}"


def s3_uri(bucket: str, rel: str) -> str:
    return f"s3://{bucket}/{rel}"


def write_latest_pointer(cfg: Config, direction: Direction, rel: str) -> str:
    """Record the newest export path so the importer resolves it dynamically."""
    base = cfg.storage["base_path"]
    pointer_rel = f"{base}/{direction.folder}/{cfg.storage['latest_pointer']}"
    pointer_path = dbfs_path(pointer_rel)
    try:
        with open(pointer_path, "w") as f:
            f.write(rel)
    except OSError as e:
        # Outside Databricks (no /dbfs); caller may set pointer another way.
        _logger.warning("Could not write latest pointer %s: %s", pointer_path, e)
    return pointer_rel


def read_latest_pointer(cfg: Config, direction: Direction) -> str:
    base = cfg.storage["base_path"]
    pointer_path = dbfs_path(f"{base}/{direction.folder}/{cfg.storage['latest_pointer']}")
    with open(pointer_path) as f:
        return f.read().strip()


def bridge(cfg: Config, direction: Direction, rel: str, dry_run: bool = False) -> None:
    """Move an export dir from source bucket -> dest bucket.

    ``sync`` mode runs ``aws s3 sync``; ``crr`` mode is a no-op (S3 replication
    handles it asynchronously) but logs a reminder to verify the manifest landed.
    """
    mode = cfg.storage.get("bridge", "sync")
    src = s3_uri(direction.source.dbfs_bucket, rel)
    dst = s3_uri(direction.dest.dbfs_bucket, rel)

    if mode == "crr":
        _logger.info("Bridge=crr: relying on S3 CRR for %s -> %s. Verify manifest before import.", src, dst)
        return

    cmd = ["aws", "s3", "sync", src, dst]
    _logger.info("Bridge=sync: %s", " ".join(cmd))
    if dry_run:
        return
    subprocess.run(cmd, check=True)
