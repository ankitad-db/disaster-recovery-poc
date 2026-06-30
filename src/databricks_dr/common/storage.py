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


def staging_root(cfg: Config | None = None) -> str:
    """FUSE prefix bundles are staged under.

    A configured ``storage.staging_volume`` (3-level UC volume) resolves to its
    ``/Volumes/<cat>/<schema>/<vol>`` FUSE path (serverless/shared-safe, S3-backed,
    governed). Otherwise we fall back to the DBFS root ``/dbfs`` for back-compat.
    """
    vol = cfg.staging_volume if cfg is not None else None
    if vol:
        return "/Volumes/" + vol.replace(".", "/")
    return "/dbfs"


def dbfs_path(rel: str, cfg: Config | None = None) -> str:
    """Absolute FUSE path for a bundle-relative path, under the staging root.

    Name kept for call-site compatibility; honours the configured staging volume
    when ``cfg`` is provided, else the DBFS root.
    """
    return f"{staging_root(cfg)}/{rel}"


def s3_uri(bucket: str, rel: str) -> str:
    return f"s3://{bucket}/{rel}"


def write_latest_pointer(cfg: Config, direction: Direction, rel: str) -> str:
    """Record the newest export path so the importer resolves it dynamically."""
    base = cfg.storage["base_path"]
    pointer_rel = f"{base}/{direction.folder}/{cfg.storage['latest_pointer']}"
    pointer_path = dbfs_path(pointer_rel, cfg)
    try:
        with open(pointer_path, "w") as f:
            f.write(rel)
    except OSError as e:
        # Outside Databricks (no /dbfs); caller may set pointer another way.
        _logger.warning("Could not write latest pointer %s: %s", pointer_path, e)
    return pointer_rel


def read_latest_pointer(cfg: Config, direction: Direction) -> str:
    base = cfg.storage["base_path"]
    pointer_path = dbfs_path(f"{base}/{direction.folder}/{cfg.storage['latest_pointer']}", cfg)
    with open(pointer_path) as f:
        return f.read().strip()


def bridge_prefix(cfg: Config, direction: Direction, dry_run: bool = False) -> None:
    """Sync the whole authored folder (exports + ``_latest.txt``) across buckets.

    Robust bridge for the split workflow: copies ``s3://<src>/dr/<folder>/`` to
    ``s3://<dst>/dr/<folder>/``. Run from a host with read on the source bucket and
    write on the dest bucket (e.g. your laptop). In production prefer S3 CRR.
    """
    base = cfg.storage["base_path"]
    src = s3_uri(direction.source.dbfs_bucket, f"{base}/{direction.folder}/")
    dst = s3_uri(direction.dest.dbfs_bucket, f"{base}/{direction.folder}/")
    cmd = ["aws", "s3", "sync", src, dst]
    _logger.info("bridge_prefix: %s", " ".join(cmd))
    if dry_run:
        return
    subprocess.run(cmd, check=True)


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
