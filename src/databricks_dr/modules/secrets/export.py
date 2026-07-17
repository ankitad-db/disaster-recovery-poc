"""Secrets EXPORT — runs in the PRIMARY workspace.

Detects changed secrets (system-tables-first), reads their values with the
``get-secret`` API, envelope-encrypts them, captures the scope ACL shape, and
writes a bundle to the primary-region S3 bucket. S3 CRR then mirrors the bundle to
the secondary region. Bookkeeping goes to the Delta control tables.

SDK-only: no ``dbutils``. Runs in a notebook, a job, or locally with a profile.
"""

from __future__ import annotations

import base64
import hashlib
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ...common.logging import get_logger
from ...common.sql import SqlExecutor
from . import changefeed, control, crypto, store
from .config import SecretsConfig

_logger = get_logger(__name__)


def _bundle_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")


def _sha256(b: bytes) -> str:
    return "sha256:" + hashlib.sha256(b).hexdigest()


def _acl_signature(acls: List) -> str:
    joined = ";".join(f"{p}={perm}" for p, perm in sorted(acls))
    return _sha256(joined.encode())


def run_export(
    cfg: SecretsConfig, *, wc=None, ex: Optional[SqlExecutor] = None, spark=None,
    force_full: bool = False, actor: str = "",
) -> Dict[str, Any]:
    """Export changed (or all, if ``force_full``) secrets to the primary bucket."""
    t0 = time.time()
    primary = cfg.workspaces["primary"]
    secondary = cfg.workspaces["secondary"]
    direction = f"{primary.region}->{secondary.region}"

    if wc is None:
        from databricks.sdk import WorkspaceClient
        wc = WorkspaceClient(profile=primary.profile) if primary.profile else WorkspaceClient()
    if ex is None:
        ex = SqlExecutor(spark=spark, workspace_client=wc, warehouse_id=cfg.warehouse_id or None)

    inv = control.load_inventory(ex, cfg.inventory_table)
    live = changefeed.live_state(wc, cfg.include, cfg.exclude)

    # ---- detect changes (system-tables-first, recon safety net) --------------
    if force_full or not cfg.use_system_tables:
        cs = changefeed.detect_via_state_diff(live, inv)
    else:
        since = control.last_export_watermark(ex, cfg.audit_table)
        cs = changefeed.detect_via_system_tables(
            ex, audit_table=cfg.audit_system_table,
            service_name=cfg.detection.get("service_name", "secrets"),
            workspace_id=primary.workspace_id, since_iso=since,
            lookback_hours=int(cfg.detection.get("lookback_hours", 48)),
        )
        if cfg.detection.get("full_recon", True):
            recon = changefeed.detect_via_state_diff(live, inv)
            cs.changed |= recon.changed
            cs.deleted |= recon.deleted
            cs.scopes_touched |= recon.scopes_touched

    # ---- read values for changed keys, capture ACLs --------------------------
    use_cse = cfg.storage.get("client_side_encryption", True)
    kms = crypto.kms_client(primary.region) if use_cse else None
    kms_key = cfg.kms_key_for(primary.region)

    items: List[Dict[str, Any]] = []
    inv_updates: List[Dict[str, Any]] = []
    scopes_in_play = {s for s, _ in cs.changed} | cs.scopes_touched
    acls_out: Dict[str, List] = {
        s: live.get(s, {}).get("acls", []) for s in scopes_in_play if s in live
    }

    for scope, key in sorted(cs.changed):
        try:
            resp = wc.secrets.get_secret(scope=scope, key=key)
            plaintext = base64.b64decode(resp.value)
        except Exception as e:  # noqa: BLE001
            _logger.error("get-secret failed for %s/%s: %s", scope, key, str(e)[:200])
            raise
        blob = (crypto.encrypt_value(kms, kms_key, plaintext, {"scope": scope, "key": key})
                if use_cse else crypto.wrap_plain(plaintext))
        items.append({"scope": scope, "key": key, "value": blob})
        inv_updates.append({
            "scope": scope, "secret_key": key,
            "value_hash": _sha256(plaintext),
            "acl_signature": _acl_signature(acls_out.get(scope, [])),
            "source_last_updated": live.get(scope, {}).get("keys", {}).get(key),
            "bundle_id": None, "status": "IN_SYNC",
        })

    deletes = [{"scope": s, "key": k} for s, k in sorted(cs.deleted)]

    bundle_id = _bundle_id()
    for u in inv_updates:
        u["bundle_id"] = bundle_id

    bundle = {
        "bundle_id": bundle_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "direction": direction,
        "source_region": primary.region,
        "encryption": "AES256-GCM" if use_cse else "PLAIN",
        "items": items,
        "acls": {s: [{"principal": p, "permission": perm} for p, perm in a] for s, a in acls_out.items()},
        "deletes": deletes,
    }

    # ---- write to the primary-region bucket (CRR mirrors it to secondary) ----
    if items or deletes:
        s3 = store.s3_client(primary.region)
        store.put_bundle(
            s3, cfg.bucket_for(primary.region), cfg.storage["prefix"], bundle_id, bundle,
            kms_key_id=kms_key or None,
            latest_pointer=cfg.storage.get("latest_pointer", "_latest.txt"),
        )
    else:
        _logger.info("No secret changes detected; nothing to export.")

    # ---- persist inventory + audit ------------------------------------------
    control.upsert_inventory(ex, cfg.inventory_table, inv_updates)
    for d in deletes:
        control.upsert_inventory(ex, cfg.inventory_table, [{
            "scope": d["scope"], "secret_key": d["key"], "status": "DELETED", "bundle_id": bundle_id,
        }])
    dur = time.time() - t0
    control.record_audit(
        ex, cfg.audit_table, operation="EXPORT", status="SUCCESS", direction=direction,
        item_count=len(items), bundle_id=bundle_id, duration_sec=dur,
        detail=f"changed={len(items)} deleted={len(deletes)} scopes={len(scopes_in_play)}",
        actor=actor or cfg.service_principal,
    )
    summary = {
        "bundle_id": bundle_id, "exported": len(items), "deleted": len(deletes),
        "scopes": sorted(scopes_in_play), "duration_sec": round(dur, 2),
    }
    _logger.info("Secrets export complete: %s", summary)
    return summary
