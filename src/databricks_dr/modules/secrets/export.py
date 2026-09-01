"""Secrets EXPORT — runs in the PRIMARY workspace.

Detects change (system-tables-first, with a full state-diff recon safety net). When
anything changed, it re-reads the *full* in-scope secret state with the ``get-secret``
API, envelope-encrypts each value, captures the scope ACLs, and writes a **complete
desired-state snapshot** bundle to the primary-region S3 bucket. S3 CRR then mirrors
the bundle to the secondary region. Bookkeeping goes to the Delta control tables.

The bundle is a full snapshot (not a delta) on purpose: ``_latest`` is therefore always
a self-contained recovery point, so a cold secondary can be rebuilt from a single bundle
on the first failover, and the destination-aware import (see ``import_.py``) can compute
a complete diff — including pruning secrets that no longer exist in the primary.

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

    # Scope the audit scan to this workspace; auto-derive the id if the config left it blank.
    workspace_id = primary.workspace_id
    if not workspace_id:
        try:
            workspace_id = str(wc.get_workspace_id())
        except Exception as e:  # noqa: BLE001
            _logger.warning("could not auto-resolve primary workspace_id: %s", str(e)[:120])

    inv = control.load_inventory(ex, cfg.inventory_table)
    live = changefeed.live_state(wc, cfg.include, cfg.exclude)

    # ---- detect change (system-tables-first, recon safety net) ---------------
    # Detection decides *whether* to cut a new snapshot and records what moved (for
    # RPO/audit). The snapshot itself is always the full in-scope state.
    if force_full or not cfg.use_system_tables:
        cs = changefeed.detect_via_state_diff(live, inv)
    else:
        since = control.last_export_watermark(ex, cfg.audit_table)
        cs = changefeed.detect_via_system_tables(
            ex, audit_table=cfg.audit_system_table,
            service_name=cfg.detection.get("service_name", "secrets"),
            workspace_id=workspace_id, since_iso=since,
            lookback_hours=int(cfg.detection.get("lookback_hours", 48)),
        )
        if cfg.detection.get("full_recon", True):
            recon = changefeed.detect_via_state_diff(live, inv)
            cs.changed |= recon.changed
            cs.deleted |= recon.deleted
            cs.scopes_touched |= recon.scopes_touched

    live_pairs = {(s, k) for s, info in live.items() for k in info["keys"]}
    inv_deleted = [
        (s, k) for (s, k), r in inv.items()
        if (s, k) not in live_pairs and r.get("status") != "DELETED"
    ]

    # Idempotent idle-skip: nothing changed and nothing dropped -> no new snapshot.
    if not force_full and not cs.changed and not cs.deleted and not inv_deleted:
        dur = time.time() - t0
        control.record_audit(
            ex, cfg.audit_table, operation="EXPORT", status="SKIPPED", direction=direction,
            item_count=0, duration_sec=dur, detail="no changes", actor=actor or cfg.service_principal,
        )
        _logger.info("No secret changes detected; nothing to export.")
        return {"bundle_id": None, "exported": 0, "deleted": 0, "scopes": [],
                "skipped": True, "duration_sec": round(dur, 2)}

    # ---- read the FULL in-scope state, envelope-encrypt each value -----------
    use_cse = cfg.storage.get("client_side_encryption", True)
    kms = crypto.kms_client(primary.region) if use_cse else None
    kms_key = cfg.kms_key_for(primary.region)

    bundle_id = _bundle_id()
    items: List[Dict[str, Any]] = []
    inv_updates: List[Dict[str, Any]] = []
    acls_out: Dict[str, List] = {s: info.get("acls", []) for s, info in live.items()}

    for scope in sorted(live):
        for key in sorted(live[scope]["keys"]):
            try:
                resp = wc.secrets.get_secret(scope=scope, key=key)
                plaintext = base64.b64decode(resp.value)
            except Exception as e:  # noqa: BLE001
                _logger.error("get-secret failed for %s/%s: %s", scope, key, str(e)[:200])
                raise
            value_hash = _sha256(plaintext)
            blob = (crypto.encrypt_value(kms, kms_key, plaintext, {"scope": scope, "key": key})
                    if use_cse else crypto.wrap_plain(plaintext))
            items.append({"scope": scope, "key": key, "value": blob, "value_hash": value_hash})
            inv_updates.append({
                "scope": scope, "secret_key": key,
                "value_hash": value_hash,
                "acl_signature": _acl_signature(acls_out.get(scope, [])),
                "source_last_updated": live[scope]["keys"].get(key),
                "bundle_id": bundle_id, "status": "IN_SYNC",
            })

    deletes = [{"scope": s, "key": k} for s, k in sorted(set(cs.deleted) | set(inv_deleted))]

    bundle = {
        "bundle_id": bundle_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "direction": direction,
        "source_region": primary.region,
        "snapshot": "full",
        "encryption": "AES256-GCM" if use_cse else "PLAIN",
        "items": items,
        "acls": {s: [{"principal": p, "permission": perm} for p, perm in a] for s, a in acls_out.items()},
        "deletes": deletes,
    }

    # ---- write the snapshot to the primary bucket (CRR mirrors to secondary) -
    s3 = store.s3_client(primary.region)
    store.put_bundle(
        s3, cfg.bucket_for(primary.region), cfg.storage["prefix"], bundle_id, bundle,
        kms_key_id=kms_key or None,
        latest_pointer=cfg.storage.get("latest_pointer", "_latest.txt"),
    )

    # ---- persist inventory + audit ------------------------------------------
    control.upsert_inventory(ex, cfg.inventory_table, inv_updates)
    for d in deletes:
        control.upsert_inventory(ex, cfg.inventory_table, [{
            "scope": d["scope"], "secret_key": d["key"], "status": "DELETED", "bundle_id": bundle_id,
        }])
    dur = time.time() - t0
    scopes_in_play = sorted(live)
    control.record_audit(
        ex, cfg.audit_table, operation="EXPORT", status="SUCCESS", direction=direction,
        item_count=len(items), bundle_id=bundle_id, duration_sec=dur,
        detail=f"snapshot items={len(items)} deleted={len(deletes)} scopes={len(scopes_in_play)}",
        actor=actor or cfg.service_principal,
    )
    summary = {
        "bundle_id": bundle_id, "exported": len(items), "deleted": len(deletes),
        "scopes": scopes_in_play, "duration_sec": round(dur, 2),
    }
    _logger.info("Secrets export complete: %s", summary)
    return summary
