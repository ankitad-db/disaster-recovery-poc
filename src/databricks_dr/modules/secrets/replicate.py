"""Direct cross-workspace secret replication (no S3, no CRR, no KMS envelope).

A single job runs in the ACTIVE workspace's compute and reconciles the DESTINATION
workspace to the SOURCE:

1. read the SOURCE secrets   (values via ``get_secret`` -> sha256, plus per-scope ACLs)
2. read the DESTINATION secrets **cross-workspace** via the Secrets API — reads/writes
   over the control plane spin up **no compute** on the destination
3. **diff** source vs destination by value hash + ACL signature
4. apply **only the delta** straight into the destination:
   ``create_scope`` / ``put_secret`` / ``put_acl`` / ``delete_secret`` / ``delete_acl``

Secret values travel source->destination over TLS (the Secrets API); both workspaces'
secret stores are encrypted at rest by the platform. Nothing is written to object storage.

Direction is parameterised (``source_key`` / ``dest_key``), so failover is just
``replicate`` with the roles swapped, and the secondary is a **warm mirror** — on a real
outage you promote it, no import step. ``mirror`` mode makes the destination an exact
replica (propagates deletes); ``additive`` never deletes on the destination.

SDK-only: no ``dbutils``. Runs in a notebook, a job, or locally with profiles.
"""

from __future__ import annotations

import base64
import hashlib
import time
from typing import Any, Dict, List, Optional, Tuple

from ...common.logging import get_logger
from ...common.sql import SqlExecutor
from . import changefeed, control
from .config import SecretsConfig

_logger = get_logger(__name__)

_PROBE_SCOPE = "__dr_writable_probe__"


def _sha256(b: bytes) -> str:
    return "sha256:" + hashlib.sha256(b).hexdigest()


def _acl_signature(acls: List[Tuple[str, str]]) -> str:
    return _sha256(";".join(f"{p}={perm}" for p, perm in sorted(acls)).encode())


def _acl_permission(value: str):
    """Map a stored/enum permission string back to the SDK AclPermission enum."""
    from databricks.sdk.service.workspace import AclPermission

    v = (value or "").split(".")[-1].upper()  # 'AclPermission.MANAGE' -> 'MANAGE'
    return getattr(AclPermission, v, AclPermission.READ)


def _client(cfg: SecretsConfig, region_key: str):
    from databricks.sdk import WorkspaceClient

    ws = cfg.workspaces[region_key]
    return WorkspaceClient(profile=ws.profile) if ws.profile else WorkspaceClient()


def _assert_writable(wc, label: str) -> None:
    """Prove the destination accepts secret writes (i.e. it's promoted / active)."""
    try:
        wc.secrets.create_scope(scope=_PROBE_SCOPE)
    except Exception as e:  # noqa: BLE001
        if "already exists" not in str(e).lower():
            raise RuntimeError(
                f"destination ({label}) is not writable — promote it before replicating "
                f"(write preflight failed: {str(e)[:200]})"
            ) from e
    finally:
        try:
            wc.secrets.delete_scope(scope=_PROBE_SCOPE)
        except Exception:  # noqa: BLE001
            pass


def _value_hash(wc, scope: str, key: str) -> Optional[Tuple[str, bytes]]:
    """(sha256, plaintext) of a secret value via get_secret, or None if unreadable."""
    try:
        raw = base64.b64decode(wc.secrets.get_secret(scope=scope, key=key).value)
        return _sha256(raw), raw
    except Exception:  # noqa: BLE001
        return None


def run_replicate(
    cfg: SecretsConfig, *, source_key: str = "primary", dest_key: str = "secondary",
    src_wc=None, dst_wc=None, ex: Optional[SqlExecutor] = None, spark=None, actor: str = "",
) -> Dict[str, Any]:
    """Reconcile the destination workspace to the source. Returns a summary dict."""
    t0 = time.time()
    source = cfg.workspaces[source_key]
    dest = cfg.workspaces[dest_key]
    direction = f"{source.region}->{dest.region}"
    mode = cfg.reconcile.get("mode", "mirror")
    prune_scopes = bool(cfg.reconcile.get("prune_extra_scopes", False))

    if src_wc is None:
        src_wc = _client(cfg, source_key)
    if dst_wc is None:
        dst_wc = _client(cfg, dest_key)
    if ex is None:
        ex = SqlExecutor(spark=spark, workspace_client=src_wc, warehouse_id=cfg.warehouse_id or None)
    audit_table = cfg.audit_table_for(source_key)
    inventory_table = cfg.inventory_table_for(source_key)

    # ---- read both sides -----------------------------------------------------
    src = changefeed.live_state(src_wc, cfg.include, cfg.exclude)
    dst = changefeed.live_state(dst_wc, cfg.include, cfg.exclude)
    src_pairs = {(s, k) for s, info in src.items() for k in info["keys"]}
    dst_pairs = {(s, k) for s, info in dst.items() for k in info["keys"]}

    # ---- diff (plan; no writes yet) -----------------------------------------
    to_put: List[Dict[str, Any]] = []   # {scope,key,plaintext,value_hash,is_add}
    skipped = 0
    for scope, key in sorted(src_pairs):
        sh = _value_hash(src_wc, scope, key)
        if sh is None:
            _logger.error("cannot read source secret %s/%s; skipping", scope, key)
            continue
        src_hash, plaintext = sh
        present = (scope, key) in dst_pairs
        if present:
            dh = _value_hash(dst_wc, scope, key)
            if dh is not None and dh[0] == src_hash:
                skipped += 1
                continue
        to_put.append({"scope": scope, "key": key, "plaintext": plaintext,
                       "value_hash": src_hash, "is_add": not present})

    delete_pairs = (dst_pairs - src_pairs) if mode == "mirror" else set()
    to_delete = [{"scope": s, "key": k} for s, k in sorted(delete_pairs)]

    src_acls = {s: info.get("acls", []) for s, info in src.items()}
    acl_plan = [
        s for s, acls in src_acls.items()
        if _acl_signature(acls) != _acl_signature(dst.get(s, {}).get("acls", []))
    ]

    if not (to_put or to_delete or acl_plan):
        dur = time.time() - t0
        control.record_audit(
            ex, audit_table, operation="REPLICATE", status="SKIPPED", direction=direction,
            item_count=0, duration_sec=dur, detail=f"in sync (skipped={skipped})",
            actor=actor or cfg.service_principal,
        )
        _logger.info("Secrets replicate: %s already in sync (skipped=%d).", dest_key, skipped)
        return {"added": 0, "updated": 0, "deleted": 0, "skipped": skipped,
                "in_sync": True, "direction": direction, "duration_sec": round(dur, 2)}

    # ---- apply (destination must be writable) -------------------------------
    _assert_writable(dst_wc, dest_key)
    existing = {s.name for s in dst_wc.secrets.list_scopes()}
    added = updated = deleted = 0
    inv_updates: List[Dict[str, Any]] = []

    for it in to_put:
        scope, key = it["scope"], it["key"]
        if scope not in existing:
            try:
                dst_wc.secrets.create_scope(scope=scope)
                existing.add(scope)
            except Exception as e:  # noqa: BLE001
                if "already exists" not in str(e).lower():
                    raise
        dst_wc.secrets.put_secret(scope=scope, key=key, string_value=it["plaintext"].decode())
        added += it["is_add"]
        updated += (not it["is_add"])
        inv_updates.append({"scope": scope, "secret_key": key, "status": "IN_SYNC",
                            "value_hash": it["value_hash"]})

    for d in to_delete:
        try:
            dst_wc.secrets.delete_secret(scope=d["scope"], key=d["key"])
            deleted += 1
        except Exception as e:  # noqa: BLE001
            _logger.debug("delete_secret %s/%s: %s", d["scope"], d["key"], str(e)[:120])
        inv_updates.append({"scope": d["scope"], "secret_key": d["key"], "status": "DELETED"})

    for scope in acl_plan:
        desired = src_acls[scope]
        desired_principals = {p for p, _ in desired}
        for principal, perm in desired:
            try:
                dst_wc.secrets.put_acl(scope=scope, principal=principal,
                                       permission=_acl_permission(perm))
            except Exception as e:  # noqa: BLE001
                _logger.debug("put_acl %s/%s: %s", scope, principal, str(e)[:120])
        if mode == "mirror":
            for principal, _perm in dst.get(scope, {}).get("acls", []):
                if principal not in desired_principals:
                    try:
                        dst_wc.secrets.delete_acl(scope=scope, principal=principal)
                    except Exception as e:  # noqa: BLE001
                        _logger.debug("delete_acl %s/%s: %s", scope, principal, str(e)[:120])

    if mode == "mirror" and prune_scopes:
        keep = {s for s, _ in src_pairs} | set(src_acls)
        for scope in sorted(set(dst) - keep):
            try:
                dst_wc.secrets.delete_scope(scope=scope)
            except Exception as e:  # noqa: BLE001
                _logger.debug("delete_scope %s: %s", scope, str(e)[:120])

    control.upsert_inventory(ex, inventory_table, inv_updates)
    dur = time.time() - t0
    control.record_audit(
        ex, audit_table, operation="REPLICATE", status="SUCCESS", direction=direction,
        item_count=added + updated, duration_sec=dur,
        detail=f"added={added} updated={updated} deleted={deleted} skipped={skipped} "
               f"acls={len(acl_plan)} mode={mode}",
        actor=actor or cfg.service_principal,
    )
    summary = {"added": added, "updated": updated, "deleted": deleted, "skipped": skipped,
               "acls": len(acl_plan), "direction": direction, "duration_sec": round(dur, 2)}
    _logger.info("Secrets replicate complete: %s", summary)
    return summary
