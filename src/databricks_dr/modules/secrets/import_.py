"""Secrets IMPORT — runs in the PROMOTED workspace on failover (destination-aware).

Reads the latest full-state bundle from the LOCAL-region bucket (populated by S3
CRR), then **diffs it against the promoted workspace's own live secret state** and
applies **only the difference**:

* scope/key missing in the destination            -> create_scope / put_secret  (ADD)
* value hash differs from the bundle               -> put_secret                 (UPDATE)
* value hash matches                               -> skip (no decrypt, no write)
* explicit tombstone in the bundle                 -> delete_secret              (DELETE)
* mirror mode: key present locally but not desired -> delete_secret              (PRUNE)
* scope ACLs differ from the bundle                -> put_acl / delete_acl

This is the "diff across both workspaces, then apply" reconcile: the bundle is the
primary's exported desired state, the live read is the secondary's actual state.
The first failover into a cold secondary is a full rebuild; later failovers are
incremental (only drift is re-applied), which is faster and idempotent.

Writes are only possible once the workspace is promoted (a read-only DR standby
rejects them), so a write preflight fails fast with an actionable message. By
default the local region is the configured secondary; pass ``region_key`` to run
the symmetric failback import on the primary.
"""

from __future__ import annotations

import base64
import hashlib
import time
from typing import Any, Dict, List, Optional, Tuple

from ...common.logging import get_logger
from ...common.sql import SqlExecutor
from . import changefeed, control, crypto, store
from .config import SecretsConfig

_logger = get_logger(__name__)

_PROBE_SCOPE = "__dr_writable_probe__"


def _sha256(b: bytes) -> str:
    return "sha256:" + hashlib.sha256(b).hexdigest()


def _acl_signature(acls: List[Tuple[str, str]]) -> str:
    joined = ";".join(f"{p}={perm}" for p, perm in sorted(acls))
    return _sha256(joined.encode())


def _acl_permission(value: str):
    """Map a stored permission string back to the SDK AclPermission enum."""
    from databricks.sdk.service.workspace import AclPermission

    v = (value or "").split(".")[-1].upper()  # 'AclPermission.MANAGE' -> 'MANAGE'
    return getattr(AclPermission, v, AclPermission.READ)


def _assert_writable(wc) -> None:
    """Prove the workspace accepts secret writes (i.e. it has been promoted).

    Creates and immediately deletes a throwaway scope. A passive/read-only DR
    standby rejects this, so we surface an actionable error instead of failing
    part-way through the real apply.
    """
    try:
        wc.secrets.create_scope(scope=_PROBE_SCOPE)
    except Exception as e:  # noqa: BLE001
        if "already exists" not in str(e).lower():
            raise RuntimeError(
                "secrets import target is not writable — promote the workspace before "
                f"importing (write preflight failed: {str(e)[:200]})"
            ) from e
    finally:
        try:
            wc.secrets.delete_scope(scope=_PROBE_SCOPE)
        except Exception:  # noqa: BLE001
            pass


def _live_value_hash(wc, scope: str, key: str) -> Optional[str]:
    """sha256 of the destination's current value for (scope, key), or None."""
    try:
        resp = wc.secrets.get_secret(scope=scope, key=key)
        return _sha256(base64.b64decode(resp.value))
    except Exception:  # noqa: BLE001
        return None


def run_import(
    cfg: SecretsConfig, *, region_key: str = "secondary", wc=None,
    ex: Optional[SqlExecutor] = None, spark=None, actor: str = "",
) -> Dict[str, Any]:
    """Reconcile the local (promoted) workspace to the latest secrets bundle."""
    t0 = time.time()
    local = cfg.workspaces[region_key]
    mode = cfg.reconcile.get("mode", "mirror")
    prune_scopes = bool(cfg.reconcile.get("prune_extra_scopes", False))

    if wc is None:
        from databricks.sdk import WorkspaceClient
        wc = WorkspaceClient(profile=local.profile) if local.profile else WorkspaceClient()
    if ex is None:
        ex = SqlExecutor(spark=spark, workspace_client=wc, warehouse_id=cfg.warehouse_id or None)

    s3 = store.s3_client(local.region)
    bundle = store.read_latest(
        s3, cfg.bucket_for(local.region), cfg.storage["prefix"],
        latest_pointer=cfg.storage.get("latest_pointer", "_latest.txt"),
    )
    direction = f"{bundle.get('source_region', '?')}->{local.region}"
    use_cse = bundle.get("encryption") == "AES256-GCM"
    kms = crypto.kms_client(local.region) if use_cse else None

    # Destination-aware: read the promoted workspace's own live state to diff against.
    live = changefeed.live_state(wc, cfg.include, cfg.exclude)
    live_pairs = {(s, k) for s, info in live.items() for k in info["keys"]}
    desired_items = bundle.get("items", [])
    desired_pairs = {(it["scope"], it["key"]) for it in desired_items}

    # ---- plan (no writes yet) -----------------------------------------------
    to_put: List[Dict[str, Any]] = []      # ADD / UPDATE
    skipped = 0
    for it in desired_items:
        scope, key = it["scope"], it["key"]
        want_hash = it.get("value_hash")
        present = (scope, key) in live_pairs
        if present and want_hash and _live_value_hash(wc, scope, key) == want_hash:
            skipped += 1
            continue
        to_put.append(it)

    delete_pairs: set = {(d["scope"], d["key"]) for d in (bundle.get("deletes", []) or [])}
    if mode == "mirror":
        # Prune keys the destination still holds that the primary no longer has.
        delete_pairs |= (live_pairs - desired_pairs)
    to_delete: List[Dict[str, str]] = [{"scope": s, "key": k} for s, k in sorted(delete_pairs)]

    # ACL diff per scope (skip scopes we're deleting keys out of entirely is fine — put_acl
    # on a live scope is what matters).
    desired_acls: Dict[str, List[Dict[str, str]]] = bundle.get("acls", {}) or {}
    acl_plan: List[str] = [
        s for s in desired_acls
        if _acl_signature([(a["principal"], a["permission"]) for a in desired_acls[s]])
        != _acl_signature(live.get(s, {}).get("acls", []))
    ]

    has_work = bool(to_put or to_delete or acl_plan)
    if not has_work:
        dur = time.time() - t0
        control.record_audit(
            ex, cfg.audit_table, operation="IMPORT", status="SKIPPED", direction=direction,
            item_count=0, bundle_id=bundle.get("bundle_id"), duration_sec=dur,
            detail=f"in sync (skipped={skipped})", actor=actor or cfg.service_principal,
        )
        _logger.info("Secrets import: destination already in sync (skipped=%d).", skipped)
        return {"bundle_id": bundle.get("bundle_id"), "added": 0, "updated": 0,
                "deleted": 0, "skipped": skipped, "in_sync": True, "duration_sec": round(dur, 2)}

    # ---- apply (writes require a promoted workspace) -------------------------
    _assert_writable(wc)
    existing_scopes = {s.name for s in wc.secrets.list_scopes()}
    added = updated = deleted = 0
    inv_updates: List[Dict[str, Any]] = []

    for it in to_put:
        scope, key, blob = it["scope"], it["key"], it["value"]
        is_add = (scope, key) not in live_pairs
        if scope not in existing_scopes:
            try:
                wc.secrets.create_scope(scope=scope)
                existing_scopes.add(scope)
            except Exception as e:  # noqa: BLE001
                if "already exists" not in str(e).lower():
                    raise
        plaintext = (crypto.decrypt_value(kms, blob, {"scope": scope, "key": key})
                     if use_cse else crypto.unwrap_plain(blob))
        wc.secrets.put_secret(scope=scope, key=key, string_value=plaintext.decode())
        added += is_add
        updated += (not is_add)
        inv_updates.append({"scope": scope, "secret_key": key, "status": "IN_SYNC",
                            "value_hash": it.get("value_hash"), "bundle_id": bundle.get("bundle_id")})

    for d in to_delete:
        try:
            wc.secrets.delete_secret(scope=d["scope"], key=d["key"])
            deleted += 1
        except Exception as e:  # noqa: BLE001
            _logger.debug("delete_secret %s/%s: %s", d["scope"], d["key"], str(e)[:120])
        inv_updates.append({"scope": d["scope"], "secret_key": d["key"], "status": "DELETED",
                            "bundle_id": bundle.get("bundle_id")})

    for scope in acl_plan:
        desired = desired_acls[scope]
        desired_principals = {a["principal"] for a in desired}
        for a in desired:
            try:
                wc.secrets.put_acl(scope=scope, principal=a["principal"],
                                   permission=_acl_permission(a["permission"]))
            except Exception as e:  # noqa: BLE001
                _logger.debug("put_acl %s/%s: %s", scope, a.get("principal"), str(e)[:120])
        if mode == "mirror":  # remove ACLs the primary no longer grants
            for principal, _perm in live.get(scope, {}).get("acls", []):
                if principal not in desired_principals:
                    try:
                        wc.secrets.delete_acl(scope=scope, principal=principal)
                    except Exception as e:  # noqa: BLE001
                        _logger.debug("delete_acl %s/%s: %s", scope, principal, str(e)[:120])

    # Optionally prune whole scopes the bundle no longer describes.
    if mode == "mirror" and prune_scopes:
        keep = desired_pairs | {(d["scope"], d["key"]) for d in bundle.get("deletes", []) or []}
        keep_scopes = {s for s, _ in keep} | set(desired_acls)
        for scope in sorted(set(live) - keep_scopes):
            try:
                wc.secrets.delete_scope(scope=scope)
            except Exception as e:  # noqa: BLE001
                _logger.debug("delete_scope %s: %s", scope, str(e)[:120])

    control.upsert_inventory(ex, cfg.inventory_table, inv_updates)
    dur = time.time() - t0
    control.record_audit(
        ex, cfg.audit_table, operation="IMPORT", status="SUCCESS", direction=direction,
        item_count=added + updated, bundle_id=bundle.get("bundle_id"), duration_sec=dur,
        detail=f"added={added} updated={updated} deleted={deleted} skipped={skipped} "
               f"acls={len(acl_plan)} mode={mode}",
        actor=actor or cfg.service_principal,
    )
    summary = {"bundle_id": bundle.get("bundle_id"), "added": added, "updated": updated,
               "deleted": deleted, "skipped": skipped, "acls": len(acl_plan),
               "duration_sec": round(dur, 2)}
    _logger.info("Secrets import complete: %s", summary)
    return summary
