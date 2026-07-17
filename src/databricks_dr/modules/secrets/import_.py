"""Secrets IMPORT — runs in the PROMOTED workspace on failover.

Reads the latest bundle from the LOCAL-region bucket (populated by S3 CRR),
decrypts the values, and re-creates the scopes/secrets/ACLs. Writes are only
possible once the workspace is promoted (a read-only DR secondary rejects them),
so this is the on-failover step.

By default the local region is the configured secondary; pass ``region_key`` to
run the symmetric failback import on the primary.
"""

from __future__ import annotations

import base64
import time
from typing import Any, Dict, Optional

from ...common.logging import get_logger
from ...common.sql import SqlExecutor
from . import control, crypto, store
from .config import SecretsConfig

_logger = get_logger(__name__)


def _acl_permission(value: str):
    """Map a stored permission string back to the SDK AclPermission enum."""
    from databricks.sdk.service.workspace import AclPermission

    v = (value or "").split(".")[-1].upper()  # 'AclPermission.MANAGE' -> 'MANAGE'
    return getattr(AclPermission, v, AclPermission.READ)


def run_import(
    cfg: SecretsConfig, *, region_key: str = "secondary", wc=None,
    ex: Optional[SqlExecutor] = None, spark=None, actor: str = "",
) -> Dict[str, Any]:
    """Import the latest secrets bundle into the local (promoted) workspace."""
    t0 = time.time()
    local = cfg.workspaces[region_key]

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

    existing_scopes = {s.name for s in wc.secrets.list_scopes()}
    applied = 0
    inv_updates = []

    # 1. Values -> scopes/secrets.
    for item in bundle.get("items", []):
        scope, key, blob = item["scope"], item["key"], item["value"]
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
        applied += 1
        inv_updates.append({"scope": scope, "secret_key": key, "status": "IN_SYNC",
                            "bundle_id": bundle.get("bundle_id")})

    # 2. Tombstones -> delete_secret.
    for d in bundle.get("deletes", []):
        try:
            wc.secrets.delete_secret(scope=d["scope"], key=d["key"])
        except Exception as e:  # noqa: BLE001
            _logger.debug("delete_secret %s/%s: %s", d["scope"], d["key"], str(e)[:120])
        inv_updates.append({"scope": d["scope"], "secret_key": d["key"], "status": "DELETED",
                            "bundle_id": bundle.get("bundle_id")})

    # 3. ACLs.
    for scope, acls in (bundle.get("acls") or {}).items():
        for a in acls:
            try:
                wc.secrets.put_acl(scope=scope, principal=a["principal"],
                                   permission=_acl_permission(a["permission"]))
            except Exception as e:  # noqa: BLE001
                _logger.debug("put_acl %s/%s: %s", scope, a.get("principal"), str(e)[:120])

    control.upsert_inventory(ex, cfg.inventory_table, inv_updates)
    dur = time.time() - t0
    control.record_audit(
        ex, cfg.audit_table, operation="IMPORT", status="SUCCESS", direction=direction,
        item_count=applied, bundle_id=bundle.get("bundle_id"), duration_sec=dur,
        detail=f"applied={applied} deleted={len(bundle.get('deletes', []))}",
        actor=actor or cfg.service_principal,
    )
    summary = {"bundle_id": bundle.get("bundle_id"), "applied": applied,
               "deleted": len(bundle.get("deletes", [])), "duration_sec": round(dur, 2)}
    _logger.info("Secrets import complete: %s", summary)
    return summary
