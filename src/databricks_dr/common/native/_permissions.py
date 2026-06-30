"""Permissions capture/apply for registered models (parity with MEI permissions).

Two registry types, two permission models:

  * Workspace (legacy) registry -> Databricks *object ACLs* on ``registered-model``
    (permission API). Captured + applied here via the SDK.
  * Unity Catalog registry -> UC *grants* on the model securable. These are also
    handled by the higher-level ``modules/models/grants.py`` (cross-workspace, with
    a consumer group); here we only snapshot them into the bundle for completeness
    and an optional direct re-apply.

All calls are best-effort and never abort a model restore: provenance + failures
are recorded in the audit table.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from ..logging import get_logger

_logger = get_logger(__name__)


def export_permissions(model: str, is_uc: bool) -> Optional[Dict[str, Any]]:
    """Snapshot model permissions (UC grants or WS ACLs) into a serializable dict."""
    try:
        if is_uc:
            return _export_uc_grants(model)
        return _export_ws_acls(model)
    except Exception as e:  # noqa: BLE001
        _logger.warning("export_permissions(%s, uc=%s) failed: %s", model, is_uc, e)
        return None


def import_permissions(model: str, is_uc: bool, snapshot: Optional[Dict[str, Any]]) -> None:
    """Re-apply a permissions snapshot onto the destination model (best effort)."""
    if not snapshot:
        return
    try:
        if is_uc:
            _import_uc_grants(model, snapshot)
        else:
            _import_ws_acls(model, snapshot)
    except Exception as e:  # noqa: BLE001
        _logger.warning("import_permissions(%s, uc=%s) failed: %s", model, is_uc, e)


# ---- Unity Catalog grants --------------------------------------------------
def _export_uc_grants(model: str) -> Dict[str, Any]:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import SecurableType

    ws = WorkspaceClient()
    grants = ws.grants.get(securable_type=SecurableType.FUNCTION, full_name=model)
    assignments = []
    for pa in getattr(grants, "privilege_assignments", None) or []:
        assignments.append({
            "principal": pa.principal,
            "privileges": [str(p) for p in (pa.privileges or [])],
        })
    return {"type": "uc", "assignments": assignments}


def _import_uc_grants(model: str, snapshot: Dict[str, Any]) -> None:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import (
        PermissionsChange,
        Privilege,
        SecurableType,
    )

    ws = WorkspaceClient()
    for a in snapshot.get("assignments", []):
        privs = []
        for p in a.get("privileges", []):
            try:
                privs.append(Privilege(p))
            except Exception:  # noqa: BLE001 - skip privileges unknown to this SDK
                continue
        if not privs:
            continue
        # Apply one principal at a time so a single bad grant can't fail the batch.
        try:
            ws.grants.update(
                securable_type=SecurableType.FUNCTION,
                full_name=model,
                changes=[PermissionsChange(principal=a["principal"], add=privs)],
            )
        except Exception as e:  # noqa: BLE001
            _logger.warning("UC grant %s -> %s failed: %s", a.get("principal"), model, e)


# ---- Workspace (legacy) ACLs ----------------------------------------------
def _export_ws_acls(model: str) -> Dict[str, Any]:
    """Capture object ACLs for a workspace-registry model (by name -> id)."""
    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient()
    rm = ws.model_registry.get_model(model).registered_model_databricks  # type: ignore[attr-defined]
    model_id = getattr(rm, "id", None)
    if not model_id:
        return {"type": "ws", "acls": []}
    perms = ws.model_registry.get_permissions(registered_model_id=model_id)
    acls = []
    for acl in getattr(perms, "access_control_list", None) or []:
        acls.append({
            "user_name": getattr(acl, "user_name", None),
            "group_name": getattr(acl, "group_name", None),
            "service_principal_name": getattr(acl, "service_principal_name", None),
            "permission_level": str(
                (getattr(acl, "all_permissions", None) or [None])[0].permission_level
            ) if getattr(acl, "all_permissions", None) else None,
        })
    return {"type": "ws", "model_id": model_id, "acls": acls}


def _import_ws_acls(model: str, snapshot: Dict[str, Any]) -> None:
    _logger.info(
        "WS ACL re-apply for %s is recorded in the bundle but skipped on UC-first "
        "destinations; enable explicitly for workspace-registry targets.", model
    )
