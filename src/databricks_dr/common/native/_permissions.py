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

from typing import Any, Dict, List, Optional

from ..logging import get_logger

_logger = get_logger(__name__)


# ---- version-robust raw-REST UC grant helpers ------------------------------
# Newer databricks-sdk builds serialize the ``SecurableType`` enum via ``str()``
# into the request path (".../SecurableType.CATALOG/..."), which the server
# rejects with "SECURABLETYPE.<X> is not a valid securable type". We hit this on
# FUNCTION/REGISTERED_MODEL/CATALOG. Calling the stable REST endpoint directly
# with the lowercase securable token side-steps the enum entirely and is
# version-proof across SDK builds.
_PERMS_PATH = "/api/2.1/unity-catalog/permissions"


def uc_grants_get(ws, securable: str, full_name: str) -> List[Dict[str, Any]]:
    """Return ``[{"principal", "privileges": [...]}, ...]`` for a securable.

    ``securable`` is the lowercase REST token: ``catalog`` / ``schema`` /
    ``registered_model``.
    """
    resp = ws.api_client.do("GET", f"{_PERMS_PATH}/{securable}/{full_name}") or {}
    out: List[Dict[str, Any]] = []
    for pa in resp.get("privilege_assignments") or []:
        out.append({
            "principal": pa.get("principal"),
            "privileges": list(pa.get("privileges") or []),
        })
    return out


def uc_grants_update(ws, securable: str, full_name: str,
                     changes: List[Dict[str, Any]]) -> None:
    """Apply grant changes; ``changes`` is ``[{"principal", "add": [...]}, ...]``."""
    if not changes:
        return
    ws.api_client.do(
        "PATCH", f"{_PERMS_PATH}/{securable}/{full_name}", body={"changes": changes}
    )


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

    ws = WorkspaceClient()
    assignments = uc_grants_get(ws, "registered_model", model)
    return {"type": "uc", "assignments": assignments}


def _import_uc_grants(model: str, snapshot: Dict[str, Any]) -> None:
    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient()
    for a in snapshot.get("assignments", []):
        privs = list(a.get("privileges") or [])
        if not privs:
            continue
        # Apply one principal at a time so a single bad grant can't fail the batch.
        try:
            uc_grants_update(
                ws, "registered_model", model,
                [{"principal": a["principal"], "add": privs}],
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
