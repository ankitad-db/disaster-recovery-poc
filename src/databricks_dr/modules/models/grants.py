"""Consumer-facing UC grants replication.

mlflow-export-import carries model-level *registered model* permissions, but not
the surrounding catalog/schema grants a consumer needs (USE CATALOG / USE SCHEMA
/ EXECUTE). This mirrors those grants from source to destination via the SDK so a
failed-over consumer can actually resolve and read the model.
"""

from __future__ import annotations

from ...common.clients import (
    is_databricks_runtime,
    workspace_client,
    workspace_client_from_creds,
)
from ...common.audit import AuditRow
from ...common.logging import get_logger
from ...common.native._permissions import uc_grants_get, uc_grants_update
from ...core.base import RunContext
from . import replicate

_logger = get_logger(__name__)

# Securable level -> lowercase REST token we replicate for consumers.
_LEVELS = ("catalog", "schema")


def replicate_grants(ctx: RunContext) -> None:
    """Mirror consumer-facing catalog/schema grants from source to dest.

    Cross-workspace aware: on a cluster the DR job runs in the LOCAL (dest)
    workspace, so the source grants are read via the remote secret-scope creds and
    applied with the local ambient identity. Off-cluster it falls back to the
    dr-west/dr-east CLI profiles. Principals (groups/SPNs) must exist in the dest
    account for the grant to apply -- which holds when both workspaces share an
    account, as in this POC.
    """
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    catalog = cfg.uc["catalog"]
    schema = cfg.uc["schema"]
    src, dst = _grant_clients(ctx)

    row = AuditRow(operation="GRANTS", direction=direction.label, model_name=f"{catalog}.{schema}",
                   triggered_by=ctx.triggered_by, actor=cfg.service_principal)
    audit.insert(row)
    try:
        mirrored = 0
        mirrored += _mirror(src, dst, "catalog", catalog, ctx.dry_run)
        mirrored += _mirror(src, dst, "schema", f"{catalog}.{schema}", ctx.dry_run)
        audit.update_status(row.audit_id, "SUCCESS", artifact_count=mirrored)
    except Exception as e:  # noqa: BLE001
        audit.update_status(row.audit_id, "FAILED", error_message=str(e))
        raise


def _grant_clients(ctx: RunContext):
    """Return (source, dest) WorkspaceClients for the current runtime context."""
    direction = ctx.direction
    if is_databricks_runtime():
        host, token = replicate._remote_creds(ctx)
        source = workspace_client_from_creds(host, token)  # remote source
        dest = workspace_client_from_creds()               # local ambient (dest)
        return source, dest
    # Off-cluster: use the configured CLI profiles.
    return workspace_client(direction.source.profile), workspace_client(direction.dest.profile)


def _mirror(src, dst, securable: str, full_name: str, dry_run: bool) -> int:
    """Copy directly-assigned grants for one securable. Returns # principals mirrored.

    ``securable`` is the lowercase REST token (``catalog`` / ``schema``). We use
    the raw REST permissions endpoint rather than the typed SDK method: newer SDK
    builds serialize the ``SecurableType`` enum into the path as
    ``SecurableType.CATALOG``, which the server rejects.
    """
    assignments = uc_grants_get(src, securable, full_name)
    changes = [
        {"principal": a["principal"], "add": list(a.get("privileges") or [])}
        for a in assignments
        if a.get("privileges")
    ]
    if not changes:
        _logger.info("No grants to mirror for %s %s", securable, full_name)
        return 0
    _logger.info("Mirroring %d principal grants to %s %s", len(changes), securable, full_name)
    if dry_run:
        return len(changes)
    uc_grants_update(dst, securable, full_name, changes)
    return len(changes)
