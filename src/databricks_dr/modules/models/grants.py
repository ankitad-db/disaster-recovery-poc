"""Consumer-facing UC grants replication.

mlflow-export-import carries model-level *registered model* permissions, but not
the surrounding catalog/schema grants a consumer needs (USE CATALOG / USE SCHEMA
/ EXECUTE). This mirrors those grants from source to destination via the SDK so a
failed-over consumer can actually resolve and read the model.
"""

from __future__ import annotations

from ...common.clients import workspace_client
from ...common.audit import AuditRow
from ...common.logging import get_logger
from ...core.base import RunContext

_logger = get_logger(__name__)

# Securable level -> privileges we replicate for consumers.
_LEVELS = ("CATALOG", "SCHEMA")


def replicate_grants(ctx: RunContext) -> None:
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    catalog = cfg.uc["catalog"]
    schema = cfg.uc["schema"]
    src = workspace_client(direction.source.profile)
    dst = workspace_client(direction.dest.profile)

    row = AuditRow(operation="GRANTS", direction=direction.label, model_name=f"{catalog}.{schema}",
                   triggered_by=ctx.triggered_by, actor=cfg.service_principal)
    audit.insert(row)
    try:
        _mirror(src, dst, "CATALOG", catalog, ctx.dry_run)
        _mirror(src, dst, "SCHEMA", f"{catalog}.{schema}", ctx.dry_run)
        audit.update_status(row.audit_id, "SUCCESS")
    except Exception as e:  # noqa: BLE001
        audit.update_status(row.audit_id, "FAILED", error_message=str(e))
        raise


def _mirror(src, dst, securable_type: str, full_name: str, dry_run: bool) -> None:
    from databricks.sdk.service.catalog import (
        PermissionsChange,
        SecurableType,
    )

    st = SecurableType(securable_type)
    current = src.grants.get(securable_type=st, full_name=full_name)
    changes = []
    for assignment in (current.privilege_assignments or []):
        changes.append(PermissionsChange(principal=assignment.principal,
                                         add=list(assignment.privileges or [])))
    if not changes:
        _logger.info("No grants to mirror for %s %s", securable_type, full_name)
        return
    _logger.info("Mirroring %d principal grants to %s %s", len(changes), securable_type, full_name)
    if dry_run:
        return
    dst.grants.update(securable_type=st, full_name=full_name, changes=changes)
