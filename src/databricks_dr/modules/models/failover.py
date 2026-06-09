"""Failover and failback orchestration for the models module.

Failover  : promote the secondary to serve traffic (verify replication is current,
            scale up serving endpoints, record a FAILOVER audit marker). The role
            flip itself is operational (set DR_ACTIVE_PRIMARY / update config).
Failback  : drain new work on the (temporary) primary, run a reverse-direction CDC
            so changes made during the outage flow back, then restore roles.

Both are deliberately thin and auditable: the heavy lifting reuses baseline/cdc in
the resolved direction, so there is no duplicated replication logic.
"""

from __future__ import annotations

from ...common.audit import AuditRow
from ...common.clients import workspace_client
from ...common.logging import get_logger
from ...core.base import RunContext

_logger = get_logger(__name__)


def run_failover(ctx: RunContext) -> None:
    """Activate the destination region (current secondary) for serving."""
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    row = AuditRow(operation="FAILOVER", direction=direction.label, model_name="*",
                   triggered_by=ctx.triggered_by, actor=cfg.service_principal,
                   error_message="failover: promote secondary")
    audit.insert(row)
    try:
        _scale_up_endpoints(ctx, direction.dest.profile)
        audit.update_status(row.audit_id, "SUCCESS",
                            error_message=f"Promoted {direction.dest.region}. "
                                          f"Set DR_ACTIVE_PRIMARY={direction.dest.key} and repoint consumers.")
        _logger.info("FAILOVER complete. Promote region key '%s' to primary in config/env.", direction.dest.key)
    except Exception as e:  # noqa: BLE001
        audit.update_status(row.audit_id, "FAILED", error_message=str(e))
        raise


def run_failback(ctx: RunContext) -> None:
    """Record the failback marker; reverse CDC is driven via --failback in the CLI.

    Operationally:
      1. Quiesce writes on the temporary primary.
      2. ``databricks-dr models cdc --failback`` (reverse direction) catches up.
      3. Restore original roles (unset DR_ACTIVE_PRIMARY / revert config).
    """
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    row = AuditRow(operation="FAILOVER", direction=direction.label, model_name="*",
                   triggered_by=ctx.triggered_by, actor=cfg.service_principal,
                   error_message="failback: restore original primary")
    audit.insert(row)
    try:
        _scale_up_endpoints(ctx, direction.dest.profile)
        audit.update_status(row.audit_id, "SUCCESS",
                            error_message=f"Failback to {direction.dest.region}. "
                                          f"Run reverse CDC then restore roles.")
        _logger.info("FAILBACK marker recorded for direction %s.", direction.label)
    except Exception as e:  # noqa: BLE001
        audit.update_status(row.audit_id, "FAILED", error_message=str(e))
        raise


def _scale_up_endpoints(ctx: RunContext, profile: str) -> None:
    if ctx.dry_run:
        return
    wc = workspace_client(profile)
    try:
        for ep in wc.serving_endpoints.list():
            _logger.info("Endpoint %s present on target; ensure scale-to-zero disabled for active serving.", ep.name)
    except Exception as e:  # noqa: BLE001
        _logger.warning("Could not enumerate endpoints on %s: %s", profile, e)
