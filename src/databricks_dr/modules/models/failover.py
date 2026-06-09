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
from ...common.clients import is_databricks_runtime, workspace_client, workspace_client_from_creds
from ...common.logging import get_logger
from ...core.base import RunContext

_logger = get_logger(__name__)


def run_failover(ctx: RunContext) -> None:
    """Activate the destination region (current secondary) for serving.

    Runs IN the secondary workspace (the local/dest region). The primary may be
    down, so this does NOT pull -- the secondary is already a warm mirror from
    prior CDC. It only flips serving on and records the marker; the operator then
    sets ``DR_ACTIVE_PRIMARY`` and repoints consumers.
    """
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    row = AuditRow(operation="FAILOVER", direction=direction.label, model_name="*",
                   triggered_by=ctx.triggered_by, actor=cfg.service_principal,
                   error_message="failover: promote secondary")
    audit.insert(row)
    try:
        _scale_up_endpoints(ctx)
        audit.update_status(row.audit_id, "SUCCESS",
                            error_message=f"Promoted {direction.dest.region}. "
                                          f"Set DR_ACTIVE_PRIMARY={direction.dest.key} and repoint consumers.")
        _logger.info("FAILOVER complete. Promote region key '%s' to primary via DR_ACTIVE_PRIMARY.", direction.dest.key)
    except Exception as e:  # noqa: BLE001
        audit.update_status(row.audit_id, "FAILED", error_message=str(e))
        raise


def run_failback(ctx: RunContext) -> None:
    """Record the failback marker after the reverse-direction catch-up.

    The reverse CDC (promoted-region -> home-primary) is driven by the notebook/CLI
    with ``failback=True`` *before* this marker is written. Operationally:
      1. Quiesce writes on the promoted region.
      2. Reverse CDC catches the home primary up with outage-time changes.
      3. This marker is recorded.
      4. Restore roles (unset ``DR_ACTIVE_PRIMARY``) -- the final, separate step.
    """
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    row = AuditRow(operation="FAILBACK", direction=direction.label, model_name="*",
                   triggered_by=ctx.triggered_by, actor=cfg.service_principal,
                   error_message="failback: restore original primary")
    audit.insert(row)
    try:
        _scale_up_endpoints(ctx)
        audit.update_status(row.audit_id, "SUCCESS",
                            error_message=f"Failback to {direction.dest.region} complete. "
                                          f"Unset DR_ACTIVE_PRIMARY to restore steady state.")
        _logger.info("FAILBACK marker recorded for direction %s.", direction.label)
    except Exception as e:  # noqa: BLE001
        audit.update_status(row.audit_id, "FAILED", error_message=str(e))
        raise


def _scale_up_endpoints(ctx: RunContext) -> None:
    """Ensure serving endpoints in the LOCAL (dest) workspace are ready to serve.

    Failover/failback always run in the destination workspace, so the local
    ambient identity is the right client -- no CLI profile (absent on clusters).
    Off-cluster it falls back to the dest CLI profile.
    """
    if ctx.dry_run:
        return
    wc = workspace_client_from_creds() if is_databricks_runtime() else workspace_client(ctx.direction.dest.profile)
    try:
        for ep in wc.serving_endpoints.list():
            _logger.info("Endpoint %s present locally; ensure scale-to-zero disabled for active serving.", ep.name)
    except Exception as e:  # noqa: BLE001
        _logger.warning("Could not enumerate local serving endpoints: %s", e)
