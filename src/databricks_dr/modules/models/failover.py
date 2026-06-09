"""Failover and failback orchestration for the models module.

Failover  : promote the secondary to serve traffic (scale up serving endpoints,
            record a FAILOVER audit marker, and persist the new active primary to
            the ``dr_state`` control table so scheduled jobs honour the flip).
Failback  : drain new work on the (temporary) primary, run a reverse-direction CDC
            so outage-time changes flow back, then reset ``dr_state`` to home.

Both are deliberately thin and auditable: the heavy lifting reuses baseline/cdc in
the resolved direction, so there is no duplicated replication logic.
"""

from __future__ import annotations

from ...common import state
from ...common.audit import AuditRow
from ...common.logging import get_logger
from ...core.base import RunContext

_logger = get_logger(__name__)


def _persist_active_primary(ctx: RunContext, key: str, reason: str) -> None:
    """Write the durable active-role state so scheduled jobs see the role change.

    Best-effort: if no spark/warehouse is available (e.g. dev CLI), the role is
    not persisted and the operator falls back to the env override -- logged loudly.
    """
    if ctx.spark is None:
        _logger.warning(
            "No spark on RunContext; dr_state NOT updated to active_primary=%s. "
            "Set DR_ACTIVE_PRIMARY=%s or run this from a notebook/job.", key, key,
        )
        return
    state.set_active_primary(
        ctx.cfg.state_table, key, reason=reason,
        actor=ctx.cfg.service_principal, spark=ctx.spark,
    )


def run_failover(ctx: RunContext) -> None:
    """Activate the destination region (current secondary) for serving.

    Runs IN the secondary workspace (the local/dest region). The primary may be
    down, so this does NOT pull -- the secondary is already a warm mirror from
    prior CDC. It scales serving on, records the marker, and **persists the new
    active primary to ``dr_state``** so scheduled jobs honour the role change
    without any per-session env var. The operator only needs to repoint consumers.
    """
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    promoted = direction.dest.key
    row = AuditRow(operation="FAILOVER", direction=direction.label, model_name="*",
                   triggered_by=ctx.triggered_by, actor=cfg.service_principal,
                   error_message="failover: promote secondary")
    audit.insert(row)
    try:
        _scale_up_endpoints(ctx)
        _persist_active_primary(ctx, promoted, reason="FAILOVER")
        audit.update_status(row.audit_id, "SUCCESS",
                            error_message=f"Promoted {direction.dest.region}; dr_state active_primary={promoted}. "
                                          f"Repoint consumers to {direction.dest.region}.")
        _logger.info("FAILOVER complete. dr_state active_primary='%s'.", promoted)
    except Exception as e:  # noqa: BLE001
        audit.update_status(row.audit_id, "FAILED", error_message=str(e))
        raise


def run_failback(ctx: RunContext) -> None:
    """Record the failback marker after the reverse-direction catch-up.

    The reverse CDC (promoted-region -> home-primary) is driven by the notebook/CLI
    with ``failback=True`` *before* this marker is written. Operationally:
      1. Quiesce writes on the promoted region.
      2. Reverse CDC catches the home primary up with outage-time changes.
      3. This marker is recorded and ``dr_state`` is reset to the home primary,
         which restores steady state -- no manual env-var cleanup needed.
    """
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    home = cfg.config_primary_key()  # destination of failback = the home primary
    row = AuditRow(operation="FAILBACK", direction=direction.label, model_name="*",
                   triggered_by=ctx.triggered_by, actor=cfg.service_principal,
                   error_message="failback: restore original primary")
    audit.insert(row)
    try:
        _scale_up_endpoints(ctx)
        _persist_active_primary(ctx, home, reason="FAILBACK")
        audit.update_status(row.audit_id, "SUCCESS",
                            error_message=f"Failback to {direction.dest.region} complete; "
                                          f"dr_state active_primary={home}. Steady state restored.")
        _logger.info("FAILBACK complete; dr_state active_primary='%s'.", home)
    except Exception as e:  # noqa: BLE001
        audit.update_status(row.audit_id, "FAILED", error_message=str(e))
        raise


def _scale_up_endpoints(ctx: RunContext) -> None:
    """Activate the LOCAL (dest) serving endpoints for in-scope models.

    Delegates to the endpoints module, which scales the standby (scale-to-zero)
    mirror up to an active posture. Non-fatal: a serving hiccup must not abort the
    role flip itself -- the ENDPOINT audit rows record the outcome.
    """
    if ctx.dry_run:
        return
    from . import endpoints
    try:
        endpoints.activate_endpoints(ctx)
    except Exception as e:  # noqa: BLE001
        _logger.warning("Endpoint activation failed (non-fatal): %s", e)
