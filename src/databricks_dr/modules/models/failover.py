"""Failover and failback orchestration for the models module.

Failover  : promote the secondary to be the active primary, so scheduled jobs
            reverse direction. It runs a **readiness preflight** first (is the
            destination a usable mirror?), records the effective **recovery point**
            (RPO) on the audit row, then persists the new active primary to
            ``dr_state`` and verifies the write.
Failback  : after a reverse-direction CDC catch-up (driven by the caller), verify
            the home primary actually caught up, then reset ``dr_state`` to home.

Design principles:
  * **Fail-loud, but recoverable.** Failover blocks only on *blockers* (a model
    missing from the destination, or nothing in scope) -- a lagging-but-present
    mirror is still promotable, with the lag recorded as the RPO. ``force=True``
    overrides even blockers for a true smoking-crater disaster.
  * **Verified role flip.** The ``dr_state`` write is read back and confirmed;
    failover requires a spark/warehouse to persist the role (no silent no-op).
  * **Auditable recovery point.** Every run records last-successful-sync time and
    per-model synced versions on the FAILOVER/FAILBACK row.
"""

from __future__ import annotations

from ...common import state
from ...common.audit import AuditRow
from ...common.logging import get_logger
from ...core.base import RunContext
from . import health

_logger = get_logger(__name__)


def run_failover(ctx: RunContext) -> None:
    """Promote the destination region (current secondary) to active primary.

    Runs IN the secondary workspace. The primary may be down, so this does NOT
    pull -- the secondary is already a warm mirror from prior CDC. Set
    ``ctx.force=True`` to promote even when the readiness preflight finds blockers.
    """
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    promoted = direction.dest.key
    _require_persistence(ctx)

    row = AuditRow(operation="FAILOVER", direction=direction.label, model_name="*",
                   triggered_by=ctx.triggered_by, actor=cfg.service_principal,
                   error_message="failover: promote secondary")
    audit.insert(row)
    try:
        rep = health.assess_replication(ctx)
        rpo = _rpo_summary(rep)

        if rep.blockers and not ctx.force:
            msg = ("failover BLOCKED: " + "; ".join(rep.blockers)
                   + " -- set force=true to promote anyway. " + rpo)
            audit.update_status(row.audit_id, "FAILED", error_message=msg)
            raise RuntimeError(msg)

        if not ctx.dry_run:
            _persist_active_primary(ctx, promoted, reason="FAILOVER")
            _verify_active_primary(ctx, promoted)

        note = (f"Promoted {direction.dest.region}; dr_state active_primary={promoted}. "
                f"Repoint consumers to {direction.dest.region}. {rpo}")
        if rep.problems:
            note += " | WARNINGS: " + "; ".join(rep.problems)
        if ctx.force and rep.blockers:
            note += " | FORCED over blockers: " + "; ".join(rep.blockers)
        audit.update_status(row.audit_id, "SUCCESS",
                            artifact_count=len(rep.models), error_message=note)
        _logger.info("FAILOVER complete. dr_state active_primary='%s'. %s", promoted, rpo)
    except Exception as e:  # noqa: BLE001
        _fail_once(audit, row.audit_id, e)
        raise


def run_failback(ctx: RunContext) -> None:
    """Restore the home primary after the reverse-direction catch-up.

    The reverse CDC (promoted-region -> home-primary) is driven by the caller with
    ``failback=True`` *before* this runs. Operationally:
      1. Quiesce writes on the promoted region.
      2. Reverse CDC catches the home primary up with outage-time changes.
      3. This verifies the catch-up converged, resets ``dr_state`` to home, and
         records the recovery point.

    Failback is a *planned* operation, so it gates on the full problem set (the home
    primary must have actually caught up) -- ``force=True`` overrides.
    """
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    home = cfg.config_primary_key()  # destination of failback = the home primary
    _require_persistence(ctx)

    row = AuditRow(operation="FAILBACK", direction=direction.label, model_name="*",
                   triggered_by=ctx.triggered_by, actor=cfg.service_principal,
                   error_message="failback: restore original primary")
    audit.insert(row)
    try:
        rep = health.assess_replication(ctx)
        rpo = _rpo_summary(rep)

        if rep.problems and not ctx.force:
            msg = ("failback BLOCKED: reverse catch-up incomplete: "
                   + "; ".join(rep.problems)
                   + " -- re-run CDC (failback direction) or set force=true. " + rpo)
            audit.update_status(row.audit_id, "FAILED", error_message=msg)
            raise RuntimeError(msg)

        if not ctx.dry_run:
            _persist_active_primary(ctx, home, reason="FAILBACK")
            _verify_active_primary(ctx, home)

        note = (f"Failback to {direction.dest.region} complete; "
                f"dr_state active_primary={home}. Steady state restored. {rpo}")
        if ctx.force and rep.problems:
            note += " | FORCED over: " + "; ".join(rep.problems)
        audit.update_status(row.audit_id, "SUCCESS",
                            artifact_count=len(rep.models), error_message=note)
        _logger.info("FAILBACK complete; dr_state active_primary='%s'. %s", home, rpo)
    except Exception as e:  # noqa: BLE001
        _fail_once(audit, row.audit_id, e)
        raise


# ── helpers ──────────────────────────────────────────────────────────

def _require_persistence(ctx: RunContext) -> None:
    """A real role flip must be durable; refuse to run without a way to write it."""
    if ctx.dry_run:
        return
    if ctx.spark is None:
        raise RuntimeError(
            "failover/failback needs a spark session (or warehouse) to persist and "
            "verify dr_state. Run from a notebook/job, or use --dry-run for a rehearsal."
        )


def _persist_active_primary(ctx: RunContext, key: str, reason: str) -> None:
    """Write the durable active-role state so scheduled jobs see the role change."""
    state.set_active_primary(
        ctx.cfg.state_table, key, reason=reason,
        actor=ctx.cfg.service_principal, spark=ctx.spark,
    )


def _verify_active_primary(ctx: RunContext, expected: str) -> None:
    """Read dr_state back and confirm the flip actually landed (fail-loud)."""
    got = state.read_active_primary(ctx.cfg.state_table, spark=ctx.spark)
    if got != expected:
        raise RuntimeError(
            f"dr_state verification failed: active_primary={got!r}, expected {expected!r}. "
            f"Role flip did not persist."
        )


def _rpo_summary(rep: "health.ReplicationReport") -> str:
    """One-line recovery-point summary for the audit row."""
    parts: list[str] = []
    if rep.last_success_ts:
        parts.append(f"last successful sync {rep.last_success_ts}")
    else:
        parts.append("no prior successful sync recorded")
    versions = ", ".join(
        f"{m.split('.')[-1]}=v{d['dest_max']}" for m, d in rep.models.items()
    )
    if versions:
        parts.append(f"synced versions [{versions}]")
    return "RPO: " + "; ".join(parts)


def _fail_once(audit, audit_id: str, err: Exception) -> None:
    """Mark the row FAILED with the error text (idempotent for the blocker path)."""
    audit.update_status(audit_id, "FAILED", error_message=str(err))
