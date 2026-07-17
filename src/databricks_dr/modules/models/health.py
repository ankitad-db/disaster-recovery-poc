"""Health check / drift detection for the models DR module.

Codifies the manual post-run validation (search the dest registry + scan the
audit table) into a single framework verb so it can run as an orchestrated job
task instead of by hand. It is intentionally *fail-loud*: on any drift it raises,
which fails the job task and triggers the job's failure notification.

Checks, per in-scope model (run in the SECONDARY / destination workspace):
  * present  -- the model exists in the local (destination) registry.
  * imported -- dest max version >= audit watermark (no silent import gap).
  * lag      -- dest max version >= source max version (best-effort; needs the
                remote secret scope). Skipped quietly when creds are unavailable.
Plus a table-wide scan for FAILED audit rows in the lookback window.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from ...common.audit import AuditRow
from ...common.logging import get_logger
from ...core.base import RunContext
from ._selection import resolve_models
from .replicate import (
    LOCAL_REGISTRY,
    _ambient_identity,
    _list_versions,
    _remote_creds,
)

_logger = get_logger(__name__)


@dataclass
class ReplicationReport:
    """Fidelity of the destination mirror relative to the source + watermark.

    ``problems`` are all detected issues (used by the health check, which is
    fail-loud). ``blockers`` is the subset that makes the destination *unsafe to
    promote* (a model missing entirely, or nothing in scope) -- failover gates only
    on these so a lagging-but-present mirror can still be promoted in a real
    disaster, with the lag recorded as the recovery point rather than hidden.
    """

    models: dict = field(default_factory=dict)
    problems: list = field(default_factory=list)
    blockers: list = field(default_factory=list)
    failures: list = field(default_factory=list)
    last_success_ts: Optional[str] = None

    @property
    def healthy(self) -> bool:
        return not self.problems


def assess_replication(ctx: RunContext, *, lookback_hours: int = 24) -> ReplicationReport:
    """Assess destination fidelity for the resolved direction. Never raises.

    Runs in the destination workspace. Local checks (present, import gap, recent
    failures) always work; the source-lag check is best-effort and skipped when the
    source is unreachable (the common case during a real failover).
    """
    cfg, audit = ctx.cfg, ctx.audit
    rep = ReplicationReport()

    models = resolve_models(LOCAL_REGISTRY, cfg.models.get("include", []))
    if not models:
        rep.problems.append("no models in scope (config models.include is empty)")
        rep.blockers.append("no models in scope")

    for model in models:
        dest = _list_versions(LOCAL_REGISTRY, model)  # local = destination registry
        dest_max = max(dest) if dest else 0
        wm = audit.watermark(model)
        source_max = _source_max(ctx, model)
        rep.models[model] = {
            "dest_versions": dest,
            "dest_max": dest_max,
            "watermark": wm,
            "source_max": source_max,
        }
        _logger.info(
            "assess %s dest=%s dest_max=%s watermark=%s source_max=%s",
            model, dest, dest_max, wm, source_max,
        )
        if not dest:
            msg = f"{model}: absent from destination registry"
            rep.problems.append(msg)
            rep.blockers.append(msg)  # can't serve a model that isn't there
        elif dest_max < wm:
            rep.problems.append(f"{model}: dest_max {dest_max} < watermark {wm} (import gap)")
        if source_max is not None and dest_max < source_max:
            rep.problems.append(
                f"{model}: dest_max {dest_max} < source_max {source_max} (replication lag)")

    rep.failures = audit.recent_failures(lookback_hours)
    if rep.failures:
        sample = rep.failures[0]
        rep.problems.append(
            f"{len(rep.failures)} FAILED audit row(s) in last {lookback_hours}h "
            f"(latest: {sample.get('operation')} {sample.get('model_name')} "
            f"{sample.get('error_message')})"
        )

    rep.last_success_ts = audit.last_success_time()
    return rep


def run_health_check(ctx: RunContext, *, lookback_hours: int = 24) -> dict:
    """Validate replication fidelity; raise on drift. Returns a per-model report."""
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    rep = assess_replication(ctx, lookback_hours=lookback_hours)

    status = "FAILED" if rep.problems else "SUCCESS"
    audit.insert(AuditRow(
        operation="HEALTH",
        direction=direction.label,
        model_name="*",
        status=status,
        triggered_by=ctx.triggered_by,
        artifact_count=len(rep.models),
        error_message="; ".join(rep.problems) or None,
        actor=cfg.service_principal,
    ))

    if rep.problems:
        raise RuntimeError("DR health check FAILED: " + "; ".join(rep.problems))
    _logger.info("DR health check OK for %d model(s): %s", len(rep.models), list(rep.models))
    return rep.models


def _source_max(ctx: RunContext, model: str) -> int | None:
    """Max source version via the remote identity, or None if unreachable."""
    try:
        host, token = _remote_creds(ctx)
    except Exception as e:  # noqa: BLE001 - off-cluster / no secret scope: skip lag check
        _logger.debug("Source lag check skipped for %s: %s", model, e)
        return None
    try:
        with _ambient_identity(host, token):
            src = _list_versions(LOCAL_REGISTRY, model)
        return max(src) if src else 0
    except Exception as e:  # noqa: BLE001 - best effort; don't fail health on a read hiccup
        _logger.warning("Could not read source versions for %s: %s", model, e)
        return None
