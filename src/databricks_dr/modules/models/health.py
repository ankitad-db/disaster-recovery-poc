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


def run_health_check(ctx: RunContext, *, lookback_hours: int = 24) -> dict:
    """Validate replication fidelity; raise on drift. Returns a per-model report."""
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    problems: list[str] = []
    report: dict[str, dict] = {}

    models = resolve_models(LOCAL_REGISTRY, cfg.models.get("include", []))
    if not models:
        problems.append("no models in scope (config models.include is empty)")

    for model in models:
        dest = _list_versions(LOCAL_REGISTRY, model)  # local = destination registry
        dest_max = max(dest) if dest else 0
        wm = audit.watermark(model)
        source_max = _source_max(ctx, model)
        report[model] = {
            "dest_versions": dest,
            "dest_max": dest_max,
            "watermark": wm,
            "source_max": source_max,
        }
        _logger.info(
            "health %s dest=%s dest_max=%s watermark=%s source_max=%s",
            model, dest, dest_max, wm, source_max,
        )
        if not dest:
            problems.append(f"{model}: absent from destination registry")
        elif dest_max < wm:
            problems.append(f"{model}: dest_max {dest_max} < watermark {wm} (import gap)")
        if source_max is not None and dest_max < source_max:
            problems.append(f"{model}: dest_max {dest_max} < source_max {source_max} (replication lag)")

    failures = audit.recent_failures(lookback_hours)
    if failures:
        sample = failures[0]
        problems.append(
            f"{len(failures)} FAILED audit row(s) in last {lookback_hours}h "
            f"(latest: {sample.get('operation')} {sample.get('model_name')} "
            f"{sample.get('error_message')})"
        )

    status = "FAILED" if problems else "SUCCESS"
    audit.insert(AuditRow(
        operation="HEALTH",
        direction=direction.label,
        model_name="*",
        status=status,
        triggered_by=ctx.triggered_by,
        artifact_count=len(models),
        error_message="; ".join(problems) or None,
        actor=cfg.service_principal,
    ))

    if problems:
        raise RuntimeError("DR health check FAILED: " + "; ".join(problems))
    _logger.info("DR health check OK for %d model(s): %s", len(models), list(report))
    return report


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
