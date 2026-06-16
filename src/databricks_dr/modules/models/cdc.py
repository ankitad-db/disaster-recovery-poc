"""Incremental CDC sync (watermark-gated, cross-workspace pull).

Steady-state DR. Runs in the LOCAL (destination) workspace, just like
``replicate``. For each in-scope model it compares the source's max version
(read under the remote identity) against the per-model watermark in the local
audit table, and re-replicates only the models that changed. Re-replication uses
the proven full-model pull with ``delete_model=True`` (clean overwrite, so no
duplicate versions) -- robust for models, where artifacts are small.

This is "incremental" at the scheduling granularity: unchanged models are
skipped, so a 15-minute schedule is cheap and idempotent. The watermark is
advanced by writing a per-model VERIFY row after a successful sync.
"""

from __future__ import annotations

from ...common.audit import AuditRow
from ...common.clients import make_mlflow_client
from ...common.logging import get_logger
from ...core.base import RunContext
from . import replicate
from ._selection import resolve_models

_logger = get_logger(__name__)


def run_cdc(ctx: RunContext) -> None:
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    host, token = replicate._remote_creds(ctx)

    # --- detect changed models under the REMOTE identity ---
    changed: dict[str, int] = {}
    with replicate._ambient_identity(host, token):
        client = make_mlflow_client(replicate.LOCAL_REGISTRY)
        for model in resolve_models(replicate.LOCAL_REGISTRY, cfg.models.get("include", [])):
            source_max = _max_version(client, model)
            watermark = audit.watermark(model)
            if source_max > watermark:
                changed[model] = source_max
                _logger.info("%s: source v%s > watermark v%s -> sync", model, source_max, watermark)
            else:
                _logger.info("%s: up to date (v%s)", model, watermark)

    if not changed:
        _logger.info("CDC: nothing to sync (%s)", direction.label)
        return

    # --- replicate only the changed models (full overwrite, no duplicates) ---
    replicate.run_replicate(ctx, full=False, delete_model=True, models_override=list(changed))

    # --- advance the watermark per model ---
    for model, source_max in changed.items():
        audit.insert(AuditRow(
            operation="VERIFY", direction=direction.label, model_name=model,
            source_version=str(source_max), status="SUCCESS", triggered_by=ctx.triggered_by,
            actor=cfg.service_principal,
        ))


def _max_version(client, model: str) -> int:
    try:
        versions = client.search_model_versions(f"name='{model}'")
    except Exception as e:  # noqa: BLE001
        _logger.error("Cannot list versions for %s: %s", model, e)
        return 0
    nums = [int(v.version) for v in versions]
    return max(nums) if nums else 0
