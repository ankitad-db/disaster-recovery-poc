"""Incremental CDC sync (change-feed driven, watermark-gated, cross-workspace pull).

Steady-state DR. Runs in the LOCAL (destination) workspace, just like
``replicate``. Change detection is delegated to
:mod:`databricks_dr.common.native.changefeed`, which combines a
``system.access.audit`` event scan (the same signal Managed DR reacts to) with an
authoritative registry diff. Only the models that changed are re-replicated, and
each is synced as a *delta*: the destination's existing versions are skipped on
export and preserved on import (``delete_model=False``), so a steady-state pass
moves only the new versions plus any metadata (aliases/tags/description) that
drifted -- never a drop-and-rebuild of the whole model.

POC trigger = a manual notebook/job run. A scheduled job and a Model Update Trigger
are defined (paused) in ``resources/dr_models_jobs.yml`` for later enablement; when
enabled they call this exact same entry point.

The per-model watermark is advanced by writing a VERIFY row after a successful sync;
the audit-scan watermark is carried on that row (``source_event_time``) for RPO.
"""

from __future__ import annotations

from ...common.audit import AuditRow
from ...common.clients import make_mlflow_client
from ...common.inventory import ObjectInventory
from ...common.logging import get_logger
from ...common.native import changefeed
from ...core.base import RunContext
from . import replicate
from ._selection import resolve_models

_logger = get_logger(__name__)


def run_cdc(ctx: RunContext) -> None:
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    host, token = replicate._remote_creds(ctx)
    catalog = cfg.uc.get("catalog")
    schema = cfg.uc.get("schema")
    since = _last_scan_watermark(audit)

    # The system.access.audit scan is only meaningful when the audit stream visible
    # to this job actually carries the SOURCE's model events (e.g. the source-side or
    # a shared-account deployment). In the default cross-account pull topology the
    # local audit stream carries the *import* events, which would cause spurious
    # resyncs -- so the scan is OFF by default and the authoritative registry diff is
    # used. Flip `models.cdc_use_system_tables: true` once the scan targets the source.
    use_sys_tables = bool(cfg.models.get("cdc_use_system_tables", False))
    scan_spark = ctx.spark if use_sys_tables else None

    # Metadata-drift detector: diff the live source signature against the one stored on
    # the last successful sync so alias/tag/description changes that don't bump a
    # version are still re-synced. Reads the LOCAL desired-state inventory (spark on the
    # dest cluster; unaffected by the ambient source identity used for source reads).
    detect_metadata_drift = bool(cfg.models.get("detect_metadata_drift", True))
    inventory = ObjectInventory(
        cfg.inventory_table, spark=getattr(audit, "spark", None),
        workspace_client=getattr(audit, "wc", None),
        warehouse_id=getattr(audit, "warehouse_id", None),
    )

    # --- detect changed models under the REMOTE (source) identity ---
    with replicate._ambient_identity(host, token):
        client = make_mlflow_client(replicate.LOCAL_REGISTRY)
        models = resolve_models(replicate.LOCAL_REGISTRY, cfg.models.get("include", []))
        result = changefeed.detect_changes(
            client=client, models=models, watermark_fn=audit.watermark,
            spark=scan_spark, catalog=catalog, schema=schema, since_iso=since,
            signature_fn=inventory.last_signature,
            detect_metadata_drift=detect_metadata_drift,
        )

    if not result.changed:
        _logger.info("CDC: nothing to sync (%s, detector=%s)", direction.label, result.detector)
        return

    _logger.info("CDC: %d changed model(s) via %s -> %s",
                 len(result.changed), result.detector, ", ".join(result.changed))

    # --- replicate only the changed models as an incremental delta ---
    # full=False => export skips versions the dest already holds; delete_model=False
    # => import appends new versions and reconciles metadata without dropping the
    # existing model (no destructive overwrite, no re-moved artifacts).
    replicate.run_replicate(ctx, full=False, delete_model=False, models_override=list(result.changed))

    # --- advance the watermark per model, correlating the triggering audit event ---
    for model, source_max in result.changed.items():
        ev = result.events.get(model)
        audit.insert(AuditRow(
            operation="VERIFY", direction=direction.label, model_name=model,
            source_version=str(source_max), status="SUCCESS",
            triggered_by="AUDIT_EVENT" if ev else ctx.triggered_by,
            actor=cfg.service_principal,
        ))


def _last_scan_watermark(audit) -> str | None:
    """Latest correlated audit event_time we have recorded (None on first run).

    Used to bound the next ``system.access.audit`` scan. Falls back to the
    changefeed's default lookback window when unavailable.
    """
    try:
        sql = (
            f"SELECT MAX(event_time) AS wm FROM {audit.table} "
            f"WHERE triggered_by = 'AUDIT_EVENT'"
        )
        res = audit._execute(sql)
        if res is not None and hasattr(res, "collect"):
            rows = res.collect()
            if rows and rows[0][0] is not None:
                return str(rows[0][0])
    except Exception as e:  # noqa: BLE001
        _logger.debug("scan watermark lookup failed: %s", e)
    return None
