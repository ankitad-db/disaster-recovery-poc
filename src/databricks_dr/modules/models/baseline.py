"""Phase 2/3: one-time full baseline export -> bridge -> import.

Captures full history (``--export-all-runs``) for all in-scope models, bridges
the export dir across regions, and imports into the destination registry.
Direction-agnostic: uses the resolved RunContext.direction.
"""

from __future__ import annotations

import time

from ...common import engine, storage
from ...common.audit import AuditRow
from ...core.base import RunContext
from ._selection import resolve_models


def run_baseline(ctx: RunContext) -> None:
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    backend = cfg.engine_backend
    mcfg = cfg.models
    models = resolve_models(direction.source.registry_uri, mcfg.get("include", []))
    if not models:
        raise ValueError("No models in scope (config models.include is empty)")

    ts = storage.new_timestamp()
    rel = storage.rel_export_dir(cfg, direction, ts)
    out_dir = storage.dbfs_path(rel)
    models_csv = ",".join(models)
    tool_version = engine.engine_version()

    row = AuditRow(
        operation="EXPORT", direction=direction.label, model_name=models_csv,
        triggered_by=ctx.triggered_by, export_dir=out_dir, tool_version=tool_version,
        actor=cfg.service_principal,
    )
    audit.insert(row)
    t0 = time.time()
    try:
        if not ctx.dry_run:
            engine.export_models(
                models=models_csv, output_dir=out_dir,
                registry_uri=direction.source.registry_uri, backend=backend,
                export_all_runs=mcfg.get("export_all_runs_on_baseline", True),
                export_version_model=mcfg.get("export_version_model", True),
                export_permissions=mcfg.get("export_permissions", True),
            )
            storage.write_latest_pointer(cfg, direction, rel)
        audit.update_status(row.audit_id, "SUCCESS", duration_sec=round(time.time() - t0, 2),
                            manifest_path=f"{out_dir}/manifest.json")
    except Exception as e:  # noqa: BLE001
        audit.update_status(row.audit_id, "FAILED", error_message=str(e))
        raise

    # Bridge across regions
    if not ctx.dry_run:
        storage.bridge(cfg, direction, rel)

    # Import on destination (full overwrite for a clean baseline)
    irow = AuditRow(
        operation="IMPORT", direction=direction.label, model_name=models_csv,
        triggered_by=ctx.triggered_by, export_dir=storage.dbfs_path(rel),
        tool_version=tool_version, actor=cfg.service_principal,
    )
    audit.insert(irow)
    t1 = time.time()
    try:
        if not ctx.dry_run:
            engine.import_models(
                input_dir=storage.dbfs_path(rel),
                registry_uri=direction.dest.registry_uri, backend=backend,
                delete_model=True,  # baseline = clean, deduped restore point
                import_permissions=mcfg.get("export_permissions", True),
                import_source_tags=True,
            )
        audit.update_status(irow.audit_id, "SUCCESS", duration_sec=round(time.time() - t1, 2))
    except Exception as e:  # noqa: BLE001
        audit.update_status(irow.audit_id, "FAILED", error_message=str(e))
        raise
