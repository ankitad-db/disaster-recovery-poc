"""Phase 2/3: one-time full baseline, split into export and import halves.

Split design (production-faithful, and required when each region runs from its own
Git folder): the **export** runs in the *source* region against its local registry
and bucket; the **import** runs in the *dest* region against its local registry and
bucket. The S3 bridge between buckets is handled out-of-band (S3 CRR, or a one-off
``aws s3 sync`` via ``storage.bridge_prefix`` from a host with both-account creds).

  run_export  -> run in PRIMARY  (writes source bucket + _latest.txt)
  [bridge]    -> CRR / aws s3 sync (source bucket -> dest bucket)
  run_import  -> run in SECONDARY (reads dest bucket via _latest.txt)

``run_baseline`` composes all three in one process for laptop/CLI use, where both
registries and both buckets are reachable.
"""

from __future__ import annotations

import time

from ...common import engine, storage
from ...common.audit import AuditRow, IdMappingLog, rows_from_import_result
from ...common.clients import local_or_profile_uri
from ...core.base import RunContext
from ._selection import resolve_models


def run_export(ctx: RunContext, *, full: bool = True) -> str:
    """Export in-scope models from the SOURCE registry to the source bucket.

    ``full=True`` captures all run history (baseline). Returns the relative
    export path (also recorded in ``_latest.txt``).
    """
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    mcfg = cfg.models
    src_uri = local_or_profile_uri(direction.source.registry_uri)
    models = resolve_models(src_uri, mcfg.get("include", []))
    if not models:
        raise ValueError("No models in scope (config models.include is empty)")

    ts = storage.new_timestamp()
    rel = storage.rel_export_dir(cfg, direction, ts)
    out_dir = storage.dbfs_path(rel, cfg)
    models_csv = ",".join(models)

    row = AuditRow(
        operation="EXPORT", direction=direction.label, model_name=models_csv,
        triggered_by=ctx.triggered_by, export_dir=out_dir, tool_version=engine.engine_version(),
        actor=cfg.service_principal,
    )
    audit.insert(row)
    t0 = time.time()
    try:
        if not ctx.dry_run:
            engine.export_models(
                models=models_csv, output_dir=out_dir,
                registry_uri=src_uri, backend=cfg.engine_backend,
                export_all_runs=full and mcfg.get("export_all_runs_on_baseline", True),
                export_version_model=mcfg.get("export_version_model", True),
                export_permissions=mcfg.get("export_permissions", True),
            )
            storage.write_latest_pointer(cfg, direction, rel)
        audit.update_status(row.audit_id, "SUCCESS", duration_sec=round(time.time() - t0, 2),
                            manifest_path=f"{out_dir}/manifest.json")
    except Exception as e:  # noqa: BLE001
        audit.update_status(row.audit_id, "FAILED", error_message=str(e))
        raise
    return rel


def run_import(ctx: RunContext, rel: str | None = None, *, delete_model: bool = True) -> None:
    """Import models into the DEST registry from the dest bucket.

    ``rel`` defaults to the path in ``_latest.txt`` (resolved on the dest bucket,
    so the bridge must have completed). ``delete_model=True`` gives a clean,
    deduped baseline restore point.
    """
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    if rel is None and not ctx.dry_run:
        rel = storage.read_latest_pointer(cfg, direction)
    in_dir = storage.dbfs_path(rel, cfg) if rel else "(dry-run)"

    row = AuditRow(
        operation="IMPORT", direction=direction.label, model_name=rel or "(latest)",
        triggered_by=ctx.triggered_by, export_dir=in_dir, tool_version=engine.engine_version(),
        actor=cfg.service_principal,
    )
    audit.insert(row)
    t0 = time.time()
    try:
        if not ctx.dry_run:
            results = engine.import_models(
                input_dir=in_dir,
                registry_uri=local_or_profile_uri(direction.dest.registry_uri), backend=cfg.engine_backend,
                delete_model=delete_model,
                import_permissions=cfg.models.get("export_permissions", True),
                import_source_tags=True,
            )
            _persist_id_mappings(ctx, results or [], row.audit_id)
        audit.update_status(row.audit_id, "SUCCESS", duration_sec=round(time.time() - t0, 2))
    except Exception as e:  # noqa: BLE001
        audit.update_status(row.audit_id, "FAILED", error_message=str(e))
        raise


def _persist_id_mappings(ctx: RunContext, results: list, audit_id: str) -> None:
    """Persist source<->dest experiment/run/version IDs for each imported model.

    Non-fatal: the audit table stays the source of truth if this write fails.
    """
    if not results:
        return
    audit = ctx.audit
    try:
        mlog = IdMappingLog(
            ctx.cfg.mapping_table, spark=getattr(audit, "spark", None),
            workspace_client=getattr(audit, "wc", None),
            warehouse_id=getattr(audit, "warehouse_id", None),
        )
        rows = []
        for result in results:
            rows.extend(rows_from_import_result(
                result, direction_label=ctx.direction.label,
                source_workspace=ctx.direction.source.workspace,
                target_workspace=ctx.direction.dest.workspace, audit_id=audit_id,
            ))
        mlog.insert_many(rows)
    except Exception as e:  # noqa: BLE001 - mapping is auxiliary, never fatal
        import logging

        logging.getLogger(__name__).warning("id-mapping persist skipped: %s", e)


def run_baseline(ctx: RunContext) -> None:
    """Combined export -> bridge -> import (laptop/CLI; both regions reachable)."""
    rel = run_export(ctx, full=True)
    if not ctx.dry_run:
        storage.bridge(ctx.cfg, ctx.direction, rel)
    run_import(ctx, rel)
