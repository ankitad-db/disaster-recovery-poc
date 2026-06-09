"""Cross-workspace pull replication (recommended, no laptop, no cross-region S3).

The DR job runs in the LOCAL (destination) workspace and authenticates to the
REMOTE (source) workspace using a secret-scoped host + SPN token. It exports the
source registry straight into the local DBFS bucket, then imports into the local
registry -- so the data never has to be bridged across regions out-of-band.

  run in SECONDARY (us-east-1):
    secret scope -> remote profile for PRIMARY (us-west-2)
    export(remote primary registry) -> /dbfs (local east)
    import(/dbfs) -> east registry (local)

Failback is symmetric: run in the primary, source = secondary, via that side's
secret scope.
"""

from __future__ import annotations

import time

from ...common import engine, storage
from ...common.audit import AuditRow
from ...common.clients import configure_remote_profile, local_or_profile_uri
from ...core.base import RunContext
from ._selection import resolve_models


def _remote_source_uri(ctx: RunContext) -> str:
    """Build the remote source registry URI from the local secret scope."""
    cfg, direction = ctx.cfg, ctx.direction
    src_key = direction.source.key
    sec = cfg.secrets.get(src_key)
    if not sec:
        raise ValueError(f"No secrets config for source region '{src_key}'")
    if ctx.dbutils is None:
        raise ValueError("RunContext.dbutils is required to read the remote secret scope")
    host = ctx.dbutils.secrets.get(scope=sec["scope"], key=sec["host_key"])
    token = ctx.dbutils.secrets.get(scope=sec["scope"], key=sec["token_key"])
    return configure_remote_profile(f"dr_remote_{src_key}", host, token)


def run_replicate(ctx: RunContext, *, full: bool = True, delete_model: bool = True) -> None:
    """Pull in-scope models from the remote source and import into the local dest."""
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    src_uri = _remote_source_uri(ctx)
    dst_uri = local_or_profile_uri(direction.dest.registry_uri)

    models = resolve_models(src_uri, cfg.models.get("include", []))
    if not models:
        raise ValueError("No models in scope (config models.include is empty)")
    models_csv = ",".join(models)

    ts = storage.new_timestamp()
    rel = storage.rel_export_dir(cfg, direction, ts)
    out_dir = storage.dbfs_path(rel)  # local (destination) DBFS bucket

    # --- export from REMOTE source -> LOCAL dbfs ---
    erow = AuditRow(
        operation="EXPORT", direction=direction.label, model_name=models_csv,
        triggered_by=ctx.triggered_by, export_dir=out_dir, tool_version=engine.engine_version(),
        actor=cfg.service_principal,
    )
    audit.insert(erow)
    t0 = time.time()
    try:
        if not ctx.dry_run:
            engine.export_models(
                models=models_csv, output_dir=out_dir, registry_uri=src_uri,
                backend=cfg.engine_backend,
                export_all_runs=full and cfg.models.get("export_all_runs_on_baseline", True),
                export_version_model=cfg.models.get("export_version_model", True),
                export_permissions=cfg.models.get("export_permissions", True),
            )
        audit.update_status(erow.audit_id, "SUCCESS", duration_sec=round(time.time() - t0, 2),
                            manifest_path=f"{out_dir}/manifest.json")
    except Exception as e:  # noqa: BLE001
        audit.update_status(erow.audit_id, "FAILED", error_message=str(e))
        raise

    # --- import LOCAL dbfs -> LOCAL dest registry ---
    irow = AuditRow(
        operation="IMPORT", direction=direction.label, model_name=models_csv,
        triggered_by=ctx.triggered_by, export_dir=out_dir, tool_version=engine.engine_version(),
        actor=cfg.service_principal,
    )
    audit.insert(irow)
    t1 = time.time()
    try:
        if not ctx.dry_run:
            engine.import_models(
                input_dir=out_dir, registry_uri=dst_uri, backend=cfg.engine_backend,
                delete_model=delete_model,
                import_permissions=cfg.models.get("export_permissions", True),
                import_source_tags=True,
            )
        audit.update_status(irow.audit_id, "SUCCESS", duration_sec=round(time.time() - t1, 2))
    except Exception as e:  # noqa: BLE001
        audit.update_status(irow.audit_id, "FAILED", error_message=str(e))
        raise
