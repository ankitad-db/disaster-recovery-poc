"""Cross-workspace pull replication (recommended, no laptop, no cross-region S3).

The DR job runs in the LOCAL (destination) workspace. It replicates in two phases,
switching the process's *ambient* Databricks identity between them:

  EXPORT phase  -> become the REMOTE source (env DATABRICKS_HOST/TOKEN from a
                   secret scope). All MLflow + UC calls -- including the
                   ``generate-temporary-credentials`` used to download model
                   artifacts -- then resolve against the source workspace.
  IMPORT phase  -> restore the LOCAL runtime identity and import into the local
                   registry.

Using the ambient identity (plain ``databricks-uc``) rather than a per-client
profile is essential: the UC artifact repository resolves credentials from the
ambient Databricks auth, not from the MlflowClient's registry profile. Exporting
into the local DBFS bucket means there is no cross-region bridge to manage.
"""

from __future__ import annotations

import contextlib
import os
import time

from ...common import engine, storage
from ...common.audit import AuditRow
from ...core.base import RunContext
from ._selection import resolve_models

LOCAL_REGISTRY = "databricks-uc"


@contextlib.contextmanager
def _ambient_identity(host: str | None = None, token: str | None = None):
    """Temporarily set (or clear) the ambient Databricks identity for this process."""
    keys = ("DATABRICKS_HOST", "DATABRICKS_TOKEN")
    saved = {k: os.environ.get(k) for k in keys}
    try:
        if host and token:
            os.environ["DATABRICKS_HOST"] = host
            os.environ["DATABRICKS_TOKEN"] = token
        else:
            for k in keys:
                os.environ.pop(k, None)
        _reset_mlflow_uris()
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        _reset_mlflow_uris()


def _reset_mlflow_uris() -> None:
    import mlflow

    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")


def _remote_creds(ctx: RunContext) -> tuple[str, str]:
    cfg, direction = ctx.cfg, ctx.direction
    src_key = direction.source.key
    sec = cfg.secrets.get(src_key)
    if not sec:
        raise ValueError(f"No secrets config for source region '{src_key}'")
    if ctx.dbutils is None:
        raise ValueError("RunContext.dbutils is required to read the remote secret scope")
    host = ctx.dbutils.secrets.get(scope=sec["scope"], key=sec["host_key"])
    token = ctx.dbutils.secrets.get(scope=sec["scope"], key=sec["token_key"])
    return host, token


def run_replicate(
    ctx: RunContext,
    *,
    full: bool = True,
    delete_model: bool = True,
    models_override: list | None = None,
) -> None:
    """Pull models from the remote source and import into the local dest.

    ``models_override`` lets CDC replicate only the models that changed; when None
    the full in-scope set is resolved on the source.
    """
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    host, token = _remote_creds(ctx)

    ts = storage.new_timestamp()
    rel = storage.rel_export_dir(cfg, direction, ts)
    out_dir = storage.dbfs_path(rel)  # local (destination) DBFS bucket

    # --- EXPORT phase: become the remote source ---
    erow = AuditRow(
        operation="EXPORT", direction=direction.label, model_name="(scope)",
        triggered_by=ctx.triggered_by, export_dir=out_dir, tool_version=engine.engine_version(),
        actor=cfg.service_principal,
    )
    audit.insert(erow)
    t0 = time.time()
    try:
        with _ambient_identity(host, token):
            models = models_override or resolve_models(LOCAL_REGISTRY, cfg.models.get("include", []))
            if not models:
                raise ValueError("No models in scope on source (config models.include is empty)")
            models_csv = ",".join(models)
            if not ctx.dry_run:
                engine.export_models(
                    models=models_csv, output_dir=out_dir, registry_uri=LOCAL_REGISTRY,
                    backend=cfg.engine_backend,
                    export_all_runs=full and cfg.models.get("export_all_runs_on_baseline", True),
                    export_version_model=cfg.models.get("export_version_model", True),
                    export_permissions=cfg.models.get("export_permissions", True),
                )
        audit.update_status(erow.audit_id, "SUCCESS", model_name=models_csv,
                            duration_sec=round(time.time() - t0, 2),
                            manifest_path=f"{out_dir}/manifest.json")
    except Exception as e:  # noqa: BLE001
        audit.update_status(erow.audit_id, "FAILED", error_message=str(e))
        raise

    # --- IMPORT phase: restore local identity, import into local registry ---
    irow = AuditRow(
        operation="IMPORT", direction=direction.label, model_name=models_csv,
        triggered_by=ctx.triggered_by, export_dir=out_dir, tool_version=engine.engine_version(),
        actor=cfg.service_principal,
    )
    audit.insert(irow)
    t1 = time.time()
    try:
        with _ambient_identity(None, None):  # local runtime identity (destination)
            if not ctx.dry_run:
                engine.import_models(
                    input_dir=out_dir, registry_uri=LOCAL_REGISTRY, backend=cfg.engine_backend,
                    delete_model=delete_model,
                    import_permissions=cfg.models.get("export_permissions", True),
                    import_source_tags=True,
                    experiment_renames=_experiment_renames(cfg),
                )
        audit.update_status(irow.audit_id, "SUCCESS", duration_sec=round(time.time() - t1, 2))
    except Exception as e:  # noqa: BLE001
        audit.update_status(irow.audit_id, "FAILED", error_message=str(e))
        raise


def _experiment_renames(cfg) -> dict | None:
    """Optional remap of source experiment paths to a DR-safe base on import.

    Set ``models.dest_experiment_base`` (e.g. ``/Shared/dr/experiments``) in config
    to avoid collisions when a source experiment's path also exists as a notebook
    in the destination (common when the same Git folder is cloned in both).
    """
    base = cfg.models.get("dest_experiment_base")
    if not base:
        return None
    # rename_utils supports a {old: new} dict; we map by basename at import time
    # via a callable-like dict is not supported, so callers that need exact control
    # should pre-list experiments. For the common single-experiment POC, map below.
    names = cfg.models.get("source_experiments", [])
    if not names:
        return None
    return {n: f"{base}/{n.rstrip('/').split('/')[-1]}" for n in names}
