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

    Per-model single export/import (not the bulk path): the single importer
    materializes MLflow 3.x logged models before registering each version, and
    surfaces real errors. Every import is verified against the source version set,
    so a partial/silent import is recorded as FAILED rather than SUCCESS.

    ``models_override`` lets CDC replicate only the models that changed; when None
    the full in-scope set is resolved on the source.
    """
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    host, token = _remote_creds(ctx)

    ts = storage.new_timestamp()
    rel = storage.rel_export_dir(cfg, direction, ts)
    base_out = storage.dbfs_path(rel)  # local (destination) DBFS bucket
    exp_base = cfg.models.get("dest_experiment_base", "/Shared/dr/experiments")
    export_version_model = cfg.models.get("export_version_model", True)
    export_permissions = cfg.models.get("export_permissions", True)

    # --- EXPORT phase: become the remote source ---
    src_versions: dict[str, list[int]] = {}
    with _ambient_identity(host, token):
        models = models_override or resolve_models(LOCAL_REGISTRY, cfg.models.get("include", []))
        if not models:
            raise ValueError("No models in scope on source (config models.include is empty)")
        for model in models:
            out_dir = f"{base_out}/models/{model}"
            erow = AuditRow(
                operation="EXPORT", direction=direction.label, model_name=model,
                triggered_by=ctx.triggered_by, export_dir=out_dir,
                tool_version=engine.engine_version(), actor=cfg.service_principal,
            )
            audit.insert(erow)
            t0 = time.time()
            try:
                if not ctx.dry_run:
                    engine.export_model(
                        model=model, output_dir=out_dir, registry_uri=LOCAL_REGISTRY,
                        backend=cfg.engine_backend,
                        export_version_model=export_version_model,
                        export_permissions=export_permissions,
                    )
                    src_versions[model] = _list_versions(LOCAL_REGISTRY, model)
                audit.update_status(
                    erow.audit_id, "SUCCESS",
                    source_version=_max_str(src_versions.get(model)),
                    artifact_count=len(src_versions.get(model, [])),
                    duration_sec=round(time.time() - t0, 2),
                )
            except Exception as e:  # noqa: BLE001
                audit.update_status(erow.audit_id, "FAILED", error_message=str(e))
                raise

    # --- IMPORT phase: restore local identity, import into local registry ---
    with _ambient_identity(None, None):  # local runtime identity (destination)
        if not ctx.dry_run:
            _ensure_workspace_dir(exp_base)
        for model in models:
            in_dir = f"{base_out}/models/{model}"
            exp_name = f"{exp_base}/{model.replace('.', '_')}"
            irow = AuditRow(
                operation="IMPORT", direction=direction.label, model_name=model,
                triggered_by=ctx.triggered_by, export_dir=in_dir,
                experiment_name=exp_name, source_version=_max_str(src_versions.get(model)),
                tool_version=engine.engine_version(), actor=cfg.service_principal,
            )
            audit.insert(irow)
            t1 = time.time()
            try:
                if not ctx.dry_run:
                    engine.import_model(
                        model=model, input_dir=in_dir, experiment_name=exp_name,
                        registry_uri=LOCAL_REGISTRY, backend=cfg.engine_backend,
                        delete_model=delete_model,
                        import_permissions=export_permissions,
                        import_source_tags=True,
                    )
                    _verify_import(model, src_versions.get(model, []))
                    dst = _list_versions(LOCAL_REGISTRY, model)
                else:
                    dst = []
                audit.update_status(
                    irow.audit_id, "SUCCESS", target_version=_max_str(dst),
                    artifact_count=len(dst), duration_sec=round(time.time() - t1, 2),
                )
            except Exception as e:  # noqa: BLE001
                audit.update_status(irow.audit_id, "FAILED", error_message=str(e))
                raise


def _list_versions(registry_uri: str, model: str) -> list[int]:
    from ...common.clients import make_mlflow_client

    client = make_mlflow_client(registry_uri)
    return sorted(int(v.version) for v in client.search_model_versions(f"name='{model}'"))


def _max_str(versions: list[int] | None) -> str | None:
    return str(max(versions)) if versions else None


def _verify_import(model: str, expected: list[int]) -> None:
    """Fail loudly if the destination registry is missing expected versions.

    The engine's importer can swallow per-version errors; this guarantees an
    incomplete import is never recorded as SUCCESS.
    """
    dst = set(_list_versions(LOCAL_REGISTRY, model))
    missing = sorted(set(expected) - dst)
    if missing:
        raise RuntimeError(
            f"Import incomplete for '{model}': missing versions {missing} "
            f"(dest has {sorted(dst)}, expected {sorted(expected)})"
        )


def _ensure_workspace_dir(path: str) -> None:
    """Create a workspace directory (and parents) for the import experiment base."""
    try:
        from databricks.sdk import WorkspaceClient

        WorkspaceClient().workspace.mkdirs(path)
    except Exception as e:  # noqa: BLE001 - best effort; import will surface real errors
        import logging

        logging.getLogger(__name__).warning("Could not mkdirs '%s': %s", path, e)
