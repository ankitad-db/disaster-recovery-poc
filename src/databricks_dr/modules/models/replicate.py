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
from ...common.clients import make_mlflow_client
from ...common.inventory import ObjectInventory
from ...common.logging import get_logger
from ...common.native import changefeed
from ...common.retry import retry_call
from ...core.base import RunContext
from . import _idmap
from ._selection import resolve_models

_logger = get_logger(__name__)
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

    ``full=True`` (baseline) exports every version and imports with
    ``delete_model=True`` -- a clean restore point. ``full=False`` (CDC) is an
    incremental *delta*: the destination's existing versions are computed up front,
    export skips them (moving only new artifacts + always-full metadata), and import
    runs append-only (``delete_model=False``) so existing versions are never dropped
    and re-fetched. Deletions on the source are not propagated to the DR copy by
    design (a DR target retains history; a source deletion never destroys it).
    """
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    host, token = _remote_creds(ctx)
    delta = not full  # CDC path: import is append-only and export skips synced versions

    ts = storage.new_timestamp()
    rel = storage.rel_export_dir(cfg, direction, ts)
    base_out = storage.dbfs_path(rel, cfg)  # local (destination) staging volume / DBFS root
    exp_base = cfg.models.get("dest_experiment_base", "/Shared/dr/experiments")
    export_version_model = cfg.models.get("export_version_model", True)
    export_permissions = cfg.models.get("export_permissions", True)
    max_workers = _int(cfg.models.get("max_workers", 1))
    notebook_formats = _csv(cfg.models.get("notebook_formats", "SOURCE"))
    prompt_names = cfg.models.get("prompts", []) or []
    eval_dataset_names = cfg.models.get("evaluation_datasets", []) or []
    replicate_traces = bool(cfg.models.get("replicate_traces", False))
    notebook_dest_dir = cfg.models.get("notebook_dest_dir")
    retry_attempts = _int(cfg.models.get("retry_attempts", 3))
    retry_base_delay = _float(cfg.models.get("retry_base_delay", 2.0))
    trig = _trigger_type(ctx.triggered_by)

    # Per-model isolation: one model's failure is recorded and the run continues with
    # the rest; an aggregated error is raised at the end so the job still fails loudly
    # (and healthy models are already imported + watermark-advanced). ``failures`` maps
    # model -> reason; ``exported_ok`` gates which models the import phase attempts.
    src_versions: dict[str, list[int]] = {}
    src_bytes: dict[str, int] = {}
    src_sig: dict[str, str] = {}          # model -> source metadata signature (for inventory)
    src_aliases: dict[str, dict] = {}     # model -> {alias: version} on the source
    exported_ok: list[str] = []
    failures: dict[str, str] = {}
    models: list[str] = []

    # --- Delta pre-scan: which versions does the destination already hold? ---
    # Computed under the LOCAL identity BEFORE we assume the source identity, so the
    # export can skip already-synced versions (and re-fetch any missing older ones,
    # self-healing a prior partial sync). The importer's own existing-version guard
    # remains the correctness backstop; this pre-scan is purely to avoid re-moving
    # artifacts. Baseline (full) always re-exports everything for a clean restore.
    dest_have: dict[str, set[int]] = {}
    if delta and models_override and not ctx.dry_run:
        with _ambient_identity(None, None):
            for model in models_override:
                try:
                    dest_have[model] = set(_list_versions(LOCAL_REGISTRY, model))
                except Exception:  # noqa: BLE001 - unknown dest set => export everything
                    dest_have[model] = set()

    # --- EXPORT phase: become the remote source ---
    with _ambient_identity(host, token):
        models = models_override or resolve_models(LOCAL_REGISTRY, cfg.models.get("include", []))
        if not models:
            raise ValueError("No models in scope on source (config models.include is empty)")
        src_client = make_mlflow_client(LOCAL_REGISTRY)
        for model in models:
            out_dir = f"{base_out}/models/{model}"
            erow = AuditRow(
                operation="EXPORT", direction=direction.label, model_name=model,
                object_type="model", action="UPDATE", trigger_type=trig,
                triggered_by=ctx.triggered_by, export_dir=out_dir,
                manifest_path=f"{out_dir}/manifest.json",
                tool_version=engine.engine_version(), actor=cfg.service_principal,
            )
            audit.insert(erow)
            t0 = time.time()
            tries = [0]
            try:
                if not ctx.dry_run:
                    def _do_export(_m=model, _out=out_dir):
                        tries[0] += 1
                        return engine.export_model(
                            model=_m, output_dir=_out, registry_uri=LOCAL_REGISTRY,
                            backend=cfg.engine_backend,
                            export_version_model=export_version_model,
                            export_permissions=export_permissions,
                            notebook_formats=notebook_formats, max_workers=max_workers,
                            prompt_names=prompt_names, eval_dataset_names=eval_dataset_names,
                            replicate_traces=replicate_traces,
                            skip_versions=dest_have.get(_m) if delta else None,
                        )

                    man, _ = retry_call(_do_export, attempts=retry_attempts,
                                        base_delay=retry_base_delay, label=f"export {model}")
                    # One metadata read captures versions, alias map, and the signature
                    # we persist to the inventory (drives metadata-drift detection).
                    st = changefeed.model_state(src_client, model)
                    src_versions[model] = st.versions
                    src_sig[model] = st.signature
                    src_aliases[model] = st.alias_map
                    src_bytes[model] = man.total_bytes() if man is not None else 0
                audit.update_status(
                    erow.audit_id, "SUCCESS",
                    source_version=_max_str(src_versions.get(model)),
                    artifact_count=len(src_versions.get(model, [])),
                    bytes_moved=src_bytes.get(model),
                    retry_count=tries[0],
                    duration_sec=round(time.time() - t0, 2),
                )
                exported_ok.append(model)
            except Exception as e:  # noqa: BLE001 - isolate: record, continue with the rest
                audit.update_status(erow.audit_id, "FAILED", error_message=str(e),
                                    retry_count=tries[0], duration_sec=round(time.time() - t0, 2))
                failures[model] = f"export: {e}"
                _logger.error("export FAILED for %s (attempts=%d): %s", model, tries[0], e)

    # --- IMPORT phase: restore local identity, import into local registry ---
    with _ambient_identity(None, None):  # local runtime identity (destination)
        if not ctx.dry_run and exported_ok:
            _ensure_workspace_dir(exp_base)
        for model in exported_ok:
            in_dir = f"{base_out}/models/{model}"
            exp_name = f"{exp_base}/{model.replace('.', '_')}"
            irow = AuditRow(
                operation="IMPORT", direction=direction.label, model_name=model,
                object_type="model", action="UPDATE", trigger_type=trig,
                triggered_by=ctx.triggered_by, export_dir=in_dir,
                manifest_path=f"{in_dir}/manifest.json",
                experiment_name=exp_name, dst_experiment=exp_name,
                source_version=_max_str(src_versions.get(model)),
                bytes_moved=src_bytes.get(model),
                tool_version=engine.engine_version(), actor=cfg.service_principal,
            )
            audit.insert(irow)
            t1 = time.time()
            tries = [0]
            try:
                if not ctx.dry_run:
                    def _do_import(_m=model, _in=in_dir, _exp=exp_name):
                        tries[0] += 1
                        return engine.import_model(
                            model=_m, input_dir=_in, experiment_name=_exp,
                            registry_uri=LOCAL_REGISTRY, backend=cfg.engine_backend,
                            delete_model=delete_model,
                            import_permissions=export_permissions,
                            # UC forbids '.' in model-version tag keys; the engine guards
                            # this. Provenance also lives in the audit table.
                            import_source_tags=False,
                            max_workers=max_workers, notebook_dest_dir=notebook_dest_dir,
                        )

                    result, _ = retry_call(_do_import, attempts=retry_attempts,
                                           base_delay=retry_base_delay, label=f"import {model}")
                    _verify_import(model, src_versions.get(model, []))
                    dst = _list_versions(LOCAL_REGISTRY, model)
                    _idmap.persist(ctx, result, irow.audit_id)
                    _upsert_inventory(ctx, model, src_versions, src_aliases, src_sig, irow.audit_id)
                else:
                    dst = []
                audit.update_status(
                    irow.audit_id, "SUCCESS", target_version=_max_str(dst),
                    artifact_count=len(dst), retry_count=tries[0],
                    duration_sec=round(time.time() - t1, 2),
                )
            except Exception as e:  # noqa: BLE001 - isolate: record, continue with the rest
                audit.update_status(irow.audit_id, "FAILED", error_message=str(e),
                                    retry_count=tries[0], duration_sec=round(time.time() - t1, 2))
                failures[model] = f"import: {e}"
                _logger.error("import FAILED for %s (attempts=%d): %s", model, tries[0], e)

    # --- Aggregate: fail loudly if any model failed, after attempting them all ---
    if failures:
        summary = "; ".join(f"{m} ({why})" for m, why in failures.items())
        raise RuntimeError(
            f"replication finished with {len(failures)}/{len(models)} model(s) failed: {summary}"
        )
    _logger.info("replication OK: %d model(s) [%s]", len(exported_ok), ", ".join(exported_ok))


def _upsert_inventory(ctx: RunContext, model: str, src_versions: dict,
                      src_aliases: dict, src_sig: dict, audit_id: str) -> None:
    """Record the last successfully-synced snapshot of ``model`` in ``dr_object_inventory``.

    Writes the source version, alias map, and metadata signature that CDC diffs
    against on the next pass to catch metadata-only drift. Non-fatal: an inventory
    write must never fail an otherwise-good replication -- the audit table remains
    the source of truth, and a missing inventory row simply forces the next CDC pass
    to fall back to the version diff for this model.
    """
    signature = src_sig.get(model)
    if not signature:
        return  # nothing meaningful to record (e.g. metadata read failed on source)
    audit = ctx.audit
    try:
        inv = ObjectInventory(
            ctx.cfg.inventory_table, spark=getattr(audit, "spark", None),
            workspace_client=getattr(audit, "wc", None),
            warehouse_id=getattr(audit, "warehouse_id", None),
        )
        inv.upsert(
            object_key=model, object_type="model",
            source_region=ctx.direction.source.region,
            last_source_version=_max_str(src_versions.get(model)),
            alias_map=src_aliases.get(model, {}),
            integrity_hash=signature,
            last_audit_id=audit_id, status="IN_SYNC",
        )
    except Exception as e:  # noqa: BLE001 - inventory is auxiliary, never fatal
        import logging

        logging.getLogger(__name__).warning("inventory upsert skipped for %s: %s", model, e)


def _list_versions(registry_uri: str, model: str) -> list[int]:
    from ...common.clients import make_mlflow_client

    client = make_mlflow_client(registry_uri)
    return sorted(int(v.version) for v in client.search_model_versions(f"name='{model}'"))


def _max_str(versions: list[int] | None) -> str | None:
    return str(max(versions)) if versions else None


def _int(v, default: int = 1) -> int:
    try:
        return max(1, int(v))
    except (TypeError, ValueError):
        return default


def _float(v, default: float = 2.0) -> float:
    try:
        return max(0.0, float(v))
    except (TypeError, ValueError):
        return default


def _csv(v) -> str:
    """Normalize a notebook_formats config value (list or str) to a CSV string."""
    if isinstance(v, (list, tuple)):
        return ",".join(str(x) for x in v)
    return str(v or "SOURCE")


def _trigger_type(triggered_by: str) -> str:
    """Map the legacy triggered_by enum to the detailed trigger_type enum."""
    return {
        "MANUAL": "MANUAL",
        "SCHEDULE": "SCHEDULE",
        "AUDIT_EVENT": "AUDIT_SCAN",
        "MODEL_TRIGGER": "MODEL_TRIGGER",
    }.get(triggered_by, "MANUAL")


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
