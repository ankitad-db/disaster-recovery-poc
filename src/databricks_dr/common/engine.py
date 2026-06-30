"""Replication engine facade (native MLflow-API backend).

This is the ONLY module the DR modules call for export/import. It is a thin facade
over the first-party native engine (:mod:`databricks_dr.common.native`), which is
built entirely on the public MLflow client + databricks-sdk. There is no
third-party replication tool: the engine stays in lock-step with whatever MLflow
version is installed on the runtime.

The ``backend`` keyword is retained on every function purely for call-site
compatibility (modules pass ``backend=cfg.engine_backend``); all values resolve to
the native engine.
"""

from __future__ import annotations

from typing import Optional

from .logging import get_logger
from .native import export as _nx
from .native import import_ as _ni

_logger = get_logger(__name__)


def engine_version() -> str:
    """Identifier recorded in the audit table's ``tool_version`` column."""
    try:
        import mlflow

        return f"native-{mlflow.__version__}"
    except Exception:  # noqa: BLE001 - version probing must never break a run
        return "native-unknown"


# --------------------------------------------------------------------------- #
# Bulk models (baseline)
# --------------------------------------------------------------------------- #
def export_models(
    models: str,
    output_dir: str,
    registry_uri: str,
    *,
    backend: str = "native",
    export_all_runs: bool = True,
    export_version_model: bool = True,
    export_permissions: bool = True,
    notebook_formats: str = "SOURCE",
    max_workers: int = 1,
) -> None:
    _logger.info("export_models models=%s out=%s uri=%s", models, output_dir, registry_uri)
    _nx.export_models(
        models, output_dir, registry_uri,
        export_all_runs=export_all_runs,
        export_version_model=export_version_model,
        export_permissions=export_permissions,
        notebook_formats=notebook_formats,
        max_workers=max_workers,
    )


def import_models(
    input_dir: str,
    registry_uri: str,
    *,
    backend: str = "native",
    delete_model: bool = False,
    import_permissions: bool = True,
    import_source_tags: bool = True,
    experiment_renames: Optional[dict] = None,
    max_workers: int = 1,
):
    """Bulk import. Returns a list of ``ImportResult`` (one per model) for ID mapping."""
    _logger.info("import_models in=%s uri=%s delete_model=%s", input_dir, registry_uri, delete_model)
    return _ni.import_models(
        input_dir, registry_uri,
        delete_model=delete_model, import_permissions=import_permissions,
        import_source_tags=import_source_tags, experiment_renames=experiment_renames,
        max_workers=max_workers,
    )


# --------------------------------------------------------------------------- #
# Single registered model (recommended replicate/CDC path)
# --------------------------------------------------------------------------- #
def export_model(
    model: str,
    output_dir: str,
    registry_uri: str,
    *,
    backend: str = "native",
    export_version_model: bool = True,
    export_permissions: bool = True,
    notebook_formats: str = "SOURCE",
    max_workers: int = 1,
    prompt_names: Optional[list] = None,
    eval_dataset_names: Optional[list] = None,
    replicate_traces: bool = False,
):
    """Export one registered model (versions, runs, logged models, GenAI).

    Returns the engine ``Manifest`` so callers can record bytes/version counts.
    """
    _logger.info("export_model model=%s out=%s uri=%s", model, output_dir, registry_uri)
    return _nx.export_model(
        model, output_dir, registry_uri,
        export_version_model=export_version_model,
        export_permissions=export_permissions, notebook_formats=notebook_formats,
        max_workers=max_workers, prompt_names=prompt_names,
        eval_dataset_names=eval_dataset_names, replicate_traces=replicate_traces,
    )


def import_model(
    model: str,
    input_dir: str,
    experiment_name: str,
    registry_uri: str,
    *,
    backend: str = "native",
    delete_model: bool = True,
    import_permissions: bool = True,
    import_source_tags: bool = False,
    max_workers: int = 1,
    notebook_dest_dir: Optional[str] = None,
):
    """Import one registered model (versions + runs + logged models + GenAI) into dest.

    Returns an ``ImportResult`` with source->destination experiment/run/version maps.
    """
    _logger.info("import_model model=%s in=%s exp=%s delete=%s", model, input_dir, experiment_name, delete_model)
    return _ni.import_model(
        model, input_dir, experiment_name, registry_uri,
        delete_model=delete_model, import_permissions=import_permissions,
        import_source_tags=import_source_tags, max_workers=max_workers,
        notebook_dest_dir=notebook_dest_dir,
    )


# --------------------------------------------------------------------------- #
# Single model version (low-level)
# --------------------------------------------------------------------------- #
def export_model_version(
    model: str,
    version: str,
    output_dir: str,
    registry_uri: str,
    *,
    backend: str = "native",
    export_version_model: bool = True,
) -> None:
    _logger.info("export_model_version %s v%s out=%s", model, version, output_dir)
    _nx.export_model_version(
        model, version, output_dir, registry_uri,
        export_version_model=export_version_model,
    )


def import_model_version(
    model: str,
    input_dir: str,
    registry_uri: str,
    *,
    backend: str = "native",
    create_model: bool = True,
) -> Optional[str]:
    """Import a single version; returns the new (target) version if discernible."""
    _logger.info("import_model_version %s in=%s", model, input_dir)
    return _ni.import_model_version(model, input_dir, registry_uri, create_model=create_model)


def copy_model_version(
    src_model: str,
    src_version: str,
    dst_model: str,
    src_registry_uri: str,
    dst_registry_uri: str,
    *,
    backend: str = "native",
    copy_stages_and_aliases: bool = True,
    copy_lineage_tags: bool = True,
) -> None:
    """Direct UC->UC version copy (fallback path; no intermediate storage)."""
    _ni.copy_model_version(
        src_model, src_version, dst_model, src_registry_uri, dst_registry_uri,
        copy_stages_and_aliases=copy_stages_and_aliases, copy_lineage_tags=copy_lineage_tags,
    )
