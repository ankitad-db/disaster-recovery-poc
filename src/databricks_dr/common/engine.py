"""Adapter over the third-party ``mlflow-export-import`` engine.

This is the ONLY module that imports or shells out to the engine. Modules call
``engine.export_models(...)`` etc., so we can swap the API/subprocess backend or
upgrade the pinned engine version without touching module logic.

Two backends:
  - ``api``: in-process calls to the engine's Python functions (typed returns).
  - ``cli``: subprocess calls to the console scripts (matches docs 1:1, isolated).
"""

from __future__ import annotations

import os
import subprocess
from typing import Optional

from .clients import make_mlflow_client
from .logging import get_logger

_logger = get_logger(__name__)


def engine_version() -> str:
    try:
        from mlflow_export_import import version  # type: ignore

        return getattr(version, "__version__", getattr(version, "VERSION", "unknown"))
    except Exception:  # noqa: BLE001 - version probing must never break a run
        return "unknown"


# --------------------------------------------------------------------------- #
# Bulk models (baseline)
# --------------------------------------------------------------------------- #
def export_models(
    models: str,
    output_dir: str,
    registry_uri: str,
    *,
    backend: str = "api",
    export_all_runs: bool = True,
    export_version_model: bool = True,
    export_permissions: bool = True,
    notebook_formats: str = "SOURCE",
) -> None:
    _logger.info("export_models models=%s out=%s uri=%s backend=%s", models, output_dir, registry_uri, backend)
    if backend == "cli":
        env = {**os.environ, "MLFLOW_TRACKING_URI": registry_uri}
        cmd = [
            "export-models", "--models", models, "--output-dir", output_dir,
            "--export-latest-versions", "False",
            "--export-all-runs", str(export_all_runs),
            "--export-version-model", str(export_version_model),
            "--export-permissions", str(export_permissions),
            "--notebook-formats", notebook_formats,
        ]
        subprocess.run(cmd, check=True, env=env)
        return
    from mlflow_export_import.bulk.export_models import export_models as _em

    _em(
        models=models,
        output_dir=output_dir,
        export_latest_versions=False,
        export_all_runs=export_all_runs,
        export_version_model=export_version_model,
        export_permissions=export_permissions,
        notebook_formats=_fmt_list(notebook_formats),
        mlflow_client=make_mlflow_client(registry_uri),
    )


def import_models(
    input_dir: str,
    registry_uri: str,
    *,
    backend: str = "api",
    delete_model: bool = False,
    import_permissions: bool = True,
    import_source_tags: bool = True,
) -> None:
    _logger.info("import_models in=%s uri=%s delete_model=%s backend=%s", input_dir, registry_uri, delete_model, backend)
    if backend == "cli":
        env = {**os.environ, "MLFLOW_TRACKING_URI": registry_uri}
        cmd = [
            "import-models", "--input-dir", input_dir,
            "--delete-model", str(delete_model),
            "--import-permissions", str(import_permissions),
            "--import-source-tags", str(import_source_tags),
        ]
        subprocess.run(cmd, check=True, env=env)
        return
    from mlflow_export_import.bulk.import_models import import_models as _im

    _im(
        input_dir=input_dir,
        delete_model=delete_model,
        import_permissions=import_permissions,
        import_source_tags=import_source_tags,
        mlflow_client=make_mlflow_client(registry_uri),
    )


# --------------------------------------------------------------------------- #
# Single model version (CDC)
# --------------------------------------------------------------------------- #
def export_model_version(
    model: str,
    version: str,
    output_dir: str,
    registry_uri: str,
    *,
    backend: str = "api",
    export_version_model: bool = True,
) -> None:
    _logger.info("export_model_version %s v%s out=%s", model, version, output_dir)
    if backend == "cli":
        env = {**os.environ, "MLFLOW_TRACKING_URI": registry_uri}
        cmd = [
            "export-model-version", "--model", model, "--version", str(version),
            "--output-dir", output_dir, "--export-version-model", str(export_version_model),
        ]
        subprocess.run(cmd, check=True, env=env)
        return
    from mlflow_export_import.model_version.export_model_version import export_model_version as _emv

    _emv(
        model_name=model,
        version=version,
        output_dir=output_dir,
        export_version_model=export_version_model,
        mlflow_client=make_mlflow_client(registry_uri),
    )


def import_model_version(
    model: str,
    input_dir: str,
    registry_uri: str,
    *,
    backend: str = "api",
    create_model: bool = True,
) -> Optional[str]:
    """Import a single version; returns the new (target) version if discernible."""
    _logger.info("import_model_version %s in=%s", model, input_dir)
    if backend == "cli":
        env = {**os.environ, "MLFLOW_TRACKING_URI": registry_uri}
        cmd = [
            "import-model-version", "--model", model, "--input-dir", input_dir,
            "--create-model", str(create_model),
        ]
        subprocess.run(cmd, check=True, env=env)
        return None
    from mlflow_export_import.model_version.import_model_version import import_model_version as _imv

    result = _imv(
        model_name=model,
        input_dir=input_dir,
        create_model=create_model,
        mlflow_client=make_mlflow_client(registry_uri),
    )
    return _version_of(result)


def copy_model_version(
    src_model: str,
    src_version: str,
    dst_model: str,
    src_registry_uri: str,
    dst_registry_uri: str,
    *,
    copy_stages_and_aliases: bool = True,
    copy_lineage_tags: bool = True,
) -> None:
    """Direct UC->UC version copy (fallback path; no intermediate storage)."""
    from mlflow_export_import.copy.copy_model_version import copy_model_version as _cmv

    _cmv(
        src_model_name=src_model,
        src_model_version=src_version,
        dst_model_name=dst_model,
        src_registry_uri=src_registry_uri,
        dst_registry_uri=dst_registry_uri,
        copy_stages_and_aliases=copy_stages_and_aliases,
        copy_lineage_tags=copy_lineage_tags,
    )


def _fmt_list(formats: str):
    return [f.strip() for f in formats.split(",") if f.strip()]


def _version_of(result) -> Optional[str]:
    if result is None:
        return None
    for attr in ("version",):
        if hasattr(result, attr):
            return str(getattr(result, attr))
    if isinstance(result, (tuple, list)) and result:
        last = result[-1]
        return str(getattr(last, "version", last))
    return None
