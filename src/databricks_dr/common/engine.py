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


def _ensure_json_patch() -> None:
    """Patch the engine's JSON writer to tolerate non-serializable MLflow objects.

    UC + MLflow 3 model versions carry ``model_metrics`` whose values are
    ``mlflow.entities.Metric`` objects; the engine's ``io_utils.write_file`` calls
    ``json.dumps`` without a ``default=`` handler and crashes with
    "Object of type Metric is not JSON serializable". We add a tolerant encoder.
    """
    from mlflow_export_import.common import filesystem as _fs
    from mlflow_export_import.common import io_utils

    if getattr(io_utils, "_dr_json_patched", False):
        return

    import json as _json

    import yaml as _yaml

    def _default(o):
        for attr in ("to_dictionary", "to_dict"):
            if hasattr(o, attr):
                try:
                    return getattr(o, attr)()
                except Exception:  # noqa: BLE001
                    pass
        if hasattr(o, "__dict__"):
            return {k: v for k, v in vars(o).items() if not k.startswith("_")}
        return str(o)

    def write_file(path, content, file_type=None):
        path = _fs.mk_local_path(path)
        if path.endswith(".json"):
            with open(path, "w", encoding="utf-8") as f:
                f.write(_json.dumps(content, indent=2, default=_default) + "\n")
        elif any(path.endswith(x) for x in [".yaml", ".yml"]) or file_type in ["yaml", "yml"]:
            with open(path, "w", encoding="utf-8") as f:
                _yaml.dump(content, f)
        else:
            with open(path, "wb") as f:
                f.write(content)

    io_utils.write_file = write_file
    io_utils._dr_json_patched = True
    _logger.info("Patched mlflow_export_import io_utils.write_file (tolerant JSON encoder)")


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
    _ensure_json_patch()
    from mlflow_export_import.bulk.export_models import export_models as _em

    _em(
        model_names=models,
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
    experiment_renames: Optional[dict] = None,
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
    _ensure_json_patch()
    from mlflow_export_import.bulk.import_models import import_models as _im

    _im(
        input_dir=input_dir,
        delete_model=delete_model,
        import_permissions=import_permissions,
        import_source_tags=import_source_tags,
        experiment_renames=experiment_renames,
        mlflow_client=make_mlflow_client(registry_uri),
    )


# --------------------------------------------------------------------------- #
# Single registered model (recommended replicate/CDC path)
# --------------------------------------------------------------------------- #
def export_model(
    model: str,
    output_dir: str,
    registry_uri: str,
    *,
    backend: str = "api",
    export_version_model: bool = True,
    export_permissions: bool = True,
    notebook_formats: str = "SOURCE",
) -> None:
    """Export one registered model with all versions, runs and (3.x) logged models.

    Unlike the bulk exporter this co-locates each version's run under the model's
    own directory, which the single importer needs to materialize MLflow 3 logged
    models before registering the version.
    """
    _logger.info("export_model model=%s out=%s uri=%s", model, output_dir, registry_uri)
    if backend == "cli":
        env = {**os.environ, "MLFLOW_TRACKING_URI": registry_uri}
        cmd = [
            "export-model", "--model", model, "--output-dir", output_dir,
            "--export-latest-versions", "False",
            "--export-version-model", str(export_version_model),
            "--export-permissions", str(export_permissions),
            "--notebook-formats", notebook_formats,
        ]
        subprocess.run(cmd, check=True, env=env)
        return
    _ensure_json_patch()
    from mlflow_export_import.model.export_model import export_model as _em

    ok, _ = _em(
        model_name=model,
        output_dir=output_dir,
        export_latest_versions=False,
        export_version_model=export_version_model,
        export_permissions=export_permissions,
        notebook_formats=_fmt_list(notebook_formats),
        mlflow_client=make_mlflow_client(registry_uri),
    )
    if not ok:
        raise RuntimeError(f"export_model failed for '{model}' (see engine logs)")


def import_model(
    model: str,
    input_dir: str,
    experiment_name: str,
    registry_uri: str,
    *,
    backend: str = "api",
    delete_model: bool = True,
    import_permissions: bool = True,
    import_source_tags: bool = False,
) -> None:
    """Import one registered model (versions + runs + logged models) into the dest.

    Uses the single-model ``ModelImporter`` whose per-version run import calls
    ``import_logged_model`` (required for MLflow 3.x logged models). Non-REST
    errors propagate so a real failure is never masked as success.
    """
    _logger.info("import_model model=%s in=%s exp=%s delete=%s", model, input_dir, experiment_name, delete_model)
    if backend == "cli":
        env = {**os.environ, "MLFLOW_TRACKING_URI": registry_uri}
        cmd = [
            "import-model", "--model", model, "--input-dir", input_dir,
            "--experiment-name", experiment_name,
            "--delete-model", str(delete_model),
            "--import-permissions", str(import_permissions),
            "--import-source-tags", str(import_source_tags),
        ]
        subprocess.run(cmd, check=True, env=env)
        return
    _ensure_json_patch()
    from mlflow_export_import.model.import_model import import_model as _im

    _im(
        model_name=model,
        experiment_name=experiment_name,
        input_dir=input_dir,
        delete_model=delete_model,
        import_permissions=import_permissions,
        import_source_tags=import_source_tags,
        mlflow_client=make_mlflow_client(registry_uri),
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
