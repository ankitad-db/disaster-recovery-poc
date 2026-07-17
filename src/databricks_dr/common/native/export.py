"""Native export: serialize a UC registered model into a portable bundle.

Runs under the SOURCE ambient identity (set by the replicate module). Captures,
with full mlflow-export-import parity:
  * registered-model metadata (description, tags, aliases-by-version, timestamps,
    permissions snapshot)
  * every version (description, tags, status, current_stage, source, run_link,
    aliases, signature presence, backing run, resolved model files)
  * the backing MLflow runs (params, metrics, tags, full artifact tree, notebooks)
  * the experiments those runs live in
  * MLflow 3 logged models attached to the backing runs
  * (optional) GenAI prompts, evaluation datasets and traces

Nothing here imports ``mlflow_export_import`` -- only the public MLflow client.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

from ..clients import make_mlflow_client
from ..logging import get_logger
from . import _artifacts, _genai, _notebooks, _permissions, _scale, manifest
from .manifest import (
    ExperimentRec,
    LoggedModelRec,
    Manifest,
    RegisteredModelRec,
    RunRec,
    VersionRec,
)

_logger = get_logger(__name__)


def _mlflow_version() -> str:
    try:
        import mlflow

        return mlflow.__version__
    except Exception:  # noqa: BLE001
        return "unknown"


def _is_uc(model: str) -> bool:
    """UC model names are 3-level (catalog.schema.model)."""
    return model.count(".") == 2


def export_model(
    model: str,
    output_dir: str,
    registry_uri: str,
    *,
    export_version_model: bool = True,
    export_permissions: bool = True,
    notebook_formats: str = "SOURCE",
    max_workers: int = 1,
    prompt_names: Optional[List[str]] = None,
    eval_dataset_names: Optional[List[str]] = None,
    replicate_traces: bool = False,
    skip_versions: Optional[set] = None,
) -> Manifest:
    """Export one registered model into ``output_dir``.

    ``skip_versions`` (delta/CDC path) is the set of source version numbers the
    destination already holds; those versions -- and their backing runs -- are
    omitted from the bundle so an incremental sync moves only the *new* artifacts.
    Registered-model metadata (description, tags, aliases) is ALWAYS captured in
    full, so a metadata-only change (e.g. an alias moved with no new version) still
    produces a bundle the importer can reconcile. When ``None`` (baseline) every
    version is exported.
    """
    client = make_mlflow_client(registry_uri)
    os.makedirs(output_dir, exist_ok=True)
    is_uc = _is_uc(model)
    fmts = [f.strip() for f in notebook_formats.split(",") if f.strip()]

    rm = client.get_registered_model(model)
    rm_rec = RegisteredModelRec(
        name=model,
        description=getattr(rm, "description", None),
        tags=dict(getattr(rm, "tags", {}) or {}),
        aliases=_aliases_by_alias(rm),
        creation_timestamp=getattr(rm, "creation_timestamp", None),
        last_updated_timestamp=getattr(rm, "last_updated_timestamp", None),
        permissions=_permissions.export_permissions(model, is_uc) if export_permissions else None,
    )

    versions = _scale.search_all_model_versions(client, model)
    if skip_versions:
        kept = [mv for mv in versions if int(mv.version) not in skip_versions]
        if len(kept) != len(versions):
            _logger.info(
                "delta export %s: %d/%d version(s) already on dest, exporting %d new",
                model, len(versions) - len(kept), len(versions), len(kept),
            )
        versions = kept

    runs_seen: Dict[str, RunRec] = {}
    experiments_seen: Dict[str, ExperimentRec] = {}
    logged_models: List[LoggedModelRec] = []

    def _do_version(mv) -> VersionRec:
        return _export_version(client, model, mv, output_dir, export_version_model)

    version_recs = _scale.map_bounded(_do_version, versions, max_workers=max_workers, label="version")

    # Backing runs + experiments + logged models (dedup by run id; run after versions
    # so we capture every distinct backing run exactly once).
    run_ids = [v.run_id for v in version_recs if v.run_id]
    for run_id in dict.fromkeys(run_ids):  # ordered unique
        rec = _export_run(client, run_id, output_dir, fmts)
        if rec is not None:
            runs_seen[run_id] = rec
            _capture_experiment(client, rec.experiment_id, experiments_seen)
            logged_models.extend(_export_logged_models(client, run_id, output_dir))

    prompts = _genai.export_prompts_for_model(output_dir, prompt_names or [])
    eval_datasets = _genai.export_evaluation_datasets(output_dir, eval_dataset_names or [])
    traces = _genai.export_traces_for_experiments(
        output_dir, list(experiments_seen.keys()), replicate_traces
    )

    man = Manifest(
        schema_version=manifest.SCHEMA_VERSION,
        engine=f"native-{_mlflow_version()}",
        mlflow_version=_mlflow_version(),
        exported_at=datetime.now(timezone.utc).isoformat(),
        source_registry_uri=registry_uri,
        registered_model=rm_rec,
        is_uc=is_uc,
        versions=version_recs,
        runs=list(runs_seen.values()),
        experiments=list(experiments_seen.values()),
        logged_models=logged_models,
        prompts=prompts,
        evaluation_datasets=eval_datasets,
        traces=traces,
    )
    path = manifest.write_manifest(output_dir, man)
    _logger.info(
        "native export %s -> %s (versions=%d runs=%d exp=%d logged=%d prompts=%d eval=%d traces=%d bytes=%d)",
        model, path, len(version_recs), len(runs_seen), len(experiments_seen),
        len(logged_models), len(prompts), len(eval_datasets), len(traces), man.total_bytes(),
    )
    return man


def export_models(
    models: str,
    output_dir: str,
    registry_uri: str,
    *,
    export_all_runs: bool = True,
    export_version_model: bool = True,
    export_permissions: bool = True,
    notebook_formats: str = "SOURCE",
    max_workers: int = 1,
) -> None:
    """Bulk export: one per-model bundle under ``<output_dir>/models/<name>``."""
    names = (
        _scale.search_all_registered_models(make_mlflow_client(registry_uri))
        if models == "all"
        else [m.strip() for m in models.split(",") if m.strip()]
    )

    def _one(name: str) -> None:
        export_model(
            name, os.path.join(output_dir, "models", name), registry_uri,
            export_version_model=export_version_model, export_permissions=export_permissions,
            notebook_formats=notebook_formats, max_workers=1,  # nest workers at version level only
        )

    _scale.map_bounded(_one, names, max_workers=max_workers, label="model")


def export_model_version(
    model: str,
    version: str,
    output_dir: str,
    registry_uri: str,
    *,
    export_version_model: bool = True,
) -> Manifest:
    """Export a single version (parity with the engine's low-level verb)."""
    client = make_mlflow_client(registry_uri)
    os.makedirs(output_dir, exist_ok=True)
    is_uc = _is_uc(model)
    rm = client.get_registered_model(model)
    mv = client.get_model_version(model, version)

    vrec = _export_version(client, model, mv, output_dir, export_version_model)

    runs: List[RunRec] = []
    experiments: Dict[str, ExperimentRec] = {}
    if vrec.run_id:
        rec = _export_run(client, vrec.run_id, output_dir, ["SOURCE"])
        if rec:
            runs.append(rec)
            _capture_experiment(client, rec.experiment_id, experiments)

    man = Manifest(
        schema_version=manifest.SCHEMA_VERSION,
        engine=f"native-{_mlflow_version()}",
        mlflow_version=_mlflow_version(),
        exported_at=datetime.now(timezone.utc).isoformat(),
        source_registry_uri=registry_uri,
        registered_model=RegisteredModelRec(
            name=model,
            description=getattr(rm, "description", None),
            tags=dict(getattr(rm, "tags", {}) or {}),
            aliases=_aliases_by_alias(rm),
        ),
        is_uc=is_uc,
        versions=[vrec],
        runs=runs,
        experiments=list(experiments.values()),
    )
    manifest.write_manifest(output_dir, man)
    return man


# --------------------------------------------------------------------------- #
# Internals
# --------------------------------------------------------------------------- #
def _export_version(client, model: str, mv, output_dir: str, export_version_model: bool) -> VersionRec:
    v = str(mv.version)
    v_dir = os.path.join(output_dir, "versions", v)
    os.makedirs(v_dir, exist_ok=True)

    has_model = False
    sig = False
    vbytes = 0
    if export_version_model:
        model_dst = os.path.join(v_dir, "model")
        has_model = _artifacts.download_model_version(model, v, model_dst)
        if has_model:
            sig = _artifacts.signature_present(model_dst)
            vbytes = _artifacts.dir_bytes(model_dst)

    return VersionRec(
        version=v,
        rel_dir=os.path.relpath(v_dir, output_dir),
        run_id=getattr(mv, "run_id", None) or None,
        source=getattr(mv, "source", None),
        run_link=getattr(mv, "run_link", None),
        status=getattr(mv, "status", None),
        current_stage=getattr(mv, "current_stage", None),
        description=getattr(mv, "description", None),
        user_id=getattr(mv, "user_id", None),
        tags=dict(getattr(mv, "tags", {}) or {}),
        aliases=list(getattr(mv, "aliases", []) or []),
        has_model_artifacts=has_model,
        signature_present=sig,
        model_id=_model_id_from_source(getattr(mv, "source", None)),
        bytes=vbytes,
    )


def _export_run(client, run_id: str, output_dir: str, notebook_formats: List[str]) -> Optional[RunRec]:
    try:
        run = client.get_run(run_id)
    except Exception as e:  # noqa: BLE001 - a missing backing run must not abort export
        _logger.warning("get_run %s failed: %s", run_id, e)
        return None

    rel_dir = os.path.join("runs", run_id)
    abs_dir = os.path.join(output_dir, rel_dir)
    artifacts_dir = os.path.join(abs_dir, "artifacts")
    os.makedirs(abs_dir, exist_ok=True)

    has_artifacts = _artifacts.download_run_artifacts(client, run_id, artifacts_dir)

    data, info = run.data, run.info
    tags = {k: v for k, v in (data.tags or {}).items()
            if not k.startswith("mlflow.") or k in (
                "mlflow.runName", _notebooks.NOTEBOOK_PATH_TAG, _notebooks.NOTEBOOK_REVISION_TAG)}

    nb = _notebooks.export_run_notebooks(data.tags or {}, artifacts_dir, notebook_formats)

    return RunRec(
        run_id=run_id,
        experiment_id=info.experiment_id,
        rel_dir=rel_dir,
        status=getattr(info, "status", "FINISHED"),
        start_time=getattr(info, "start_time", None),
        end_time=getattr(info, "end_time", None),
        params=dict(data.params or {}),
        metrics={k: float(v) for k, v in (data.metrics or {}).items()},
        tags=tags,
        lifecycle_stage=getattr(info, "lifecycle_stage", "active"),
        has_artifacts=has_artifacts,
        bytes=_artifacts.dir_bytes(artifacts_dir),
        notebooks=[nb] if nb else [],
    )


def _capture_experiment(client, experiment_id: str, sink: Dict[str, ExperimentRec]) -> None:
    if experiment_id in sink:
        return
    try:
        exp = client.get_experiment(experiment_id)
    except Exception as e:  # noqa: BLE001
        _logger.warning("get_experiment %s failed: %s", experiment_id, e)
        return
    sink[experiment_id] = ExperimentRec(
        experiment_id=experiment_id,
        name=exp.name,
        tags=dict(getattr(exp, "tags", {}) or {}),
        artifact_location=getattr(exp, "artifact_location", None),
        lifecycle_stage=getattr(exp, "lifecycle_stage", None),
    )


def _export_logged_models(client, run_id: str, output_dir: str) -> List[LoggedModelRec]:
    """Capture MLflow 3 logged models attached to a run (gated on 3.x)."""
    recs: List[LoggedModelRec] = []
    if not _genai.has_logged_model_support():
        return recs
    search = getattr(client, "search_logged_models", None)
    if search is None:
        return recs
    try:
        lms = search(filter_string=f"source_run_id='{run_id}'")
    except Exception as e:  # noqa: BLE001
        _logger.debug("search_logged_models run=%s unavailable: %s", run_id, e)
        return recs

    for lm in lms or []:
        lm_id = getattr(lm, "model_id", None) or getattr(lm, "logged_model_id", None)
        if not lm_id:
            continue
        rel_dir = os.path.join("logged_models", lm_id)
        abs_dir = os.path.join(output_dir, rel_dir)
        art_dir = os.path.join(abs_dir, "artifacts")
        os.makedirs(art_dir, exist_ok=True)
        has = False
        try:
            import mlflow

            mlflow.artifacts.download_artifacts(artifact_uri=f"models:/{lm_id}", dst_path=art_dir)
            has = _artifacts._has_files(art_dir)
        except Exception as e:  # noqa: BLE001
            _logger.debug("logged model %s artifact download skipped: %s", lm_id, e)
        recs.append(LoggedModelRec(
            logged_model_id=lm_id,
            name=getattr(lm, "name", None),
            experiment_id=getattr(lm, "experiment_id", None),
            rel_dir=rel_dir,
            source_run_id=run_id,
            model_type=getattr(lm, "model_type", None),
            status=str(getattr(lm, "status", "")) or None,
            tags=dict(getattr(lm, "tags", {}) or {}),
            params=dict(getattr(lm, "params", {}) or {}),
            metrics={k: float(v) for k, v in (getattr(lm, "metrics", {}) or {}).items()},
            has_artifacts=has,
        ))
    return recs


def _aliases_by_alias(rm) -> Dict[str, str]:
    """Return {alias_name: version} from a RegisteredModel entity."""
    aliases = getattr(rm, "aliases", None) or {}
    if isinstance(aliases, dict):
        return {str(k): str(v) for k, v in aliases.items()}
    out: Dict[str, str] = {}
    for a in aliases:
        out[str(getattr(a, "alias", a))] = str(getattr(a, "version", ""))
    return out


def _model_id_from_source(source: Optional[str]) -> Optional[str]:
    """Extract a logged-model id from a ``models:/<id>`` version source (MLflow 3)."""
    if source and source.startswith("models:/"):
        return source.split("models:/", 1)[1].split("/")[0] or None
    return None
