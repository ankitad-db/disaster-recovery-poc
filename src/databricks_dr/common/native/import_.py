"""Native import: rebuild a registered model from a bundle into the dest registry.

Runs under the DEST ambient identity. Reverses :mod:`.export` with full fidelity:
  1. (optional) delete the existing registered model for a clean restore point
  2. recreate the backing runs inside the destination experiment (params, metrics,
     tags, artifacts, notebooks) so lineage is preserved
  3. register versions in ascending source order -- on a clean model UC then assigns
     the same sequential version numbers; append-only mode adds just the missing ones
  4. reapply version description/tags/stage, registered-model description/tags/aliases,
     permissions, and (gated) GenAI prompts / evaluation datasets / traces

MLflow run IDs and experiment IDs are workspace-local and WILL differ after import;
names, version numbers, params/metrics/tags, aliases, stages and artifacts are preserved.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from ..clients import make_mlflow_client
from ..logging import get_logger
from . import _genai, _notebooks, _permissions, _scale, manifest
from .manifest import Manifest, RunRec, VersionRec

_logger = get_logger(__name__)

_BATCH = 90  # log_batch caps at 100 params/metrics/tags per call


@dataclass
class ImportResult:
    """Source->destination ID correspondence produced by one model import.

    Names (model name, experiment name) are identical across workspaces; the IDs
    are workspace-local and differ. Callers persist this into ``dr_id_mapping``.
    """

    model: str
    is_uc: bool = True
    dest_experiment_id: Optional[str] = None
    dest_experiment_name: Optional[str] = None
    # source experiment_id -> (source experiment name)
    source_experiments: Dict[str, str] = field(default_factory=dict)
    # source run_id -> dest run_id
    run_id_map: Dict[str, str] = field(default_factory=dict)
    # source run_id -> source experiment_id (to resolve a run's experiment)
    run_experiment: Dict[str, str] = field(default_factory=dict)
    # source version -> dest version (str)
    version_map: Dict[str, str] = field(default_factory=dict)
    # source run_id -> source model version it backs (when known)
    run_version: Dict[str, str] = field(default_factory=dict)


def import_model(
    model: str,
    input_dir: str,
    experiment_name: str,
    registry_uri: str,
    *,
    delete_model: bool = True,
    import_permissions: bool = True,
    import_source_tags: bool = False,
    max_workers: int = 1,
    notebook_dest_dir: Optional[str] = None,
) -> ImportResult:
    """Rebuild ``model`` (versions + runs + lineage + GenAI) from ``input_dir``.

    Returns an :class:`ImportResult` carrying the source->destination ID maps
    (experiment, run, version) so the caller can persist them for lineage.
    """
    import mlflow

    mlflow.set_registry_uri(registry_uri)
    client = make_mlflow_client(registry_uri)
    man = manifest.read_manifest(input_dir)

    if delete_model:
        _delete_registered_model(client, man.is_uc, model)
    _ensure_registered_model(client, model)

    dest_experiment_id = _ensure_experiment(client, experiment_name)
    # On a clean restore, clear the DR experiment's prior backing runs so repeated
    # baseline/CDC cycles don't leak orphan runs (the experiment is DR-exclusive).
    if delete_model:
        _purge_experiment_runs(client, dest_experiment_id)

    # Recreate each backing run once; map source run_id -> new dest run_id.
    def _do_run(run: RunRec):
        return run.run_id, _recreate_run(client, run, input_dir, dest_experiment_id, notebook_dest_dir)

    pairs = _scale.map_bounded(_do_run, man.runs, max_workers=max_workers, label="run")
    run_id_map: Dict[str, str] = dict(pairs)

    existing = set() if delete_model else _existing_versions(client, model)
    to_register = [v for v in sorted(man.versions, key=lambda v: int(v.version))
                   if delete_model or int(v.version) not in existing]
    version_map: Dict[str, str] = {}
    for vrec in to_register:
        mv = _register_version(client, model, vrec, input_dir, run_id_map, man.is_uc)
        if mv is not None:
            version_map[str(vrec.version)] = str(mv.version)

    _apply_registered_model_metadata(client, model, man)
    if import_permissions:
        _permissions.import_permissions(model, man.is_uc, man.registered_model.permissions)

    _genai.import_prompts(input_dir, man.prompts)
    _genai.import_evaluation_datasets(input_dir, man.evaluation_datasets)
    _genai.import_traces(input_dir, man.traces, dest_experiment_id)

    _logger.info(
        "native import %s complete (registered=%d/%d runs=%d prompts=%d eval=%d traces=%d)",
        model, len(to_register), len(man.versions), len(man.runs),
        len(man.prompts), len(man.evaluation_datasets), len(man.traces),
    )
    return ImportResult(
        model=model,
        is_uc=man.is_uc,
        dest_experiment_id=dest_experiment_id,
        dest_experiment_name=experiment_name,
        source_experiments={e.experiment_id: e.name for e in man.experiments},
        run_id_map=run_id_map,
        run_experiment={r.run_id: r.experiment_id for r in man.runs},
        version_map=version_map,
        run_version={v.run_id: str(v.version) for v in man.versions if v.run_id},
    )


def import_models(
    input_dir: str,
    registry_uri: str,
    *,
    delete_model: bool = False,
    import_permissions: bool = True,
    import_source_tags: bool = True,
    experiment_renames: Optional[dict] = None,
    max_workers: int = 1,
) -> List[ImportResult]:
    """Bulk import: every per-model bundle under ``<input_dir>/models/<name>``.

    Returns one :class:`ImportResult` per imported model (for ID-mapping persistence).
    """
    models_root = os.path.join(input_dir, "models")
    if not os.path.isdir(models_root):
        raise FileNotFoundError(f"No models/ dir in bundle {input_dir}")

    def _one(name: str) -> Optional[ImportResult]:
        bundle = os.path.join(models_root, name)
        if not os.path.isfile(manifest.manifest_path(bundle)):
            return None
        man = manifest.read_manifest(bundle)
        exp = f"/Shared/dr/experiments/{man.registered_model.name.replace('.', '_')}"
        return import_model(
            man.registered_model.name, bundle, exp, registry_uri,
            delete_model=delete_model, import_permissions=import_permissions,
            import_source_tags=import_source_tags, max_workers=1,
        )

    names = sorted(d for d in os.listdir(models_root) if os.path.isdir(os.path.join(models_root, d)))
    results = _scale.map_bounded(_one, names, max_workers=max_workers, label="model")
    return [r for r in results if r is not None]


def import_model_version(
    model: str,
    input_dir: str,
    registry_uri: str,
    *,
    create_model: bool = True,
) -> Optional[str]:
    """Import a single-version bundle; returns the new dest version."""
    import mlflow

    mlflow.set_registry_uri(registry_uri)
    client = make_mlflow_client(registry_uri)
    man = manifest.read_manifest(input_dir)
    if create_model:
        _ensure_registered_model(client, model)

    exp_name = f"/Shared/dr/experiments/{model.replace('.', '_')}"
    dest_experiment_id = _ensure_experiment(client, exp_name)
    run_id_map: Dict[str, str] = {
        run.run_id: _recreate_run(client, run, input_dir, dest_experiment_id, None)
        for run in man.runs
    }

    new_version: Optional[str] = None
    for vrec in sorted(man.versions, key=lambda v: int(v.version)):
        mv = _register_version(client, model, vrec, input_dir, run_id_map, man.is_uc)
        if mv is not None:
            new_version = str(mv.version)
    _apply_registered_model_metadata(client, model, man)
    return new_version


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
    """Direct UC->UC version copy using the client's native copy verb."""
    client = make_mlflow_client(dst_registry_uri)
    _ensure_registered_model(client, dst_model)
    client.copy_model_version(f"models:/{src_model}/{src_version}", dst_model)


# --------------------------------------------------------------------------- #
# Internals
# --------------------------------------------------------------------------- #
def _delete_registered_model(client, is_uc: bool, model: str) -> None:
    try:
        if not is_uc:
            # WS registry: archive versions before delete to avoid stage conflicts.
            for mv in client.search_model_versions(f"name='{model}'"):
                stage = getattr(mv, "current_stage", None)
                if stage and stage not in ("Archived", "None"):
                    try:
                        client.transition_model_version_stage(model, mv.version, "Archived")
                    except Exception:  # noqa: BLE001
                        pass
        client.delete_registered_model(model)
        _logger.info("deleted existing registered model %s (clean restore)", model)
    except Exception as e:  # noqa: BLE001 - absent model is fine
        _logger.debug("no existing model %s to delete: %s", model, e)


def _ensure_registered_model(client, model: str) -> None:
    try:
        client.get_registered_model(model)
    except Exception:  # noqa: BLE001
        client.create_registered_model(model)
        _logger.info("created registered model %s", model)


def _ensure_experiment(client, name: str) -> str:
    exp = client.get_experiment_by_name(name)
    if exp is not None:
        return exp.experiment_id
    _ensure_workspace_dir(os.path.dirname(name))
    return client.create_experiment(name)


def _purge_experiment_runs(client, experiment_id: str) -> None:
    """Soft-delete all runs in a DR-managed experiment for a clean restore.

    ``import_model(delete_model=True)`` drops the registered model but MLflow keeps
    the experiment and its runs. Since ``/Shared/dr/experiments/<model>`` is
    DR-exclusive, we clear its prior runs here so repeated baseline/CDC overwrites
    don't accumulate orphaned (model-less) backing runs cycle after cycle.
    """
    from mlflow.entities import ViewType

    deleted, token = 0, None
    while True:
        page = client.search_runs(
            [experiment_id], run_view_type=ViewType.ACTIVE_ONLY,
            max_results=1000, page_token=token,
        )
        for r in page:
            try:
                client.delete_run(r.info.run_id)
                deleted += 1
            except Exception as e:  # noqa: BLE001 - best effort; import proceeds
                _logger.debug("delete_run %s failed: %s", r.info.run_id, e)
        token = getattr(page, "token", None)
        if not token:
            break
    if deleted:
        _logger.info("purged %d prior run(s) from experiment %s (clean restore)",
                     deleted, experiment_id)


def _existing_versions(client, model: str) -> set:
    try:
        return {int(v.version) for v in client.search_model_versions(f"name='{model}'")}
    except Exception:  # noqa: BLE001
        return set()


def _recreate_run(client, run: RunRec, input_dir: str, dest_experiment_id: str,
                  notebook_dest_dir: Optional[str]) -> str:
    """Create a destination run mirroring a source run's data + artifacts + notebook."""
    from mlflow.entities import Metric, Param, RunTag

    tags = {k: str(v) for k, v in run.tags.items()}
    new = client.create_run(experiment_id=dest_experiment_id, start_time=run.start_time, tags=tags)
    new_run_id = new.info.run_id

    params = [Param(k, str(v)) for k, v in run.params.items()]
    ts = run.end_time or run.start_time or int(time.time() * 1000)
    metrics = [Metric(k, float(v), ts, 0) for k, v in run.metrics.items()]
    run_tags = [RunTag(k, str(v)) for k, v in tags.items()]
    for i in range(0, max(len(params), len(metrics), len(run_tags), 1), _BATCH):
        client.log_batch(
            new_run_id,
            metrics=metrics[i:i + _BATCH],
            params=params[i:i + _BATCH],
            tags=run_tags[i:i + _BATCH],
        )

    if run.has_artifacts:
        art_dir = os.path.join(input_dir, run.rel_dir, "artifacts")
        if os.path.isdir(art_dir):
            client.log_artifacts(new_run_id, art_dir)

    for nb in run.notebooks:
        _notebooks.import_run_notebook(os.path.join(input_dir, run.rel_dir), nb, notebook_dest_dir)

    client.set_terminated(new_run_id, status=run.status or "FINISHED")
    _logger.debug("recreated run %s -> %s", run.run_id, new_run_id)
    return new_run_id


def _register_version(client, model: str, vrec: VersionRec, input_dir: str,
                      run_id_map: Dict[str, str], is_uc: bool):
    """Register one version from its bundled model artifacts; apply version metadata."""
    import mlflow

    dest_run_id = run_id_map.get(vrec.run_id) if vrec.run_id else None
    source = _stage_model_source(client, vrec, input_dir, dest_run_id)
    if source is None:
        _logger.warning("version %s of %s has no model artifacts; skipping", vrec.version, model)
        return None

    # The runs:/ URI links the backing run automatically; register_model takes no run_id.
    mv = mlflow.register_model(model_uri=source, name=model)
    _wait_ready(client, model, mv.version)

    if vrec.description:
        client.update_model_version(model, mv.version, description=vrec.description)
    for k, v in vrec.tags.items():
        if "." in k:  # UC forbids '.' in version tag keys
            continue
        try:
            client.set_model_version_tag(model, mv.version, k, str(v))
        except Exception as e:  # noqa: BLE001
            _logger.debug("set_model_version_tag %s v%s %s failed: %s", model, mv.version, k, e)

    # Legacy stages only apply to the workspace registry; UC uses aliases.
    if not is_uc and vrec.current_stage and vrec.current_stage not in ("None", None):
        try:
            client.transition_model_version_stage(model, mv.version, vrec.current_stage)
        except Exception as e:  # noqa: BLE001
            _logger.warning("stage transition %s v%s -> %s failed: %s", model, mv.version, vrec.current_stage, e)

    _logger.info("registered %s v%s (source bundle v%s)", model, mv.version, vrec.version)
    return mv


def _stage_model_source(client, vrec: VersionRec, input_dir: str, dest_run_id: Optional[str]) -> Optional[str]:
    """Upload the bundled model files under the dest run and return a runs:/ URI.

    The staged copy is sanitized first (see :func:`_neutralize_logged_model_ref`) so
    MLflow 3 does not try to resolve the *source* logged-model id embedded in
    ``MLmodel`` against the destination workspace (which would 404).
    """
    import shutil
    import tempfile

    candidates = [os.path.join(input_dir, vrec.rel_dir, "model")]
    if vrec.run_id:
        candidates.append(os.path.join(input_dir, "runs", vrec.run_id, "artifacts", "model"))

    from . import _artifacts

    model_dir = next((c for c in candidates if _artifacts._has_files(c)), None)
    if model_dir is None or dest_run_id is None:
        return None

    resolved = _artifacts.find_model_subdir(model_dir) or model_dir
    # Copy to a local temp dir (never mutate the source bundle on the Volume) and drop
    # the MLflow-3 logged-model linkage before uploading + registering.
    tmp = tempfile.mkdtemp(prefix="dr_stage_")
    staged = os.path.join(tmp, "model")
    shutil.copytree(resolved, staged)
    _neutralize_logged_model_ref(os.path.join(staged, "MLmodel"))

    artifact_path = f"dr_models/v{vrec.version}"
    client.log_artifacts(dest_run_id, staged, artifact_path=artifact_path)
    shutil.rmtree(tmp, ignore_errors=True)
    return f"runs:/{dest_run_id}/{artifact_path}"


def _neutralize_logged_model_ref(mlmodel_path: str) -> None:
    """Strip the source ``model_id`` from an ``MLmodel`` file.

    MLflow 3 stamps the source LoggedModel id into ``MLmodel`` (top-level and/or
    under ``metadata``). On cross-workspace import ``register_model`` would try to
    resolve that id in the destination and fail with NOT_FOUND. Removing it makes
    the model self-contained; MLflow assigns a fresh logged model on registration.
    """
    if not os.path.isfile(mlmodel_path):
        return
    try:
        import yaml

        with open(mlmodel_path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        changed = False
        for key in ("model_id", "logged_model_id"):
            if key in data:
                data.pop(key)
                changed = True
        meta = data.get("metadata")
        if isinstance(meta, dict):
            for key in ("model_id", "logged_model_id"):
                if key in meta:
                    meta.pop(key)
                    changed = True
        if changed:
            with open(mlmodel_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
            _logger.info("neutralized logged-model ref in %s", mlmodel_path)
    except Exception as e:  # noqa: BLE001 - best effort; registration will surface real errors
        _logger.debug("could not neutralize MLmodel %s: %s", mlmodel_path, e)


def _apply_registered_model_metadata(client, model: str, man: Manifest) -> None:
    rm = man.registered_model
    if rm.description:
        try:
            client.update_registered_model(model, description=rm.description)
        except Exception as e:  # noqa: BLE001
            _logger.debug("update_registered_model description failed: %s", e)
    for k, v in rm.tags.items():
        if "." in k:
            continue
        try:
            client.set_registered_model_tag(model, k, str(v))
        except Exception as e:  # noqa: BLE001
            _logger.debug("set_registered_model_tag %s failed: %s", k, e)
    # Aliases reference version numbers, which we preserved by ascending registration.
    for alias, version in rm.aliases.items():
        try:
            client.set_registered_model_alias(model, alias, str(version))
            _logger.info("alias %s -> %s v%s", alias, model, version)
        except Exception as e:  # noqa: BLE001
            _logger.warning("set alias %s on %s v%s failed: %s", alias, model, version, e)


def _wait_ready(client, model: str, version: str, timeout_s: int = 300) -> None:
    """Poll a UC model version until READY (UC copies artifacts asynchronously)."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            mv = client.get_model_version(model, version)
        except Exception:  # noqa: BLE001
            time.sleep(2)
            continue
        status = str(getattr(mv, "status", "READY"))
        if status in ("READY", "None", ""):
            return
        if status == "FAILED_REGISTRATION":
            raise RuntimeError(f"{model} v{version} registration FAILED")
        time.sleep(3)
    _logger.warning("%s v%s not READY within %ss; continuing", model, version, timeout_s)


def _ensure_workspace_dir(path: str) -> None:
    if not path:
        return
    try:
        from databricks.sdk import WorkspaceClient

        WorkspaceClient().workspace.mkdirs(path)
    except Exception as e:  # noqa: BLE001
        _logger.debug("mkdirs %s skipped: %s", path, e)
