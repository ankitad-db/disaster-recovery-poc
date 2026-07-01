"""Phase 1: seed the PRIMARY workspace with test material.

Creates UC structure and registers one or more small sklearn models, each with
multiple versions (= multiple backing runs), aliases, and tags so baseline + CDC
have realistic, multi-model, multi-component material. Intended to run inside a
Databricks notebook/job on the primary (ML runtime).

What gets seeded is config-driven (``models.seed`` in ``dr_config.yaml``); if that
list is absent it falls back to seeding every name in ``models.include`` as a
2-version iris model.
"""

from __future__ import annotations

from typing import List

from ...common.config import Config
from ...common.logging import get_logger

_logger = get_logger(__name__)

# sklearn datasets we can seed from (all classification, all return_X_y/as_frame).
_DATASETS = {
    "iris": "load_iris",
    "wine": "load_wine",
    "breast_cancer": "load_breast_cancer",
}


def seed_models(cfg: Config) -> List[str]:
    """Seed every model in the POC spec; returns the list of full model names.

    Spec source (in priority order):
      1. ``cfg.models["seed"]`` — list of ``{name|model, dataset, n_versions}`` dicts.
      2. fallback: ``cfg.models["include"]`` names, seeded as 2-version iris models.
    """
    import mlflow

    mlflow.set_registry_uri("databricks-uc")
    _ensure_uc(cfg)
    _ensure_workspace_dir("/Shared/dr/experiments")  # MLflow won't create the tree

    seeded: List[str] = []
    for spec in _seed_specs(cfg):
        seeded.append(_seed_one_model(cfg, **spec))
    _logger.info("Seed complete: %d model(s) -> %s", len(seeded), ", ".join(seeded))
    return seeded


def seed_primary(cfg: Config, n_versions: int = 2) -> str:
    """Back-compat single-model entry point (iris). Prefer :func:`seed_models`."""
    catalog, schema = cfg.uc["catalog"], cfg.uc["schema"]
    return _seed_one_model(cfg, name=f"{catalog}.{schema}.iris_dr_model",
                           dataset="iris", n_versions=n_versions)


def _seed_specs(cfg: Config) -> List[dict]:
    """Normalize the seed spec from config into a list of kwargs for _seed_one_model."""
    catalog, schema = cfg.uc["catalog"], cfg.uc["schema"]
    raw = cfg.models.get("seed")
    specs: List[dict] = []
    if raw:
        for s in raw:
            # Accept either a full 3-level "name" or a short "model" id.
            name = s.get("name") or f"{catalog}.{schema}.{s['model']}"
            specs.append({
                "name": name,
                "dataset": s.get("dataset", "iris"),
                "n_versions": int(s.get("n_versions", 2)),
            })
    else:
        for name in cfg.models.get("include", []):
            if name and name != "all":
                specs.append({"name": name, "dataset": "iris", "n_versions": 2})
    if not specs:  # last-resort default so the notebook always has something to do
        specs.append({"name": f"{catalog}.{schema}.iris_dr_model",
                      "dataset": "iris", "n_versions": 2})
    return specs


def _seed_one_model(cfg: Config, *, name: str, dataset: str = "iris", n_versions: int = 2) -> str:
    """Register ``n_versions`` of one model (one backing run per version) + aliases/tags."""
    import importlib

    import mlflow
    import mlflow.sklearn
    from mlflow import MlflowClient
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split

    if dataset not in _DATASETS:
        raise ValueError(f"Unknown seed dataset '{dataset}'; choose from {sorted(_DATASETS)}")
    loader = getattr(importlib.import_module("sklearn.datasets"), _DATASETS[dataset])

    client = MlflowClient()

    # Stable Shared experiment per model (NOT the notebook path, which collides on
    # import when the same Git folder exists in the destination workspace).
    experiment_path = f"/Shared/dr/experiments/{name.replace('.', '_')}"
    mlflow.set_experiment(experiment_path)
    _logger.info("Seeding %s (dataset=%s, n_versions=%s) in experiment %s",
                 name, dataset, n_versions, experiment_path)

    # Clean slate so re-seeding yields a single, consistent lineage.
    try:
        client.delete_registered_model(name)
        _logger.info("Deleted existing registered model %s for a clean reseed", name)
    except Exception as e:  # noqa: BLE001
        _logger.info("No existing model to delete (%s)", e)

    X, y = loader(return_X_y=True, as_frame=True)
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)

    last_version = None
    for i in range(1, n_versions + 1):
        with mlflow.start_run(run_name=f"{name.split('.')[-1]}_seed_v{i}"):
            clf = RandomForestClassifier(n_estimators=40 + i * 10, max_depth=4 + i, random_state=42)
            clf.fit(X_tr, y_tr)
            mlflow.log_param("dataset", dataset)
            mlflow.log_param("n_estimators", 40 + i * 10)
            mlflow.log_metric("accuracy", float(clf.score(X_te, y_te)))
            info = mlflow.sklearn.log_model(
                sk_model=clf,
                artifact_path="model",
                registered_model_name=name,
                input_example=X_tr.iloc[[0]],  # ensures a signature (required for UC)
            )
            last_version = info.registered_model_version
            _logger.info("Registered %s version %s", name, last_version)
        # Version-level metadata so DR fidelity is exercised on every object type
        # (per-version description + tags), uniformly for each seeded model.
        client.update_model_version(
            name, last_version,
            description=f"{dataset} RandomForest v{i} (n_estimators={40 + i * 10}, max_depth={4 + i})",
        )
        client.set_model_version_tag(name, last_version, "seed_iteration", str(i))
        client.set_model_version_tag(name, last_version, "validation_status", "passed")

    # Registered-model description + aliases + tags (consumer-facing handles we replicate)
    client.update_registered_model(
        name, description=f"DR POC {dataset} classifier — {n_versions} versions, seeded for replication testing")
    client.set_registered_model_alias(name, "Champion", last_version)
    if int(last_version) > 1:
        client.set_registered_model_alias(name, "Challenger", str(int(last_version) - 1))
    client.set_registered_model_tag(name, "owner", "dr-poc")
    client.set_registered_model_tag(name, "dr_managed", "true")

    _logger.info("Seeded %s (versions=1..%s)", name, last_version)
    return name


def _ensure_workspace_dir(path: str) -> None:
    """Create a workspace directory tree (MLflow needs the experiment's parent)."""
    try:
        from databricks.sdk import WorkspaceClient

        WorkspaceClient().workspace.mkdirs(path)
        _logger.info("Ensured workspace directory %s", path)
    except Exception as e:  # noqa: BLE001
        _logger.warning("Could not create workspace dir %s: %s", path, e)


def _ensure_uc(cfg: Config) -> None:
    """Create catalog/schemas via spark if available (notebook context)."""
    catalog = cfg.uc["catalog"]
    schema = cfg.uc["schema"]
    control = cfg.uc["control_schema"]
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            _logger.warning("No active Spark session; create UC objects via sql/ DDL instead.")
            return
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{control}")
        _logger.info("Ensured UC objects: %s.%s, %s.%s", catalog, schema, catalog, control)
    except Exception as e:  # noqa: BLE001
        _logger.warning("Could not create UC objects automatically (%s); use sql/ DDL.", e)
