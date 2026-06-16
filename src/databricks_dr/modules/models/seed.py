"""Phase 1: seed the PRIMARY workspace with test material.

Creates UC structure and registers a small sklearn model with multiple versions,
aliases, and tags so baseline + CDC have realistic, multi-component material.
Intended to run inside a Databricks notebook/job on the primary (ML runtime).
"""

from __future__ import annotations

from ...common.config import Config
from ...common.logging import get_logger

_logger = get_logger(__name__)


def seed_primary(cfg: Config, n_versions: int = 2) -> str:
    """Register ``n_versions`` of an iris model in the primary registry.

    Returns the full model name. Requires mlflow + scikit-learn on the cluster.
    """
    import mlflow
    import mlflow.sklearn
    from mlflow import MlflowClient
    from sklearn.datasets import load_iris
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split

    catalog = cfg.uc["catalog"]
    schema = cfg.uc["schema"]
    model_name = f"{catalog}.{schema}.iris_dr_model"

    mlflow.set_registry_uri("databricks-uc")
    client = MlflowClient()

    _ensure_uc(cfg)

    # Log to a stable Shared experiment (NOT the notebook path). A notebook-path
    # experiment collides on import when the same Git folder exists in the
    # destination workspace, so DR-managed models use a dedicated location.
    experiment_path = f"/Shared/dr/experiments/{catalog}_{schema}_iris_dr_model"
    _ensure_workspace_dir("/Shared/dr/experiments")  # MLflow won't create the tree
    mlflow.set_experiment(experiment_path)
    _logger.info("Using experiment %s", experiment_path)

    # Clean slate so re-seeding yields a single, consistent lineage.
    try:
        client.delete_registered_model(model_name)
        _logger.info("Deleted existing registered model %s for a clean reseed", model_name)
    except Exception as e:  # noqa: BLE001
        _logger.info("No existing model to delete (%s)", e)

    X, y = load_iris(return_X_y=True, as_frame=True)
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)

    last_version = None
    for i in range(1, n_versions + 1):
        with mlflow.start_run(run_name=f"iris_dr_seed_v{i}"):
            clf = RandomForestClassifier(n_estimators=40 + i * 10, max_depth=4 + i, random_state=42)
            clf.fit(X_tr, y_tr)
            mlflow.log_metric("accuracy", float(clf.score(X_te, y_te)))
            info = mlflow.sklearn.log_model(
                sk_model=clf,
                artifact_path="model",
                registered_model_name=model_name,
                input_example=X_tr.iloc[[0]],  # ensures a signature (required for UC)
            )
            last_version = info.registered_model_version
            _logger.info("Registered %s version %s", model_name, last_version)

    # Aliases + tags (the consumer-facing handles we will replicate)
    client.set_registered_model_alias(model_name, "Champion", last_version)
    if int(last_version) > 1:
        client.set_registered_model_alias(model_name, "Challenger", str(int(last_version) - 1))
    client.set_registered_model_tag(model_name, "owner", "dr-poc")
    client.set_registered_model_tag(model_name, "dr_managed", "true")

    _logger.info("Seed complete: %s (versions=1..%s)", model_name, last_version)
    return model_name


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
