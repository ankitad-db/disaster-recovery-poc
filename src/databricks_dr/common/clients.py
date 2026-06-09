"""Per-region client factory.

Builds MLflow registry clients and Databricks SDK WorkspaceClients for a given
region, keyed off the CLI profile in ``dr_config.yaml``. Centralizing this keeps
cross-region URI handling in one place so modules never hardcode hosts/tokens.
"""

from __future__ import annotations

from functools import lru_cache

from .config import RegionConfig
from .logging import get_logger

_logger = get_logger(__name__)


def make_mlflow_client(registry_uri: str):
    """Create an MlflowClient bound to a specific (UC) registry URI.

    ``registry_uri`` looks like ``databricks-uc://<profile>``. The matching
    profile must exist in ~/.databrickscfg (``databricks auth login ... --profile``).
    """
    from mlflow import MlflowClient  # imported lazily so config/CLI work without mlflow

    _logger.debug("Creating MlflowClient registry_uri=%s", registry_uri)
    return MlflowClient(registry_uri=registry_uri)


def tracking_uri_for(region: RegionConfig) -> str:
    """MLFLOW_TRACKING_URI value for subprocess engine calls."""
    return region.registry_uri


@lru_cache(maxsize=8)
def workspace_client(profile: str):
    """Databricks SDK WorkspaceClient for a profile (grants, serving endpoints, jobs)."""
    from databricks.sdk import WorkspaceClient

    _logger.debug("Creating WorkspaceClient profile=%s", profile)
    return WorkspaceClient(profile=profile)
