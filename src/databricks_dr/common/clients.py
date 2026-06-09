"""Per-region client factory.

Builds MLflow registry clients and Databricks SDK WorkspaceClients for a given
region, keyed off the CLI profile in ``dr_config.yaml``. Centralizing this keeps
cross-region URI handling in one place so modules never hardcode hosts/tokens.
"""

from __future__ import annotations

import os
from functools import lru_cache

from .config import RegionConfig
from .logging import get_logger

_logger = get_logger(__name__)

LOCAL_UC_REGISTRY = "databricks-uc"


def is_databricks_runtime() -> bool:
    """True when running on a Databricks cluster (vs. a laptop/CI host)."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def local_or_profile_uri(profile_uri: str) -> str:
    """Registry URI to use for the *local* workspace.

    On a Databricks cluster the local UC registry is just ``databricks-uc`` (the
    ``dr-west``/``dr-east`` CLI profiles only exist off-cluster). Off-cluster we
    use the profile-suffixed URI so a laptop/CI can target a specific workspace.
    """
    return LOCAL_UC_REGISTRY if is_databricks_runtime() else profile_uri


def tracking_uri_from_registry(registry_uri: str) -> str:
    """Derive the matching tracking URI for a UC registry URI.

    ``databricks-uc://prof`` -> ``databricks://prof`` ; ``databricks-uc`` ->
    ``databricks``. Export reads runs/experiments (tracking) *and* models
    (registry), so both must point at the same workspace.
    """
    if registry_uri.startswith("databricks-uc://"):
        return "databricks://" + registry_uri.split("://", 1)[1]
    return "databricks"


def make_mlflow_client(registry_uri: str, tracking_uri: str | None = None):
    """Create an MlflowClient bound to a (UC) registry URI + matching tracking URI.

    For a *remote* workspace, ``registry_uri`` is ``databricks-uc://<profile>`` and
    the profile must exist in ~/.databrickscfg (see ``configure_remote_profile``).
    For the *local* workspace on a cluster, pass ``databricks-uc``.
    """
    from mlflow import MlflowClient  # imported lazily so config/CLI work without mlflow

    tracking_uri = tracking_uri or tracking_uri_from_registry(registry_uri)
    _logger.debug("Creating MlflowClient tracking=%s registry=%s", tracking_uri, registry_uri)
    return MlflowClient(tracking_uri=tracking_uri, registry_uri=registry_uri)


def configure_remote_profile(profile: str, host: str, token: str) -> str:
    """Write/update a ~/.databrickscfg profile so MLflow can reach a remote workspace.

    Lets the DR job (running in the local workspace) authenticate to the remote
    source workspace using a secret-scoped host + SPN token, with no laptop step.
    Returns the UC registry URI for that profile.
    """
    import configparser
    import os

    cfg_path = os.path.expanduser("~/.databrickscfg")
    parser = configparser.ConfigParser()
    if os.path.exists(cfg_path):
        parser.read(cfg_path)
    if not parser.has_section(profile):
        parser.add_section(profile)
    parser.set(profile, "host", host)
    parser.set(profile, "token", token)
    with open(cfg_path, "w") as f:
        parser.write(f)
    os.chmod(cfg_path, 0o600)
    _logger.info("Configured remote databricks profile '%s' for host %s", profile, host)
    return f"databricks-uc://{profile}"


def tracking_uri_for(region: RegionConfig) -> str:
    """MLFLOW_TRACKING_URI value for subprocess engine calls."""
    return region.registry_uri


@lru_cache(maxsize=8)
def workspace_client(profile: str):
    """Databricks SDK WorkspaceClient for a profile (grants, serving endpoints, jobs)."""
    from databricks.sdk import WorkspaceClient

    _logger.debug("Creating WorkspaceClient profile=%s", profile)
    return WorkspaceClient(profile=profile)
