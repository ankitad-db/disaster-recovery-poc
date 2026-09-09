"""Dual-mode configuration and Databricks client bootstrap.

Runs in two modes:
  - Inside a Databricks App: uses the auto-injected service-principal OAuth.
  - Local dev: uses a Databricks CLI profile (DATABRICKS_PROFILE, default dr2-east).
"""
import os
from functools import lru_cache

from databricks.sdk import WorkspaceClient

# DATABRICKS_APP_NAME is only set when running inside a Databricks App.
IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

WAREHOUSE_ID = os.environ.get("DR_WAREHOUSE_ID", "cc56ad79aa69c967")
SCHEMA = os.environ.get("DR_SCHEMA", "dr_recon.control")
RECON_JOB_ID = os.environ.get("DR_RECON_JOB_ID", "805214227604364")
GENIE_SPACE_ID = os.environ.get("DR_GENIE_SPACE_ID", "")

# Static topology facts for the identity header (from the DR failover group).
TOPOLOGY = {
    "failover_group": "fg-dr-recon",
    "primary": {
        "name": "dr-wp (east)",
        "region": "us-east-1",
        "workspace_id": "7474649395937386",
        "role": "Primary · active",
        "state": "WRITABLE",
    },
    "secondary": {
        "name": "dr-wp (west)",
        "region": "us-west-2",
        "role": "Secondary · passive standby",
        "state": "READ-ONLY",
    },
}


@lru_cache(maxsize=1)
def get_workspace_client() -> WorkspaceClient:
    if IS_DATABRICKS_APP:
        return WorkspaceClient()
    profile = os.environ.get("DATABRICKS_PROFILE", "dr2-east")
    return WorkspaceClient(profile=profile)


def current_user() -> str:
    """Best-effort identity for audit trails (ack author)."""
    # In a Databricks App the invoking user is forwarded here.
    fwd = os.environ.get("DATABRICKS_APP_USER")
    if fwd:
        return fwd
    try:
        return get_workspace_client().current_user.me().user_name or "unknown"
    except Exception:
        return "unknown"
