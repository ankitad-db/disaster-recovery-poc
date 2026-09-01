"""Configuration for the workspace-secrets DR module.

Loads ``config/secrets_dr_config.yaml`` into a typed, self-validating object.
Kept separate from the models ``Config`` because the shape is different (no MLflow
registry URIs; S3 buckets + KMS + secret scopes instead).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

import yaml

from ...common.logging import get_logger

_logger = get_logger(__name__)

DEFAULT_CONFIG_PATH = "config/secrets_dr_config.yaml"


class SecretsConfigError(ValueError):
    """Raised when secrets_dr_config.yaml is missing required structure."""


@dataclass(frozen=True)
class Workspace:
    key: str                 # "primary" | "secondary"
    region: str              # us-east-2 | us-west-2
    host: str
    profile: str
    workspace_id: str = ""


@dataclass
class SecretsConfig:
    raw: Dict[str, Any]
    path: str | None = None
    workspaces: Dict[str, Workspace] = field(default_factory=dict)

    def __post_init__(self):
        ws = self.raw.get("workspaces") or {}
        if set(ws) < {"primary", "secondary"}:
            raise SecretsConfigError(
                "workspaces.primary and workspaces.secondary are both required"
            )
        self.workspaces = {
            k: Workspace(
                key=k,
                region=v["region"],
                host=v["host"],
                profile=v.get("profile", ""),
                workspace_id=str(v.get("workspace_id", "")),
            )
            for k, v in ws.items()
        }

    # ---- section accessors -----------------------------------------------------
    @property
    def scopes(self) -> Dict[str, Any]:
        return self.raw.get("scopes", {"include": ["all"], "exclude": []})

    @property
    def detection(self) -> Dict[str, Any]:
        return self.raw.get("detection", {})

    @property
    def reconcile(self) -> Dict[str, Any]:
        """Destination-aware import (failover) behaviour.

        mode: ``mirror`` (secondary ends up == primary, incl. pruning secrets/scopes
        that no longer exist in primary) or ``additive`` (only add/update, never
        delete). ``prune_extra_scopes`` also removes whole scopes absent from the
        bundle when mirroring.
        """
        r = dict(self.raw.get("reconcile") or {})
        r.setdefault("mode", "mirror")
        r.setdefault("prune_extra_scopes", False)
        return r

    @property
    def storage(self) -> Dict[str, Any]:
        return self.raw["storage"]

    @property
    def control(self) -> Dict[str, Any]:
        return self.raw["control"]

    @property
    def service_principal(self) -> str:
        return self.raw.get("service_principal", "")

    @property
    def seed(self) -> List[Dict[str, Any]]:
        """POC-only sample scopes to create in the primary (see modules/secrets/seed.py)."""
        return list((self.raw.get("seed") or {}).get("scopes", []))

    # ---- convenience -----------------------------------------------------------
    @property
    def include(self) -> List[str]:
        return list(self.scopes.get("include", ["all"]))

    @property
    def exclude(self) -> List[str]:
        return list(self.scopes.get("exclude", []))

    @property
    def use_system_tables(self) -> bool:
        return (self.detection.get("strategy", "system_tables") == "system_tables")

    @property
    def audit_system_table(self) -> str:
        return self.detection.get("audit_table", "system.access.audit")

    # Control tables can live in a different catalog per workspace: some metastores
    # have no managed storage for a fresh `dr_poc` catalog, so a workspace may point
    # at a catalog that already has managed storage (e.g. its default catalog).
    def control_catalog_for(self, region_key: str) -> str:
        overrides = self.control.get("catalog_by_workspace") or {}
        return overrides.get(region_key) or self.control["catalog"]

    def audit_table_for(self, region_key: str) -> str:
        return f"{self.control_catalog_for(region_key)}.{self.control['schema']}.dr_secrets_audit"

    def inventory_table_for(self, region_key: str) -> str:
        return f"{self.control_catalog_for(region_key)}.{self.control['schema']}.dr_secrets_inventory"

    @property
    def audit_table(self) -> str:  # convenience: the primary's audit table
        return self.audit_table_for("primary")

    @property
    def inventory_table(self) -> str:
        return self.inventory_table_for("primary")

    @property
    def warehouse_id(self) -> str:
        return self.control.get("warehouse_id", "") or ""

    def bucket_for(self, region: str) -> str:
        s = self.storage
        if region == self.workspaces["primary"].region:
            return s["primary_bucket"]
        if region == self.workspaces["secondary"].region:
            return s["secondary_bucket"]
        raise SecretsConfigError(f"no bucket configured for region {region!r}")

    def kms_key_for(self, region: str) -> str:
        return self.storage.get("kms_key", {}).get(region, "")

    def validate(self) -> "SecretsConfig":
        errors: List[str] = []
        for k in ("primary_bucket", "secondary_bucket", "prefix"):
            if not self.storage.get(k):
                errors.append(f"storage.{k} is required")
        for k in ("catalog", "schema"):
            if not self.control.get(k):
                errors.append(f"control.{k} is required")
        if self.storage.get("client_side_encryption", True):
            for ws in self.workspaces.values():
                if not self.kms_key_for(ws.region):
                    errors.append(f"storage.kms_key.{ws.region} required for client-side encryption")
        if errors:
            raise SecretsConfigError(
                "invalid secrets DR config"
                + (f" ({self.path})" if self.path else "")
                + ":\n  - " + "\n  - ".join(errors)
            )
        for pb in (self.storage["primary_bucket"], self.storage["secondary_bucket"]):
            if "REPLACE" in pb:
                _logger.warning("storage bucket %s still has a REPLACE placeholder -- set the real bucket", pb)
        return self


def load_config(path: str | None = None) -> SecretsConfig:
    resolved = path or os.environ.get("SECRETS_DR_CONFIG") or DEFAULT_CONFIG_PATH
    p = Path(resolved)
    if not p.exists():
        raise FileNotFoundError(f"secrets DR config not found: {p.resolve()}")
    with p.open() as f:
        raw = yaml.safe_load(f)
    return SecretsConfig(raw=raw, path=str(p)).validate()
