"""Configuration loading and the Direction resolver.

The Direction resolver is the heart of failover/failback: instead of hardcoding
"west -> east", every replication run asks the config which region is currently
``primary`` and derives (source_uri, dest_uri, storage_folder) from that. Failover
and failback just flip the ``role`` of each region (or the ``DR_ACTIVE_PRIMARY``
override) and the same code runs in the opposite direction.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml

DEFAULT_CONFIG_PATH = "config/dr_config.yaml"


@dataclass(frozen=True)
class RegionConfig:
    key: str  # "west" | "east"
    role: str  # "primary" | "secondary"
    region: str
    profile: str
    registry_uri: str
    host: str
    metastore: str
    workspace: str
    dbfs_bucket: str


@dataclass(frozen=True)
class Direction:
    """Resolved replication direction for a single run."""

    source: RegionConfig
    dest: RegionConfig
    folder: str  # storage subfolder authored by the source (primary|secondary)

    @property
    def label(self) -> str:
        return f"{self.source.region}->{self.dest.region}"


class Config:
    """Loaded DR configuration with convenience accessors."""

    def __init__(self, raw: Dict[str, Any], path: str | None = None):
        self._raw = raw
        self.path = path
        self.regions: Dict[str, RegionConfig] = {
            key: RegionConfig(key=key, **vals) for key, vals in raw["regions"].items()
        }

    # ---- raw section accessors -------------------------------------------------
    @property
    def uc(self) -> Dict[str, Any]:
        return self._raw["uc"]

    @property
    def storage(self) -> Dict[str, Any]:
        return self._raw["storage"]

    @property
    def models(self) -> Dict[str, Any]:
        return self._raw.get("models", {})

    @property
    def secrets(self) -> Dict[str, Any]:
        return self._raw.get("secrets", {})

    @property
    def engine_backend(self) -> str:
        return self._raw.get("engine", {}).get("backend", "api")

    @property
    def service_principal(self) -> str:
        return self._raw.get("service_principal", "")

    @property
    def audit_table(self) -> str:
        return self.uc["audit_table"]

    @property
    def state_table(self) -> str:
        """Single-row control table holding the active-primary role (orchestration)."""
        return self.uc.get("state_table") or (
            f"{self.uc['catalog']}.{self.uc['control_schema']}.dr_state"
        )

    # ---- role / direction ------------------------------------------------------
    def active_primary_key(self, spark=None) -> str:
        """Region key currently acting as primary.

        Resolution order (first match wins):
          1. ``DR_ACTIVE_PRIMARY`` env override ("west"|"east") -- dev/drill only.
          2. The ``dr_state`` control table (the orchestration source of truth,
             written by failover/failback). Read only when ``spark`` is provided.
          3. The region whose ``role`` is ``primary`` in config.
        """
        override = os.environ.get("DR_ACTIVE_PRIMARY")
        if override:
            if override not in self.regions:
                raise ValueError(f"DR_ACTIVE_PRIMARY='{override}' not in regions {list(self.regions)}")
            return override
        if spark is not None:
            from . import state
            key = state.read_active_primary(self.state_table, spark=spark)
            if key:
                if key not in self.regions:
                    raise ValueError(f"dr_state active_primary='{key}' not in regions {list(self.regions)}")
                return key
        for key, rc in self.regions.items():
            if rc.role == "primary":
                return key
        raise ValueError("No region has role 'primary' in config")

    def secondary_key(self, spark=None) -> str:
        primary = self.active_primary_key(spark)
        others = [k for k in self.regions if k != primary]
        if len(others) != 1:
            raise ValueError(f"Expected exactly one secondary region, got {others}")
        return others[0]

    def config_primary_key(self) -> str:
        """Region key with ``role: primary`` in config, ignoring any override.

        This is the *home* primary. Failback always returns here, regardless of
        ``DR_ACTIVE_PRIMARY``, which is why it resolves from config rather than the
        active role.
        """
        for key, rc in self.regions.items():
            if rc.role == "primary":
                return key
        raise ValueError("No region has role 'primary' in config")

    def direction(self, failback: bool = False, spark=None) -> Direction:
        """Resolve the replication direction.

        Normal/failover sync: active-primary -> secondary, into the ``primary``
        folder. ``active-primary`` is resolved via :meth:`active_primary_key`
        (env override > ``dr_state`` table > config role), so a persisted failover
        keeps the (new) secondary a warm mirror across fresh job processes.

        Failback: secondary -> home-primary, into the ``secondary`` folder. This is
        deliberately resolved from the *config* roles (the home primary), NOT the
        active role -- failback means "return changes to the original primary" even
        while ``dr_state`` still points at the promoted region. Restoring the role
        (failback writes ``dr_state`` back to home) is the final step.
        """
        storage = self.storage
        if failback:
            home = self.regions[self.config_primary_key()]
            others = [k for k in self.regions if k != home.key]
            promoted = self.regions[others[0]]
            return Direction(source=promoted, dest=home, folder=storage["secondary_folder"])
        primary = self.regions[self.active_primary_key(spark)]
        secondary = self.regions[self.secondary_key(spark)]
        return Direction(source=primary, dest=secondary, folder=storage["primary_folder"])


def load_config(path: str | None = None) -> Config:
    """Load config from an explicit path, the ``DR_CONFIG`` env var, or the default."""
    resolved = path or os.environ.get("DR_CONFIG") or DEFAULT_CONFIG_PATH
    p = Path(resolved)
    if not p.exists():
        raise FileNotFoundError(f"DR config not found: {p.resolve()}")
    with p.open() as f:
        raw = yaml.safe_load(f)
    return Config(raw, path=str(p))
