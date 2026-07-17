"""BaseDRModule: the contract every object-type DR module implements.

A new object type (Genie, Apps, Vector Search, Secrets, Volumes) becomes a DR
module by subclassing this and registering it. The CLI, config, audit table,
storage bridge, and direction/failover logic are reused unchanged.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from ..common.audit import AuditLog
from ..common.config import Config, Direction
from ..common.logging import get_logger


@dataclass
class RunContext:
    """Everything a module action needs for one run."""

    cfg: Config
    direction: Direction
    audit: AuditLog
    triggered_by: str = "MANUAL"
    dry_run: bool = False
    spark: Optional[object] = None  # set inside Databricks notebooks/jobs
    dbutils: Optional[object] = None  # for reading cross-workspace secret scopes
    force: bool = False  # override failover/failback readiness gates (true disaster)


class BaseDRModule(ABC):
    """Lifecycle every DR module supports."""

    #: unique object-type key, e.g. "models", "genie", "apps"
    object_type: str = "base"

    def __init__(self, ctx: RunContext):
        self.ctx = ctx
        self.cfg = ctx.cfg
        self.log = get_logger(f"databricks_dr.{self.object_type}")

    @abstractmethod
    def seed(self) -> None:
        """Populate the primary with test material (POC)."""

    @abstractmethod
    def baseline(self) -> None:
        """One-time full export -> bridge -> import."""

    @abstractmethod
    def cdc(self) -> None:
        """Incremental delta sync (idempotent via watermark)."""

    @abstractmethod
    def failover(self) -> None:
        """Promote the secondary to active."""

    @abstractmethod
    def failback(self) -> None:
        """Resync to the original primary and restore roles."""

    # Optional hooks (override as needed)
    def validate(self) -> None:
        """Verify replication fidelity on the destination."""
        self.log.info("validate() not implemented for %s", self.object_type)

    def health(self) -> None:
        """Drift/failure detection; should raise on problems so a job task fails."""
        self.log.info("health() not implemented for %s", self.object_type)
