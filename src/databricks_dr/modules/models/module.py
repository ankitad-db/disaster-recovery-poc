"""ModelsDRModule: wires the model phases into the BaseDRModule lifecycle."""

from __future__ import annotations

from ...common import storage
from ...core.base import BaseDRModule
from ...core.registry import register
from . import baseline, cdc, dependencies, failover, grants, replicate, seed
from ._selection import resolve_models


@register
class ModelsDRModule(BaseDRModule):
    """DR for Unity Catalog models and everything a consumer needs around them."""

    object_type = "models"

    def seed(self) -> None:
        # Seeds the active primary registry. Requires an ML cluster (notebook/job).
        model_name = seed.seed_primary(self.cfg)
        self.log.info("Seeded primary with %s", model_name)

    def baseline(self) -> None:
        baseline.run_baseline(self.ctx)
        if self.cfg.models.get("replicate_grants", False):
            grants.replicate_grants(self.ctx)

    def replicate(self) -> None:
        """Recommended: pull from remote source into local dest (one job, no bridge)."""
        replicate.run_replicate(self.ctx, full=True)
        if self.cfg.models.get("replicate_grants", False):
            # Grants mirroring still uses CLI profiles and needs the cross-workspace
            # ambient treatment; keep it non-fatal so it never fails a good replication.
            try:
                grants.replicate_grants(self.ctx)
            except Exception as e:  # noqa: BLE001
                self.log.warning("Grants replication skipped (not yet cross-workspace aware): %s", e)

    # Split-workflow phases (each runs in a single workspace) ------------------
    def export(self) -> None:
        """Run in the PRIMARY: export models to the source bucket."""
        rel = baseline.run_export(self.ctx, full=True)
        self.log.info("Export complete -> %s", rel)

    def import_(self) -> None:
        """Run in the SECONDARY: import models from the dest bucket."""
        baseline.run_import(self.ctx)
        if self.cfg.models.get("replicate_grants", False):
            grants.replicate_grants(self.ctx)

    def bridge(self) -> None:
        """Sync source bucket -> dest bucket (laptop/CI; prefer S3 CRR in prod)."""
        storage.bridge_prefix(self.cfg, self.ctx.direction, dry_run=self.ctx.dry_run)
        self.log.info("Bridge complete for %s", self.ctx.direction.label)

    def cdc(self) -> None:
        cdc.run_cdc(self.ctx)
        if self.cfg.models.get("replicate_grants", False):
            grants.replicate_grants(self.ctx)

    def validate(self) -> None:
        for model in resolve_models(self.ctx.direction.source.registry_uri,
                                    self.cfg.models.get("include", [])):
            dependencies.replicate_dependencies(self.ctx, model)
        self.log.info("Validation/dependency pass complete.")

    def failover(self) -> None:
        failover.run_failover(self.ctx)

    def failback(self) -> None:
        failover.run_failback(self.ctx)
