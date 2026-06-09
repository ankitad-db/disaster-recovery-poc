"""ModelsDRModule: wires the model phases into the BaseDRModule lifecycle."""

from __future__ import annotations

from ...core.base import BaseDRModule
from ...core.registry import register
from . import baseline, cdc, dependencies, failover, grants, seed
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
