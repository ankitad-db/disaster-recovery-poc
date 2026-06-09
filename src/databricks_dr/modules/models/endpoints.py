"""Serving endpoint mirroring.

Serving endpoints are not exported by mlflow-export-import. To make failover real
for a consumer hitting a REST endpoint, we mirror the endpoint's served-model
config (model name + version/alias + scale) onto the destination so the same
URL/route exists there. By default we create endpoints in a stopped/min-scale
posture to control cost; failover scales them up.
"""

from __future__ import annotations

from ...common.audit import AuditRow
from ...common.clients import workspace_client
from ...common.logging import get_logger
from ...core.base import RunContext

_logger = get_logger(__name__)


def mirror_for_model(ctx: RunContext, model: str) -> None:
    """Mirror any source serving endpoints that serve ``model`` onto the dest."""
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    src = workspace_client(direction.source.profile)
    dst = workspace_client(direction.dest.profile)

    for ep in _endpoints_serving(src, model):
        row = AuditRow(operation="ENDPOINT", direction=direction.label, model_name=model,
                       triggered_by=ctx.triggered_by, actor=cfg.service_principal,
                       error_message=None)
        audit.insert(row)
        try:
            _upsert_endpoint(dst, ep, ctx.dry_run)
            audit.update_status(row.audit_id, "SUCCESS")
        except Exception as e:  # noqa: BLE001
            audit.update_status(row.audit_id, "FAILED", error_message=str(e))
            _logger.warning("Endpoint mirror failed for %s: %s", ep.name, e)


def _endpoints_serving(wc, model: str):
    out = []
    try:
        for ep in wc.serving_endpoints.list():
            cfgobj = getattr(ep, "config", None)
            served = getattr(cfgobj, "served_entities", None) or getattr(cfgobj, "served_models", None) or []
            if any(getattr(s, "entity_name", getattr(s, "model_name", None)) == model for s in served):
                out.append(ep)
    except Exception as e:  # noqa: BLE001
        _logger.warning("Could not list serving endpoints: %s", e)
    return out


def _upsert_endpoint(dst, source_ep, dry_run: bool) -> None:
    name = source_ep.name
    _logger.info("Mirroring serving endpoint %s", name)
    if dry_run:
        return
    from databricks.sdk.service.serving import (
        EndpointCoreConfigInput,
        ServedEntityInput,
    )

    src_cfg = source_ep.config
    served_src = getattr(src_cfg, "served_entities", None) or getattr(src_cfg, "served_models", None) or []
    served = []
    for s in served_src:
        served.append(
            ServedEntityInput(
                entity_name=getattr(s, "entity_name", getattr(s, "model_name", None)),
                entity_version=getattr(s, "entity_version", getattr(s, "model_version", None)),
                workload_size="Small",
                scale_to_zero_enabled=True,  # cost-controlled standby posture
            )
        )
    core = EndpointCoreConfigInput(name=name, served_entities=served)
    existing = {e.name for e in dst.serving_endpoints.list()}
    if name in existing:
        dst.serving_endpoints.update_config(name=name, served_entities=served)
    else:
        dst.serving_endpoints.create(name=name, config=core)
