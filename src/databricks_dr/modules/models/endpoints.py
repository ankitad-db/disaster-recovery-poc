"""Model-serving endpoint DR.

Serving endpoints are not carried by mlflow-export-import, so a consumer hitting a
REST endpoint would have nothing to call after failover. This module makes endpoint
failover real, using the same cross-workspace identity pattern as the rest of the
framework (run in the LOCAL/dest workspace; read the remote source via secret-scope
creds; apply with the local ambient identity):

  * ``mirror_endpoints`` -- steady state (called from replicate/cdc). For every
    in-scope model, find the source endpoints serving it and create/update a
    matching endpoint on the dest in a **standby** posture (scale-to-zero) so the
    same route exists and is cheap to keep warm.
  * ``activate_endpoints`` -- failover (called from run_failover). Scale the dest
    endpoints for in-scope models **up** (disable scale-to-zero) so they serve.

Every action writes an ENDPOINT audit row.
"""

from __future__ import annotations

from ...common.audit import AuditRow
from ...common.clients import (
    is_databricks_runtime,
    workspace_client,
    workspace_client_from_creds,
)
from ...common.logging import get_logger
from ...core.base import RunContext
from . import replicate
from ._selection import resolve_models

_logger = get_logger(__name__)


def mirror_endpoints(ctx: RunContext, *, standby: bool = True) -> int:
    """Mirror source serving endpoints for in-scope models onto the dest (standby).

    Returns the number of endpoints mirrored. Cross-workspace aware.
    """
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    src, dst = _endpoint_clients(ctx, need_source=True)
    models = resolve_models(replicate.LOCAL_REGISTRY, cfg.models.get("include", []))

    mirrored = 0
    for model in models:
        for ep in _endpoints_serving(src, model):
            row = AuditRow(operation="ENDPOINT", direction=direction.label, model_name=model,
                           triggered_by=ctx.triggered_by, actor=cfg.service_principal,
                           error_message=f"mirror {ep.name} (standby={standby})")
            audit.insert(row)
            try:
                if not ctx.dry_run:
                    _upsert_endpoint(dst, ep, scale_to_zero=standby)
                audit.update_status(row.audit_id, "SUCCESS")
                mirrored += 1
            except Exception as e:  # noqa: BLE001
                audit.update_status(row.audit_id, "FAILED", error_message=str(e))
                _logger.warning("Endpoint mirror failed for %s: %s", ep.name, e)
    _logger.info("Mirrored %d serving endpoint(s) to %s (standby).", mirrored, direction.dest.region)
    return mirrored


def activate_endpoints(ctx: RunContext) -> int:
    """Failover: scale up dest endpoints serving in-scope models. Returns count.

    Only needs the LOCAL (dest) workspace -- the source may be down during failover.
    """
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    _, dst = _endpoint_clients(ctx, need_source=False)
    models = set(resolve_models(replicate.LOCAL_REGISTRY, cfg.models.get("include", [])))

    activated = 0
    for ep in _endpoints_for_models(dst, models):
        row = AuditRow(operation="ENDPOINT", direction=direction.label, model_name="*",
                       triggered_by=ctx.triggered_by, actor=cfg.service_principal,
                       error_message=f"activate {ep.name}")
        audit.insert(row)
        try:
            if not ctx.dry_run:
                _upsert_endpoint(dst, ep, scale_to_zero=False)
            audit.update_status(row.audit_id, "SUCCESS")
            activated += 1
        except Exception as e:  # noqa: BLE001
            audit.update_status(row.audit_id, "FAILED", error_message=str(e))
            _logger.warning("Endpoint activate failed for %s: %s", ep.name, e)
    _logger.info("Activated %d serving endpoint(s) in %s.", activated, direction.dest.region)
    return activated


def _endpoint_clients(ctx: RunContext, *, need_source: bool):
    """Return (source, dest) WorkspaceClients; source is None when not needed."""
    direction = ctx.direction
    if is_databricks_runtime():
        dest = workspace_client_from_creds()  # local ambient (dest)
        if not need_source:
            return None, dest
        host, token = replicate._remote_creds(ctx)
        return workspace_client_from_creds(host, token), dest
    # Off-cluster: configured CLI profiles.
    source = workspace_client(direction.source.profile) if need_source else None
    return source, workspace_client(direction.dest.profile)


def _served_entities(ep):
    cfgobj = getattr(ep, "config", None)
    return getattr(cfgobj, "served_entities", None) or getattr(cfgobj, "served_models", None) or []


def _entity_name(served) -> str | None:
    return getattr(served, "entity_name", None) or getattr(served, "model_name", None)


def _endpoints_serving(wc, model: str):
    """Source endpoints whose served entity is ``model``."""
    out = []
    try:
        for ep in wc.serving_endpoints.list():
            if any(_entity_name(s) == model for s in _served_entities(ep)):
                out.append(ep)
    except Exception as e:  # noqa: BLE001
        _logger.warning("Could not list source serving endpoints: %s", e)
    return out


def _endpoints_for_models(wc, models: set[str]):
    """Dest endpoints serving any in-scope model."""
    out = []
    try:
        for ep in wc.serving_endpoints.list():
            if any(_entity_name(s) in models for s in _served_entities(ep)):
                out.append(ep)
    except Exception as e:  # noqa: BLE001
        _logger.warning("Could not list dest serving endpoints: %s", e)
    return out


def _upsert_endpoint(dst, source_ep, *, scale_to_zero: bool) -> None:
    """Create or update an endpoint on the dest mirroring ``source_ep``'s served models.

    ``scale_to_zero`` controls the cost posture: True = standby (steady state),
    False = active (failover). Model versions match across registries because
    mlflow-export-import preserves version numbers.
    """
    from databricks.sdk.service.serving import (
        EndpointCoreConfigInput,
        ServedEntityInput,
    )

    name = source_ep.name
    served = []
    for s in _served_entities(source_ep):
        served.append(ServedEntityInput(
            entity_name=_entity_name(s),
            entity_version=getattr(s, "entity_version", None) or getattr(s, "model_version", None),
            workload_size=getattr(s, "workload_size", None) or "Small",
            scale_to_zero_enabled=scale_to_zero,
        ))
    if not served:
        _logger.warning("Endpoint %s has no served entities to mirror; skipping.", name)
        return

    existing = {e.name for e in dst.serving_endpoints.list()}
    posture = "standby" if scale_to_zero else "active"
    _logger.info("%s serving endpoint %s (%s)", "Updating" if name in existing else "Creating", name, posture)
    if name in existing:
        dst.serving_endpoints.update_config(name=name, served_entities=served)
    else:
        dst.serving_endpoints.create(name=name, config=EndpointCoreConfigInput(name=name, served_entities=served))
