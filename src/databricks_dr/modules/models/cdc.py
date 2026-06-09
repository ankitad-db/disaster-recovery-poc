"""Phase 4c: incremental, watermark-driven CDC sync.

For each in-scope model, find source versions newer than the audit watermark and
replicate only those (version-grained export -> bridge -> import). No
``--export-all-runs`` here -- only deltas move. After import, re-map aliases and
mirror serving-endpoint config drift.
"""

from __future__ import annotations

import time
from typing import List

from ...common import engine, storage
from ...common.audit import AuditRow
from ...common.clients import make_mlflow_client
from ...common.logging import get_logger
from ...core.base import RunContext
from ._selection import resolve_models
from . import endpoints

_logger = get_logger(__name__)


def run_cdc(ctx: RunContext) -> None:
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    backend = cfg.engine_backend
    mcfg = cfg.models
    models = resolve_models(direction.source.registry_uri, mcfg.get("include", []))
    src_client = make_mlflow_client(direction.source.registry_uri)
    dst_client = make_mlflow_client(direction.dest.registry_uri)

    for model in models:
        watermark = audit.watermark(model)
        new_versions = _versions_above(src_client, model, watermark)
        if not new_versions:
            _logger.info("%s: up to date (watermark=%s)", model, watermark)
            continue
        _logger.info("%s: %d new version(s) above watermark %s", model, len(new_versions), watermark)
        for version in new_versions:
            _sync_one_version(ctx, model, version, backend)

        if mcfg.get("replicate_serving_endpoints", False) and not ctx.dry_run:
            endpoints.mirror_for_model(ctx, model)

        _remap_aliases(ctx, src_client, dst_client, model)


def _sync_one_version(ctx: RunContext, model: str, version: str, backend: str) -> None:
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    ts = storage.new_timestamp()
    rel = storage.rel_export_dir(cfg, direction, ts, model_name=f"{model}_v{version}")
    out_dir = storage.dbfs_path(rel)

    row = AuditRow(
        operation="EXPORT", direction=direction.label, model_name=model,
        source_version=str(version), triggered_by=ctx.triggered_by, export_dir=out_dir,
        tool_version=engine.engine_version(), actor=cfg.service_principal,
    )
    audit.insert(row)
    t0 = time.time()
    try:
        if not ctx.dry_run:
            engine.export_model_version(
                model=model, version=str(version), output_dir=out_dir,
                registry_uri=direction.source.registry_uri, backend=backend,
                export_version_model=cfg.models.get("export_version_model", True),
            )
            storage.bridge(cfg, direction, rel)
        audit.update_status(row.audit_id, "SUCCESS", duration_sec=round(time.time() - t0, 2))
    except Exception as e:  # noqa: BLE001
        audit.update_status(row.audit_id, "FAILED", error_message=str(e))
        raise

    irow = AuditRow(
        operation="IMPORT", direction=direction.label, model_name=model,
        source_version=str(version), triggered_by=ctx.triggered_by,
        export_dir=out_dir, tool_version=engine.engine_version(), actor=cfg.service_principal,
    )
    audit.insert(irow)
    t1 = time.time()
    try:
        target_version = None
        if not ctx.dry_run:
            target_version = engine.import_model_version(
                model=model, input_dir=out_dir,
                registry_uri=direction.dest.registry_uri, backend=backend, create_model=True,
            )
        audit.update_status(irow.audit_id, "SUCCESS", target_version=target_version,
                            duration_sec=round(time.time() - t1, 2))
    except Exception as e:  # noqa: BLE001
        audit.update_status(irow.audit_id, "FAILED", error_message=str(e))
        raise


def _versions_above(client, model: str, watermark: int) -> List[str]:
    try:
        versions = client.search_model_versions(f"name='{model}'")
    except Exception as e:  # noqa: BLE001
        _logger.error("Cannot list versions for %s: %s", model, e)
        return []
    nums = sorted(int(v.version) for v in versions if int(v.version) > watermark)
    return [str(n) for n in nums]


def _remap_aliases(ctx: RunContext, src_client, dst_client, model: str) -> None:
    """Point destination aliases at the version with the matching source version tag.

    Import tags carry source lineage; here we best-effort copy aliases by version
    number (works when version numbers are preserved). Logged as VERIFY.
    """
    if ctx.dry_run:
        return
    try:
        src_mv = src_client.get_registered_model(model)
        aliases = getattr(src_mv, "aliases", {}) or {}
        for alias, src_version in aliases.items():
            try:
                dst_client.set_registered_model_alias(model, alias, str(src_version))
                _logger.info("%s: alias @%s -> v%s (dest)", model, alias, src_version)
            except Exception as e:  # noqa: BLE001
                _logger.warning("%s: could not set alias @%s: %s", model, alias, e)
    except Exception as e:  # noqa: BLE001
        _logger.warning("%s: alias re-map skipped: %s", model, e)
