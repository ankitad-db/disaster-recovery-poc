"""Shared persistence of source<->destination MLflow ID maps into ``dr_id_mapping``.

Both replication paths (the per-model pull in :mod:`.replicate` and the bulk
split-import in :mod:`.baseline`) produce engine ``ImportResult`` objects carrying
the source->dest experiment/run/version correspondence. This is the single place
that turns them into ``dr_id_mapping`` rows, so the two callers can't drift.

Non-fatal by contract: a mapping-write failure must never fail an otherwise-good
replication -- the audit table remains the source of truth, and the map is auxiliary
lineage. Failures are logged and swallowed.
"""

from __future__ import annotations

from ...common.audit import IdMappingLog, rows_from_import_result
from ...common.logging import get_logger
from ...core.base import RunContext

_logger = get_logger(__name__)


def persist(ctx: RunContext, results, audit_id: str) -> int:
    """Write ID-mapping rows for one or many ``ImportResult``s. Returns rows written.

    ``results`` may be a single result or an iterable of them. Never raises.
    """
    if results is None:
        return 0
    if not isinstance(results, (list, tuple)):
        results = [results]
    results = [r for r in results if r is not None]
    if not results:
        return 0

    audit = ctx.audit
    try:
        mlog = IdMappingLog(
            ctx.cfg.mapping_table,
            spark=getattr(audit, "spark", None),
            workspace_client=getattr(audit, "wc", None),
            warehouse_id=getattr(audit, "warehouse_id", None),
        )
        rows = []
        for result in results:
            rows.extend(rows_from_import_result(
                result,
                direction_label=ctx.direction.label,
                source_workspace=ctx.direction.source.workspace,
                target_workspace=ctx.direction.dest.workspace,
                audit_id=audit_id,
            ))
        return mlog.insert_many(rows)
    except Exception as e:  # noqa: BLE001 - mapping is auxiliary, never fatal
        _logger.warning("id-mapping persist skipped: %s", e)
        return 0
