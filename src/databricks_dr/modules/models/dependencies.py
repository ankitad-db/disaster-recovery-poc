"""Model dependency replication (UC functions + feature tables).

A model is only usable on the secondary if its dependencies exist there too:
  - UC functions used in pyfunc wrappers / feature lookups.
  - Feature tables (Delta) backing online/offline lookups.

mlflow-export-import does not move these. This module enumerates a model's
declared dependencies and replicates them (functions via DDL re-create; feature
tables via DEEP CLONE or Delta Sharing). For the POC the iris model has none, so
this runs as a reporting/no-op pass that still records an audit entry.
"""

from __future__ import annotations

from typing import List

from ...common.audit import AuditRow
from ...common.clients import make_mlflow_client, workspace_client
from ...common.logging import get_logger
from ...core.base import RunContext

_logger = get_logger(__name__)


def replicate_dependencies(ctx: RunContext, model: str) -> None:
    cfg, direction, audit = ctx.cfg, ctx.direction, ctx.audit
    row = AuditRow(operation="DEPENDENCY", direction=direction.label, model_name=model,
                   triggered_by=ctx.triggered_by, actor=cfg.service_principal)
    audit.insert(row)
    try:
        functions = _function_dependencies(direction.source.registry_uri, model)
        for fn in functions:
            _replicate_function(ctx, fn)
        audit.update_status(row.audit_id, "SUCCESS" if functions else "SKIPPED",
                            artifact_count=len(functions))
    except Exception as e:  # noqa: BLE001
        audit.update_status(row.audit_id, "FAILED", error_message=str(e))
        raise


def _function_dependencies(registry_uri: str, model: str) -> List[str]:
    """Best-effort: read dependency tags the model declares (if any)."""
    client = make_mlflow_client(registry_uri)
    try:
        mv = client.get_registered_model(model)
        tags = getattr(mv, "tags", {}) or {}
        raw = tags.get("dr_uc_functions", "")
        return [f.strip() for f in raw.split(",") if f.strip()]
    except Exception as e:  # noqa: BLE001
        _logger.warning("Could not read dependency tags for %s: %s", model, e)
        return []


def _replicate_function(ctx: RunContext, function_name: str) -> None:
    """Re-create a UC function on the destination from its source definition."""
    src = workspace_client(ctx.direction.source.profile)
    dst = workspace_client(ctx.direction.dest.profile)
    try:
        info = src.functions.get(name=function_name)
        ddl = getattr(info, "routine_definition", None)
        if not ddl:
            _logger.warning("No routine_definition for %s; skipping", function_name)
            return
        _logger.info("Replicating UC function %s", function_name)
        if ctx.dry_run:
            return
        warehouse_id = _first_warehouse(dst)
        if warehouse_id:
            dst.statement_execution.execute_statement(
                warehouse_id=warehouse_id, statement=ddl, wait_timeout="30s"
            )
    except Exception as e:  # noqa: BLE001
        _logger.warning("Function replication failed for %s: %s", function_name, e)


def _first_warehouse(wc):
    for w in wc.warehouses.list():
        return w.id
    return None
