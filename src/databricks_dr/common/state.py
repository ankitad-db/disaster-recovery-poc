"""Durable active-role state for orchestrated failover/failback.

A single-row control table (``dr_control.dr_state``) records which region is the
active primary *right now*. Every job run reads it, so a failover persists across
fresh job processes -- unlike the ``DR_ACTIVE_PRIMARY`` env var, which only lives
in one interactive session. The failover/failback jobs are the only writers; all
other code reads it via :meth:`Config.active_primary_key`.

Reads are best-effort: if the table does not exist yet (pre-bootstrap), callers
fall back to the config role, so nothing breaks before the DDL is applied.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from .logging import get_logger
from .sql import SqlExecutor, lit as _sql_literal, scalar as _scalar

_logger = get_logger(__name__)
_SINGLETON = "global"


def _execute(sql: str, spark=None, wc=None, warehouse_id: str | None = None):
    return SqlExecutor(spark=spark, workspace_client=wc, warehouse_id=warehouse_id).execute(sql)


def read_active_primary(table: str, *, spark=None, wc=None, warehouse_id: str | None = None) -> Optional[str]:
    """Active-primary region key from the control table, or None if unavailable."""
    sql = f"SELECT active_primary FROM {table} WHERE singleton_id = {_sql_literal(_SINGLETON)}"
    try:
        val = _scalar(_execute(sql, spark, wc, warehouse_id))
        return str(val) if val is not None else None
    except Exception as e:  # noqa: BLE001 - table may not exist yet; caller falls back to config role
        _logger.debug("dr_state read failed (%s); falling back to config role", e)
        return None


def set_active_primary(
    table: str,
    key: str,
    *,
    reason: str | None = None,
    actor: str | None = None,
    spark=None,
    wc=None,
    warehouse_id: str | None = None,
) -> None:
    """Persist the active-primary role (single-row upsert). Writers: failover/failback."""
    now = datetime.now(timezone.utc).isoformat()
    sql = (
        f"MERGE INTO {table} t USING ("
        f"SELECT {_sql_literal(_SINGLETON)} AS singleton_id, "
        f"{_sql_literal(key)} AS active_primary, "
        f"to_timestamp({_sql_literal(now)}) AS updated_at, "
        f"{_sql_literal(actor)} AS updated_by, "
        f"{_sql_literal(reason)} AS reason) s "
        f"ON t.singleton_id = s.singleton_id "
        f"WHEN MATCHED THEN UPDATE SET * "
        f"WHEN NOT MATCHED THEN INSERT *"
    )
    _execute(sql, spark, wc, warehouse_id)
    _logger.info("dr_state active_primary -> %s (reason=%s)", key, reason)
