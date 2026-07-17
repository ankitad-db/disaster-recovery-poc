"""Control-table access for secrets DR (audit + inventory).

Reuses the shared :class:`SqlExecutor` (Spark on-cluster, or the SDK
statement-execution API off-cluster) so the same code writes the Delta control
tables from a notebook, a job, or locally against a SQL warehouse.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ...common.logging import get_logger
from ...common.sql import SqlExecutor, lit, rows, scalar

_logger = get_logger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def record_audit(
    ex: SqlExecutor, table: str, *, operation: str, status: str,
    direction: str = "", scope: str = "*", item_count: int = 0,
    bundle_id: str = "", duration_sec: float = 0.0, detail: str = "", actor: str = "",
) -> str:
    """Append one operation row to dr_secrets_audit. Returns the audit_id."""
    audit_id = uuid.uuid4().hex
    if not ex.available:
        _logger.warning("No SQL backend; skipping audit row (%s %s scope=%s)", operation, status, scope)
        return audit_id
    ex.execute(
        f"INSERT INTO {table} (audit_id, event_time, operation, direction, scope, "
        f"item_count, status, bundle_id, duration_sec, detail, actor) VALUES ("
        f"{lit(audit_id)}, current_timestamp(), {lit(operation)}, {lit(direction)}, "
        f"{lit(scope)}, {lit(int(item_count))}, {lit(status)}, {lit(bundle_id)}, "
        f"{lit(float(duration_sec))}, {lit(detail[:1000] if detail else '')}, {lit(actor)})"
    )
    return audit_id


def load_inventory(ex: SqlExecutor, table: str) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """Return {(scope, key) -> row} of the current desired-state snapshot."""
    if not ex.available:
        return {}
    cols = ["scope", "secret_key", "value_hash", "acl_signature",
            "source_last_updated", "last_synced_at", "bundle_id", "status"]
    res = ex.execute(f"SELECT {', '.join(cols)} FROM {table}")
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in rows(res, cols):
        out[(r["scope"], r["secret_key"])] = r
    return out


def upsert_inventory(ex: SqlExecutor, table: str, records: List[Dict[str, Any]]) -> int:
    """MERGE per-secret desired state. ``records`` carry scope/secret_key + fields."""
    if not ex.available or not records:
        return 0
    n = 0
    for rec in records:
        vals = {
            "scope": rec["scope"],
            "secret_key": rec["secret_key"],
            "value_hash": rec.get("value_hash"),
            "acl_signature": rec.get("acl_signature"),
            "source_last_updated": rec.get("source_last_updated"),
            "last_synced_at": rec.get("last_synced_at") or _now(),
            "bundle_id": rec.get("bundle_id"),
            "status": rec.get("status", "IN_SYNC"),
            "updated_at": _now(),
        }
        set_clause = ", ".join(f"t.{k} = s.{k}" for k in vals if k not in ("scope", "secret_key"))
        select_cols = ", ".join(f"{_ts_or_lit(k, v)} AS {k}" for k, v in vals.items())
        ex.execute(
            f"MERGE INTO {table} t USING (SELECT {select_cols}) s "
            f"ON t.scope = s.scope AND t.secret_key = s.secret_key "
            f"WHEN MATCHED THEN UPDATE SET {set_clause} "
            f"WHEN NOT MATCHED THEN INSERT *"
        )
        n += 1
    return n


def last_export_watermark(ex: SqlExecutor, audit_table: str) -> Optional[str]:
    """Most recent successful EXPORT event_time (ISO string) or None."""
    if not ex.available:
        return None
    res = ex.execute(
        f"SELECT max(event_time) FROM {audit_table} "
        f"WHERE operation = 'EXPORT' AND status = 'SUCCESS'"
    )
    v = scalar(res)
    return str(v) if v is not None else None


# Timestamp-typed columns need a CAST so the MERGE source column types line up.
_TS_COLS = {"source_last_updated", "last_synced_at", "updated_at"}


def _ts_or_lit(col: str, v: Any) -> str:
    if col in _TS_COLS:
        return "NULL" if v is None else f"CAST({lit(v)} AS TIMESTAMP)"
    return lit(v)
