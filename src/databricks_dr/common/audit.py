"""Audit + watermark.

``dr_replication_audit`` is the source of truth for what was replicated, when,
and with what outcome. The per-model watermark (max successfully-synced source
version) is what makes CDC idempotent and incremental.

Writes use the Databricks SDK statement-execution API so this works from a plain
runner; inside a Databricks notebook you can pass a ``spark`` session instead.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional

from .logging import get_logger

_logger = get_logger(__name__)


@dataclass
class AuditRow:
    operation: str  # EXPORT|IMPORT|VERIFY|GRANTS|ENDPOINT|DEPENDENCY|FAILOVER
    direction: str  # e.g. us-west-2->us-east-1
    model_name: str
    status: str = "IN_PROGRESS"  # IN_PROGRESS|SUCCESS|FAILED|SKIPPED
    triggered_by: str = "MANUAL"  # SCHEDULE|AUDIT_EVENT|MANUAL
    source_version: Optional[str] = None
    target_version: Optional[str] = None
    source_run_id: Optional[str] = None
    target_run_id: Optional[str] = None
    experiment_name: Optional[str] = None
    export_dir: Optional[str] = None
    manifest_path: Optional[str] = None
    artifact_count: Optional[int] = None
    duration_sec: Optional[float] = None
    error_message: Optional[str] = None
    tool_version: Optional[str] = None
    actor: Optional[str] = None
    audit_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_time: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


def _sql_literal(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


class AuditLog:
    """Writes/reads the audit table.

    Provide either a ``spark`` session (notebook) or a ``warehouse_id`` +
    ``WorkspaceClient`` (runner) for statement execution.
    """

    COLUMNS = [
        "audit_id", "event_time", "operation", "direction", "triggered_by",
        "model_name", "source_version", "target_version", "source_run_id",
        "target_run_id", "experiment_name", "export_dir", "manifest_path",
        "artifact_count", "status", "duration_sec", "error_message",
        "tool_version", "actor",
    ]

    def __init__(self, table: str, spark=None, workspace_client=None, warehouse_id: str | None = None):
        self.table = table
        self.spark = spark
        self.wc = workspace_client
        self.warehouse_id = warehouse_id

    def _execute(self, sql: str):
        if self.spark is not None:
            return self.spark.sql(sql)
        if self.wc is not None and self.warehouse_id:
            return self.wc.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id, statement=sql, wait_timeout="30s"
            )
        _logger.warning("AuditLog has no spark/warehouse; SQL not executed:\n%s", sql)
        return None

    def insert(self, row: AuditRow) -> str:
        d = asdict(row)
        cols = ", ".join(self.COLUMNS)
        ts_expr = f"to_timestamp({_sql_literal(d['event_time'])})"
        vals = []
        for c in self.COLUMNS:
            vals.append(ts_expr if c == "event_time" else _sql_literal(d.get(c)))
        sql = f"INSERT INTO {self.table} ({cols}) VALUES ({', '.join(vals)})"
        self._execute(sql)
        _logger.info("audit %s %s %s status=%s", row.operation, row.model_name,
                     row.source_version or "", row.status)
        return row.audit_id

    def update_status(self, audit_id: str, status: str, **fields) -> None:
        sets = [f"status = {_sql_literal(status)}"]
        for k, v in fields.items():
            sets.append(f"{k} = {_sql_literal(v)}")
        sql = f"UPDATE {self.table} SET {', '.join(sets)} WHERE audit_id = {_sql_literal(audit_id)}"
        self._execute(sql)

    def watermark(self, model_name: str) -> int:
        """Max successfully-synced source version for a model (0 if none)."""
        sql = (
            f"SELECT MAX(CAST(source_version AS INT)) AS wm FROM {self.table} "
            f"WHERE model_name = {_sql_literal(model_name)} AND operation IN ('IMPORT','VERIFY') "
            f"AND status = 'SUCCESS'"
        )
        res = self._execute(sql)
        wm = _extract_scalar(res)
        return int(wm) if wm is not None else 0


def _extract_scalar(res) -> Optional[int]:
    """Best-effort scalar extraction across spark DataFrame and SDK responses."""
    if res is None:
        return None
    # Spark DataFrame
    if hasattr(res, "collect"):
        rows = res.collect()
        if rows and rows[0][0] is not None:
            return rows[0][0]
        return None
    # Databricks SDK StatementResponse
    try:
        data = res.result.data_array
        if data and data[0] and data[0][0] is not None:
            return int(data[0][0])
    except (AttributeError, IndexError, TypeError):
        pass
    return None
