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
from .sql import SqlExecutor, lit as _sql_literal, rows as _rows, scalar as _scalar

_logger = get_logger(__name__)


@dataclass
class AuditRow:
    operation: str  # EXPORT|IMPORT|VERIFY|GRANTS|DEPENDENCY|FAILOVER|FAILBACK|HEALTH
    direction: str  # e.g. us-west-2->us-east-1
    model_name: str
    status: str = "IN_PROGRESS"  # IN_PROGRESS|SUCCESS|FAILED|SKIPPED
    triggered_by: str = "MANUAL"  # SCHEDULE|AUDIT_EVENT|MANUAL (legacy enum)
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
    # --- detailed replication tracking (schema 2) ---
    object_type: str = "model"  # model|version|run|experiment|prompt|trace|eval_dataset|logged_model|alias|grant|notebook
    action: Optional[str] = None  # CREATE|UPDATE|DELETE|ALIAS_SET|NONE
    trigger_type: Optional[str] = None  # MANUAL|SCHEDULE|AUDIT_SCAN|MODEL_TRIGGER
    source_event_id: Optional[str] = None  # system.access.audit event_id correlation
    source_event_time: Optional[str] = None  # ISO; UTC time the change happened on source
    rpo_lag_sec: Optional[float] = None  # event_time - source_event_time (recovery point lag)
    bytes_moved: Optional[int] = None  # artifact bytes transferred for this op
    src_experiment: Optional[str] = None  # source experiment name (lineage mapping)
    dst_experiment: Optional[str] = None  # destination experiment name
    retry_count: Optional[int] = 0  # attempts taken for this op
    worker: Optional[str] = None  # thread/worker label (scale visibility)
    checksum: Optional[str] = None  # integrity hash of moved artifacts (optional)
    audit_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_time: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


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
        # schema 2 (detailed tracking)
        "object_type", "action", "trigger_type", "source_event_id",
        "source_event_time", "rpo_lag_sec", "bytes_moved", "src_experiment",
        "dst_experiment", "retry_count", "worker", "checksum",
    ]

    #: columns stored as SQL TIMESTAMP (inserted via to_timestamp).
    TIMESTAMP_COLS = {"event_time", "source_event_time"}

    #: mutable columns update_status may set (guards against typo'd/injected keys).
    UPDATABLE_COLS = frozenset(COLUMNS) - {"audit_id"}

    def __init__(self, table: str, spark=None, workspace_client=None, warehouse_id: str | None = None):
        self.table = table
        self.spark = spark
        self.wc = workspace_client
        self.warehouse_id = warehouse_id
        self._sql = SqlExecutor(spark=spark, workspace_client=workspace_client, warehouse_id=warehouse_id)

    def _execute(self, sql: str):
        return self._sql.execute(sql)

    def insert(self, row: AuditRow) -> str:
        d = asdict(row)
        cols = ", ".join(self.COLUMNS)
        vals = []
        for c in self.COLUMNS:
            v = d.get(c)
            if c in self.TIMESTAMP_COLS and v is not None:
                vals.append(f"to_timestamp({_sql_literal(v)})")
            else:
                vals.append(_sql_literal(v))
        sql = f"INSERT INTO {self.table} ({cols}) VALUES ({', '.join(vals)})"
        self._execute(sql)
        _logger.info("audit %s %s %s status=%s", row.operation, row.model_name,
                     row.source_version or "", row.status)
        return row.audit_id

    def update_status(self, audit_id: str, status: str, **fields) -> None:
        sets = [f"status = {_sql_literal(status)}"]
        for k, v in fields.items():
            if k not in self.UPDATABLE_COLS:
                raise ValueError(f"update_status: unknown audit column {k!r}")
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
        wm = _scalar(self._execute(sql))
        return int(wm) if wm is not None else 0

    def last_success_time(self, model_name: str | None = None) -> Optional[str]:
        """Max event_time of a successful IMPORT/VERIFY (the effective recovery point).

        With ``model_name`` it's per-model; without, it's the newest successful sync
        across all in-scope models. Used by failover/failback to record the RPO
        (how stale the promoted mirror is) on the role-change audit row.
        """
        where = "operation IN ('IMPORT','VERIFY') AND status = 'SUCCESS'"
        if model_name:
            where += f" AND model_name = {_sql_literal(model_name)}"
        sql = f"SELECT MAX(event_time) AS ts FROM {self.table} WHERE {where}"
        ts = _scalar(self._execute(sql))
        return str(ts) if ts is not None else None

    def recent_failures(self, hours: int = 24) -> list[dict]:
        """FAILED audit rows in the last ``hours`` (newest first).

        Used by the health-check task to fail the orchestrating job (and fire
        notifications) when any replication step errored recently.
        """
        cols = ["event_time", "operation", "direction", "model_name", "error_message"]
        sql = (
            f"SELECT {', '.join(cols)} FROM {self.table} "
            f"WHERE status = 'FAILED' "
            f"AND event_time >= current_timestamp() - INTERVAL {int(hours)} HOURS "
            f"ORDER BY event_time DESC"
        )
        return _rows(self._execute(sql), cols)


@dataclass
class IdMappingRow:
    """One source<->destination MLflow ID correspondence."""

    id_type: str  # experiment|run|model_version
    model_name: Optional[str] = None
    object_name: Optional[str] = None  # stable name (e.g. experiment name)
    source_id: Optional[str] = None
    target_id: Optional[str] = None
    source_version: Optional[str] = None
    source_workspace: Optional[str] = None
    target_workspace: Optional[str] = None
    direction: Optional[str] = None
    audit_id: Optional[str] = None
    mapping_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_time: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class IdMappingLog:
    """Writes/reads the ``dr_id_mapping`` table (experiment/run/version IDs).

    Same execution plumbing as :class:`AuditLog` (spark session in a notebook, or a
    ``WorkspaceClient`` + ``warehouse_id`` from a plain runner).
    """

    COLUMNS = [
        "mapping_id", "event_time", "id_type", "model_name", "object_name",
        "source_id", "target_id", "source_version", "source_workspace",
        "target_workspace", "direction", "audit_id",
    ]
    TIMESTAMP_COLS = {"event_time"}

    def __init__(self, table: str, spark=None, workspace_client=None, warehouse_id: str | None = None):
        self.table = table
        self.spark = spark
        self.wc = workspace_client
        self.warehouse_id = warehouse_id
        self._sql = SqlExecutor(spark=spark, workspace_client=workspace_client, warehouse_id=warehouse_id)

    def _execute(self, sql: str):
        return self._sql.execute(sql)

    def _row_values(self, row: "IdMappingRow") -> str:
        d = asdict(row)
        vals = []
        for c in self.COLUMNS:
            v = d.get(c)
            if c in self.TIMESTAMP_COLS and v is not None:
                vals.append(f"to_timestamp({_sql_literal(v)})")
            else:
                vals.append(_sql_literal(v))
        return "(" + ", ".join(vals) + ")"

    def insert_many(self, rows: list["IdMappingRow"]) -> int:
        """Batch-insert mapping rows; returns the count written (0 if none)."""
        rows = [r for r in rows if r and r.source_id and r.target_id]
        if not rows:
            return 0
        cols = ", ".join(self.COLUMNS)
        values = ", ".join(self._row_values(r) for r in rows)
        self._execute(f"INSERT INTO {self.table} ({cols}) VALUES {values}")
        _logger.info("id-mapping wrote %d row(s) to %s", len(rows), self.table)
        return len(rows)


def rows_from_import_result(
    result,
    *,
    direction_label: str,
    source_workspace: str,
    target_workspace: str,
    audit_id: Optional[str] = None,
) -> list[IdMappingRow]:
    """Build ``IdMappingRow``s from an engine ``ImportResult`` (duck-typed).

    Captures experiment, run, and model-version source->target correspondences.
    """
    rows: list[IdMappingRow] = []
    model = getattr(result, "model", None)
    common = dict(model_name=model, source_workspace=source_workspace,
                  target_workspace=target_workspace, direction=direction_label,
                  audit_id=audit_id)
    dest_exp = getattr(result, "dest_experiment_id", None)

    # Source experiments collapse into the single per-model dest experiment.
    for src_exp_id, src_exp_name in (getattr(result, "source_experiments", {}) or {}).items():
        rows.append(IdMappingRow(id_type="experiment", object_name=src_exp_name,
                                 source_id=src_exp_id, target_id=dest_exp, **common))
    run_version = getattr(result, "run_version", {}) or {}
    for src_run, dst_run in (getattr(result, "run_id_map", {}) or {}).items():
        rows.append(IdMappingRow(id_type="run", source_id=src_run, target_id=dst_run,
                                 source_version=run_version.get(src_run), **common))
    for sv, dv in (getattr(result, "version_map", {}) or {}).items():
        rows.append(IdMappingRow(id_type="model_version", source_id=str(sv), target_id=str(dv),
                                 source_version=str(sv), **common))
    return rows
