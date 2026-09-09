"""SQL access layer over the dr_recon.control tables via the SQL warehouse.

All reads and writes go through the app service principal using the
Statement Execution API on warehouse DR_WAREHOUSE_ID.
"""
from typing import Any

from databricks.sdk.service.sql import StatementParameterListItem, StatementState

from .config import SCHEMA, WAREHOUSE_ID, get_workspace_client


def run_sql(statement: str, params: list[dict] | None = None) -> list[dict[str, Any]]:
    """Execute SQL and return rows as list of dicts. Raises on error."""
    w = get_workspace_client()
    sdk_params = (
        [StatementParameterListItem(name=p["name"], value=p.get("value")) for p in params]
        if params else None
    )
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        parameters=sdk_params,
        wait_timeout="50s",
    )
    state = resp.status.state if resp.status else None
    if state != StatementState.SUCCEEDED:
        msg = ""
        if resp.status and resp.status.error:
            msg = resp.status.error.message or ""
        raise RuntimeError(f"SQL failed ({state}): {msg}")

    if not resp.manifest or not resp.manifest.schema or not resp.result:
        return []
    cols = [c.name for c in (resp.manifest.schema.columns or [])]
    data = resp.result.data_array or []
    return [dict(zip(cols, row)) for row in data]


def run_sql_raw(statement: str) -> dict:
    """Execute SQL returning columns + rows (for Genie-style tabular payloads)."""
    w = get_workspace_client()
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=statement, wait_timeout="50s"
    )
    if not resp.manifest or not resp.result:
        return {"columns": [], "rows": []}
    cols = [c.name for c in (resp.manifest.schema.columns or [])]
    return {"columns": cols, "rows": resp.result.data_array or []}


# ---- Reconciliation reads -------------------------------------------------

def latest_run() -> dict | None:
    rows = run_sql(
        f"""SELECT run_id, run_ts, failover_group, effective_primary_region,
                   rpo_lag_ms, rpo_target_ms, readiness, objects_in_scope,
                   objects_ok, objects_attention, blocking_errors,
                   replication_state, mode
            FROM {SCHEMA}.dr_recon_runs
            ORDER BY run_ts DESC LIMIT 1"""
    )
    return rows[0] if rows else None


def run_history(limit: int = 30) -> list[dict]:
    return run_sql(
        f"""SELECT run_id, run_ts, rpo_lag_ms, rpo_target_ms, readiness,
                   objects_in_scope, objects_ok, objects_attention, blocking_errors
            FROM {SCHEMA}.dr_recon_runs
            ORDER BY run_ts ASC LIMIT {int(limit)}"""
    )


def coverage(run_id: str) -> list[dict]:
    return run_sql(
        f"""SELECT object_type, status, cnt
            FROM {SCHEMA}.dr_recon_coverage
            WHERE run_id = :rid
            ORDER BY object_type, status""",
        params=[{"name": "rid", "value": run_id}],
    )


def inventory(run_id: str) -> list[dict]:
    return run_sql(
        f"""SELECT object_type, asset_key, fqn, in_scope, status, severity,
                   primary_sig, secondary_sig, detail, last_reconciled
            FROM {SCHEMA}.dr_recon_inventory
            WHERE run_id = :rid
            ORDER BY
              CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'HIGH' THEN 1
                            WHEN 'MEDIUM' THEN 2 ELSE 3 END,
              object_type, fqn""",
        params=[{"name": "rid", "value": run_id}],
    )


def findings(run_id: str) -> list[dict]:
    return run_sql(
        f"""SELECT f.run_id, f.object_type, f.fqn, f.drift_kind, f.error_class,
                   f.detail, f.severity, f.first_seen,
                   a.ack_by, a.ack_ts, a.note AS ack_note
            FROM {SCHEMA}.dr_recon_findings f
            LEFT JOIN {SCHEMA}.dr_recon_ack a ON a.fqn = f.fqn
            WHERE f.run_id = :rid
            ORDER BY
              CASE f.severity WHEN 'CRITICAL' THEN 0 WHEN 'HIGH' THEN 1
                              WHEN 'MEDIUM' THEN 2 ELSE 3 END,
              f.object_type, f.fqn""",
        params=[{"name": "rid", "value": run_id}],
    )


def audit_feed(limit: int = 100) -> list[dict]:
    return run_sql(
        f"""SELECT event_time, run_id, object_type, fqn, change_type,
                   prev_status, new_status, prev_sig, new_sig, direction, detail
            FROM {SCHEMA}.dr_recon_audit
            ORDER BY event_time DESC LIMIT {int(limit)}"""
    )


def events(limit: int = 50) -> list[dict]:
    return run_sql(
        f"""SELECT event_time, event_type, direction, from_region, to_region,
                   trigger, duration_sec, data_loss_window_ms, objects_reconciled,
                   objects_total, outcome, detail
            FROM {SCHEMA}.dr_recon_events
            ORDER BY event_time DESC LIMIT {int(limit)}"""
    )


def acks() -> list[dict]:
    return run_sql(
        f"SELECT fqn, ack_by, ack_ts, note FROM {SCHEMA}.dr_recon_ack ORDER BY ack_ts DESC"
    )


# ---- Writes ---------------------------------------------------------------

def ensure_ack_table() -> None:
    run_sql(
        f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.dr_recon_ack (
                fqn STRING,
                ack_by STRING,
                ack_ts TIMESTAMP,
                note STRING
            ) USING DELTA"""
    )


def acknowledge(fqn: str, ack_by: str, note: str) -> None:
    ensure_ack_table()
    # Upsert: latest ack per fqn wins.
    run_sql(
        f"DELETE FROM {SCHEMA}.dr_recon_ack WHERE fqn = :fqn",
        params=[{"name": "fqn", "value": fqn}],
    )
    run_sql(
        f"""INSERT INTO {SCHEMA}.dr_recon_ack (fqn, ack_by, ack_ts, note)
            VALUES (:fqn, :ack_by, current_timestamp(), :note)""",
        params=[
            {"name": "fqn", "value": fqn},
            {"name": "ack_by", "value": ack_by},
            {"name": "note", "value": note or ""},
        ],
    )


def unacknowledge(fqn: str) -> None:
    run_sql(
        f"DELETE FROM {SCHEMA}.dr_recon_ack WHERE fqn = :fqn",
        params=[{"name": "fqn", "value": fqn}],
    )
