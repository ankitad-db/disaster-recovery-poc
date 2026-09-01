"""Single SQL execution + result-extraction path for the DR control plane.

Every control table (``dr_replication_audit``, ``dr_id_mapping``,
``dr_object_inventory``, ``dr_state``) runs SQL the same way:

  * a **Spark session** when the code runs on a cluster/notebook, else
  * the **Databricks SDK statement-execution API** with a SQL warehouse (off-cluster
    runner), else
  * a logged **no-op** (nothing wired -- e.g. a bare CLI invocation) so a missing
    execution backend degrades loudly-in-logs rather than crashing.

Centralising it here removes four near-identical copies of the dispatch +
literal-escaping + result-parsing logic and guarantees they stay consistent.
"""

from __future__ import annotations

from typing import Any, List, Optional

from .logging import get_logger

_logger = get_logger(__name__)


def lit(v: Any) -> str:
    """Render a Python value as a SQL literal: ``NULL`` / bool / number / quoted string.

    Single quotes in strings are doubled (basic escaping); these tables only ever
    receive framework-internal values (model names, versions, hashes, ISO
    timestamps), never raw user input.
    """
    if v is None:
        return "NULL"
    if isinstance(v, bool):  # must precede int: bool is a subclass of int
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


class SqlExecutor:
    """Runs SQL via a Spark session, or a WorkspaceClient + warehouse, or a no-op.

    Shared by every control-table accessor. ``available`` tells callers whether a
    write would actually persist (used to fail-loud where durability is required,
    e.g. failover's role flip).
    """

    def __init__(
        self,
        *,
        spark=None,
        workspace_client=None,
        warehouse_id: Optional[str] = None,
        wait_timeout: str = "30s",
    ):
        self.spark = spark
        self.wc = workspace_client
        self.warehouse_id = warehouse_id
        self.wait_timeout = wait_timeout

    @property
    def available(self) -> bool:
        """True when SQL will actually execute (a Spark session or warehouse is wired)."""
        return self.spark is not None or (self.wc is not None and bool(self.warehouse_id))

    def execute(self, sql: str):
        """Execute ``sql`` on whichever backend is configured; return its raw result.

        On the SDK (warehouse) path a failed statement comes back as a ``FAILED``
        ``StatementResponse`` rather than an exception, so we inspect the status and
        raise — otherwise control-plane writes/DDL would silently no-op (and callers
        that read the result would just see empty data).
        """
        if self.spark is not None:
            return self.spark.sql(sql)
        if self.wc is not None and self.warehouse_id:
            resp = self.wc.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id, statement=sql, wait_timeout=self.wait_timeout
            )
            status = getattr(resp, "status", None)
            state = str(getattr(status, "state", "") or "")
            if state.endswith(("FAILED", "CANCELED", "CLOSED")):
                err = getattr(getattr(status, "error", None), "message", None) or state
                raise RuntimeError(f"SQL statement {state}: {err}\nSQL: {sql[:500]}")
            return resp
        _logger.warning("No spark/warehouse configured; SQL not executed:\n%s", sql)
        return None


def scalar(res) -> Optional[Any]:
    """First cell of the first row, or ``None`` -- across Spark + SDK result shapes.

    Returns the raw value (no casting); callers cast as needed.
    """
    if res is None:
        return None
    if hasattr(res, "collect"):  # Spark DataFrame
        collected = res.collect()
        if collected and collected[0][0] is not None:
            return collected[0][0]
        return None
    try:  # Databricks SDK StatementResponse
        data = res.result.data_array
        if data and data[0] and data[0][0] is not None:
            return data[0][0]
    except (AttributeError, IndexError, TypeError):
        pass
    return None


def rows(res, columns: List[str]) -> List[dict]:
    """All rows as dicts keyed by ``columns`` -- across Spark + SDK result shapes."""
    if res is None:
        return []
    if hasattr(res, "collect"):  # Spark DataFrame
        return [r.asDict() for r in res.collect()]
    try:  # Databricks SDK StatementResponse
        data = res.result.data_array or []
        return [dict(zip(columns, row)) for row in data]
    except (AttributeError, TypeError):
        return []
