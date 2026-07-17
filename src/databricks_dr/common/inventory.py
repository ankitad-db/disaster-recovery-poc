"""Desired-state inventory for the DR control plane (``dr_object_inventory``).

Stores one row per replicated object (model) capturing the *last successfully
synced* snapshot: source version, alias map, and a metadata signature
(``integrity_hash``). CDC diffs the live source signature against the stored one to
catch metadata-only changes (a new/re-pointed alias, an edited description, added
tags) that never bump a model version -- independent of ``system.access.audit``, so
it works in the cross-account pull topology where the local audit stream can't see
the source.

The row is written only on a *successful* sync, so a failed resync leaves the old
signature in place and is naturally retried on the next pass (mirroring how the
per-model version watermark only advances on success).

Same execution plumbing as :class:`databricks_dr.common.audit.AuditLog`: pass a
``spark`` session (notebook/job) or a ``WorkspaceClient`` + ``warehouse_id``
(off-cluster runner).
"""

from __future__ import annotations

import json
from typing import Optional

from .logging import get_logger
from .sql import SqlExecutor, lit as _lit, scalar as _scalar

_logger = get_logger(__name__)


class ObjectInventory:
    """Reads/writes ``dr_object_inventory`` (per-object desired-state snapshot)."""

    def __init__(self, table: str, spark=None, workspace_client=None, warehouse_id: str | None = None):
        self.table = table
        self.spark = spark
        self.wc = workspace_client
        self.warehouse_id = warehouse_id
        self._sql = SqlExecutor(spark=spark, workspace_client=workspace_client, warehouse_id=warehouse_id)

    def _execute(self, sql: str):
        return self._sql.execute(sql)

    def last_signature(self, object_key: str) -> Optional[str]:
        """The ``integrity_hash`` stored at the last successful sync (``None`` if never)."""
        sql = f"SELECT integrity_hash FROM {self.table} WHERE object_key = {_lit(object_key)}"
        try:
            res = self._execute(sql)
        except Exception as e:  # noqa: BLE001 - a missing/unreadable inventory must not break CDC
            _logger.debug("inventory read failed for %s: %s", object_key, e)
            return None
        val = _scalar(res)
        return str(val) if val not in (None, "") else None

    def upsert(
        self,
        *,
        object_key: str,
        object_type: str = "model",
        source_region: Optional[str] = None,
        last_source_version=None,
        alias_map: Optional[dict] = None,
        integrity_hash: Optional[str] = None,
        last_audit_id: Optional[str] = None,
        status: str = "IN_SYNC",
    ) -> None:
        """Idempotently record the last-synced snapshot for ``object_key`` (MERGE on key)."""
        alias_json = json.dumps(alias_map or {}, sort_keys=True)
        version_lit = _lit(str(last_source_version)) if last_source_version is not None else "NULL"
        sql = f"""
        MERGE INTO {self.table} AS t
        USING (SELECT {_lit(object_key)} AS object_key) AS s
        ON t.object_key = s.object_key
        WHEN MATCHED THEN UPDATE SET
          object_type = {_lit(object_type)},
          source_region = {_lit(source_region)},
          last_source_version = {version_lit},
          alias_map = {_lit(alias_json)},
          last_synced_at = current_timestamp(),
          last_audit_id = {_lit(last_audit_id)},
          integrity_hash = {_lit(integrity_hash)},
          status = {_lit(status)}
        WHEN NOT MATCHED THEN INSERT
          (object_key, object_type, source_region, last_source_version, alias_map,
           last_synced_at, last_audit_id, integrity_hash, status)
          VALUES ({_lit(object_key)}, {_lit(object_type)}, {_lit(source_region)},
                  {version_lit}, {_lit(alias_json)}, current_timestamp(),
                  {_lit(last_audit_id)}, {_lit(integrity_hash)}, {_lit(status)})
        """
        self._execute(sql)
        _logger.info("inventory upsert %s v=%s status=%s", object_key, last_source_version, status)
