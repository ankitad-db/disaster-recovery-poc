# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Incremental CDC  (run in the SECONDARY region — currently west)
# MAGIC Steady-state DR. Pulls from the PRIMARY (east) via the same **secret scope** as
# MAGIC notebook 02, but only re-replicates models whose source version is newer
# MAGIC than the per-model audit watermark. Unchanged models are skipped, so this is
# MAGIC cheap to schedule (e.g. every 15 min). Re-sync overwrites cleanly — no
# MAGIC duplicate versions. Change detection uses the registry diff by default
# MAGIC (`changefeed.py`); set `models.cdc_use_system_tables: true` to also scan
# MAGIC `system.access.audit`. Schedule via `dr_models_cdc` in resources/.

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
from databricks_dr.common.audit import AuditLog
from databricks_dr.common.config import load_config
from databricks_dr.core.base import RunContext
from databricks_dr.modules.models.module import ModelsDRModule

cfg = load_config(CONFIG_PATH)  # noqa: F821 (from _bootstrap)
ctx = RunContext(
    cfg=cfg,
    direction=cfg.direction(spark=spark),            # primary (remote) -> secondary (local); reads dr_state  # noqa: F821
    audit=AuditLog(cfg.audit_table, spark=spark),    # noqa: F821
    triggered_by="SCHEDULE",
    spark=spark,                                     # noqa: F821
    dbutils=dbutils,                                 # noqa: F821 (for secret scope reads)
)
ModelsDRModule(ctx).cdc()
print("CDC pass complete for", ctx.direction.label)
