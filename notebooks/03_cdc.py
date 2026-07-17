# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Incremental CDC  (run in the SECONDARY region — currently west)
# MAGIC Steady-state DR. Pulls from the PRIMARY (east) via the same **secret scope** as
# MAGIC notebook 02, but only re-replicates models whose source version is newer
# MAGIC than the per-model audit watermark (or whose metadata signature drifted).
# MAGIC Unchanged models are skipped, so this is cheap to schedule (e.g. every 15 min).
# MAGIC Each changed model syncs as an **append-only delta**: only new versions +
# MAGIC metadata move, existing destination versions are never dropped. Per-model
# MAGIC failures are isolated (retried with backoff, recorded, and the run continues),
# MAGIC then the task fails loudly if any model failed. Change detection uses the
# MAGIC registry diff by default (`changefeed.py`); set `models.cdc_use_system_tables:
# MAGIC true` to also scan `system.access.audit`. Schedule via `dr_models_cdc` in resources/.

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
