# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Health check  (run in SECONDARY / us-east-1)
# MAGIC Codifies the manual post-run validation into one task so orchestration can
# MAGIC catch drift automatically. For every in-scope model it confirms the local
# MAGIC (destination) registry is present and at/above its audit watermark, checks
# MAGIC for replication lag vs the source, and scans the audit table for recent
# MAGIC `FAILED` rows. **Raises on any problem** so the job task fails and the job's
# MAGIC failure notification fires. Runs as the `health_check` task after `03_cdc`,
# MAGIC and standalone via the `dr_models_health` job.

# COMMAND ----------
# MAGIC %pip install "mlflow-export-import @ git+https://github.com/mlflow/mlflow-export-import@master"
# MAGIC dbutils.library.restartPython()

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
    direction=cfg.direction(),                       # primary (remote) -> secondary (local)
    audit=AuditLog(cfg.audit_table, spark=spark),    # noqa: F821
    triggered_by="SCHEDULE",
    spark=spark,                                     # noqa: F821
    dbutils=dbutils,                                 # noqa: F821 (for source-lag secret reads)
)
ModelsDRModule(ctx).health()   # raises -> fails the task -> notifies, on drift
print("Health check OK for", ctx.direction.label)
