# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Incremental CDC
# MAGIC Replicates only versions newer than the audit watermark (per model), then
# MAGIC re-maps aliases and mirrors serving endpoints. Schedule this as a Databricks
# MAGIC Workflow (e.g. every 15 min).

# COMMAND ----------
# MAGIC %pip install -e /Workspace/Repos/dr-poc \
# MAGIC   "mlflow-export-import @ git+https://github.com/mlflow/mlflow-export-import@master"
# dbutils.library.restartPython()

# COMMAND ----------
from databricks_dr.common.audit import AuditLog
from databricks_dr.common.config import load_config
from databricks_dr.core.base import RunContext
from databricks_dr.modules.models.module import ModelsDRModule

cfg = load_config("/Workspace/Repos/dr-poc/config/dr_config.yaml")
ctx = RunContext(
    cfg=cfg,
    direction=cfg.direction(),
    audit=AuditLog(cfg.audit_table, spark=spark),    # noqa: F821
    triggered_by="SCHEDULE",
    spark=spark,                                     # noqa: F821
)
ModelsDRModule(ctx).cdc()
print("CDC pass complete for", ctx.direction.label)
