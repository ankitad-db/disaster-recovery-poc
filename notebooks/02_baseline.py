# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Baseline
# MAGIC One-time full export -> S3 bridge -> import. Uses the resolved direction
# MAGIC (primary -> secondary). Pass an active Spark session to the audit log so
# MAGIC inserts run in-cluster.

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
    direction=cfg.direction(),                       # primary -> secondary
    audit=AuditLog(cfg.audit_table, spark=spark),    # noqa: F821 (Databricks-provided)
    triggered_by="MANUAL",
    spark=spark,                                     # noqa: F821
)
ModelsDRModule(ctx).baseline()
print("Baseline complete for", ctx.direction.label)
