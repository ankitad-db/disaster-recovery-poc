# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Baseline
# MAGIC One-time full export -> S3 bridge -> import in the resolved direction
# MAGIC (primary -> secondary). Run from a **Git folder** clone.

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
    direction=cfg.direction(),                       # primary -> secondary
    audit=AuditLog(cfg.audit_table, spark=spark),    # noqa: F821 (Databricks-provided)
    triggered_by="MANUAL",
    spark=spark,                                     # noqa: F821
)
ModelsDRModule(ctx).baseline()
print("Baseline complete for", ctx.direction.label)
