# Databricks notebook source
# MAGIC %md
# MAGIC # 02b · Baseline Import  (run in SECONDARY / us-east-1)
# MAGIC Imports models into this workspace's registry from the **secondary** DBFS
# MAGIC bucket, resolving the export dir from `_latest.txt`. Run this only **after**
# MAGIC the bucket bridge (CRR / `aws s3 sync`) has completed. Touches only this
# MAGIC workspace -- no cross-region credentials needed.

# COMMAND ----------
# MAGIC %pip install "mlflow-export-import @ git+https://github.com/mlflow/mlflow-export-import@master"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
from databricks_dr.common.audit import AuditLog
from databricks_dr.common.config import load_config
from databricks_dr.core.base import RunContext
from databricks_dr.modules.models import baseline

cfg = load_config(CONFIG_PATH)  # noqa: F821 (from _bootstrap)
ctx = RunContext(
    cfg=cfg,
    direction=cfg.direction(),                       # primary -> secondary (import side reads dest bucket)
    audit=AuditLog(cfg.audit_table, spark=spark),    # noqa: F821
    triggered_by="MANUAL",
    spark=spark,                                     # noqa: F821
)
baseline.run_import(ctx)                             # rel resolved from _latest.txt
print("Imported into secondary registry. Direction:", ctx.direction.label)
