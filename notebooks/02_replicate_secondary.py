# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Replicate  (run in SECONDARY / us-east-1)  — RECOMMENDED
# MAGIC One self-contained DR job: authenticates to the PRIMARY via a **secret
# MAGIC scope**, exports its registry straight into this (east) workspace's DBFS
# MAGIC bucket, then imports into the east registry. No laptop, no cross-region S3.
# MAGIC
# MAGIC ### One-time setup (do once, not at runtime)
# MAGIC In the PRIMARY: generate a PAT for `ad-dr-spn`.
# MAGIC In THIS (secondary) workspace, create the scope holding it:
# MAGIC ```
# MAGIC databricks secrets create-scope dr_remote_west
# MAGIC databricks secrets put-secret dr_remote_west host  # https://fe-sandbox-ps-dr-wp-us-west-2.cloud.databricks.com
# MAGIC databricks secrets put-secret dr_remote_west token # the ad-dr-spn PAT from primary
# MAGIC ```

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
    direction=cfg.direction(spark=spark),            # primary (remote) -> secondary (local); reads dr_state  # noqa: F821
    audit=AuditLog(cfg.audit_table, spark=spark),    # noqa: F821 (Databricks-provided)
    triggered_by="MANUAL",
    spark=spark,                                     # noqa: F821
    dbutils=dbutils,                                 # noqa: F821 (for secret scope reads)
)
ModelsDRModule(ctx).replicate()
print("Replication complete:", ctx.direction.label)
