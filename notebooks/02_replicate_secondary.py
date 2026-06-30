# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Replicate  (run in the SECONDARY region)  — RECOMMENDED
# MAGIC One self-contained DR job: authenticates to the PRIMARY via a **secret
# MAGIC scope**, exports its registry straight into this (secondary) workspace's
# MAGIC `dr_staging` Volume (`/Volumes/dr_poc/dr_control/dr_staging/...`), then imports
# MAGIC into the local registry. No laptop, no cross-region S3.
# MAGIC
# MAGIC **Current mapping:** secondary = **west** (`fe-sandbox-ankita-dr-wp-us-west-2`),
# MAGIC source/primary = **east** (`fe-sandbox-krish-us-eat-1-sandbox`).
# MAGIC
# MAGIC ### One-time setup (do once, not at runtime)
# MAGIC In the PRIMARY (east): generate a PAT for `ad-dr-spn`.
# MAGIC In THIS (secondary / west) workspace, create the scope holding it
# MAGIC (name = `secrets.east.scope` in config):
# MAGIC ```
# MAGIC databricks secrets create-scope dr_remote_east
# MAGIC databricks secrets put-secret dr_remote_east host  # https://fe-sandbox-krish-us-eat-1-sandbox.cloud.databricks.com
# MAGIC databricks secrets put-secret dr_remote_east token # the ad-dr-spn PAT from east (primary)
# MAGIC ```

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
