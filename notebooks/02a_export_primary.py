# Databricks notebook source
# MAGIC %md
# MAGIC # 02a · Baseline Export  (run in PRIMARY / us-east-1)  — OPTIONAL split path
# MAGIC Alternative to notebook `02` (the **recommended** cross-workspace pull). Use the
# MAGIC split path only when each region must run from its own Git folder and bundles
# MAGIC are moved between buckets out-of-band.
# MAGIC
# MAGIC Exports all in-scope models (full history) from this (PRIMARY = **east**)
# MAGIC workspace's registry into its `dr_staging` Volume and writes `_latest.txt`.
# MAGIC Touches only this workspace -- no cross-region credentials needed.
# MAGIC
# MAGIC Next: bridge the bucket (S3 CRR, or `databricks-dr models bridge` from a host
# MAGIC with both-account creds), then run `02b_import_secondary` in the SECONDARY
# MAGIC (**west** / us-west-2).

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
    direction=cfg.direction(),                       # primary -> secondary
    audit=AuditLog(cfg.audit_table, spark=spark),    # noqa: F821 (Databricks-provided)
    triggered_by="MANUAL",
    spark=spark,                                     # noqa: F821
)
rel = baseline.run_export(ctx, full=True)
print("Exported to (primary bucket):", rel)
