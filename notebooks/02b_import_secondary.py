# Databricks notebook source
# MAGIC %md
# MAGIC # 02b · Baseline Import  (run in SECONDARY / us-west-2)  — OPTIONAL split path
# MAGIC Pairs with `02a` (the optional split path; prefer notebook `02` pull instead).
# MAGIC Imports models into this (SECONDARY = **west**) workspace's registry from its
# MAGIC `dr_staging` Volume, resolving the export dir from `_latest.txt`. Run this only
# MAGIC **after** the bucket bridge (CRR / `aws s3 sync`) has completed. Touches only
# MAGIC this workspace -- no cross-region credentials needed.

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
