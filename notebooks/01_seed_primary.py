# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Seed Primary
# MAGIC Seeds the PRIMARY (us-west-2) registry with a multi-version iris model
# MAGIC (aliases + tags). Run from a **Git folder** clone on an ML-runtime cluster.

# COMMAND ----------
# MAGIC %pip install "mlflow-export-import @ git+https://github.com/mlflow/mlflow-export-import@master"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
from databricks_dr.common.config import load_config
from databricks_dr.modules.models.seed import seed_primary

cfg = load_config(CONFIG_PATH)  # noqa: F821 (from _bootstrap)
model_name = seed_primary(cfg, n_versions=2)
print("Seeded:", model_name)
