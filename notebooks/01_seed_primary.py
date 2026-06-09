# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Seed Primary
# MAGIC Thin wrapper: installs the framework and seeds the PRIMARY (us-west-2) registry
# MAGIC with a multi-version iris model (aliases + tags). Run on an ML runtime cluster.

# COMMAND ----------
# MAGIC %pip install -e /Workspace/Repos/dr-poc \
# MAGIC   "mlflow-export-import @ git+https://github.com/mlflow/mlflow-export-import@master"
# dbutils.library.restartPython()

# COMMAND ----------
from databricks_dr.common.config import load_config
from databricks_dr.modules.models.seed import seed_primary

cfg = load_config("/Workspace/Repos/dr-poc/config/dr_config.yaml")
model_name = seed_primary(cfg, n_versions=2)
print("Seeded:", model_name)
