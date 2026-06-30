# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Seed Primary
# MAGIC Seeds the **PRIMARY** registry with the models in `models.seed` (each: multiple
# MAGIC versions = multiple backing runs, aliases + tags). Default POC set: iris (v1-2),
# MAGIC wine (v1-3), breast_cancer (v1-2).
# MAGIC Run in the region whose `role: primary` in `config/dr_config.yaml`.
# MAGIC **Current mapping:** primary = **east** (`fe-sandbox-krish-us-eat-1-sandbox`, us-east-1).
# MAGIC Run from a **Git folder** clone on an ML-runtime cluster.

# COMMAND ----------
# MAGIC %pip install scikit-learn
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
from databricks_dr.common.config import load_config
from databricks_dr.modules.models.seed import seed_models

cfg = load_config(CONFIG_PATH)  # noqa: F821 (from _bootstrap)
names = seed_models(cfg)  # seeds every model in models.seed (or models.include)
print("Seeded:", names)
