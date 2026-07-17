# Databricks notebook source
# MAGIC %md
# MAGIC # Secrets DR — seed sample scopes (run in PRIMARY, POC only)
# MAGIC Creates the sample secret scopes/keys/ACLs from `secrets_dr_config.yaml`
# MAGIC (`seed.scopes`) so the export has something to protect. Secret VALUES are
# MAGIC generated randomly at runtime. Skip this in production (real scopes exist).

# COMMAND ----------
import os
import sys

sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
nb_path = ctx.notebookPath().get()
REPO_ROOT = "/Workspace" + "/".join(nb_path.split("/")[:-3])  # strip /notebooks/secrets/<name>
CONFIG_PATH = f"{REPO_ROOT}/config/secrets_dr_config.yaml"
sys.path.insert(0, f"{REPO_ROOT}/src")
print("CONFIG_PATH:", CONFIG_PATH)

# COMMAND ----------
from databricks_dr.modules.secrets.config import load_config
from databricks_dr.modules.secrets.seed import run_seed

cfg = load_config(CONFIG_PATH)
print(run_seed(cfg))
