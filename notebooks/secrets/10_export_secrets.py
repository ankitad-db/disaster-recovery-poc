# Databricks notebook source
# MAGIC %md
# MAGIC # Secrets DR — export (run in PRIMARY)
# MAGIC Detects changed secrets (system-tables-first), reads their values via the
# MAGIC get-secret API, envelope-encrypts them, and writes a bundle to the
# MAGIC primary-region S3 bucket. S3 CRR mirrors it to the secondary region.
# MAGIC Schedule this in the primary workspace as the DR service principal.

# COMMAND ----------
# MAGIC %pip install boto3 cryptography
# MAGIC dbutils.library.restartPython()

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
for _m in [m for m in sys.modules if m.startswith("databricks_dr")]:
    del sys.modules[_m]
print("CONFIG_PATH:", CONFIG_PATH)

# COMMAND ----------
# "full" = force a complete baseline export (set true for the first run).
dbutils.widgets.dropdown("full", "false", ["false", "true"])  # noqa: F821

from databricks_dr.modules.secrets.config import load_config
from databricks_dr.modules.secrets.export import run_export

cfg = load_config(CONFIG_PATH)
force_full = dbutils.widgets.get("full") == "true"  # noqa: F821
summary = run_export(cfg, spark=spark, force_full=force_full)  # noqa: F821
print(summary)
