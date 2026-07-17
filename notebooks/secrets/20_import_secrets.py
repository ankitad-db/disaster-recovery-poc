# Databricks notebook source
# MAGIC %md
# MAGIC # Secrets DR — import (run in the PROMOTED workspace on failover)
# MAGIC Reads the latest bundle from the LOCAL-region bucket (populated by S3 CRR),
# MAGIC decrypts the values, and re-creates scopes/secrets/ACLs. Writes are only
# MAGIC possible once the workspace is promoted, so this is the on-failover step.
# MAGIC `region` = secondary for failover, primary for the symmetric failback.

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
REPO_ROOT = "/Workspace" + "/".join(nb_path.split("/")[:-3])
CONFIG_PATH = f"{REPO_ROOT}/config/secrets_dr_config.yaml"
sys.path.insert(0, f"{REPO_ROOT}/src")
for _m in [m for m in sys.modules if m.startswith("databricks_dr")]:
    del sys.modules[_m]
print("CONFIG_PATH:", CONFIG_PATH)

# COMMAND ----------
dbutils.widgets.dropdown("region", "secondary", ["secondary", "primary"])  # noqa: F821

from databricks_dr.modules.secrets.config import load_config
from databricks_dr.modules.secrets.import_ import run_import

cfg = load_config(CONFIG_PATH)
region_key = dbutils.widgets.get("region")  # noqa: F821
summary = run_import(cfg, region_key=region_key, spark=spark)  # noqa: F821
print(summary)
