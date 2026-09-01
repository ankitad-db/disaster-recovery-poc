# Databricks notebook source
# MAGIC %md
# MAGIC # Secrets DR — replicate (direct, cross-workspace)
# MAGIC Runs in the ACTIVE workspace. Reads this workspace's secrets (source) and the
# MAGIC PEER workspace's secrets (destination), diffs them, and pushes only the delta
# MAGIC into the peer via the Secrets API. No S3 / CRR / KMS — values move over TLS.
# MAGIC
# MAGIC The peer is reached with a host + token: put the peer workspace's PAT in a
# MAGIC secret scope in THIS workspace (default scope/key `dr_peer`/`token`).
# MAGIC `direction`: `forward` = primary→secondary (steady state); `failback` = secondary→primary.

# COMMAND ----------
# MAGIC %pip install databricks-sdk PyYAML
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
dbutils.widgets.dropdown("direction", "forward", ["forward", "failback"])  # noqa: F821
dbutils.widgets.text("peer_scope", "dr_peer")                              # noqa: F821
dbutils.widgets.text("peer_key", "token")                                 # noqa: F821

from databricks.sdk import WorkspaceClient

from databricks_dr.modules.secrets.config import load_config
from databricks_dr.modules.secrets.replicate import run_replicate

cfg = load_config(CONFIG_PATH)
direction = dbutils.widgets.get("direction")  # noqa: F821
source_key, dest_key = ("primary", "secondary") if direction == "forward" else ("secondary", "primary")

# This workspace = the SOURCE (ambient identity). Reach the DESTINATION with a PAT
# stored locally in a secret scope.
src_wc = WorkspaceClient()
peer_token = dbutils.secrets.get(scope=dbutils.widgets.get("peer_scope"),  # noqa: F821
                                 key=dbutils.widgets.get("peer_key"))       # noqa: F821
dst_wc = WorkspaceClient(host=cfg.workspaces[dest_key].host, token=peer_token)

summary = run_replicate(cfg, source_key=source_key, dest_key=dest_key,
                        src_wc=src_wc, dst_wc=dst_wc, spark=spark)  # noqa: F821
print(summary)
