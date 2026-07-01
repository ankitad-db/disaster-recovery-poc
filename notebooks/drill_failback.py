# Databricks notebook source
# MAGIC %md
# MAGIC # Drill · FAILBACK  (run in EAST / us-east-1)
# MAGIC Self-asserting failback half of the DR drill. One run does the whole EAST side:
# MAGIC reverse CDC `west → east` (pulls the outage-time version) → `failback`
# MAGIC (audit marker + reset `dr_state=east`). Then asserts the outage version landed
# MAGIC and steady state is restored, **raising on failure**.
# MAGIC
# MAGIC Prereq: run `drill_failover` in WEST first, and the `dr_remote_west` secret
# MAGIC scope must exist in EAST (see docs/architecture.md §10).

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
import mlflow
from databricks_dr.common.audit import AuditLog
from databricks_dr.common.config import load_config
from databricks_dr.core.base import RunContext
from databricks_dr.modules.models.module import ModelsDRModule

mlflow.set_registry_uri("databricks-uc")
cfg = load_config(CONFIG_PATH)  # noqa: F821
MODEL = cfg.models.get("include", ["dr_poc.ml.iris_dr_model"])[0]


def versions(name):
    return sorted(int(v.version) for v in mlflow.MlflowClient().search_model_versions(f"name='{name}'"))


before = versions(MODEL)
print("EAST versions before failback:", before)

# COMMAND ----------
# MAGIC %md ## 1. Reverse CDC + failback (west -> east)
# COMMAND ----------
# direction(failback=True) resolves west->east from config roles (override-safe);
# dbutils reads the dr_remote_west scope for the reverse pull.
ctx = RunContext(cfg=cfg, direction=cfg.direction(failback=True, spark=spark), triggered_by="MANUAL",  # noqa: F821
                 audit=AuditLog(cfg.audit_table, spark=spark), spark=spark, dbutils=dbutils)  # noqa: F821
ModelsDRModule(ctx).cdc()       # reverse catch-up: pulls outage-time versions into EAST
ModelsDRModule(ctx).failback()  # FAILBACK marker + reset dr_state=east

after = versions(MODEL)
print("EAST versions after failback:", after)

# COMMAND ----------
# MAGIC %md ## 2. Assertions
# COMMAND ----------
state = spark.sql(f"SELECT active_primary FROM {cfg.state_table}").collect()[0][0]  # noqa: F821
failback_rows = spark.sql(  # noqa: F821
    f"SELECT count(*) FROM {cfg.audit_table} WHERE operation='FAILBACK' AND status='SUCCESS'"
).collect()[0][0]

problems = []
if state != "east":
    problems.append(f"dr_state active_primary={state}, expected east (steady state not restored)")
if before and max(after) <= max(before):
    problems.append(f"outage version not recovered (before={before}, after={after})")
if not after:
    problems.append("model missing in EAST after failback")
if failback_rows < 1:
    problems.append("no successful FAILBACK audit row")

if problems:
    raise AssertionError("FAILBACK DRILL FAILED: " + "; ".join(problems))
print(f"FAILBACK DRILL PASSED — recovered version {max(after)} into EAST, dr_state=east. "
      f"Steady-state east→west CDC resumes automatically.")
