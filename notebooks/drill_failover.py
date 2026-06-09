# Databricks notebook source
# MAGIC %md
# MAGIC # Drill · FAILOVER  (run in EAST / us-east-1)
# MAGIC Self-asserting failover half of the DR drill. One run does the whole EAST side:
# MAGIC baseline check → `failover` (scale up endpoints, persist `dr_state=east`, audit)
# MAGIC → log a simulated "outage" model version in EAST. Then asserts the expected
# MAGIC state and **raises on failure** so the job task goes red.
# MAGIC
# MAGIC After this, run `drill_failback` in **WEST** to recover and restore steady state.

# COMMAND ----------
# MAGIC %pip install scikit-learn "mlflow-export-import @ git+https://github.com/mlflow/mlflow-export-import@master"
# MAGIC dbutils.library.restartPython()

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
print("EAST versions before:", before)

# COMMAND ----------
# MAGIC %md ## 1. Failover — promote EAST
# COMMAND ----------
ctx = RunContext(cfg=cfg, direction=cfg.direction(spark=spark), triggered_by="MANUAL",  # noqa: F821
                 audit=AuditLog(cfg.audit_table, spark=spark), spark=spark, dbutils=dbutils)  # noqa: F821
ModelsDRModule(ctx).failover()

# COMMAND ----------
# MAGIC %md ## 2. Simulate outage work — log a new version in EAST
# COMMAND ----------
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from mlflow.models.signature import infer_signature

X, y = load_iris(return_X_y=True, as_frame=True)
model = RandomForestClassifier(n_estimators=12, random_state=0).fit(X, y)
exp_base = cfg.models.get("dest_experiment_base", "/Shared/dr/experiments")
mlflow.set_experiment(f"{exp_base}/{MODEL.replace('.', '_')}_drill")
with mlflow.start_run(run_name="drill-outage"):
    mlflow.sklearn.log_model(model, "model", registered_model_name=MODEL,
                             signature=infer_signature(X, model.predict(X)), input_example=X.head())

after = versions(MODEL)
print("EAST versions after:", after)

# COMMAND ----------
# MAGIC %md ## 3. Assertions
# COMMAND ----------
state = spark.sql(f"SELECT active_primary FROM {cfg.state_table}").collect()[0][0]  # noqa: F821
failover_rows = spark.sql(  # noqa: F821
    f"SELECT count(*) FROM {cfg.audit_table} WHERE operation='FAILOVER' AND status='SUCCESS'"
).collect()[0][0]

problems = []
if state != "east":
    problems.append(f"dr_state active_primary={state}, expected east")
if not after or (before and max(after) <= max(before)):
    problems.append(f"no new EAST version (before={before}, after={after})")
if failover_rows < 1:
    problems.append("no successful FAILOVER audit row")

if problems:
    raise AssertionError("FAILOVER DRILL FAILED: " + "; ".join(problems))
print(f"FAILOVER DRILL PASSED — dr_state=east, new version {max(after)} in EAST. "
      f"Now run drill_failback in WEST.")
