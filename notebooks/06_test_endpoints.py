# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Test serving-endpoint DR  (run in SECONDARY / us-east-1)
# MAGIC Exercises the two endpoint-DR phases against the real workspaces:
# MAGIC `mirror` (steady state — recreate the WEST endpoint in EAST as scale-to-zero
# MAGIC standby) and `activate` (failover — scale it up). Pick the phase with the
# MAGIC `action` widget. Verifies the result and prints the `ENDPOINT` audit rows.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC 1. A serving endpoint exists in **WEST** that serves an in-scope model
# MAGIC    (`dr_poc.ml.iris_dr_model`). Create one in a WEST notebook:
# MAGIC    ```python
# MAGIC    from databricks.sdk import WorkspaceClient
# MAGIC    from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
# MAGIC    w = WorkspaceClient(); NAME = "iris-dr-endpoint"
# MAGIC    w.serving_endpoints.create(name=NAME, config=EndpointCoreConfigInput(name=NAME,
# MAGIC        served_entities=[ServedEntityInput(entity_name="dr_poc.ml.iris_dr_model",
# MAGIC            entity_version="4", workload_size="Small", scale_to_zero_enabled=True)]))
# MAGIC    ```
# MAGIC 2. The `dr_remote_west` secret scope exists in EAST (already set up for CDC).

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
dbutils.widgets.dropdown("action", "mirror", ["mirror", "activate"])  # noqa: F821
action = dbutils.widgets.get("action")                                # noqa: F821

# COMMAND ----------
from databricks_dr.common.audit import AuditLog
from databricks_dr.common.config import load_config
from databricks_dr.core.base import RunContext
from databricks_dr.modules.models import endpoints

cfg = load_config(CONFIG_PATH)  # noqa: F821 (from _bootstrap)
ctx = RunContext(
    cfg=cfg,
    direction=cfg.direction(spark=spark),            # primary (remote) -> secondary (local)  # noqa: F821
    audit=AuditLog(cfg.audit_table, spark=spark),    # noqa: F821
    triggered_by="MANUAL",
    spark=spark,                                     # noqa: F821
    dbutils=dbutils,                                 # noqa: F821 (for dr_remote_west scope)
)

if action == "mirror":
    print("mirrored:", endpoints.mirror_endpoints(ctx))   # WEST -> EAST standby (scale-to-zero)
else:
    print("activated:", endpoints.activate_endpoints(ctx))  # scale EAST endpoints up

# COMMAND ----------
# MAGIC %md ## Verify local (EAST) endpoint posture
# COMMAND ----------
from databricks.sdk import WorkspaceClient

# list() returns lightweight ServedEntitySpec (no scale_to_zero); get() has the
# full ServedEntityOutput with the posture flag.
w = WorkspaceClient()
for ep in w.serving_endpoints.list():
    detail = w.serving_endpoints.get(ep.name)
    # A config change lands in pending_config first; config keeps the OLD value
    # until the rollout completes. Prefer pending so the verify reflects the
    # desired posture immediately after mirror/activate.
    conf = detail.pending_config or detail.config
    served = [(s.entity_name, s.entity_version, getattr(s, "scale_to_zero_enabled", None))
              for s in ((conf.served_entities if conf else None) or [])]
    # Skip platform Foundation Model API endpoints (no UC entity_name) — show only
    # endpoints that serve a UC model, i.e. the ones DR manages.
    if not any(e[0] for e in served):
        continue
    updating = bool(detail.pending_config)
    print(ep.name, "updating=", updating, served)
# after mirror  -> scale_to_zero_enabled = True   (standby)
# after activate-> scale_to_zero_enabled = False  (serving)

# COMMAND ----------
# MAGIC %md ## ENDPOINT audit rows
# COMMAND ----------
_audit_sql = f"""
  SELECT event_time, operation, status, model_name, error_message
  FROM {cfg.audit_table}
  WHERE operation = 'ENDPOINT'
  ORDER BY event_time DESC
  LIMIT 20
"""
display(spark.sql(_audit_sql))  # noqa: F821
