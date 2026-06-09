# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Failover / Failback
# MAGIC
# MAGIC **Run each action in the workspace that becomes the *local/destination* region:**
# MAGIC
# MAGIC | action | run in | what it does |
# MAGIC |---|---|---|
# MAGIC | `failover` | **EAST** (secondary) | Promote east to serve. No pull (primary may be down) — east is already a warm mirror. Records a `FAILOVER` audit row. Afterwards set `DR_ACTIVE_PRIMARY=east` and repoint consumers. |
# MAGIC | `failback` | **WEST** (home primary) | Reverse CDC `east -> west` to pull outage-time changes back, then a `FAILBACK` marker. Afterwards unset `DR_ACTIVE_PRIMARY` to restore steady state. |
# MAGIC
# MAGIC > Failback pulls from east, so the **WEST** workspace needs a secret scope
# MAGIC > `dr_remote_east` (host + an east SPN PAT) — mirror of the `dr_remote_west`
# MAGIC > scope that already lives in east. See `docs/architecture.md` §10.

# COMMAND ----------
# MAGIC %pip install "mlflow-export-import @ git+https://github.com/mlflow/mlflow-export-import@master"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
dbutils.widgets.dropdown("action", "failover", ["failover", "failback"])  # noqa: F821
action = dbutils.widgets.get("action")                                    # noqa: F821

# COMMAND ----------
from databricks_dr.common.audit import AuditLog
from databricks_dr.common.config import load_config
from databricks_dr.core.base import RunContext
from databricks_dr.modules.models.module import ModelsDRModule

cfg = load_config(CONFIG_PATH)  # noqa: F821 (from _bootstrap)

if action == "failover":
    # Promote the local (secondary) region. direction() is override-aware; with
    # DR_ACTIVE_PRIMARY unset it resolves west->east so dest=east (this workspace).
    ctx = RunContext(cfg=cfg, direction=cfg.direction(), triggered_by="MANUAL",
                     audit=AuditLog(cfg.audit_table, spark=spark),
                     spark=spark, dbutils=dbutils)  # noqa: F821
    ModelsDRModule(ctx).failover()
else:
    # Failback resolves east->west from CONFIG roles (override-independent), so the
    # reverse pull is correct even while DR_ACTIVE_PRIMARY still points at east.
    # dbutils is required to read the dr_remote_east scope for the reverse pull.
    ctx = RunContext(cfg=cfg, direction=cfg.direction(failback=True), triggered_by="MANUAL",
                     audit=AuditLog(cfg.audit_table, spark=spark),
                     spark=spark, dbutils=dbutils)  # noqa: F821
    ModelsDRModule(ctx).cdc()       # reverse CDC catch-up (east -> west)
    ModelsDRModule(ctx).failback()  # FAILBACK marker + local endpoint scale-up

print(action, "complete:", ctx.direction.label)
