# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Failover / Failback
# MAGIC - **Failover**: promote secondary (us-east-1), scale up endpoints, mark audit.
# MAGIC   Then set `DR_ACTIVE_PRIMARY=east` and repoint consumers.
# MAGIC - **Failback**: reverse CDC (`failback=True`), then restore roles.

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
    ctx = RunContext(cfg=cfg, direction=cfg.direction(),
                     audit=AuditLog(cfg.audit_table, spark=spark), spark=spark)  # noqa: F821
    ModelsDRModule(ctx).failover()
else:
    # Reverse direction: secondary -> primary catch-up, then restore roles.
    ctx = RunContext(cfg=cfg, direction=cfg.direction(failback=True),
                     audit=AuditLog(cfg.audit_table, spark=spark), spark=spark)  # noqa: F821
    ModelsDRModule(ctx).cdc()       # reverse CDC catch-up
    ModelsDRModule(ctx).failback()  # marker + endpoint scale-up
print(action, "complete")
