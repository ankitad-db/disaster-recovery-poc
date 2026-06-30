# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Failover / Failback
# MAGIC
# MAGIC **Run each action in the workspace that becomes the *local/destination* region.**
# MAGIC **Current mapping:** primary = **east** (krish), secondary = **west** (ankita).
# MAGIC
# MAGIC | action | run in | what it does |
# MAGIC |---|---|---|
# MAGIC | `failover` | **SECONDARY (west)** | Promote west to serve. No pull (primary may be down) — west is already a warm mirror. Records a `FAILOVER` audit row and **persists `dr_state` active_primary=west**, so scheduled jobs follow automatically. Just repoint consumers. |
# MAGIC | `failback` | **HOME PRIMARY (east)** | Reverse CDC `west -> east` to pull outage-time changes back, a `FAILBACK` marker, and **resets `dr_state` active_primary=east** — steady state restored, no manual env-var cleanup. |
# MAGIC
# MAGIC > Role state lives in the `dr_state` control table (the source of truth), not an
# MAGIC > env var. `DR_ACTIVE_PRIMARY` remains only as a dev/drill override.
# MAGIC >
# MAGIC > Failback pulls from west, so the **EAST** workspace needs a secret scope
# MAGIC > `dr_remote_west` (host + a west SPN PAT) — mirror of the `dr_remote_east`
# MAGIC > scope that lives in west. See `docs/architecture.md` §10.

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
    # Promote the local (secondary) region. direction() reads dr_state (currently
    # east primary) so dest=west (this workspace); failover() then persists west.
    ctx = RunContext(cfg=cfg, direction=cfg.direction(spark=spark), triggered_by="MANUAL",  # noqa: F821
                     audit=AuditLog(cfg.audit_table, spark=spark),  # noqa: F821
                     spark=spark, dbutils=dbutils)  # noqa: F821
    ModelsDRModule(ctx).failover()
else:
    # Failback resolves west->east from CONFIG roles (role-state-independent), so the
    # reverse pull is correct even while dr_state still points at west. dbutils is
    # required to read the dr_remote_west scope; failback() resets dr_state to east.
    ctx = RunContext(cfg=cfg, direction=cfg.direction(failback=True, spark=spark), triggered_by="MANUAL",  # noqa: F821
                     audit=AuditLog(cfg.audit_table, spark=spark),  # noqa: F821
                     spark=spark, dbutils=dbutils)  # noqa: F821
    ModelsDRModule(ctx).cdc()       # reverse CDC catch-up (east -> west)
    ModelsDRModule(ctx).failback()  # FAILBACK marker + dr_state reset + endpoint scale-up

print(action, "complete:", ctx.direction.label)
