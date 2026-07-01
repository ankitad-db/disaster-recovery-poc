# Databricks notebook source
# MAGIC %md
# MAGIC # 98 · Checkpoints (verification queries)
# MAGIC Copy-paste-free verification for each stage of the DR flow. Run the relevant
# MAGIC section after each step. Most checks run in the **destination** (WEST for the
# MAGIC normal east→west sync; EAST during failback). Model/registry checks are
# MAGIC identity-local, so run them in whichever workspace you want to inspect.
# MAGIC
# MAGIC Table/model names are read from `config/dr_config.yaml` — nothing hardcoded.

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
from databricks_dr.common.config import load_config

cfg = load_config(CONFIG_PATH)  # noqa: F821 (from _bootstrap)
CAT = cfg.uc["catalog"]
CTL = cfg.uc["control_schema"]
AUDIT = cfg.audit_table
MAPPING = cfg.mapping_table
STATE = cfg.state_table
INVENTORY = f"{CAT}.{CTL}.dr_object_inventory"
print("workspace:", spark.conf.get("spark.databricks.workspaceUrl"))  # noqa: F821
print("audit :", AUDIT, "| mapping:", MAPPING, "| state:", STATE)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage A — after `01_seed_primary` (run in EAST)
# MAGIC Confirms the source registry has the expected models/versions/aliases/tags.

# COMMAND ----------
import mlflow
from mlflow import MlflowClient

mlflow.set_registry_uri("databricks-uc")
_c = MlflowClient()
for m in sorted({s.get("name") or f"{CAT}.{cfg.uc['schema']}.{s['model']}"
                 for s in (cfg.models.get("seed") or [])}):
    try:
        rm = _c.get_registered_model(m)
        vers = sorted(int(v.version) for v in _c.search_model_versions(f"name='{m}'"))
        print(f"{m} versions={vers} aliases={rm.aliases} desc={bool(rm.description)}")
    except Exception as e:  # noqa: BLE001
        print(f"{m}: MISSING ({e})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage B — after `02_replicate_secondary` (run in WEST)
# MAGIC ### B1. Outcome counts — expect EXPORT/IMPORT SUCCESS = #models, GRANTS SUCCESS=1, no FAILED

# COMMAND ----------
spark.sql(f"""
  SELECT operation, status, count(*) AS n
  FROM {AUDIT}
  GROUP BY operation, status
  ORDER BY operation, status
""").display()  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ### B2. Per-model export→import detail (bytes / versions / timing)

# COMMAND ----------
spark.sql(f"""
  SELECT operation, model_name, source_version, target_version,
         artifact_count, bytes_moved, duration_sec, status, event_time
  FROM {AUDIT}
  WHERE operation IN ('EXPORT','IMPORT')
  ORDER BY model_name, event_time
""").display()  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ### B3. Failures — expect 0 rows

# COMMAND ----------
spark.sql(f"SELECT * FROM {CAT}.{CTL}.v_dr_failures").display()  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ### B4. Grants — newest row should be SUCCESS with no error

# COMMAND ----------
spark.sql(f"""
  SELECT event_time, operation, direction, model_name, status, artifact_count, error_message
  FROM {AUDIT}
  WHERE operation = 'GRANTS'
  ORDER BY event_time DESC
""").display()  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ### B5. Watermark — max synced source version per model

# COMMAND ----------
spark.sql(f"SELECT * FROM {CAT}.{CTL}.v_dr_watermark ORDER BY model_name").display()  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ### B6. ID mapping (deduped view) — model_version count should equal each model's #versions

# COMMAND ----------
spark.sql(f"""
  SELECT model_name, id_type, count(*) AS n
  FROM {CAT}.{CTL}.v_dr_id_mapping_latest
  GROUP BY model_name, id_type
  ORDER BY model_name, id_type
""").display()  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ### B7. Mirrored models in WEST — versions/aliases match source

# COMMAND ----------
import mlflow
from mlflow import MlflowClient

mlflow.set_registry_uri("databricks-uc")
_c = MlflowClient()
for m in sorted({s.get("name") or f"{CAT}.{cfg.uc['schema']}.{s['model']}"
                 for s in (cfg.models.get("seed") or [])}):
    try:
        rm = _c.get_registered_model(m)
        vers = sorted(int(v.version) for v in _c.search_model_versions(f"name='{m}'"))
        print(f"{m} versions={vers} aliases={rm.aliases}")
    except Exception as e:  # noqa: BLE001
        print(f"{m}: MISSING ({e})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage C — CDC / incremental (run in WEST after a source change + CDC run)
# MAGIC ### C1. VERIFY watermark rows (CDC advances the watermark via VERIFY)

# COMMAND ----------
spark.sql(f"""
  SELECT event_time, model_name, source_version, triggered_by, trigger_type, status
  FROM {AUDIT}
  WHERE operation = 'VERIFY'
  ORDER BY event_time DESC
""").display()  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ### C2. RPO lag (recovery-point) for CDC-triggered syncs

# COMMAND ----------
spark.sql(f"""
  SELECT model_name, source_event_time, event_time, rpo_lag_sec, trigger_type
  FROM {AUDIT}
  WHERE rpo_lag_sec IS NOT NULL
  ORDER BY event_time DESC
""").display()  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage D — failover / failback state
# MAGIC ### D1. Active-primary role (dr_state source of truth)

# COMMAND ----------
spark.sql(f"SELECT * FROM {STATE}").display()  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ### D2. FAILOVER / FAILBACK audit trail

# COMMAND ----------
spark.sql(f"""
  SELECT event_time, operation, direction, model_name, status, error_message
  FROM {AUDIT}
  WHERE operation IN ('FAILOVER','FAILBACK')
  ORDER BY event_time DESC
""").display()  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ## Reference — object inventory & full trail for one model

# COMMAND ----------
spark.sql(f"SELECT * FROM {INVENTORY} ORDER BY object_key").display()  # noqa: F821

# COMMAND ----------
# Full timeline for a single model (edit the widget/name as needed)
dbutils.widgets.text("model_name", f"{CAT}.{cfg.uc['schema']}.iris_dr_model", "Model for timeline")  # noqa: F821
_m = dbutils.widgets.get("model_name")  # noqa: F821
spark.sql(f"""
  SELECT event_time, operation, action, object_type, source_version, target_version,
         status, duration_sec, error_message, actor
  FROM {AUDIT}
  WHERE model_name = '{_m}'
  ORDER BY event_time
""").display()  # noqa: F821
