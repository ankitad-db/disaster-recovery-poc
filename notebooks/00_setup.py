# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — UC control tables  (run once per metastore: WEST **and** EAST)
# MAGIC Creates the DR namespace + control plane in the **local** metastore so
# MAGIC replication has somewhere to land: catalog/schemas → audit table (+ views) →
# MAGIC `dr_state` (active-role, seeded to west). Names come from `config/dr_config.yaml`.
# MAGIC
# MAGIC Self-contained and idempotent (`CREATE … IF NOT EXISTS`), safe to re-run.
# MAGIC Orchestrated by the `dr_models_bootstrap` job:
# MAGIC `databricks bundle run dr_models_bootstrap -t east` (and `-t west`). No `%pip`.

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
from databricks_dr.common.config import load_config

cfg = load_config(CONFIG_PATH)  # noqa: F821 (from _bootstrap)
CAT = cfg.uc["catalog"]
SCH = cfg.uc["schema"]
CTL = cfg.uc["control_schema"]
AUDIT = cfg.audit_table
STATE = cfg.state_table

STATEMENTS = [
    # --- namespace ---
    f"CREATE CATALOG IF NOT EXISTS {CAT} COMMENT 'Disaster Recovery POC catalog'",
    f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SCH} COMMENT 'Replicated models, experiments and runs'",
    f"CREATE SCHEMA IF NOT EXISTS {CAT}.{CTL} COMMENT 'DR control plane: audit + state'",

    # --- audit table (source of truth for what replicated + CDC watermark) ---
    f"""CREATE TABLE IF NOT EXISTS {AUDIT} (
      audit_id        STRING    NOT NULL COMMENT 'UUID for this audit row',
      event_time      TIMESTAMP NOT NULL COMMENT 'UTC event time',
      operation       STRING    NOT NULL COMMENT 'EXPORT|IMPORT|VERIFY|GRANTS|ENDPOINT|DEPENDENCY|FAILOVER|FAILBACK|HEALTH',
      direction       STRING             COMMENT 'e.g. us-west-2->us-east-1',
      triggered_by    STRING             COMMENT 'SCHEDULE|AUDIT_EVENT|MANUAL',
      model_name      STRING             COMMENT 'Fully-qualified model name or CSV/*',
      source_version  STRING             COMMENT 'Source registry version (CDC watermark key)',
      target_version  STRING             COMMENT 'Version created on destination',
      source_run_id   STRING             COMMENT 'Backing MLflow run on source',
      target_run_id   STRING             COMMENT 'Backing MLflow run on destination',
      experiment_name STRING             COMMENT 'Experiment of the backing run',
      export_dir      STRING             COMMENT 'Dynamic timestamped export dir',
      manifest_path   STRING             COMMENT 'Engine manifest.json path',
      artifact_count  INT                COMMENT 'Files/objects moved',
      status          STRING    NOT NULL COMMENT 'IN_PROGRESS|SUCCESS|FAILED|SKIPPED',
      duration_sec    DOUBLE             COMMENT 'Operation wall-clock seconds',
      error_message   STRING             COMMENT 'Failure detail / operator note',
      tool_version    STRING             COMMENT 'mlflow-export-import version',
      actor           STRING             COMMENT 'Principal that ran the op (SPN)'
    ) USING DELTA COMMENT 'DR replication audit + CDC watermark source'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.columnMapping.mode' = 'name')""",

    # --- convenience views ---
    f"""CREATE OR REPLACE VIEW {CAT}.{CTL}.v_dr_watermark AS
        SELECT model_name, MAX(CAST(source_version AS INT)) AS synced_version
        FROM {AUDIT}
        WHERE operation IN ('IMPORT','VERIFY') AND status = 'SUCCESS'
        GROUP BY model_name""",
    f"""CREATE OR REPLACE VIEW {CAT}.{CTL}.v_dr_failures AS
        SELECT event_time, operation, direction, model_name, source_version, error_message
        FROM {AUDIT}
        WHERE status = 'FAILED'
        ORDER BY event_time DESC""",

    # --- active-role state (failover/failback source of truth) ---
    f"""CREATE TABLE IF NOT EXISTS {STATE} (
      singleton_id   STRING    NOT NULL COMMENT 'Always "global" — enforces a single row',
      active_primary STRING    NOT NULL COMMENT 'Region key acting as primary now (west|east)',
      updated_at     TIMESTAMP          COMMENT 'UTC time of the last role change',
      updated_by     STRING             COMMENT 'Principal that changed the role',
      reason         STRING             COMMENT 'INIT|FAILOVER|FAILBACK note'
    ) USING DELTA COMMENT 'DR active-role state (failover/failback source of truth)'""",
    f"""INSERT INTO {STATE} (singleton_id, active_primary, updated_at, updated_by, reason)
        SELECT 'global', 'west', current_timestamp(), 'init', 'INIT'
        WHERE NOT EXISTS (SELECT 1 FROM {STATE} WHERE singleton_id = 'global')""",
]

for sql in STATEMENTS:
    preview = " ".join(sql.split())[:80]
    spark.sql(sql)  # noqa: F821 (Databricks-provided)
    print("ok:", preview)

print("Setup complete for the local metastore.")

# COMMAND ----------
# MAGIC %md ## Verify
# COMMAND ----------
print("audit rows:", spark.sql(f"SELECT count(*) FROM {AUDIT}").collect()[0][0])  # noqa: F821
display(spark.sql(f"SELECT * FROM {STATE}"))  # noqa: F821 (expect one row, active_primary=west)
