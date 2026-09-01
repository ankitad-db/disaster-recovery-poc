# Databricks notebook source
# MAGIC %md
# MAGIC # Secrets DR — setup control tables
# MAGIC Creates the `dr_secrets_inventory` + `dr_secrets_audit` control tables (and
# MAGIC views) in the LOCAL workspace. **Run once per workspace** (primary AND
# MAGIC secondary). Idempotent. Assumes the `dr_poc` catalog + `dr_control` schema
# MAGIC already exist (from the shared setup); creates the schema if missing.

# COMMAND ----------
# Catalog for the control tables. Default `dr_poc`; on a workspace whose metastore has no
# managed storage for a fresh catalog, set this to a catalog that already has managed storage
# (e.g. the workspace default catalog) — matching `control.catalog_by_workspace` in the config.
dbutils.widgets.text("catalog", "dr_poc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")   # noqa: F821
SCHEMA = "dr_control"

STATEMENTS = [
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'DR control plane'",
    f"""CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.dr_secrets_inventory (
        scope STRING NOT NULL, secret_key STRING NOT NULL, value_hash STRING,
        acl_signature STRING, source_last_updated TIMESTAMP, last_synced_at TIMESTAMP,
        bundle_id STRING, status STRING, updated_at TIMESTAMP
      ) USING DELTA
      COMMENT 'Per-secret desired state for workspace-secrets DR (reconciliation).'""",
    f"""CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.dr_secrets_audit (
        audit_id STRING NOT NULL, event_time TIMESTAMP, operation STRING, direction STRING,
        scope STRING, item_count INT, status STRING, bundle_id STRING, duration_sec DOUBLE,
        detail STRING, actor STRING
      ) USING DELTA
      COMMENT 'Append-only operation history for workspace-secrets DR.'""",
    f"""CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.v_dr_secrets_watermark AS
        SELECT scope,
          MAX(CASE WHEN operation='EXPORT' AND status='SUCCESS' THEN event_time END) AS last_export,
          MAX(CASE WHEN operation='IMPORT' AND status='SUCCESS' THEN event_time END) AS last_import
        FROM {CATALOG}.{SCHEMA}.dr_secrets_audit GROUP BY scope""",
    f"""CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.v_dr_secrets_failures AS
        SELECT event_time, operation, direction, scope, detail, actor
        FROM {CATALOG}.{SCHEMA}.dr_secrets_audit WHERE status='FAILED' ORDER BY event_time DESC""",
]

for sql in STATEMENTS:
    preview = " ".join(sql.split())[:80]
    spark.sql(sql)  # noqa: F821
    print("ok:", preview)

print("Secrets DR control tables ready in", f"{CATALOG}.{SCHEMA}")
