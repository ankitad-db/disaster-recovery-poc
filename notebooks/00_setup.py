# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — UC control tables  (run once per metastore: WEST **and** EAST)
# MAGIC Creates the DR namespace + control plane in the **local** metastore so
# MAGIC replication has somewhere to land: catalog/schemas → audit table (+ views) →
# MAGIC `dr_state` (active-role, seeded to the config `role: primary` region).
# MAGIC Names + roles come from `config/dr_config.yaml`.
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
MAPPING = cfg.mapping_table
# Seed dr_state to whichever region is `role: primary` in config (NOT a hardcoded
# region) so the active-primary control row always matches the configured topology.
PRIMARY = cfg.config_primary_key()

# Identify the LOCAL region (this workspace) so we create the staging volume on the
# correct region's S3 external location. workspaceUrl is host-only (no scheme).
WS_URL = spark.conf.get("spark.databricks.workspaceUrl")  # noqa: F821
LOCAL_KEY = cfg.local_region_key(WS_URL)
LOCAL = cfg.regions[LOCAL_KEY]
STAGING_VOLUME = cfg.staging_volume  # e.g. dr_poc.dr_control.dr_staging  (None => DBFS root)

# Catalog MANAGED LOCATION: where managed tables/models land. On metastores WITHOUT a
# root storage credential, a catalog created without this can't allocate managed storage
# (DAC_DOES_NOT_EXIST). When the region has an external location, point the catalog's
# managed storage at a 'dr_managed' subpath there (separate from the staging volume).
# NOTE: managed location is settable only at CREATE time — to add it to an existing
# catalog, DROP CATALOG ... CASCADE and re-run.
_managed_loc = (LOCAL.external_location_url.rstrip("/") + "/dr_managed") if LOCAL.external_location_url else None

# Build the external-volume DDL only when a staging volume + region S3 location are
# configured. LOCATION is a 'dr_staging' subpath under this region's external location.
VOLUME_STMTS = []
if STAGING_VOLUME and LOCAL.external_location_url:
    _vol_loc = LOCAL.external_location_url.rstrip("/") + "/dr_staging"
    VOLUME_STMTS.append(
        f"CREATE EXTERNAL VOLUME IF NOT EXISTS {STAGING_VOLUME} "
        f"LOCATION '{_vol_loc}' "
        f"COMMENT 'DR export-bundle staging (serverless/shared-safe, S3-backed)'"
    )
    print(f"staging volume: {STAGING_VOLUME} -> {_vol_loc}")
else:
    print("staging volume: (not configured) -> falling back to DBFS root /dbfs")

# Privileges for the replication service principal (the identity the DR jobs run as).
# In UC, reference a service principal by its application-id (or registered name).
SPN = cfg.service_principal
GRANT_STMTS = [
    # Traverse the namespace.
    f"GRANT USE CATALOG ON CATALOG {CAT} TO `{SPN}`",
    f"GRANT USE SCHEMA ON SCHEMA {CAT}.{SCH} TO `{SPN}`",
    f"GRANT USE SCHEMA ON SCHEMA {CAT}.{CTL} TO `{SPN}`",
    # Models module: register/read replicated models, runs, experiments under <cat>.ml.
    f"GRANT ALL PRIVILEGES ON SCHEMA {CAT}.{SCH} TO `{SPN}`",
    # Control plane: read/write the audit + state + inventory tables under <cat>.dr_control
    # (ALL PRIVILEGES also covers READ/WRITE VOLUME on the staging volume in this schema).
    f"GRANT ALL PRIVILEGES ON SCHEMA {CAT}.{CTL} TO `{SPN}`",
]
if STAGING_VOLUME:
    # Explicit volume grants (redundant with the schema grant above, but clearer intent).
    GRANT_STMTS += [
        f"GRANT READ VOLUME ON VOLUME {STAGING_VOLUME} TO `{SPN}`",
        f"GRANT WRITE VOLUME ON VOLUME {STAGING_VOLUME} TO `{SPN}`",
    ]

STATEMENTS = [
    # --- namespace ---
    # Include MANAGED LOCATION when a region external location is configured so managed
    # tables work even on metastores without a root storage credential. (Only applied on
    # first CREATE; an already-existing catalog keeps whatever it was created with.)
    (f"CREATE CATALOG IF NOT EXISTS {CAT} MANAGED LOCATION '{_managed_loc}' "
     f"COMMENT 'Disaster Recovery POC catalog'"
     if _managed_loc else
     f"CREATE CATALOG IF NOT EXISTS {CAT} COMMENT 'Disaster Recovery POC catalog'"),
    f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SCH} COMMENT 'Replicated models, experiments and runs'",
    f"CREATE SCHEMA IF NOT EXISTS {CAT}.{CTL} COMMENT 'DR control plane: audit + state'",

    # NOTE: the staging external Volume is created in its own tolerant step below
    # (it needs CREATE EXTERNAL VOLUME on the external location, which a less-privileged
    # runner may lack — that must not block the control-plane tables).

    # --- audit table (source of truth for what replicated + CDC watermark) ---
    f"""CREATE TABLE IF NOT EXISTS {AUDIT} (
      audit_id          STRING    NOT NULL COMMENT 'UUID for this audit row',
      event_time        TIMESTAMP NOT NULL COMMENT 'UTC time this DR op was recorded',
      operation         STRING    NOT NULL COMMENT 'EXPORT|IMPORT|VERIFY|GRANTS|ENDPOINT|DEPENDENCY|FAILOVER|FAILBACK|HEALTH',
      direction         STRING             COMMENT 'e.g. us-west-2->us-east-1',
      triggered_by      STRING             COMMENT 'SCHEDULE|AUDIT_EVENT|MANUAL (legacy)',
      model_name        STRING             COMMENT 'Fully-qualified model name or CSV/*',
      source_version    STRING             COMMENT 'Source registry version (CDC watermark key)',
      target_version    STRING             COMMENT 'Version created on destination',
      source_run_id     STRING             COMMENT 'Backing MLflow run on source',
      target_run_id     STRING             COMMENT 'Backing MLflow run on destination',
      experiment_name   STRING             COMMENT 'Experiment of the backing run',
      export_dir        STRING             COMMENT 'Dynamic timestamped export dir',
      manifest_path     STRING             COMMENT 'Engine manifest.json path',
      artifact_count    INT                COMMENT 'Files/objects moved',
      status            STRING    NOT NULL COMMENT 'IN_PROGRESS|SUCCESS|FAILED|SKIPPED',
      duration_sec      DOUBLE             COMMENT 'Operation wall-clock seconds',
      error_message     STRING             COMMENT 'Failure detail / operator note',
      tool_version      STRING             COMMENT 'Replication engine version (native-<mlflow>)',
      actor             STRING             COMMENT 'Principal that ran the op (SPN)',
      object_type       STRING             COMMENT 'model|version|run|experiment|prompt|trace|eval_dataset|logged_model|alias|grant|endpoint|notebook',
      action            STRING             COMMENT 'CREATE|UPDATE|DELETE|ALIAS_SET|NONE',
      trigger_type      STRING             COMMENT 'MANUAL|SCHEDULE|AUDIT_SCAN|MODEL_TRIGGER',
      source_event_id   STRING             COMMENT 'system.access.audit event_id correlation',
      source_event_time TIMESTAMP          COMMENT 'UTC time the change happened on the source',
      rpo_lag_sec       DOUBLE             COMMENT 'event_time - source_event_time (recovery-point lag)',
      bytes_moved       BIGINT             COMMENT 'Artifact bytes transferred for this op',
      src_experiment    STRING             COMMENT 'Source experiment name (lineage mapping)',
      dst_experiment    STRING             COMMENT 'Destination experiment name',
      retry_count       INT                COMMENT 'Attempts taken for this op',
      worker            STRING             COMMENT 'Thread/worker label (scale visibility)',
      checksum          STRING             COMMENT 'Integrity hash of moved artifacts (optional)'
    ) USING DELTA COMMENT 'DR replication audit + CDC watermark source'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.columnMapping.mode' = 'name')""",

    # Idempotent upgrade for tables created under schema 1 (no-op on fresh installs).
    f"""ALTER TABLE {AUDIT} ADD COLUMNS (
      object_type STRING, action STRING, trigger_type STRING, source_event_id STRING,
      source_event_time TIMESTAMP, rpo_lag_sec DOUBLE, bytes_moved BIGINT,
      src_experiment STRING, dst_experiment STRING, retry_count INT, worker STRING, checksum STRING
    )""",

    # --- desired-state inventory (idempotent reconciliation + health) ---
    f"""CREATE TABLE IF NOT EXISTS {CAT}.{CTL}.dr_object_inventory (
      object_key          STRING    NOT NULL COMMENT 'Fully-qualified object id (e.g. model name)',
      object_type         STRING    NOT NULL COMMENT 'model|endpoint|... (DR module object type)',
      source_region       STRING             COMMENT 'Region the object was authored in',
      last_source_version STRING             COMMENT 'Last source version observed/synced',
      alias_map           STRING             COMMENT 'JSON {{alias: version}} last synced',
      last_synced_at      TIMESTAMP          COMMENT 'UTC time of the last successful sync',
      last_audit_id       STRING             COMMENT 'audit_id of the sync that wrote this row',
      integrity_hash      STRING             COMMENT 'Optional integrity hash of last synced artifacts',
      status              STRING             COMMENT 'IN_SYNC|STALE|FAILED'
    ) USING DELTA COMMENT 'DR per-object desired-state snapshot (reconciliation + health)'
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""",

    # --- ID mapping (source<->dest experiment/run/version; names match, IDs differ) ---
    f"""CREATE TABLE IF NOT EXISTS {MAPPING} (
      mapping_id        STRING    NOT NULL COMMENT 'UUID for this mapping row',
      event_time        TIMESTAMP NOT NULL COMMENT 'UTC time this mapping was recorded',
      id_type           STRING    NOT NULL COMMENT 'experiment|run|model_version',
      model_name        STRING             COMMENT 'Registered model this mapping was produced for',
      object_name       STRING             COMMENT 'Stable name (experiment name; same on both sides)',
      source_id         STRING             COMMENT 'ID in the SOURCE workspace (experiment_id|run_id|version)',
      target_id         STRING             COMMENT 'ID in the DESTINATION workspace (workspace-local)',
      source_version    STRING             COMMENT 'Model version this run/version backs (when applicable)',
      source_workspace  STRING             COMMENT 'Source workspace host/name',
      target_workspace  STRING             COMMENT 'Destination workspace host/name',
      direction         STRING             COMMENT 'e.g. us-east-1->us-west-2',
      audit_id          STRING             COMMENT 'audit_id of the IMPORT op that created this mapping'
    ) USING DELTA COMMENT 'Source<->destination MLflow ID mapping (experiment/run/version)'
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""",

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
    # Latest source->target ID mapping per (id_type, source_id) — newest wins.
    f"""CREATE OR REPLACE VIEW {CAT}.{CTL}.v_dr_id_mapping_latest AS
        SELECT id_type, model_name, object_name, source_id, target_id, source_version,
               source_workspace, target_workspace, direction, event_time
        FROM (
          SELECT *, ROW_NUMBER() OVER (
                   PARTITION BY id_type, source_id ORDER BY event_time DESC) AS rn
          FROM {MAPPING}
        ) WHERE rn = 1""",

    # --- active-role state (failover/failback source of truth) ---
    f"""CREATE TABLE IF NOT EXISTS {STATE} (
      singleton_id   STRING    NOT NULL COMMENT 'Always "global" — enforces a single row',
      active_primary STRING    NOT NULL COMMENT 'Region key acting as primary now (west|east)',
      updated_at     TIMESTAMP          COMMENT 'UTC time of the last role change',
      updated_by     STRING             COMMENT 'Principal that changed the role',
      reason         STRING             COMMENT 'INIT|FAILOVER|FAILBACK note'
    ) USING DELTA COMMENT 'DR active-role state (failover/failback source of truth)'""",
    f"""INSERT INTO {STATE} (singleton_id, active_primary, updated_at, updated_by, reason)
        SELECT 'global', '{PRIMARY}', current_timestamp(), 'init', 'INIT'
        WHERE NOT EXISTS (SELECT 1 FROM {STATE} WHERE singleton_id = 'global')""",
]

for sql in STATEMENTS:
    preview = " ".join(sql.split())[:80]
    try:
        spark.sql(sql)  # noqa: F821 (Databricks-provided)
        print("ok:", preview)
    except Exception as e:  # noqa: BLE001
        # The schema-2 ALTER is only needed when upgrading a schema-1 table; on a
        # fresh CREATE the columns already exist, so an "already exists" error here
        # is expected and safe to skip. Any other error is re-raised.
        msg = str(e).lower()
        if "add columns" in sql.lower() and ("already exist" in msg or "field_already_exists" in msg):
            print("skip (columns already present):", preview)
        else:
            raise


def _external_location_name_for(url):  # best-effort: map an S3 url -> external location name
    try:
        u = (url or "").rstrip("/")
        for r in spark.sql("SHOW EXTERNAL LOCATIONS").collect():  # noqa: F821
            d = r.asDict()
            loc_url = (d.get("url") or "").rstrip("/")
            if loc_url and (u.startswith(loc_url) or loc_url.startswith(u)):
                return d.get("name")
    except Exception:  # noqa: BLE001
        pass
    return None


# --- staging external Volume (tolerant) ----------------------------------------
# Creating it needs CREATE EXTERNAL VOLUME on the backing external location. A
# less-privileged runner may lack that; we DON'T fail setup (the control tables are
# already created), we print the exact grant an external-location owner / metastore
# admin can apply, then this cell can be re-run.
for sql in VOLUME_STMTS:
    preview = " ".join(sql.split())[:90]
    try:
        spark.sql(sql)  # noqa: F821
        print("volume ok:", preview)
    except Exception as e:  # noqa: BLE001
        el = _external_location_name_for(LOCAL.external_location_url)
        el_ref = f"`{el}`" if el else "<external-location-name>"
        print(f"volume SKIPPED ({type(e).__name__}): {preview}")
        print(f"   -> {str(e)[:200]}")
        print("   Remediation — run as the external-location OWNER or a metastore admin, then re-run this cell:")
        print(f"     GRANT CREATE EXTERNAL VOLUME ON EXTERNAL LOCATION {el_ref} TO `{SPN}`;")
        print(f"     -- (and to the interactive runner if seeding/replicating manually)")
        print(f"   external location backs: {LOCAL.external_location_url}")
        print("   Alternative: leave storage.staging_volume unset in dr_config.yaml to use /dbfs.")

print("Setup complete for the local metastore.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Grant privileges to the replication service principal
# MAGIC Tolerant pass: applying grants needs the runner to **own/admin** these securables
# MAGIC and the SPN to exist as a UC principal. We warn (not fail) on each grant so a
# MAGIC less-privileged runner can still stand up the control plane; a metastore admin
# MAGIC can re-run this cell (or apply the printed grants) later.
# MAGIC
# MAGIC > Note: the SPN also needs `SELECT` on `system.access.audit` **only if**
# MAGIC > `models.cdc_use_system_tables: true`. That grant is metastore-admin-only and
# MAGIC > requires the `system.access` schema to be enabled — apply it out-of-band.

# COMMAND ----------
for sql in GRANT_STMTS:
    preview = " ".join(sql.split())[:90]
    try:
        spark.sql(sql)  # noqa: F821 (Databricks-provided)
        print("grant ok:", preview)
    except Exception as e:  # noqa: BLE001
        # Missing OWNER/admin on the securable, or the SPN not yet a UC principal.
        # Non-fatal: print the grant so an admin can apply it.
        print(f"grant SKIPPED ({type(e).__name__}): {preview}\n   -> {str(e)[:160]}")

# COMMAND ----------
# MAGIC %md ## Verify
# COMMAND ----------
print("audit rows:", spark.sql(f"SELECT count(*) FROM {AUDIT}").collect()[0][0])  # noqa: F821
print("active_primary seeded to:", PRIMARY)
display(spark.sql(f"SELECT * FROM {STATE}"))  # noqa: F821 (expect one row, active_primary=<config primary>)
