-- Audit table: the source of truth for what DR replicated, when, and the outcome.
-- It also backs the CDC watermark (max successfully-synced source version per model).
-- Create in BOTH metastores so each side can be queried independently after failover.

CREATE TABLE IF NOT EXISTS dr_poc.dr_control.dr_replication_audit (
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
)
USING DELTA
COMMENT 'DR replication audit + CDC watermark source'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.columnMapping.mode'   = 'name'
);

-- Upgrade a schema-1 table in place (no-op / safe-to-ignore "already exists" on fresh installs).
ALTER TABLE dr_poc.dr_control.dr_replication_audit ADD COLUMNS (
  object_type STRING, action STRING, trigger_type STRING, source_event_id STRING,
  source_event_time TIMESTAMP, rpo_lag_sec DOUBLE, bytes_moved BIGINT,
  src_experiment STRING, dst_experiment STRING, retry_count INT, worker STRING, checksum STRING
);

-- Desired-state inventory: one row per replicated object, for idempotent
-- reconciliation and health/drift checks.
CREATE TABLE IF NOT EXISTS dr_poc.dr_control.dr_object_inventory (
  object_key          STRING    NOT NULL COMMENT 'Fully-qualified object id (e.g. model name)',
  object_type         STRING    NOT NULL COMMENT 'model|endpoint|... (DR module object type)',
  source_region       STRING             COMMENT 'Region the object was authored in',
  last_source_version STRING             COMMENT 'Last source version observed/synced',
  alias_map           STRING             COMMENT 'JSON {alias: version} last synced',
  last_synced_at      TIMESTAMP          COMMENT 'UTC time of the last successful sync',
  last_audit_id       STRING             COMMENT 'audit_id of the sync that wrote this row',
  integrity_hash      STRING             COMMENT 'Optional integrity hash of last synced artifacts',
  status              STRING             COMMENT 'IN_SYNC|STALE|FAILED'
)
USING DELTA
COMMENT 'DR per-object desired-state snapshot (reconciliation + health)'
TBLPROPERTIES ('delta.columnMapping.mode' = 'name');

-- ID mapping: source<->destination MLflow IDs. Names (experiment name, model name)
-- are identical across workspaces, but experiment_id / run_id are workspace-local;
-- this records the correspondence so lineage can be stitched after replication.
CREATE TABLE IF NOT EXISTS dr_poc.dr_control.dr_id_mapping (
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
)
USING DELTA
COMMENT 'Source<->destination MLflow ID mapping (experiment/run/version)'
TBLPROPERTIES ('delta.columnMapping.mode' = 'name');

-- Convenience views ---------------------------------------------------------

CREATE OR REPLACE VIEW dr_poc.dr_control.v_dr_watermark AS
SELECT model_name,
       MAX(CAST(source_version AS INT)) AS synced_version
FROM dr_poc.dr_control.dr_replication_audit
WHERE operation IN ('IMPORT', 'VERIFY') AND status = 'SUCCESS'
GROUP BY model_name;

CREATE OR REPLACE VIEW dr_poc.dr_control.v_dr_failures AS
SELECT event_time, operation, direction, model_name, source_version, error_message
FROM dr_poc.dr_control.dr_replication_audit
WHERE status = 'FAILED'
ORDER BY event_time DESC;

-- Latest source->target ID mapping per (id_type, source_id) — newest wins.
CREATE OR REPLACE VIEW dr_poc.dr_control.v_dr_id_mapping_latest AS
SELECT id_type, model_name, object_name, source_id, target_id, source_version,
       source_workspace, target_workspace, direction, event_time
FROM (
  SELECT *, ROW_NUMBER() OVER (
           PARTITION BY id_type, source_id ORDER BY event_time DESC) AS rn
  FROM dr_poc.dr_control.dr_id_mapping
) WHERE rn = 1;
