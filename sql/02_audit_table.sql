-- Audit table: the source of truth for what DR replicated, when, and the outcome.
-- It also backs the CDC watermark (max successfully-synced source version per model).
-- Create in BOTH metastores so each side can be queried independently after failover.

CREATE TABLE IF NOT EXISTS dr_poc.dr_control.dr_replication_audit (
  audit_id          STRING    NOT NULL COMMENT 'UUID for this audit row',
  event_time        TIMESTAMP NOT NULL COMMENT 'UTC event time',
  operation         STRING    NOT NULL COMMENT 'EXPORT|IMPORT|VERIFY|GRANTS|ENDPOINT|DEPENDENCY|FAILOVER|FAILBACK|HEALTH',
  direction         STRING             COMMENT 'e.g. us-west-2->us-east-1',
  triggered_by      STRING             COMMENT 'SCHEDULE|AUDIT_EVENT|MANUAL',
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
  tool_version      STRING             COMMENT 'mlflow-export-import version',
  actor             STRING             COMMENT 'Principal that ran the op (SPN)'
)
USING DELTA
COMMENT 'DR replication audit + CDC watermark source'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.columnMapping.mode'   = 'name'
);

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
