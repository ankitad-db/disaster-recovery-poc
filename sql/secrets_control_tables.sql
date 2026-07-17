-- Workspace Secrets DR — UC control tables.
-- Created once per workspace (run in BOTH primary and secondary) by
-- notebooks/secrets/00_setup_secrets.py. Idempotent (IF NOT EXISTS).
--
-- dr_secrets_inventory : per-secret desired-state / reconciliation table. One row
--                        per (scope, key). The change detector diffs live state
--                        against this to decide what to (re)export.
-- dr_secrets_audit     : append-only run/operation history for observability and
--                        RPO tracking (last successful export/import per scope).

CREATE TABLE IF NOT EXISTS dr_poc.dr_control.dr_secrets_inventory (
  scope               STRING  NOT NULL,   -- secret scope name
  secret_key          STRING  NOT NULL,   -- secret key within the scope
  value_hash          STRING,             -- sha256 of the plaintext value (rotation detection)
  acl_signature       STRING,             -- sha256 over the sorted scope ACL list
  source_last_updated TIMESTAMP,          -- last_updated_timestamp reported by list-secrets
  last_synced_at      TIMESTAMP,          -- when this key was last exported successfully
  bundle_id           STRING,             -- bundle that last carried this key
  status              STRING,             -- IN_SYNC | PENDING | DELETED
  updated_at          TIMESTAMP
)
USING DELTA
COMMENT 'Per-secret desired state for workspace-secrets DR (reconciliation source).';

CREATE TABLE IF NOT EXISTS dr_poc.dr_control.dr_secrets_audit (
  audit_id     STRING  NOT NULL,          -- uuid per operation
  event_time   TIMESTAMP,
  operation    STRING,                    -- SETUP | DETECT | EXPORT | IMPORT
  direction    STRING,                    -- e.g. us-east-2->us-west-2
  scope        STRING,                    -- affected scope (or '*' for whole-run rows)
  item_count   INT,                       -- secrets processed in this operation
  status       STRING,                    -- SUCCESS | FAILED | SKIPPED
  bundle_id    STRING,
  duration_sec DOUBLE,
  detail       STRING,                    -- free-form (error message / summary / gate)
  actor        STRING                     -- SPN / user that ran it
)
USING DELTA
COMMENT 'Append-only operation history for workspace-secrets DR.';

-- Convenience views ---------------------------------------------------------

-- Latest export/import watermark per scope (RPO view).
CREATE OR REPLACE VIEW dr_poc.dr_control.v_dr_secrets_watermark AS
SELECT scope,
       MAX(CASE WHEN operation = 'EXPORT' AND status = 'SUCCESS' THEN event_time END) AS last_export,
       MAX(CASE WHEN operation = 'IMPORT' AND status = 'SUCCESS' THEN event_time END) AS last_import
FROM dr_poc.dr_control.dr_secrets_audit
GROUP BY scope;

-- Recent failures for alerting/triage.
CREATE OR REPLACE VIEW dr_poc.dr_control.v_dr_secrets_failures AS
SELECT event_time, operation, direction, scope, detail, actor
FROM dr_poc.dr_control.dr_secrets_audit
WHERE status = 'FAILED'
ORDER BY event_time DESC;
