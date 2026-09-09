-- DR Reconciliation — control tables (Delta, Unity Catalog).
-- Written by the recon engine; read by the AI/BI reconciliation dashboard.
-- The dashboard reads ONLY these tables, so it is decoupled from Managed DR
-- enrollment: the engine is what queries system.replication.states + information_schema
-- and folds the results (including RPO lag) into these tables.
--
-- Catalog/schema are configurable; the POC uses dr_poc.dr_control.

-- One row per reconciliation run — headline metrics + RPO for the RPO trend.
CREATE TABLE IF NOT EXISTS dr_poc.dr_control.dr_recon_runs (
  run_id                   STRING  NOT NULL,   -- unique per recon pass
  run_ts                   TIMESTAMP,          -- when the run executed
  failover_group           STRING,             -- system.replication failover group
  effective_primary_region STRING,             -- current primary at run time
  rpo_lag_ms               BIGINT,             -- replication_lag_ms from system.replication.states (null = never replicated)
  rpo_target_ms            BIGINT,             -- the RPO objective to compare against
  readiness                STRING,             -- GREEN | AT_RISK | CRITICAL (rolled up)
  objects_in_scope         INT,
  objects_ok               INT,                -- IN_SYNC count
  objects_attention        INT,               -- LAGGING+DRIFTED+MISSING+FAILED+UNSUPPORTED
  blocking_errors          INT                 -- distinct blocking error occurrences
) USING DELTA
COMMENT 'DR reconciliation run summary (one row per run) — drives KPIs + RPO trend.';

-- Per (object_type, status) rollup for one run — powers the coverage scorecard
-- without materializing every object as its own row.
CREATE TABLE IF NOT EXISTS dr_poc.dr_control.dr_recon_coverage (
  run_id       STRING  NOT NULL,
  object_type  STRING,                          -- tables | grants | views_functions | jobs | notebooks_files
  status       STRING,                          -- IN_SYNC | LAGGING | DRIFTED | MISSING | FAILED | UNSUPPORTED
  cnt          INT
) USING DELTA
COMMENT 'Per-object-type x status counts per run — coverage scorecard rollup.';

-- Per-object reconciliation detail — powers the drill-down table (full inventory in
-- production; a representative sample in the POC).
CREATE TABLE IF NOT EXISTS dr_poc.dr_control.dr_recon_inventory (
  run_id          STRING  NOT NULL,
  object_type     STRING,
  catalog         STRING,
  schema_name     STRING,
  fqn             STRING,                        -- fully-qualified object name
  in_scope        BOOLEAN,
  status          STRING,                        -- IN_SYNC | LAGGING | DRIFTED | MISSING | FAILED | UNSUPPORTED
  severity        STRING,                        -- INFO | WARNING | CRITICAL
  primary_sig     STRING,                        -- primary-side signature (version/hash/grant-set/...)
  secondary_sig   STRING,                        -- secondary-side signature
  detail          STRING,                        -- human-readable drift description
  last_reconciled TIMESTAMP
) USING DELTA
COMMENT 'Per-object reconciliation state (primary vs secondary).';

-- Blocking errors + silent-gap findings for one run — powers the errors panel.
CREATE TABLE IF NOT EXISTS dr_poc.dr_control.dr_recon_findings (
  run_id       STRING  NOT NULL,
  object_type  STRING,
  fqn          STRING,
  drift_kind   STRING,                           -- MISSING | FAILED | UNSUPPORTED | DRIFT | ...
  error_class  STRING,                           -- from system.replication.states.errors[] (or SILENT_GAP.*)
  detail       STRING,
  severity     STRING,                           -- INFO | WARNING | CRITICAL
  first_seen   TIMESTAMP
) USING DELTA
COMMENT 'Blocking-error + silent-gap findings per run.';

-- ---------------------------------------------------------------------------
-- Incremental-tracking + DR-event tables (added for the live implementation).
-- ---------------------------------------------------------------------------

-- Append-only audit of per-object status/signature changes across recon runs.
CREATE TABLE IF NOT EXISTS dr_recon.control.dr_recon_audit (
  audit_id     STRING NOT NULL,
  event_time   TIMESTAMP,
  run_id       STRING,
  object_type  STRING,
  fqn          STRING,
  change_type  STRING,                 -- NEW | CHANGED | REMOVED
  prev_status  STRING,
  new_status   STRING,
  prev_sig     STRING,
  new_sig      STRING,
  direction    STRING,
  detail       STRING
) USING DELTA
COMMENT 'Incremental change trail: what changed per object across recon runs.';

-- Failover / failback event log + post-event reconciliation outcome.
CREATE TABLE IF NOT EXISTS dr_recon.control.dr_recon_events (
  event_id            STRING NOT NULL,
  event_time          TIMESTAMP,
  event_type          STRING,          -- FAILOVER | FAILBACK
  direction           STRING,
  from_region         STRING,
  to_region           STRING,
  trigger             STRING,          -- drill | planned | unplanned
  duration_sec        DOUBLE,
  data_loss_window_ms BIGINT,
  objects_reconciled  INT,
  objects_total       INT,
  outcome             STRING,
  detail              STRING
) USING DELTA
COMMENT 'Failover/failback events for the recon dashboard DR-event history.';
