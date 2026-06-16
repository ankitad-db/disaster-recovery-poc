-- DR active-role state: the single source of truth for "who is primary right now".
-- A failover/failback job UPDATEs this row; every other job READs it (via
-- Config.active_primary_key) so a role change survives across fresh job processes.
-- This is what replaces the per-session DR_ACTIVE_PRIMARY env var in orchestration.
-- Create in BOTH metastores so either side can resolve its role after failover.

CREATE TABLE IF NOT EXISTS dr_poc.dr_control.dr_state (
  singleton_id   STRING    NOT NULL COMMENT 'Always "global" — enforces a single row',
  active_primary STRING    NOT NULL COMMENT 'Region key acting as primary now (west|east)',
  updated_at     TIMESTAMP          COMMENT 'UTC time of the last role change',
  updated_by     STRING             COMMENT 'Principal that changed the role',
  reason         STRING             COMMENT 'INIT|FAILOVER|FAILBACK note'
)
USING DELTA
COMMENT 'DR active-role state (failover/failback source of truth)';

-- Seed the home primary once (no-op on reruns).
INSERT INTO dr_poc.dr_control.dr_state (singleton_id, active_primary, updated_at, updated_by, reason)
SELECT 'global', 'west', current_timestamp(), 'init', 'INIT'
WHERE NOT EXISTS (
  SELECT 1 FROM dr_poc.dr_control.dr_state WHERE singleton_id = 'global'
);
