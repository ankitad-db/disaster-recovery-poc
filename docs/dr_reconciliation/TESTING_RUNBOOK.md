# DR Reconciliation — Testing Runbook

How to test the workspace-asset reconciliation end to end: steady-state parity, **incremental
change tracking**, and **failover / failback**. Each scenario lists the action, the command, and
**what to check**. Fill the blank check-boxes with your own assertions.

## Setup (once)
- Profiles `dr2-east` (primary), `dr2-west` (secondary), `dr2-acct` (account) authenticated.
- Failover group `fg-dr-recon` exists; wait for it to reach **ACTIVE** for true parity (during
  `INITIAL_REPLICATION`, assets legitimately show `MISSING` in the secondary).
- Recon tables in `dr_recon.control`. Engine: `recon/recon_engine.py`.

```bash
# check failover-group state / RPO before testing
databricks api get /api/disaster-recovery/v1/accounts/0d26daa6-5e44-4c97-a497-ef015f91254a/failover-groups/fg-dr-recon --profile dr2-acct
databricks api post /api/2.0/sql/statements --profile dr2-east --json '{"warehouse_id":"cc56ad79aa69c967",
  "statement":"SELECT replication_state, replication_lag_ms/1000 AS lag_s, effective_primary_region FROM system.replication.states WHERE failover_group_name LIKE \"%/fg-dr-recon\" ORDER BY event_time DESC LIMIT 1","wait_timeout":"30s"}'
```

The recon run command (used in every scenario below):
```bash
.venv/bin/python recon/recon_engine.py --primary dr2-east --secondary dr2-west \
  --warehouse cc56ad79aa69c967 --catalog dr_recon --schema control \
  --account 0d26daa6-5e44-4c97-a497-ef015f91254a --failover-group fg-dr-recon
```

Query the results after any run:
```sql
-- latest run summary
SELECT * FROM dr_recon.control.dr_recon_runs ORDER BY run_ts DESC LIMIT 1;
-- coverage by object type (latest run)
SELECT object_type, status, cnt FROM dr_recon.control.dr_recon_coverage
 WHERE run_id = (SELECT run_id FROM dr_recon.control.dr_recon_runs ORDER BY run_ts DESC LIMIT 1);
-- per-object detail
SELECT object_type, fqn, status, detail FROM dr_recon.control.dr_recon_inventory
 WHERE run_id = (SELECT run_id FROM dr_recon.control.dr_recon_runs ORDER BY run_ts DESC LIMIT 1)
 ORDER BY object_type, fqn;
-- incremental change audit (what changed vs the previous run)
SELECT event_time, change_type, object_type, fqn, prev_status, new_status
 FROM dr_recon.control.dr_recon_audit ORDER BY event_time DESC LIMIT 50;
```

---

## Scenario 1 — Baseline parity (after ACTIVE)
**Action:** with the group ACTIVE, run recon.
**✅ Check:** every seeded asset is `IN_SYNC`; `dr_recon_runs.readiness = GREEN`; `objects_attention = 0`;
`rpo_lag_ms` present and < target. `dr_recon_findings` empty.
- [ ] _your check:_ ______

## Scenario 2 — Incremental change: MODIFY a notebook (drift)
**Action (primary):**
```bash
databricks workspace import /Workspace/Shared/dr_poc_seed/etl_bronze --language PYTHON \
  --format SOURCE --overwrite --file <(printf '# Databricks notebook source\nprint("bronze etl v2 CHANGED")\n') --profile dr2-east
```
Wait for the replication point to advance (check `system.replication.states`), then run recon **twice**
(once before the replication point catches up, once after).
**✅ Check:**
- Run A (before replica catches up): notebook `DRIFTED` (primary sig ≠ secondary sig) or `LAGGING`.
- **Incremental audit:** `dr_recon_audit` has a `CHANGED` row for the notebook with `prev_status=IN_SYNC → new_status=DRIFTED`.
- Run B (after replication): back to `IN_SYNC`; audit shows `CHANGED` `DRIFTED → IN_SYNC`.
- [ ] _your check:_ ______

## Scenario 3 — Incremental change: ADD a new asset
**Action (primary):** create a new notebook/job under `/Workspace/Shared/dr_poc_seed`.
**✅ Check:** first recon shows the new object `MISSING` (not yet replicated) + a `NEW` row in
`dr_recon_audit`; after replication it flips to `IN_SYNC`. `objects_in_scope` increments.
- [ ] _your check:_ ______

## Scenario 4 — Incremental change: DELETE an asset
**Action (primary):** delete a seeded notebook.
**✅ Check:** recon audit shows a `REMOVED` row; the object drops out of the latest inventory;
`objects_in_scope` decrements. (In `mirror` semantics Managed DR removes it from the secondary too.)
- [ ] _your check:_ ______

## Scenario 5 — Job schedule state
**Action:** inspect the replicated job in the secondary.
**✅ Check:** the job exists in the secondary with its **schedule PAUSED** (Managed DR pauses
secondary schedules). Recon flags `DRIFTED` if a secondary schedule is unexpectedly **active** or absent.
- [ ] _your check:_ ______

## Scenario 6 — Unsupported / silent gap
**Action:** create an unsupported object in scope (e.g. a **published** dashboard, or a table with a
column mask).
**✅ Check:** it does **not** appear in `system.replication.states` as success; recon marks it
`UNSUPPORTED` (findings, `INFO`/`CRITICAL`) so it is not mistaken for replicated.
- [ ] _your check:_ ______

## Scenario 7 — FAILOVER (east → west)
**Action (account):**
```bash
databricks api post /api/disaster-recovery/v1/accounts/0d26daa6-5e44-4c97-a497-ef015f91254a/failover-groups/fg-dr-recon:failover \
  --profile dr2-acct --json '{"target_primary_region":"us-west-2"}'
```
Wait for `effective_primary_region = us-west-2`. Run recon (it auto-detects direction from
`effective_primary_region` and reconciles west→east).
**✅ Check:**
- Promoted **west** workspace has all in-scope assets usable; SQL warehouses arrive `STOPPED`,
  clusters `TERMINATED`, job schedules **paused** → resume the ones you need.
- Record a `FAILOVER` row in `dr_recon_events` (direction `us-east-1→us-west-2`, data-loss window = lag at cutover).
- Recon `readiness` computed against the new primary.
- [ ] _your check:_ ______

## Scenario 8 — FAILBACK (west → east) + reconciliation
**Action:** make a change in **west** while it is primary (simulate outage-time work), then fail back:
```bash
databricks api post /api/disaster-recovery/v1/accounts/.../failover-groups/fg-dr-recon:failover \
  --profile dr2-acct --json '{"target_primary_region":"us-east-1"}'
```
Run recon.
**✅ Check:** the west-side change is reconciled back into east (`dr_recon_audit` shows it); a
`FAILBACK` row in `dr_recon_events` with `objects_reconciled / objects_total`; steady state restored
(`effective_primary_region = us-east-1`, readiness GREEN).
- [ ] _your check:_ ______

## Scenario 9 — Idempotency
**Action:** run recon twice with no changes in between.
**✅ Check:** second run adds **no** `dr_recon_audit` rows (nothing changed); inventory identical.
- [ ] _your check:_ ______

---

## Incremental change tracking — how it works
Every recon run compares each object's `(status, primary_sig)` against the **previous run** and
appends to `dr_recon_audit`:
- `NEW` — object appeared this run (not in prior run)
- `CHANGED` — status or signature changed (`prev_status → new_status`)
- `REMOVED` — object was in the prior run, gone now

So `dr_recon_audit` is a full, queryable timeline of drift/repair/add/delete across runs — the
"what changed since last time" the native Managed DR signal does not give you. Schedule the engine
on your RPO cadence (e.g. every 15 min) to get continuous incremental tracking.

## Acceptance checklist (DR test sign-off)
- [ ] Group ACTIVE; RPO lag < target.
- [ ] All in-scope workspace assets `IN_SYNC` at steady state.
- [ ] Modify / add / delete each produce the right `dr_recon_audit` change type and converge to `IN_SYNC`.
- [ ] Job schedules paused in the secondary; warehouses STOPPED; clusters TERMINATED.
- [ ] Failover promotes west and assets are usable; failback reconciles west-side changes into east.
- [ ] `dr_recon_events` logs both events with data-loss window and objects reconciled.
- [ ] Dashboard reflects all of the above.
