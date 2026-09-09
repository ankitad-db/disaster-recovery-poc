# DR Reconciliation — START HERE

Read this first. It's the single guide to what was built, how it works, how to use it, the current
state, and what's pending. Everything is on branch **`feat/dr-reconciliation-report`**.

---

## 1. What this is (in one paragraph)
Databricks **Managed DR** replicates a workspace to a secondary region, but its native signal is
**coarse** — one aggregate replication lag + a blocking-error list per failover group, *not* a
per-object view. This project adds the missing layer: an **object-by-object reconciliation** of the
secondary against the primary for **workspace assets**, with **incremental change tracking**,
**state-awareness**, **failover/failback** handling, a **Databricks AI/BI dashboard**, and (in
progress) a **Databricks App** operator console with **Ask Genie**.

## 2. The environment (live)
| | Primary | Secondary |
|---|---|---|
| Workspace | `ankita-ps-dr-wp-us-east-1` (id 7474649395937386) | `ankita-ps-dr-wp-us-west-2` (id 7474657792684810) |
| Region | us-east-1 | us-west-2 |
| CLI profile | `dr2-east` | `dr2-west` |
| Serverless warehouse | `cc56ad79aa69c967` | `8b4c84807df6a2aa` |
- Account: `0d26daa6-5e44-4c97-a497-ef015f91254a` (profile `dr2-acct`) · AWS `332745928618`
- **Failover group:** `fg-dr-recon` (workspace-assets-only) · **Recon catalog:** `dr_recon.control`

## 3. What was done (the flow, in order)
1. **Auth** to both workspaces + the account (OAuth profiles `dr2-east`/`dr2-west`/`dr2-acct`).
2. **Confirmed Managed DR is enabled** on the account (gated — it already was; `system.replication.states` is live).
3. **Enabled Mission Critical** on both workspaces (you did this; the failover-group create confirms it).
4. **Seeded workspace assets** in the primary (`recon/seed_primary_assets.py`): 2 notebooks, a file,
   a SQL warehouse, a paused-schedule job, a draft dashboard — under `/Workspace/Shared/dr_poc_seed`
   (+ Workflows/SQL Warehouses/Dashboards). *(Clusters N/A — these are serverless-only workspaces.)*
5. **Created the failover group** `fg-dr-recon` (east→west, `replicate_workspace_assets: true`) via the
   account DR REST API. State: `INITIAL_REPLICATION` (bootstrapping).
6. **Created recon control + audit tables** in `dr_recon.control` (catalog uses an explicit managed
   location; this metastore has no default managed storage).
7. **Built the recon engine** (`recon/recon_engine.py`) and ran it — records to the control tables.
8. **Deployed the AI/BI dashboard** + a **scheduled serverless Workflow** (Asset Bundle).
9. **Hardened it** (see §6): state-awareness, Managed-DR limitation fixes, fresh-state via DR API,
   and the secondary-reachability fix.

## 4. How the recon works
- **Two signal layers.** (a) *Managed-DR signals*: failover-group **state** + `effective_primary_region`
  from the **DR REST API** (real-time, no lag), and **RPO lag + `errors[]`** from
  `system.replication.states` (system table, up to ~3h lag — no API alternative). (b) *Object parity*:
  the engine reads each of the **7 workspace-asset types from BOTH workspaces live** (control-plane
  REST — no compute on the secondary), diffs by signature, and classifies each object.
- **Statuses:** `IN_SYNC · LAGGING · DRIFTED · MISSING · FAILED · UNSUPPORTED` (+ severity).
- **State-aware.** During `INITIAL_REPLICATION` it runs in **BASELINE** mode (records everything,
  **suppresses alarms** — objects legitimately mid-copy show MISSING); once the group is **ACTIVE**
  it auto-promotes to **ASSURANCE** (real drift detection + alerts, readiness GREEN/AT_RISK/CRITICAL).
- **Incremental change feed.** Each run diffs every object's status/signature vs the previous run and
  appends `NEW/CHANGED/REMOVED` rows to `dr_recon_audit` — a queryable timeline of drift/repair.
- **Recon is periodic, not streaming** — a snapshot diff on the RPO cadence (15 min) + event-triggered
  around failover/failback. Per-object parity is real-time (live reads); only the RPO/errors metadata
  can lag.

## 5. The control tables (`dr_recon.control`)
| Table | What it holds |
|---|---|
| `dr_recon_runs` | one row per run — KPIs, RPO lag, readiness, replication_state, mode |
| `dr_recon_coverage` | per (object_type × status) counts per run — the scorecard |
| `dr_recon_inventory` | per-object detail (status, severity, signatures, detail) |
| `dr_recon_findings` | blocking findings + Managed DR's own `errors[]` + silent gaps |
| `dr_recon_audit` | incremental change feed (NEW/CHANGED/REMOVED across runs) |
| `dr_recon_events` | failover/failback event log + post-event reconciliation |

## 6. Managed-DR recon limitations we fixed
- **Aggregate → per-object.** We produce per-object status, not just a group lag.
- **Surfaced Managed DR's own `errors[]`** into findings (was ignored) — its native blocking errors.
- **Secondary dormant-state checks** — warehouses must be `STOPPED`, clusters `TERMINATED`, job
  schedules `PAUSED` in the secondary; otherwise `DRIFTED`.
- **Silent-gap detection** — in-scope objects of unsupported types (published dashboards, materialized
  views, …) flagged `UNSUPPORTED` so they're never mistaken for replicated.
- **Fresh state** — job-mode reads failover-group state from the **DR API** (not the ≤3h-laggy system table).
- **No false MISSING** — if the secondary can't be read (IP ACL / network / auth), the run is recorded
  `UNKNOWN` with a `SECONDARY_UNREACHABLE` finding — never a false "missing".

## 7. How to use it
**App — operator console (dashboard + actions + Ask Genie):**
`https://dr-recon-console-7474649395937386.aws.databricksapps.com` (state ACTIVE). Tabs: Summary
(readiness verdict + RPO gauge, topology, KPIs, coverage, trend), Findings & changes (with
Acknowledge), Failover history, Per-object drill-down, and **Ask Genie** (NL Q&A). Actions:
**Run recon now** (triggers the job), **Acknowledge finding** (mutes it via `dr_recon.control.dr_recon_ack`),
**Export DR-test sign-off** (`/api/export/signoff.md`|`.json`). Genie space `01f1ac1d3e09122fbb00b86e6d851083`.
Runs as app SP `dr-recon-console` (feaa9288-…) with SELECT/MODIFY on `dr_recon.control`, CAN_USE on the
warehouse, CAN_MANAGE_RUN on the job, CAN_RUN on the Genie space. Code under `app/`; to redeploy: rebuild
`frontend`, `workspace import-dir frontend/dist`, `databricks apps deploy dr-recon-console`.

**Dashboard (AI/BI, monitoring):** `https://fe-sandbox-ankita-ps-dr-wp-us-east-1.cloud.databricks.com/sql/dashboardsv3/01f1abfaf70418bca73da438028036ba/published`
(runs on east's serverless warehouse; reads `dr_recon.control`.)

**Run a recon manually (from an allowlisted host — see §9):**
```bash
.venv/bin/python recon/recon_engine.py --primary dr2-east --secondary dr2-west \
  --warehouse cc56ad79aa69c967 --catalog dr_recon --schema control \
  --account 0d26daa6-5e44-4c97-a497-ef015f91254a --failover-group fg-dr-recon
```
**Scheduled Workflow:** `dr_recon_scheduled` (serverless, 15-min). Deploy/unpause with the Asset Bundle:
```bash
databricks bundle deploy -t east --var recon_pause_status=UNPAUSED   # once §9 is resolved
```
**Testing scenarios** (incremental change, failover/failback, idempotency): see
[TESTING_RUNBOOK.md](TESTING_RUNBOOK.md). **What was built, with IDs:** [IMPLEMENTATION_STEPS.md](IMPLEMENTATION_STEPS.md).

## 8. Current state
- **Managed DR:** `fg-dr-recon` = `INITIAL_REPLICATION` (2 of 6 assets replicated: job + dashboard).
  Bootstrap is one-time; recon shows full green once it reaches `ACTIVE`.
- **Recon:** working; latest run is correct (job `IN_SYNC`, rest `MISSING` — expected during bootstrap).
- **Dashboard:** live over the real tables; KPI and coverage agree.
- **Scheduled job:** **PAUSED** (see §9).

## 9. Known blocker + pending
- **IP access list (the one thing to resolve for server-side recon):** both workspaces enforce an IP
  allowlist. My allowlisted session reaches the secondary fine, but the **serverless job's egress is
  blocked by west's IP ACL**, so the scheduled job can't read the secondary. It's **paused** to avoid
  noise. To run server-side recon continuously, **allowlist the primary's serverless egress on the
  secondary** (network/admin task) or use PrivateLink/NCC — then unpause (§7).
- **Managed DR → ACTIVE:** waiting on the one-time bootstrap (Databricks-side timing). Recon auto-flips
  to ASSURANCE then.
- **Databricks App (operator console + Ask Genie):** ✅ **DONE** — deployed, ACTIVE, verified against
  live data (URL + Genie id in §7). Note: to redeploy, rebuild the frontend and re-import
  `frontend/dist` (the repo `.gitignore` skips `dist/` during `databricks sync`).

## 10. Agents (for continued work)
`.claude/agents/`: `dr-recon-orchestrator` (master), `dr-prereq-provisioner`, `managed-dr-operator`,
`ws-asset-recon-dev`, `recon-tester`, `dashboard-builder`.
