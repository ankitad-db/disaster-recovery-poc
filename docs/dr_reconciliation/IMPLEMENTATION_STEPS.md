# DR Reconciliation — Implementation Steps (what was done)

Chronological, reproducible record of standing up Managed DR + the recon engine for the two
sandbox workspaces. Real IDs included so you can verify each artifact.

## Environment

| | Primary | Secondary |
|---|---|---|
| Workspace | `ankita-ps-dr-wp-us-east-1` | `ankita-ps-dr-wp-us-west-2` |
| Host | `https://fe-sandbox-ankita-ps-dr-wp-us-east-1.cloud.databricks.com` | `https://fe-sandbox-ankita-ps-dr-wp-us-west-2.cloud.databricks.com` |
| Workspace ID | `7474649395937386` | `7474657792684810` |
| Region | us-east-1 | us-west-2 |
| Metastore | `metastore_aws_us_east_1_vending_machine` | `metastore_aws_us_west_2_vending_machine` |
| CLI profile | `dr2-east` | `dr2-west` |
| Serverless warehouse | `cc56ad79aa69c967` | `8b4c84807df6a2aa` |
| Tier / compute | ENTERPRISE / SERVERLESS | ENTERPRISE / SERVERLESS |

- **Account:** `0d26daa6-5e44-4c97-a497-ef015f91254a` (account CLI profile `dr2-acct`)
- **AWS account:** `332745928618`
- **Failover group:** `fg-dr-recon` (created here)
- **Recon catalog:** `dr_recon.control` (managed location on `s3://ankita-ps-dr-wp-us-east-1-ext-s3-332745928618-qy1rve/dr_recon`)

## Steps

### 1. Authenticate (browser OAuth)
```bash
databricks auth login --host https://fe-sandbox-ankita-ps-dr-wp-us-east-1.cloud.databricks.com --profile dr2-east
databricks auth login --host https://fe-sandbox-ankita-ps-dr-wp-us-west-2.cloud.databricks.com --profile dr2-west
databricks auth login --host https://accounts.cloud.databricks.com --account-id 0d26daa6-5e44-4c97-a497-ef015f91254a --profile dr2-acct
```

### 2. Confirm Managed DR is enabled on the account
- `system.replication.states` exists and is populated (12,480+ rows) → Managed DR **already gated‑approved** on this account.
- DR REST API reachable with **account** auth (workspace token returns 404 — the API is account‑scoped):
  ```bash
  databricks api get /api/disaster-recovery/v1/accounts/0d26daa6-.../failover-groups --profile dr2-acct
  ```
- Verified our two workspaces were **not** in any existing failover group.

### 3. Enable Mission Critical add‑on (console, by you)
Account console → each workspace → **Add‑ons → Mission Critical → on**. Not exposed via API; the
failover‑group create rejects without it — a successful create (step 5) confirms it is on.

### 4. Seed representative workspace assets in the PRIMARY
`recon/seed_primary_assets.py` created under `/Workspace/Shared/dr_poc_seed`:
- notebooks `etl_bronze`, `ml_train`; file `params.json`
- SQL warehouse `dr-poc-wh` (`0a1405994c0c3cb6`, stopped)
- job `dr-poc-nightly-etl` (`773403365453540`, **schedule PAUSED**)
- draft AI/BI dashboard `dr-poc-draft-dashboard` (`01f1abfaa1761b31b4c058cce095a103`)
- (cluster create timed out at 5 min — optional; re‑run to add)

### 5. Create the failover group (workspace assets)
Via the DR REST API (no typed CLI command exists; raw `api post`). Body shape discovered by
probing the API (fields are top‑level, `initial_primary_region` required):
```bash
databricks api post \
  "/api/disaster-recovery/v1/accounts/0d26daa6-.../failover-groups?failover_group_id=fg-dr-recon" \
  --profile dr2-acct --json '{
    "initial_primary_region": "us-east-1",
    "regions": ["us-east-1","us-west-2"],
    "workspace_sets": [{
      "name": "fg-dr-recon-ws",
      "replicate_workspace_assets": true,
      "workspace_ids": ["7474649395937386","7474657792684810"]
    }]
  }'
```
Result: state `INITIAL_REPLICATION`, `effective_primary_region: us-east-1`,
`replicate_workspace_assets: true`. Goes `CREATING → INITIAL_REPLICATION → ACTIVE` (workspace‑asset
bootstrap can take up to ~2 weeks; recon shows real IN_SYNC once it reaches ACTIVE).

> ⚠️ On first bootstrap Managed DR **deletes in‑scope workspace assets in the secondary**. The
> secondary was near‑empty, so this was safe.

### 6. Create the recon control catalog + tables
A bare `CREATE CATALOG` fails here (metastore has no default managed storage), so the catalog was
created with an explicit managed location under the workspace's existing external location:
```sql
CREATE CATALOG IF NOT EXISTS dr_recon
  MANAGED LOCATION 's3://ankita-ps-dr-wp-us-east-1-ext-s3-332745928618-qy1rve/dr_recon';
CREATE SCHEMA IF NOT EXISTS dr_recon.control;
```
Tables in `dr_recon.control`: `dr_recon_runs`, `dr_recon_coverage`, `dr_recon_inventory`,
`dr_recon_findings`, **`dr_recon_audit`** (incremental change trail), **`dr_recon_events`**
(failover/failback log). DDL: `sql/dr_recon_tables.sql` (+ audit/events added here).

### 7. Run the recon engine
```bash
.venv/bin/python recon/recon_engine.py --primary dr2-east --secondary dr2-west \
  --warehouse cc56ad79aa69c967 --catalog dr_recon --schema control \
  --account 0d26daa6-5e44-4c97-a497-ef015f91254a --failover-group fg-dr-recon
```
What it does: reads the DR API + `system.replication.states` (RPO lag, state); reads the 7
workspace‑asset types from **both** workspaces (control‑plane REST — no compute on the secondary);
diffs per object by signature; classifies `IN_SYNC / LAGGING / DRIFTED / MISSING / FAILED /
UNSUPPORTED`; writes runs/coverage/inventory/findings; and appends an **incremental audit** row for
every object whose status/signature changed vs the previous run.

**Baseline result (during INITIAL_REPLICATION):** all seeded assets `MISSING` in the secondary —
the correct "not yet replicated" state, and proof the engine works end‑to‑end. As replication
completes, re‑running flips objects to `IN_SYNC`.

## Known gotchas encountered
- **IP access list:** the primary workspace enforces an IP ACL; a rotating egress IP got blocked
  mid‑seed (`122.170.197.136`). If asset reads/writes fail with *"Source IP … blocked by IP ACL"*,
  allowlist your egress IP (workspace admin → Security → IP access list) or run from an allowlisted network.
- **No default managed storage** on these metastores → new catalogs need an explicit `MANAGED LOCATION`.
- **DR API is account‑scoped** → use the `dr2-acct` (account) profile, not a workspace token.
- **Cluster create** is slow (async, ~5 min) — optional for the POC.

## Artifacts
- `recon/seed_primary_assets.py` — seeds workspace assets in the primary
- `recon/recon_engine.py` — the reconciliation engine (7 workspace‑asset readers + differ + classifier + audit)
- `sql/dr_recon_tables.sql` — control‑table DDL
- `docs/dr_reconciliation/TESTING_RUNBOOK.md` — how to test (incremental, failover/failback)
- Dashboard: `docs/dr_reconciliation/build_recon_dashboard.py` (+ `.lvdash.json`) — AI/BI over `dr_recon.control`
