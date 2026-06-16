# Databricks DR Framework

Disaster Recovery for **unsupported Databricks resources**, built as an extensible framework.
The first module replicates **Unity Catalog models** (and everything related to them) between two
cross-region workspaces. Future object types (Delta sharing, External tables, Vector Search, Secrets, Volumes) plug in
as new modules under `src/databricks_dr/modules/` without changing the core.

## Design

- **Engine:** [`mlflow-export-import`](https://github.com/mlflow/mlflow-export-import) is a pinned
  third-party dependency (in `requirements.txt`). Only `common/engine.py` calls it.
- **Strategy:** one-time **baseline** (full history) + steady-state **incremental CDC** (per new
  version). Approach **cross-workspace pull**: the DR job runs in the destination and reads the
  source registry via a secret scope (no laptop, no cross-region S3 bridge). An **audit table**
  records every action; a single-row **`dr_state`** table holds the active-primary role so failover
  survives across job runs.
- **Direction is parameterized:** failover/failback flip the role (in `dr_state`); the same code
  runs both ways. Consumer-facing extras (UC grants, serving endpoints) replicate alongside models.

See the architecture and runbooks in [docs/architecture.md](docs/architecture.md).

## Layout

```
src/databricks_dr/
  cli.py                 # python -m databricks_dr <module> <action>
  common/                # config, clients, engine adapter, storage, audit, logging
  core/                  # BaseDRModule ABC + module registry
  modules/models/        # models DR: seed/baseline/replicate/cdc/grants/deps/endpoints/health/failover
config/dr_config.yaml    # workspaces, metastores, buckets, UC names, secret scopes
sql/                     # catalog/schema + audit + dr_state DDL
notebooks/               # thin Databricks wrappers (00 setup … drills)
resources/               # Asset Bundle job definitions
docs/architecture.md     # flows, failover/failback, orchestration
```

---

## Getting started from scratch (Databricks Git folder)

This is the end-to-end path for a brand-new user, run **interactively from notebooks** inside
Databricks.

### 0. Prerequisites (one-time, by an admin)

- Two cross-region workspaces (primary + secondary), each with its own Unity Catalog metastore.
- A service principal present in both workspaces (default `ad-dr-spn`) with: UC **read** in the
  source and **write** in the destination (and the reverse, for failback).
- The values in [config/dr_config.yaml](config/dr_config.yaml) (hosts, metastores, DBFS buckets,
  catalog/schema names) matched to your workspaces.

### 1. Clone the repo into Databricks (both workspaces)

In **each** workspace: **Workspace → Git folders (Repos) → Add Git folder**, paste the GitHub repo
URL, and authenticate with your Git credentials (PAT). The repo lands under
`/Workspace/Users/<you>/<repo>`; `notebooks/_bootstrap.py` derives the repo root from the notebook
path, so nothing is hardcoded. After any `git push`, hit **Pull** in the Git folder before re-running.

### 2. Create the cross-workspace secret scopes

The pull job runs in the destination and reads the source via a secret scope held locally:

```bash
# In the EAST (secondary) workspace — lets the steady-state pull reach WEST:
databricks secrets create-scope dr_remote_west --profile dr-east
databricks secrets put-secret  dr_remote_west host  --string-value "https://<west-host>"   --profile dr-east
databricks secrets put-secret  dr_remote_west token --string-value "<west-spn-PAT>"         --profile dr-east

# In the WEST (primary) workspace — only needed for failback (reverse pull EAST→WEST):
databricks secrets create-scope dr_remote_east --profile dr-west
databricks secrets put-secret  dr_remote_east host  --string-value "https://<east-host>"   --profile dr-west
databricks secrets put-secret  dr_remote_east token --string-value "<east-spn-PAT>"         --profile dr-west
```

Scope/key names are configured under `secrets:` in `dr_config.yaml`.

### 3. Run setup — create the control plane (one-time, in BOTH workspaces)

Run **`notebooks/00_setup.py`** in **EAST and WEST**. It is self-contained and idempotent
(`CREATE … IF NOT EXISTS`), creating in the local metastore:
- the `dr_poc` catalog + `ml` / `dr_control` schemas,
- the `dr_replication_audit` table (+ convenience views), and
- the single-row `dr_state` table (seeded to `active_primary = west`).

### 4. First-time run (seed → baseline)

| Step | Notebook | Run in | What it does |
|---|---|---|---|
| 4a | `01_seed_primary.py` | **WEST** | Seeds the POC model (iris v1/v2/v3, aliases, runs). *In production this is your real training pipeline — skip it; the models already exist.* |
| 4b | `02_replicate_secondary.py` | **EAST** | **Baseline pull** WEST→EAST: full export/import of all in-scope models + versions + runs, plus grants and serving endpoints (standby). |

After 4b, EAST is a warm mirror of WEST.

### 5. Verify

Run **`05_health_check.py`** in **EAST**. For every in-scope model it confirms the destination
registry is present and at/above its audit watermark, checks lag vs the source, and scans for
recent `FAILED` rows. It **raises** on any problem (so as a job task it would fail + notify).

### 6. Incremental sync (steady state)

Once the baseline is good, you only ever need the incremental path — it re-pulls **only models whose
source version advanced past the audit watermark** (idempotent, safe to re-run).

- **Current approach — interactive notebook:** re-run **`03_cdc.py`** in **EAST** whenever new model
  versions are produced in WEST (or on a manual cadence). The RPO is simply how often you run it.
- **Future approach — scheduled, hands-off:** the same `cdc` logic runs as the **`dr_models_cdc`**
  Databricks Workflow (CDC → health every 15 min), with `dr_models_health` as an hourly safety-net
  scan. These are deployed via the Asset Bundle and start **PAUSED**; you flip them on after a clean
  baseline (see below). Tune the cron to your target RPO.

### 7. Failover / failback — real event vs. drill

Direction is always resolved from `dr_state` (the source of truth), so the same code runs both
ways. There are **two entry points**, for two different situations:

| | Use when | Run in | Behaviour |
|---|---|---|---|
| **Real event** — `04_failover_failback.py` | An actual regional outage / planned migration. | `failover` in **EAST**, then `failback` in **WEST** (via the `action` widget) | Performs the real action only. `failover` promotes EAST (no pull — primary may be down — just records a `FAILOVER` audit row, scales up endpoints, sets `dr_state=east`; you repoint consumers). `failback` runs reverse CDC `east→west` to recover outage-time versions, writes a `FAILBACK` marker, and resets `dr_state=west`. |
| **Drill / rehearsal** — `drill_failover.py` + `drill_failback.py` | Proving the runbook works — scheduled DR tests, audits/compliance, after infra changes, onboarding. **No real outage.** | `drill_failover.py` in **EAST** first, then `drill_failback.py` in **WEST** | Self-asserting, end-to-end loop. Failover side promotes EAST **and simulates outage work** (logs a new model version in EAST), then asserts `dr_state=east`, a new version exists, and a `FAILOVER` audit row landed. Failback side reverse-CDCs that simulated version into WEST, then asserts it was recovered and `dr_state=west` (steady state restored). Each half **raises on failure**, so as a scheduled job task it goes red and alerts. |

**When to use the drills**

- **Periodic DR validation** (e.g. quarterly) — schedule the `dr_models_drill_failover` /
  `dr_models_drill_failback` jobs to continuously prove RTO/RPO and that failover→failback works.
- **After any change** to workspaces, the SPN, secret scopes, or this code — run the pair once to
  confirm nothing regressed before you rely on it.
- **Onboarding / demos** — a safe, repeatable way to see the whole DR lifecycle without taking a
  region down.

The drills are safe to re-run: they always restore steady state (`dr_state=west`, west→east CDC
resumes automatically) at the end. Both halves require the secret scopes from step 2 — in
particular `drill_failback` needs `dr_remote_east` in **WEST** for the reverse pull. Run them as an
ordered **pair** (failover in EAST → failback in WEST); the failback half asserts the failover half
already ran.

---

## Quick start (local CLI, optional)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
pip install "mlflow-export-import @ git+https://github.com/mlflow/mlflow-export-import@master"

databricks auth login https://<west-host> --profile dr-west
databricks auth login https://<east-host> --profile dr-east

databricks-dr config show          # inspect resolved config / direction
databricks-dr models seed          # populate primary (POC)
databricks-dr models baseline      # one-time full export -> import
databricks-dr models cdc           # incremental sync
```

## Deploy to Databricks (Asset Bundles) — production orchestration (Need to test)

The framework ships as a Databricks Asset Bundle (`databricks.yml` + `resources/`), deploying the
code and jobs (all run as `ad-dr-spn`): `dr_models_bootstrap`, `dr_models_replicate` (baseline),
`dr_models_cdc` (CDC → health, every 15 min, starts PAUSED), `dr_models_health` (hourly scan),
`dr_models_failover`, and the two drill jobs. Steady-state jobs live in the **secondary**
(default target `east`).

```bash
databricks bundle validate -t east
databricks bundle deploy   -t east        # secondary (us-east-1): steady-state DR
databricks bundle deploy   -t west        # primary: failback + bootstrap jobs
databricks bundle run dr_models_bootstrap -t east   # create UC control tables
databricks bundle run dr_models_bootstrap -t west
databricks bundle run dr_models_replicate -t east   # baseline
# verify, then go live (unpause schedules):
databricks bundle deploy -t east --var cdc_pause_status=UNPAUSED
```

## Regions

| Role | Region | Workspace |
|------|--------|-----------|
| Primary | us-west-2 | fe-sandbox-ps-dr-wp-us-west-2 |
| Secondary | us-east-1 | fe-sandbox-ps-dr-wp-us-east-1 |

## Status

POC. Models module under active development
