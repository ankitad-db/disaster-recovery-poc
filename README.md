# Databricks DR Framework

Disaster Recovery for **unsupported Databricks resources**, built as an extensible framework.
The first module replicates **Unity Catalog models** (and everything related to them) between two
cross-region workspaces. Future object types (Delta sharing, External tables, Vector Search, Secrets, Volumes) plug in
as new modules under `src/databricks_dr/modules/` without changing the core.

## Design

- **Engine:** a **first-party native engine** (`common/native/`) built on the public MLflow client +
  `databricks-sdk` — no third-party replication tool. It exports a self-describing bundle (see
  `common/native/manifest.py`) and rebuilds the registered model, all versions, backing runs,
  experiments, aliases/tags and MLflow 3 logged models on the destination. Staying on the public
  APIs keeps DR in lock-step with whatever MLflow version the runtime ships. Only `common/engine.py`
  (a thin facade) calls it.
- **Strategy:** one-time **baseline** (full history) + steady-state **incremental CDC** (per new
  version). Approach **cross-workspace pull**: the DR job runs in the destination and reads the
  source registry via a secret scope (no laptop, no cross-region S3 bridge). An **audit table**
  records every action; a single-row **`dr_state`** table holds the active-primary role so failover
  survives across job runs.
- **Direction is parameterized:** failover/failback flip the role (in `dr_state`); the same code
  runs both ways. Consumer-facing extras (UC grants, serving endpoints) replicate alongside models.

See the architecture in [docs/architecture.md](docs/architecture.md). For the **asserted,
copy-paste end-to-end test** (seed → baseline → change → CDC → failover → failback, with
pass/fail assertions at every step), follow **[docs/smoke_test_runbook.md](docs/smoke_test_runbook.md)** —
that is the fastest way to validate a fresh deployment.

### Native engine — the API approach

There is **no third-party replication tool**. The engine is first-party code in `common/native/`,
built only on the **public MLflow client** (`mlflow`, `MlflowClient`) and the **Databricks SDK**
(`WorkspaceClient`). `common/engine.py` is a thin facade (`export_model` / `import_model`); the
replicate/cdc modules call that, never the engine internals. Because it rides the public APIs, DR
tracks whatever MLflow version the cluster runtime ships.

**`common/native/` module map**

| File | Responsibility |
|---|---|
| `manifest.py` | Self-describing bundle schema (dataclasses: `Manifest`, `RegisteredModelRec`, `VersionRec`, `RunRec`, `ExperimentRec`, `LoggedModelRec`, `PromptRec`, …) + JSON read/write + byte accounting. |
| `export.py` | `export_model` / `export_models` / `export_model_version` — serialize a UC model (all versions + lineage + GenAI) into a bundle. |
| `import_.py` | `import_model` / `import_models` / `import_model_version` / `copy_model_version` — rebuild the model from a bundle in the destination registry. |
| `_artifacts.py` | Download/upload model + run artifacts, signature detection, byte sizing. |
| `_notebooks.py` | Export/import each backing run's notebook revision (`SOURCE`/`HTML`/`JUPYTER`/`DBC`). |
| `_permissions.py` | Snapshot + re-apply model permissions (UC grants, or workspace-registry ACLs). |
| `_genai.py` | MLflow 3 prompts, evaluation datasets, traces — **version-gated**, degrade to `SKIPPED` when the runtime lacks the API. |
| `_scale.py` | Bounded thread pool (`map_bounded`, driven by `models.max_workers`) + paginated `search_*` helpers. |
| `changefeed.py` | CDC detection — `system.access.audit` scan (when enabled) or authoritative registry diff. |

**Public APIs actually used**

- **Export (read, source identity):** `MlflowClient.get_registered_model`, `search_model_versions`,
  `get_model_version`, `get_run`, `get_experiment`, `search_logged_models` (MLflow 3), and
  `mlflow.artifacts.download_artifacts`. Notebooks via `WorkspaceClient.workspace.export`; permissions
  via `ws.grants.get` (UC) / `ws.model_registry.get_permissions` (WS).
- **Import (write, dest identity):** `create_registered_model`, `get_experiment_by_name` /
  `create_experiment`, `create_run` + `log_batch` (params/metrics/tags, batched ≤90) + `log_artifacts`
  + `set_terminated` to rebuild backing runs, then **`mlflow.register_model("runs:/…")`** per version
  (in ascending order so UC re-assigns the same version numbers), poll `get_model_version` until
  `READY`, then `update_model_version`, `set_model_version_tag`, `update_registered_model`,
  `set_registered_model_tag`, and **`set_registered_model_alias`**. Notebooks via
  `ws.workspace.import_`; permissions via `ws.grants.update`. `copy_model_version` is available for the
  same-metastore UC→UC case (it does **not** work cross-metastore, which is why export/import exists).

**On-disk bundle layout** (written to the `dr_staging` Volume):
```
<bundle>/
  manifest.json                      # full inventory + schema_version + engine/mlflow version
  versions/<v>/model/…               # resolved model files per version
  runs/<run_id>/artifacts/…          # params/metrics/tags live in manifest; artifacts + notebooks here
  logged_models/<id>/artifacts/…     # MLflow 3 logged models
  prompts/ · evaluation_datasets/ · traces/   # GenAI (when in scope)
```

> **What's preserved vs. workspace-local:** names, version numbers, params/metrics/tags, aliases,
> stages, signatures and artifacts are reproduced faithfully. MLflow **run IDs and experiment IDs are
> workspace-local and will differ** after import — by design, not a failure. The source↔destination
> ID correspondence is persisted in `dr_id_mapping` (see below), so lineage can still be stitched.

For the full script-by-script API reference (every public MLflow/SDK call, the bundle
schema, and the control tables) see **[docs/api_approach.md](docs/api_approach.md)**.

#### Experiment/run ID mapping (`dr_id_mapping`)

Names match across workspaces but `experiment_id` / `run_id` are workspace-local. After every
successful import the engine records the source→target IDs into
`dr_poc.dr_control.dr_id_mapping` (experiment, run, and model-version rows). Query the latest
mapping via the `v_dr_id_mapping_latest` view:

```sql
-- source (east) IDs -> destination (west) IDs for a model; names are identical, IDs differ
SELECT id_type, object_name, source_id, target_id, source_version
FROM dr_poc.dr_control.v_dr_id_mapping_latest
WHERE model_name = 'dr_poc.ml.iris_dr_model'
ORDER BY id_type, source_version;
```

## Layout

```
src/databricks_dr/
  cli.py                 # python -m databricks_dr <module> <action>
  common/                # config, clients, engine adapter, storage, audit, logging
  core/                  # BaseDRModule ABC + module registry
  modules/models/        # models DR: seed/baseline/replicate/cdc/grants/deps/endpoints/health/failover
config/dr_config.yaml    # workspaces, metastores, external locations + staging volume, UC names, secret scopes
sql/                     # catalog/schema + audit + dr_state DDL
notebooks/               # thin Databricks wrappers (00 setup … drills)
resources/               # Asset Bundle job definitions
docs/architecture.md     # flows, failover/failback, orchestration
```

---

## Getting started from scratch (Databricks Git folder)

This is the end-to-end path for a brand-new user, run **interactively from notebooks** inside
Databricks. No laptop setup is required — each notebook installs the engine and bootstraps its own
`sys.path`.

> **Testing end-to-end?** This section is the narrated walkthrough. For a tight, asserted checklist
> of the same flow (with the exact SQL/Python to run and expected results at each step), use
> **[docs/smoke_test_runbook.md](docs/smoke_test_runbook.md)**. Run order is always
> **failover in WEST (secondary) → failback in EAST (home primary)**.

### 0. Prerequisites (one-time, by an admin)

- Two cross-region workspaces (primary + secondary), each with its own Unity Catalog metastore.
- A service principal present in both workspaces (default `ad-dr-spn`) with: UC **read** in the
  source and **write** in the destination (and the reverse, for failback).
- A **UC external location** (S3-backed) in each workspace. Its `s3://…` URL goes in the region's
  `external_location_url` in the config; `00_setup` creates a `dr_staging` external **Volume** on it
  so export bundles land on governed, serverless/shared-safe storage (not the DBFS root). Leave
  `storage.staging_volume` unset to fall back to `/dbfs`.
- The values in [config/dr_config.yaml](config/dr_config.yaml) (hosts, metastores, buckets/external
  locations, catalog/schema names) matched to your workspaces.

### 1. Clone the repo into Databricks (both workspaces)

In **each** workspace: **Workspace → Git folders (Repos) → Add Git folder**, paste the GitHub repo
URL, and authenticate with your Git credentials (PAT). The repo lands under
`/Workspace/Users/<you>/<repo>`; `notebooks/_bootstrap.py` derives the repo root from the notebook
path, so nothing is hardcoded. After any `git push`, hit **Pull** in the Git folder before re-running.

### 2. Create the cross-workspace secret scopes

The pull job runs in the destination and reads the source via a secret scope held
locally. **Current topology:** primary = **east** (`fe-sandbox-krish-us-eat-1-sandbox`),
secondary = **west** (`fe-sandbox-ankita-dr-wp-us-west-2`).

```bash
# In the WEST (secondary) workspace — lets the steady-state pull reach EAST (primary):
databricks secrets create-scope dr_remote_east --profile dr-west
databricks secrets put-secret  dr_remote_east host  --string-value "https://fe-sandbox-krish-us-eat-1-sandbox.cloud.databricks.com" --profile dr-west
databricks secrets put-secret  dr_remote_east token --string-value "<east-spn-PAT>"  --profile dr-west

# In the EAST (home primary) workspace — only needed for failback (reverse pull WEST→EAST):
databricks secrets create-scope dr_remote_west --profile dr-east
databricks secrets put-secret  dr_remote_west host  --string-value "https://fe-sandbox-ankita-dr-wp-us-west-2.cloud.databricks.com" --profile dr-east
databricks secrets put-secret  dr_remote_west token --string-value "<west-spn-PAT>" --profile dr-east
```

Scope/key names are configured under `secrets:` in `dr_config.yaml` (the scope for a
source region lives in the *other* workspace, which pulls from it).

### 3. Run setup — create the control plane (one-time, in BOTH workspaces)

Run **`notebooks/00_setup.py`** in **EAST and WEST**. It is self-contained and idempotent
(`CREATE … IF NOT EXISTS`), creating in the local metastore:
- the `dr_poc` catalog + `ml` / `dr_control` schemas,
- the `dr_staging` external Volume on the **local** region's `external_location_url` (the bundle
  landing zone; skipped if `storage.staging_volume` is unset),
- the `dr_replication_audit` table (schema 2, + convenience views), the `dr_object_inventory`
  table, the `dr_id_mapping` table (source↔dest experiment/run/version IDs), and
- the single-row `dr_state` table (seeded to the config `role: primary` region — **east** now).

The notebook self-identifies the local region from `spark.databricks.workspaceUrl`, so the **same
notebook** creates the volume on the correct S3 bucket in each workspace.

### 4. First-time run (seed → baseline)

| Step | Notebook | Run in | What it does |
|---|---|---|---|
| 4a | `01_seed_primary.py` | **EAST** (primary) | Seeds the POC models from `models.seed` (default: `iris` v1-2, `wine` v1-3, `cancer` v1-2 — each version is its own backing run), with aliases + tags. *In production this is your real training pipeline — skip it; the models already exist.* |
| 4b | `02_replicate_secondary.py` | **WEST** (secondary) | **Baseline pull** EAST→WEST: full export/import of all in-scope models + versions + runs, plus grants and serving endpoints (standby). |

After 4b, WEST is a warm mirror of EAST.

### 5. Verify (post-baseline checks)

Run **`05_health_check.py`** in **WEST**. For every in-scope model it confirms the destination
registry is present and at/above its audit watermark, checks lag vs the source, and scans for
recent `FAILED` rows. It **raises** on any problem (so as a job task it would fail + notify).

Then spot-check in the workspace (Catalog Explorer → `dr_poc.ml`, or SQL):
```python
# run in WEST — destination is a faithful mirror of EAST (loops over all seeded models)
from mlflow import MlflowClient
c = MlflowClient(registry_uri="databricks-uc")
for m in ["dr_poc.ml.iris_dr_model", "dr_poc.ml.wine_dr_model", "dr_poc.ml.cancer_dr_model"]:
    vers = sorted(int(v.version) for v in c.search_model_versions(f"name='{m}'"))
    aliases = {k.lower(): v for k, v in c.get_registered_model(m).aliases.items()}
    print(m, "versions:", vers, "aliases:", aliases)  # iris [1,2], wine [1,2,3], cancer [1,2]
```
```sql
SELECT operation, status, bytes_moved, duration_sec FROM dr_poc.dr_control.dr_replication_audit ORDER BY event_time;  -- EXPORT+IMPORT SUCCESS, bytes_moved>0
SELECT * FROM dr_poc.dr_control.v_dr_watermark;   -- synced_version == source max
SELECT * FROM dr_poc.dr_control.v_dr_failures;    -- empty
```
> Experiment & run **IDs are workspace-local and will differ** on WEST; names/versions/aliases/
> params/metrics match. That's expected, not a failure.

### 6. Incremental sync (steady state)

Once the baseline is good, you only ever need the incremental path — it re-pulls **only models whose
source version advanced past the audit watermark** (idempotent, safe to re-run).

- **Current approach — interactive notebook:** re-run **`03_cdc.py`** in **WEST** whenever new model
  versions are produced in EAST (or on a manual cadence). The RPO is simply how often you run it.
- **Future approach — scheduled, hands-off:** the same `cdc` logic runs as the **`dr_models_cdc`**
  Databricks Workflow (CDC → health every 15 min), with `dr_models_health` as an hourly safety-net
  scan. These are deployed via the Asset Bundle and start **PAUSED**; you flip them on after a clean
  baseline (see below). Tune the cron to your target RPO.

### 7. Failover / failback — real event vs. drill

Direction is always resolved from `dr_state` (the source of truth), so the same code runs both
ways. There are **two entry points**, for two different situations:

| | Use when | Run in | Behaviour |
|---|---|---|---|
| **Real event** — `04_failover_failback.py` | An actual regional outage / planned migration. | `failover` in **WEST** (secondary), then `failback` in **EAST** (home primary) (via the `action` widget) | Performs the real action only. `failover` promotes WEST (no pull — primary may be down — just records a `FAILOVER` audit row, scales up endpoints, sets `dr_state=west`; you repoint consumers). `failback` runs reverse CDC `west→east` to recover outage-time versions, writes a `FAILBACK` marker, and resets `dr_state=east`. |
| **Drill / rehearsal** — `drill_failover.py` + `drill_failback.py` | Proving the runbook works — scheduled DR tests, audits/compliance, after infra changes, onboarding. **No real outage.** | `drill_failover.py` in **WEST** first, then `drill_failback.py` in **EAST** | Self-asserting, end-to-end loop. Failover side promotes WEST **and simulates outage work** (logs a new model version in WEST), then asserts `dr_state=west`, a new version exists, and a `FAILOVER` audit row landed. Failback side reverse-CDCs that simulated version into EAST, then asserts it was recovered and `dr_state=east` (steady state restored). Each half **raises on failure**, so as a scheduled job task it goes red and alerts. |

**When to use the drills**

- **Periodic DR validation** (e.g. quarterly) — schedule the `dr_models_drill_failover` /
  `dr_models_drill_failback` jobs to continuously prove RTO/RPO and that failover→failback works.
- **After any change** to workspaces, the SPN, secret scopes, or this code — run the pair once to
  confirm nothing regressed before you rely on it.
- **Onboarding / demos** — a safe, repeatable way to see the whole DR lifecycle without taking a
  region down.

The drills are safe to re-run: they always restore steady state (`dr_state=east`, east→west CDC
resumes automatically) at the end. Both halves require the secret scopes from step 2 — in
particular `drill_failback` needs `dr_remote_west` in **EAST** for the reverse pull. Run them as an
ordered **pair** (failover in WEST → failback in EAST); the failback half asserts the failover half
already ran.

---

## DR scenarios — end-to-end with post-run checks

Two concrete walk-throughs: the **first DR cycle** (right after the baseline) and a **second DR
event** later (steady state already established). Each lists the exact steps and **what to verify in
the workspace** after every run. All SQL runs against `dr_poc.dr_control.*`; `m =
dr_poc.ml.iris_dr_model`.

> **Where to look in the workspace:** **Catalog Explorer → `dr_poc` → `ml`** for models/versions/
> aliases/tags and **lineage** (runs + experiments); **`dr_control`** for the `dr_replication_audit`,
> `dr_state`, `dr_object_inventory` tables and the `v_dr_watermark` / `v_dr_failures` views.

### Scenario 1 — First DR cycle (baseline → failover → failback)

**Pre-condition:** Steps 1–5 done. WEST is a warm mirror of EAST and `dr_state.active_primary = east`.

| # | Action | Run in | Notebook / job |
|---|---|---|---|
| 1 | Keep WEST current (steady state) | WEST | `03_cdc.py` (or scheduled `dr_models_cdc`) |
| 2 | **Outage:** EAST region down → **failover** | WEST | `04_failover_failback.py` `action=failover` |
| 3 | Repoint consumers/endpoints to WEST | — | (operational) |
| 4 | New model versions produced while WEST is primary | WEST | your training pipeline |
| 5 | EAST recovers → **failback** | EAST | `04_failover_failback.py` `action=failback` |

**Post-run checks**

After **failover** (in WEST):
```sql
SELECT active_primary, reason, updated_at FROM dr_state;          -- active_primary = west, reason = FAILOVER
SELECT * FROM dr_replication_audit WHERE operation='FAILOVER' ORDER BY event_time DESC LIMIT 1;  -- one SUCCESS row
```
- **Historical fidelity (the point of DR):** WEST still has every pre-outage version — run in WEST:
  ```python
  from mlflow import MlflowClient
  c = MlflowClient(registry_uri="databricks-uc")
  print(sorted(int(v.version) for v in c.search_model_versions(f"name='{m}'")))   # all baseline versions present
  print({k.lower(): v for k, v in c.get_registered_model(m).aliases.items()})      # Champion/Challenger preserved
  ```
- If serving endpoints are in scope: the WEST standby endpoint scaled up (Catalog Explorer → Serving, or the `ENDPOINT` audit rows).

After **failback** (in EAST):
```sql
SELECT active_primary, reason, updated_at FROM dr_state;          -- active_primary = east (home), reason = FAILBACK
SELECT operation, model_name, source_version, target_version, status, bytes_moved
FROM dr_replication_audit WHERE operation IN ('IMPORT','VERIFY','FAILBACK') ORDER BY event_time DESC;
```
- **No data loss:** every version created in WEST during the outage now exists in EAST. Compare both
  registries (run the `sorted(... versions ...)` snippet in EAST and WEST — the version sets match).
- **Experiment IDs differ, names match** (workspace-local IDs are expected to differ; lineage names converge).
- `v_dr_failures` is empty for the cycle; `v_dr_watermark.synced_version` equals the current source max.

### Scenario 2 — Second DR event (steady state already running)

**Pre-condition:** Scenario 1 finished. `dr_state.active_primary = east`, east→west CDC resumed.

**Setup steps:** *none new.* Secret scopes, control plane, staging volume and grants already exist
from the first setup. You only **confirm steady state is healthy** before relying on it again:
```sql
-- WEST is current (no un-synced source versions):
SELECT * FROM v_dr_watermark;                       -- synced_version == source max for each model
SELECT * FROM v_dr_failures;                          -- empty
SELECT operation, status, source_version, event_time
FROM dr_replication_audit WHERE operation='VERIFY' ORDER BY event_time DESC LIMIT 5;
```

| # | Action | Run in | Notebook / job |
|---|---|---|---|
| 1 | **Incremental sync** keeps WEST warm between events | WEST | `03_cdc.py` (or scheduled job) — re-pulls only models whose source version advanced; idempotent ("nothing to sync" when current) |
| 2 | **Second outage** → failover | WEST | `04_failover_failback.py` `action=failover` |
| 3 | Recover → failback | EAST | `04_failover_failback.py` `action=failback` |

**What to check in the workspace**
- **Incremental correctness (step 1):** after producing a new version in EAST and running `03_cdc.py`
  in WEST, the new version appears in WEST and a `VERIFY` row is written; a second run logs
  *"nothing to sync"*. `rpo_lag_sec` on the row shows recovery-point lag.
- **History is cumulative:** `dr_state` is a single row updated in place, but the **audit table retains
  every cycle** — after the second failback you should see **two** `FAILOVER` + two `FAILBACK` rows:
  ```sql
  SELECT operation, direction, status, event_time
  FROM dr_replication_audit WHERE operation IN ('FAILOVER','FAILBACK') ORDER BY event_time;  -- 2 + 2
  ```
- **Versions/aliases converged** in both registries (same checks as Scenario 1).
- **`dr_object_inventory`** reflects the latest `last_source_version` / `last_synced_at` per model
  (desired-state snapshot for reconciliation + health).

> **Drills instead of a real event:** for periodic validation without an outage, run the
> `drill_failover.py` (WEST) + `drill_failback.py` (EAST) pair — they self-assert all of the above and
> always restore steady state (`dr_state=east`).

---

## Quick start (local CLI, optional)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .            # native engine uses runtime MLflow + databricks-sdk (no extra engine)

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
(currently the `west` target).

```bash
databricks bundle validate -t west
databricks bundle deploy   -t west        # secondary (us-west-2): steady-state DR
databricks bundle deploy   -t east        # primary (us-east-1): failback + bootstrap jobs
databricks bundle run dr_models_bootstrap -t west   # create UC control tables
databricks bundle run dr_models_bootstrap -t east
databricks bundle run dr_models_replicate -t west   # baseline (in secondary)
# verify, then go live (unpause schedules):
databricks bundle deploy -t west --var cdc_pause_status=UNPAUSED
```

## Regions

| Role | Region | Workspace |
|------|--------|-----------|
| Primary | us-east-1 | fe-sandbox-krish-us-eat-1-sandbox |
| Secondary | us-west-2 | fe-sandbox-ankita-dr-wp-us-west-2 |

## Status

POC. Models module under active development
