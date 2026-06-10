# Databricks DR Framework

Disaster Recovery for **unsupported Databricks resources**, built as an extensible framework.
The first module replicates **Unity Catalog models** (and everything related to them) between two
cross-region workspaces. Future object types (Genie, Apps, Vector Search, Secrets, Volumes) plug in
as new modules under `src/databricks_dr/modules/` without changing the core.

## Design

- **Engine:** [`mlflow-export-import`](https://github.com/mlflow/mlflow-export-import) is a pinned
  third-party dependency (in `requirements.txt`), never modified and never committed here.
  Only `common/engine.py` calls it.
- **Strategy:** one-time **baseline** (full history) + steady-state **incremental CDC** (per new
  version). The recommended path is a **cross-workspace pull**: the DR job runs in the destination
  and reads the source registry via a secret scope (no laptop, no cross-region S3 bridge). An
  **audit table** records every action; a single-row **`dr_state`** table holds the active-primary
  role so failover survives across job runs.
- **Direction is parameterized:** failover/failback flip the role (in `dr_state`); the same code
  runs both ways. Consumer-facing extras (UC grants, serving endpoints) replicate alongside models.

See the architecture and phased plan in the project plan document.

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

## Quick start (local)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
pip install "mlflow-export-import @ git+https://github.com/mlflow/mlflow-export-import@master"

# Configure profiles for both regions
databricks auth login https://fe-sandbox-ps-dr-wp-us-west-2.cloud.databricks.com --profile dr-west
databricks auth login https://fe-sandbox-ps-dr-wp-us-east-1.cloud.databricks.com --profile dr-east

# Inspect resolved config / direction
databricks-dr config show
databricks-dr models seed        # populate primary (POC)
databricks-dr models baseline    # one-time full export -> bridge -> import
databricks-dr models cdc         # incremental sync
```

## Run order (Git folder — interactive)

Add this repo as a **Git folder** (Repos) in both workspaces, create the secret scopes
(`dr_remote_west` in EAST, `dr_remote_east` in WEST), then run the notebooks in order.
Each notebook self-installs the engine and bootstraps `sys.path`.

| # | Notebook | Run in | Purpose |
|---|----------|--------|---------|
| 1 | `00_setup.py` | EAST **and** WEST | create catalog/schemas, audit table, `dr_state` (one-time) |
| 2 | `01_seed_primary.py` | WEST | seed the POC model (one-time) |
| 3 | `02_replicate_secondary.py` | EAST | baseline pull WEST→EAST (models, versions, grants, endpoints) |
| 4 | `03_cdc.py` | EAST | incremental sync — steady state, re-run anytime |
| 5 | `05_health_check.py` | EAST | drift / failure validation |

On-demand: `06_test_endpoints.py` (EAST), `drill_failover.py` (EAST) + `drill_failback.py`
(WEST), or the production `04_failover_failback.py` (`action` widget). After any `git push`,
**Pull** the Git folder before running.

## Deploy to Databricks (Asset Bundles)

The framework ships as a Databricks Asset Bundle (`databricks.yml` + `resources/`),
deploying the code and jobs (all run as `ad-dr-spn`): `dr_models_bootstrap`,
`dr_models_replicate` (baseline), `dr_models_cdc` (CDC → health, every 15 min, starts
PAUSED), `dr_models_health` (hourly scan), `dr_models_failover`, and the two drill jobs.
Steady-state jobs live in the **secondary** (default target `east`).

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

POC. Models module under active development; see the plan for phase status.
