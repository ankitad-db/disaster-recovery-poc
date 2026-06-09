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
  version), bridged across regions via S3 (sync or CRR). An **audit table** is the source of truth
  for what was replicated; `system.access.audit` is a trigger only.
- **Direction is parameterized:** failover/failback flip a role flag; the same code runs both ways.

See the architecture and phased plan in the project plan document.

## Layout

```
src/databricks_dr/
  cli.py                 # python -m databricks_dr <module> <action>
  common/                # config, clients, engine adapter, storage, audit, logging
  core/                  # BaseDRModule ABC + module registry
  modules/models/        # the models DR module (seed/baseline/cdc/grants/deps/endpoints/failover)
config/dr_config.yaml    # workspaces, metastores, buckets, UC names
sql/                     # catalog/schema + audit table DDL
notebooks/               # thin Databricks wrappers
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

## Deploy to Databricks (Asset Bundles)

The framework ships as a Databricks Asset Bundle (`databricks.yml` + `resources/`).
It deploys the code and three jobs: a one-off `baseline`, a scheduled incremental
`cdc` (every 15 min, starts PAUSED), and a manual `failover`/`failback` job. All run
as the `ad-dr-spn` service principal.

```bash
databricks bundle validate -t west
databricks bundle deploy   -t west        # primary (us-west-2)
databricks bundle run dr_models_baseline -t west
# verify, then unpause the schedule:
databricks bundle run dr_models_cdc -t west
databricks bundle deploy -t east          # secondary, used for failback
```

## Regions

| Role | Region | Workspace |
|------|--------|-----------|
| Primary | us-west-2 | fe-sandbox-ps-dr-wp-us-west-2 |
| Secondary | us-east-1 | fe-sandbox-ps-dr-wp-us-east-1 |

## Status

POC. Models module under active development; see the plan for phase status.
