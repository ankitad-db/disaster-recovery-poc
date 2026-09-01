# Workspace Secrets DR

Disaster recovery for **Databricks workspace secrets** — secret scopes, their values,
and their ACLs — across two cross-region workspaces. Databricks Managed DR does not
cover workspace secrets; this fills that gap with a small, SDK-only, config-driven flow.

- **Primary** (active): `us-east-2` — `fe-sandbox-fe-sandbox-ps-dr-wp-us-east-2`
- **Secondary** (passive standby): `us-west-2` — `fe-sandbox-fe-sandbox-ps-dr-wp-us-west-2`

> Full step-by-step test plan with what to verify at each stage:
> **[docs/secrets_dr_runbook.md](docs/secrets_dr_runbook.md)**. Architecture diagram:
> **[docs/secrets_dr_architecture.excalidraw](docs/secrets_dr_architecture.excalidraw)**.

## How it works

Active-passive, following Databricks managed-DR best practice: **no compute runs on the
passive secondary in steady state**. All steady-state work happens in the primary; the
secondary only receives the replicated, encrypted bundle over S3.

```
PRIMARY (us-east-2, active)                         SECONDARY (us-west-2, passive)
┌─────────────────────────────────────┐            ┌──────────────────────────────┐
│ EXPORT job (scheduled, run as SPN)   │            │  (no compute in steady state) │
│  1 detect change  system.access.audit│            │                               │
│  2 read values    get_secret + sha256 │            │   S3 bucket (secondary)       │
│  3 encrypt        KMS data key + GCM  │            │   dr-secrets-us-west-2-…       │
│  4 snapshot ──────► S3 bucket (primary)│  S3 CRR   │        ▲                      │
│     dr-secrets-us-east-2-…            │──────────►│────────┘  encrypted bundle    │
└─────────────────────────────────────┘            └──────────────────────────────┘
                                                       on FAILOVER (promoted):
                                                       IMPORT = diff bundle vs live,
                                                       apply only the delta.
```

1. **Export (primary).** Detect changed secrets via `system.access.audit` (with a full
   state-diff recon as a safety net). When anything changed, read the **full** in-scope
   state with `get_secret`, **envelope-encrypt** each value (KMS data key + AES-256-GCM,
   with `{scope,key}` bound as associated data), and write a **complete desired-state
   snapshot** bundle to the primary S3 bucket. Plaintext never lands in S3.
2. **Replicate.** S3 **Cross-Region Replication** (bidirectional, `secrets/` prefix,
   KMS re-encrypt at destination) mirrors the bundle to the secondary bucket. No
   secondary compute is involved.
3. **Import on failover (destination-aware).** Once the secondary is promoted (writable),
   it reads the bundle from its **local** bucket, reads its **own live** secret state,
   **diffs the two**, and applies **only the difference** (`create_scope` / `put_secret`
   / `put_acl` / `delete_secret`). First failover into a cold secondary is a full
   rebuild; later failovers are incremental. Failback is symmetric.

Control tables (`dr_secrets_inventory`, `dr_secrets_audit`) in each workspace record
state, RPO, and history. Everything is SDK-only (no `dbutils`), so the same code runs in
a notebook, a job, or locally.

## Layout

```
src/databricks_dr/
  modules/secrets/      config · changefeed · crypto · store · control · export · import_ · runner · seed
  common/               logging · sql  (SqlExecutor: Spark on-cluster or SDK statement-exec off-cluster)
config/secrets_dr_config.yaml   workspaces · scopes · detection · storage(S3/KMS) · reconcile · control · seed
notebooks/secrets/      00_setup · 01_seed · 10_export · 20_import   (thin Databricks wrappers)
sql/secrets_control_tables.sql  control-table DDL
scripts/provision_secrets_dr_aws.sh   idempotent AWS provisioner (S3 + KMS + IAM + bidirectional CRR)
resources/dr_secrets_jobs.yml   Asset Bundle jobs (scheduled export + failover import)
docs/secrets_dr_runbook.md      end-to-end test playbook
```

## Quick start

```bash
# 0. AWS assets (once). Uses the caller's AWS creds; idempotent.
bash scripts/provision_secrets_dr_aws.sh

# 1. Auth to both workspaces.
databricks auth login https://fe-sandbox-fe-sandbox-ps-dr-wp-us-east-2.cloud.databricks.com --profile dr-east
databricks auth login https://fe-sandbox-fe-sandbox-ps-dr-wp-us-west-2.cloud.databricks.com --profile dr-west

# 2. Control tables — run notebooks/secrets/00_setup_secrets.py in BOTH workspaces.
# 3. (POC) Seed sample scopes — notebooks/secrets/01_seed_secrets.py in the PRIMARY.
# 4. Export — notebooks/secrets/10_export_secrets.py in the PRIMARY (full=true first run).
# 5. Failover import — notebooks/secrets/20_import_secrets.py in the SECONDARY.
```

Run steps 4–5 locally instead of in notebooks:

```bash
pip install -e .
python -m databricks_dr.modules.secrets.runner export --full
python -m databricks_dr.modules.secrets.runner import --region secondary
```

The **full test playbook** (with assertions at each step) is in
[docs/secrets_dr_runbook.md](docs/secrets_dr_runbook.md).

## Configuration

Everything is driven by [config/secrets_dr_config.yaml](config/secrets_dr_config.yaml):
workspaces (host/profile/region), which `scopes` to protect, `detection` strategy,
`storage` (S3 buckets + per-region KMS + client-side encryption), `reconcile` mode
(`mirror` vs `additive`), and the `control` tables. The `seed` section is POC-only.

## Status

POC — **verified end-to-end on 2026-09-01**: replicate east→west, incremental diff-and-apply
(rotate + delete), and failback west→east all passed against the live sandbox workspaces +
real S3/KMS/CRR. See the test report ([docs/secrets_dr_test_report.md](docs/secrets_dr_test_report.md))
for the run log, observations, and findings (incl. the KMS Multi-Region-Key requirement).
