# Workspace Secrets DR

Disaster recovery for **Databricks workspace secrets** — secret scopes, their values,
and their ACLs — across two cross-region workspaces. Databricks Managed DR does not
cover workspace secrets; this fills that gap with a small, SDK-only, config-driven flow.

- **Primary** (active): `us-east-2` — `fe-sandbox-fe-sandbox-ps-dr-wp-us-east-2`
- **Secondary** (passive standby): `us-west-2` — `fe-sandbox-fe-sandbox-ps-dr-wp-us-west-2`

> Full step-by-step test plan with what to verify at each stage:
> **[docs/secrets_dr_runbook.md](docs/secrets_dr_runbook.md)**. Live test results & findings:
> **[docs/secrets_dr_test_report.md](docs/secrets_dr_test_report.md)**.

## How it works

**Direct cross-workspace replication** — no object storage, no AWS. A single `replicate`
job runs in the **active** workspace and reconciles the passive one over the Secrets API.
Active-passive: **no compute runs on the passive side** (reads/writes to it are control-plane
API calls, not clusters).

```
        ACTIVE (us-east-2)                                 PASSIVE (us-west-2)
┌───────────────────────────────────────┐        ┌──────────────────────────────┐
│ replicate job (run as DR SPN)          │  read  │  Secrets API (no compute)     │
│  1 read SOURCE secrets  get_secret+hash│◄──────►│  scopes · secrets · ACLs      │
│  2 read DEST secrets   (cross-workspace)│        │                               │
│  3 diff by value-hash + ACL signature  │  write │                               │
│  4 apply ONLY the delta ───────────────┼───────►│  put_secret/put_acl/delete    │
└───────────────────────────────────────┘  TLS    └──────────────────────────────┘
        failover = run replicate with the roles swapped (secondary is a warm mirror)
```

1. **Read source.** Enumerate in-scope scopes; read each value via `get_secret` → `sha256`;
   capture per-scope ACLs.
2. **Read destination** the same way, **cross-workspace** (a `WorkspaceClient` pointed at the
   peer). No cluster runs on the passive side.
3. **Diff** by value hash + ACL signature → classify each item `ADD` / `UPDATE` / `DELETE` /
   unchanged.
4. **Apply only the delta** straight into the destination (`create_scope` / `put_secret` /
   `put_acl` / `delete_secret` / `delete_acl`). Unchanged secrets are never rewritten.

Values move source→destination **directly over TLS**; both workspaces' secret stores are
encrypted at rest by the platform. Direction is parameterised, so **failover** is just
`replicate` with the roles swapped, and the secondary is a **warm mirror** — on a real outage
you promote it, no import step. `mirror` mode makes the destination an exact replica
(propagates deletes); `additive` never deletes. Control tables (`dr_secrets_inventory`,
`dr_secrets_audit`) record state, RPO, and history in each workspace.

## Layout

```
src/databricks_dr/
  modules/secrets/      config · changefeed (live-state reader) · replicate · runner · control · seed
  common/               logging · sql  (SqlExecutor: Spark on-cluster or SDK statement-exec off-cluster)
config/secrets_dr_config.yaml   workspaces · scopes · reconcile · control · seed
notebooks/secrets/      00_setup · 01_seed · 10_replicate   (thin Databricks wrappers)
sql/secrets_control_tables.sql  control-table DDL
resources/dr_secrets_jobs.yml   Asset Bundle job (scheduled replicate in the active workspace)
docs/secrets_dr_runbook.md      end-to-end test playbook · docs/secrets_dr_test_report.md  live results
```

## Quick start

```bash
# 1. Auth to both workspaces.
databricks auth login --host https://fe-sandbox-fe-sandbox-ps-dr-wp-us-east-2.cloud.databricks.com --profile dr-east
databricks auth login --host https://fe-sandbox-fe-sandbox-ps-dr-wp-us-west-2.cloud.databricks.com --profile dr-west

# 2. Control tables — run notebooks/secrets/00_setup_secrets.py in BOTH workspaces.
# 3. (POC) Seed sample scopes — notebooks/secrets/01_seed_secrets.py in the PRIMARY.
# 4. Replicate primary -> secondary:
pip install -e .
python -m databricks_dr.modules.secrets.runner replicate     # primary -> secondary
python -m databricks_dr.modules.secrets.runner failback       # secondary -> primary
```

In a Databricks job/notebook use `notebooks/secrets/10_replicate_secrets.py` — it reaches the
peer workspace with a PAT stored in a local secret scope (default `dr_peer`/`token`).

The **full test playbook** (assertions at each step) is in
[docs/secrets_dr_runbook.md](docs/secrets_dr_runbook.md).

## Configuration

Everything is driven by [config/secrets_dr_config.yaml](config/secrets_dr_config.yaml):
workspaces (host/profile/region), which `scopes` to protect, `reconcile` mode
(`mirror` vs `additive`), and the `control` tables (with a per-workspace catalog override
for metastores that lack managed storage for a fresh catalog). The `seed` section is POC-only.

## Status

POC. Direct cross-workspace replication implemented and verified end-to-end against the
sandbox workspaces (replicate east→west, incremental diff-and-apply, failback west→east) —
see [docs/secrets_dr_test_report.md](docs/secrets_dr_test_report.md).
