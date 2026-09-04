# DR Reconciliation Report — Research & Gap Analysis

**Goal:** design a **DR reconciliation dashboard** that gives a per-object, trustworthy view of
whether a Databricks **Managed DR** secondary is actually a faithful replica of the primary —
because the native monitoring is coarse (an aggregate replication lag + blocking-error list per
failover group, *not* a per-object parity/coverage view).

> Status: research + design. No code yet. This doc captures what Managed DR covers, what native
> reconciliation exists, where the gaps are, what we can leverage, the **top 5 objects** to
> reconcile, and the proposed architecture (see `dr_reconciliation_architecture.excalidraw`).
> Sources: Databricks *Managed disaster recovery* docs and the *Replication system table
> reference* (both updated 2026-08), plus the Databricks DR REST API.

---

## 1. What Managed DR is

Managed DR is a **gated** Databricks capability (Premium + "Mission Critical" add-on, serverless
required) that replicates a workspace + its Unity Catalog metastore to a **secondary region** on a
continuous schedule, and lets you fail over from the account console. Databricks owns the
replication pipeline; the customer does not write replication scripts. Two independently-optional
categories are replicated. A **stable URL** (and **stable workspace ID**) can front the current
primary so clients / Terraform / DABs keep working after failover.

Key operational facts that shape reconciliation:
- The **secondary in-scope catalogs are read-only** and **you cannot run compute on the secondary
  workspace** while Managed DR is enabled. Databricks recommends a **separate read-only monitor
  workspace** in the secondary region to run validation queries. *(This is where our recon reads run.)*
- Managed DR creates auxiliary UC resources per failover group: a **connection** to the other
  region and a **foreign catalog** per replicated catalog (visible in Catalog Explorer). Do not delete.
- **Initial bootstrap** can take up to **2 weeks** for large workspaces; steady-state is continuous.

## 2. What Managed DR replicates (supported objects) — and what it does NOT

### Replicated
| Category | Objects | Notes |
|---|---|---|
| **UC metadata + data** | **Managed tables** (Delta, **with data**); **external tables & volumes** (**metadata only**); **views**; **functions**; **all permission grants**; catalog **isolation mode** | External table/volume *data* stays in place (resolved via storage mappings); managed **volume data is not** replicated (metadata only). |
| **Workspace assets** (optional) | **Notebooks, jobs, SQL warehouses, clusters, draft AI/BI dashboards, files, folders** — and their **ACLs** | SQL warehouses replicate `STOPPED`, clusters `TERMINATED`, **job schedules paused** in the secondary. Workspace **asset IDs are preserved** across regions. |

Ownership of replicated securables is transferred to match the primary owner (falls back to the DR
service principal if the primary owner was deleted).

### NOT replicated (explicit limitations)
Materialized views · streaming tables · **Lakeflow pipelines** · **managed volume data** · **UC &
workspace secrets** · **ML models** · **model serving endpoints** · **vector search indexes** ·
**Delta shares** · **published** AI/BI dashboards (drafts do replicate) · Structured Streaming
outside Lakeflow. Tables with **row filters / column masks** or **ABAC** tags are flagged
**Failed to replicate** and *hold up RPO* until removed from scope. **External locations & storage
credentials are NOT auto-created** in the secondary — the customer must create them + storage mappings.

> **Scope note for this project:** the reconciliation report targets **only Managed-DR-supported
> objects** (per the ask). Unsupported assets — notably **workspace secrets** — are out of scope
> here (secrets DR is a separate project on `feat/dr-workspace-secrets`).

## 3. Native reconciliation / monitoring — what we can leverage

Three native surfaces exist. All are useful **inputs**, none is a per-object reconciliation view.

### 3.1 Failover groups tab (account console)
Shows each failover group's **state** (`CREATING` → `INITIAL_REPLICATION` → `ACTIVE` →
`FAILING_OVER` → …), a **replication point**, and active errors. The replication point is *"the last
time **all** in-scope resources were copied together"* — individual resources may be newer, but
**not all data after the replication point is guaranteed in the secondary** and can be lost on failover.

### 3.2 `system.replication.states` system table (primary data source)
Each row is a **status event per failover group** (emitted periodically + on change). **Schema:**

| Column | Type | Meaning |
|---|---|---|
| `event_id` | string | Unique status-event id |
| `event_time` | timestamp | When emitted |
| `account_id` | string | Account id |
| `failover_group_name` | string | e.g. `accounts/<acct>/failover-group/<name>` |
| `replication_state` | string | `INITIAL_REPLICATION`, `ACTIVE`, `CREATED`, `UPDATED`, `DELETED`, `FAILOVER_STARTED`, `FAILOVER_FINISHED`, `FAILOVER_ABORTED` |
| `errors` | array<struct> | Aggregated blocking errors: `error{error_class, parameters, message}` + `affected_assets_counts[]{asset_type, failing_count}` |
| `replication_lag_ms` | long | ms since the last successful replication of **all** in-scope assets; **`null`** = at least one asset never replicated; a **rising** value = at least one asset is stuck |
| `effective_primary_region` | string | Current primary region at event time |
| `managed_assets` | struct | `metastore_ids[]`, `workspace_sets[]{name, workspace_ids[]}`, `catalogs[]{name}` |

Caveats: **"It does not list which individual objects replicated successfully."** Data can take
**up to 3 hours** to populate. Unsupported asset types **do not appear at all** (neither success nor error).

### 3.3 Disaster Recovery REST API (`/api/disaster-recovery/v1`)
Account-level API for failover groups + stable URLs (e.g. `GET /api/disaster-recovery/v1/accounts/<id>/stable-urls`).
Useful to enumerate failover groups, scope (catalogs, workspace sets), and stable-URL / stable-workspace-id.

## 4. The gaps (why native recon is not enough)

1. **Aggregate, not per-object.** `system.replication.states` gives **one lag number + an
   aggregated error list per failover group** — explicitly *not* a per-table / per-job status. You
   cannot answer "is *this* table in sync?" from it.
2. **Replication success ≠ data fidelity.** Lag means "copied up to time T," not "secondary is
   byte/row/schema identical." No row-count, commit-version, schema, or ACL parity is computed.
3. **Silent coverage gaps.** Unsupported asset types are simply **absent** from the table —
   absence is indistinguishable from "replicated." In-scope objects using unsupported features
   (row filters, masks, ABAC) fail silently except as an aggregated error count.
4. **No completeness/coverage scorecard.** There's no native view of "N in-scope objects → X
   in-sync, Y lagging, Z failed, W unsupported" per object type.
5. **Verification is manual.** Confirming a specific asset requires **inspecting it in the
   secondary** via a separate read-only monitor workspace — object by object, by hand.
6. **Latency + coarseness.** Up to **3h** to populate; the single "replication point" hides
   per-object currency.
7. **No historical, shareable reporting.** No dashboard, no per-object RPO trend, no audit-ready
   evidence for compliance / DR-test sign-off.

**Conclusion:** Managed DR answers *"is the pipeline healthy and how stale is the group?"* It does
**not** answer *"object-by-object, is my secondary a faithful, complete replica, and where is the
drift?"* — which is exactly what a DR reconciliation report must provide.

## 5. What we can leverage to close the gap (data sources)

| Source | What it gives | Role in recon |
|---|---|---|
| `system.replication.states` | failover-group state, RPO lag trend, blocking errors + affected asset-type counts | **Pipeline health + RPO** panel; error triage |
| DR REST API `/api/disaster-recovery/v1` | failover groups, replication **scope** (catalogs, workspace sets), stable URL/workspace id | Define the **expected in-scope inventory** |
| `system.information_schema.*` (both metastores) | catalogs, schemas, **tables**, columns, **views**, **routines/functions**, **table_privileges** / grants | **Object inventory + metadata/grant parity** (primary vs secondary) |
| Delta metadata — `DESCRIBE DETAIL`, `DESCRIBE HISTORY`, `SHOW CREATE TABLE` | table **commit version**, `numFiles`, `sizeInBytes`, `lastModified`, schema DDL | **Data-fidelity signals** for managed tables (version/count/size/schema drift) |
| `system.access.audit` | secret/table/job mutation events, write recency | Change activity + "written after replication point?" risk flag |
| `system.lakeflow.jobs` (+ Jobs API) | job definitions, schedules, last runs | **Jobs** parity (definition + schedule-paused state) |
| Workspace API (`workspace list/export`, permissions API) | notebooks/files inventory + **ACLs** | **Workspace-asset** parity (presence + ACLs) |
| Read-only **monitor workspace** in secondary | lets recon compute *read* the read-only secondary metastore | Where the secondary-side reads execute |

The recon engine runs in the **primary (or a neutral) workspace**, reads the **secondary** via a
cross-workspace client / the read-only monitor workspace, diffs per object type, and writes results
to **recon control tables** feeding an **AI/BI dashboard**.

## 6. Top 5 objects to reconcile (all Managed-DR-supported, ranked by importance)

Chosen for (a) Managed-DR support, (b) blast radius if drifted, (c) a concrete, computable parity signal.

| # | Object | Why it matters | Recon signal (primary vs secondary) | Source |
|---|---|---|---|---|
| **1** | **UC managed tables** (data + metadata) | The crown jewel; silent data drift = wrong results after failover | existence · **commit version** · **row count** (or per-partition count) · `numFiles`/`sizeInBytes` · **schema hash** · replication lag | `information_schema.tables/columns`, `DESCRIBE DETAIL/HISTORY`, `system.replication.states` |
| **2** | **UC grants & ownership** | Security: drift = broken access or over-broad access after failover | grant set per securable (principal, privilege) · **owner** parity · isolation mode | `information_schema.*_privileges`, `SHOW GRANTS`, Catalog Explorer owner |
| **3** | **UC views & functions** | Consumption layer; a missing/definition-drifted view breaks dashboards & apps; cross-catalog views need owner perms in secondary | existence · **definition (DDL) hash** · view-owner privileges on referenced objects | `information_schema.views/routines`, `SHOW CREATE`, error class `DR_..CROSS_CATALOG_VIEW_PERMISSION` |
| **4** | **Jobs / workflows** | Operational recovery; jobs replicate but **schedules are paused** in secondary — must confirm definition parity + intended schedule | existence · definition/tasks parity · **schedule present & paused** · owner/ACL | `system.lakeflow.jobs`, Jobs API, permissions API |
| **5** | **Notebooks & workspace files (+ ACLs)** | The code assets; presence + ACL parity so the promoted workspace is usable by the right people | inventory count/paths · content hash (optional) · **ACL parity** | Workspace API (`list`/`export`), permissions API |

**Tier-2 (supported, lower priority):** external tables & volumes (**metadata**-only parity),
SQL warehouses & clusters (config parity, expected `STOPPED`/`TERMINATED`), draft AI/BI dashboards.

### 6.1 What we track & showcase per object (the metrics)

Every object gets a **status** — `IN_SYNC` · `LAGGING` (copied but behind RPO target) · `DRIFTED`
(present but a signal differs) · `MISSING` (in primary, absent in secondary) · `FAILED` (Managed DR
error) · `UNSUPPORTED-in-scope` (in scope but silently unreplicable) — plus **severity** and
**first_seen**. The specific signals per object type:

**1. UC managed tables** — the data-fidelity core:
- *Coverage:* in the failover-group scope? present in the secondary metastore?
- *Freshness / RPO:* group `replication_lag_ms` + per-table **last commit timestamp** (`DESCRIBE HISTORY`) primary vs secondary.
- *Data fidelity:* **Delta commit version**, **row count** (total, or per-partition for large tables), **`numFiles`** + **`sizeInBytes`** (`DESCRIBE DETAIL`); optional **per-partition checksum** for a canary set.
- *Schema parity:* column names/types/order **schema hash** (`information_schema.columns`), partition columns, table properties, and table features (CDF, deletion vectors).
- *Replicability blockers:* **row filters / column masks / ABAC** tags → these fail Managed DR; surface as `FAILED` with the reason.
- *Governance:* **owner** parity + catalog **isolation mode** (open vs isolated/bound).
- **Showcased:** version drift, row-count Δ, size Δ, schema diff, lag vs target.

**2. UC grants & ownership** — security parity:
- *Grant set* per securable = set of `(principal, privilege)` from `information_schema.table_privileges` /
  `schema_privileges` / `catalog_privileges` / `routine_privileges`, **primary vs secondary** → **added / removed** grants.
- *Owner* parity per securable (Managed DR falls back to the DR service principal if the primary owner was deleted — flag it).
- *Isolation mode / workspace binding* parity.
- **Showcased:** `+/- principal × privilege` diff, owner changes, count of privilege drifts (**high severity** — security).

**3. UC views & functions** — consumption-layer integrity:
- *Presence* + **definition (DDL) hash** (`information_schema.views.view_definition`, `routines.routine_definition`, `SHOW CREATE`).
- *Dependency resolvability in the secondary* — especially **cross-catalog views**, whose owner must hold `USE CATALOG`/`USE SCHEMA`/`SELECT` on referenced objects in the secondary (`DR_INVALID_CONFIGURATION.CROSS_CATALOG_VIEW_PERMISSION`).
- **Showcased:** DDL diff, missing-dependency reason, invalid-view flag.

**4. Jobs / workflows** — operational recovery:
- *Presence* + **definition parity** (tasks, job clusters, libraries, params) from `system.lakeflow.jobs` / Jobs API.
- *Schedule state:* schedule **present AND `PAUSED`** in the secondary (Managed DR pauses secondary schedules) — flag missing or **unexpectedly active** schedules.
- *Run-as / owner / job ACLs* parity; **referenced assets exist** (notebook paths, clusters).
- **Showcased:** definition diff, schedule state, missing job, ACL diff.

**5. Notebooks & workspace files (+ ACLs)** — code assets:
- *Inventory parity* by path/count (Workspace API). Managed DR **preserves workspace asset IDs** across regions — leverage the ID to match objects exactly (not just by path).
- *Content* hash (optional; export + hash for critical notebooks).
- *ACL parity* = set of `(principal, permission_level)` from the permissions API, primary vs secondary.
- **Showcased:** missing/extra files, content drift, ACL diff.

**Cross-cutting (all objects):** last-reconciled time, the raw primary/secondary signature pair
(for evidence/audit), and a link back to the blocking `error_class` from `system.replication.states`
when one applies. This is what makes the report **audit-ready** for DR-test sign-off.

## 7. Proposed architecture (what the dashboard is)

Depicted in **`dr_reconciliation_architecture.excalidraw`**. In one paragraph:

- A scheduled **recon job** runs in a **primary/neutral workspace**. It (1) pulls **expected scope**
  from the DR API + `system.replication.states`, (2) reads the **primary** object inventory + parity
  signals from `information_schema` / Delta metadata / Jobs & Workspace APIs, (3) reads the **secondary**
  the same way via the **read-only monitor workspace**, (4) **diffs per object type** for the top-5
  objects → classifies each object `IN_SYNC` / `LAGGING` / `DRIFTED` / `MISSING` / `FAILED` /
  `UNSUPPORTED-in-scope`, and (5) writes to **recon tables** (`dr_recon_inventory`,
  `dr_recon_findings`, `dr_recon_runs`).
- An **AI/BI dashboard** reads those tables + `system.replication.states` and presents: a **coverage
  scorecard** (per object type: in-sync / drifted / missing / failed / unsupported), an **RPO trend**
  (from `replication_lag_ms`), a **blocking-errors** panel (by `error_class` + affected asset type),
  a **per-object drill-down** (table version/row/schema drift; grant diffs; view DDL diffs; job
  schedule state; notebook/ACL parity), and a **failover-readiness** headline (green only when scope
  is fully covered and lag < RPO target).

This turns Managed DR's coarse "pipeline is healthy, lag = N" into an **object-level, auditable,
shareable reconciliation report** — the piece Managed DR does not provide.

## 8. Open questions / assumptions

- **Cloud = AWS.** The replicated-object list, limitations, and `system.replication.states`
  monitoring are **identical across clouds**; only the infrastructure differs. AWS specifics:
  storage mappings are **S3→S3** (e.g. `s3://primary-bucket/data/*` → `s3://secondary-bucket/data/*`);
  a **storage credential** (IAM role) + external location must exist in the secondary for each one
  the primary catalogs use, with **ALL PRIVILEGES** for the account admin; if S3/DBFS network access
  is restricted, **allow the peer region's control-plane IPs** at the S3 firewall; stable URLs need
  **front-end (inbound) PrivateLink**. Source + secondary storage must allow serverless access both ways.
- **Reading the read-only secondary:** you cannot run compute on the secondary workspace; the recon
  reads run from a **read-only monitor workspace** in the secondary region, bound to the secondary
  metastore (for `information_schema` reads).
- **`system.replication.states` latency (≤3h)** means RPO panels are near-real-time, not instant.
- **Data-fidelity depth vs cost:** full row hashing is expensive; default to count + commit-version +
  schema-hash, with optional per-partition checksum for critical tables (canary set).
- Confirm the account is enrolled in Managed DR (**gated**) before the dashboard has live
  `system.replication` data.

## 9. References (AWS)
- Databricks (AWS) — *Managed disaster recovery* `docs.databricks.com/aws/en/admin/managed-disaster-recovery`: supported objects, limitations, failover, monitoring, S3 storage mappings.
- Databricks (AWS) — *Disaster recovery* `docs.databricks.com/aws/en/admin/disaster-recovery`: concepts & best practices.
- Databricks — *Replication system table reference* (`system.replication.states`, updated 2026-08): schema above.
- Databricks — *Disaster Recovery REST API* `/api/disaster-recovery/v1` (failover groups, stable URLs).
- Databricks — Unity Catalog `information_schema`, `DESCRIBE DETAIL`/`HISTORY`, `SHOW GRANTS`; `system.lakeflow.jobs`, `system.access.audit`.

> Cross-cloud note: the most complete public write-up of the object list, limitations, and the
> `system.replication.states` schema currently lives in the Azure doc; per Databricks it is
> identical on AWS (verified against the AWS Managed DR page).
