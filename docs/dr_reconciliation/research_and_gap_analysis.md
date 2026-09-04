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

## 6. Top 10 objects to reconcile — workspace assets prioritized (all Managed-DR-supported)

Ranked for (a) Managed-DR support, (b) blast radius if drifted, (c) a concrete, computable parity
signal. **Workspace assets are the priority tier (ranks 1–7)** — they are what makes the promoted
workspace *operable* the moment you fail over — followed by the Unity Catalog tier (8–10).

| # | Tier | Object | Why it matters | Recon signal (primary vs secondary) | Source |
|---|---|---|---|---|---|
| **1** | WS | **Notebooks** | The code that runs everything; matched exactly by the **preserved asset ID** across regions | presence (by asset id + path) · **content hash** · language | Workspace API (`list`/`export`) |
| **2** | WS | **Jobs / workflows** | Operational recovery; jobs replicate but **schedules are paused** in the secondary | definition/tasks/libs/params parity · **schedule present & PAUSED** · run-as/owner · job ACLs · referenced assets exist | `system.lakeflow.jobs`, Jobs API, permissions API |
| **3** | WS | **SQL warehouses** | Serving/BI compute; expected to arrive **`STOPPED`** | config parity (size, type, channel, auto-stop, tags) · **state = STOPPED** · permissions | Warehouses API, permissions API |
| **4** | WS | **Clusters** | Job/interactive compute; expected **`TERMINATED`** | config parity (policy, node types, DBR, init scripts, libraries) · **state = TERMINATED** · permissions | Clusters API, permissions API |
| **5** | WS | **Draft AI/BI dashboards** | Analyst-facing; **only drafts replicate — published do NOT** | presence · **spec/definition hash** · referenced datasets resolvable · flag published-only | Lakeview API |
| **6** | WS | **Files & folders** | Repos/workspace files backing notebooks & apps | inventory (by asset id + path) · **content hash** · folder structure | Workspace API |
| **7** | WS | **Workspace ACLs** | Least-privilege after failover — assets are useless (or over-exposed) with wrong ACLs | per-asset **`(principal, permission_level)` set** diff across notebooks/jobs/warehouses/clusters/dashboards/folders | permissions API |
| **8** | UC | **Managed tables** (data + metadata) | The data crown jewel; silent drift = wrong results after failover | existence · **commit version** · **row count** · `numFiles`/`sizeInBytes` · **schema hash** · lag | `information_schema.tables/columns`, `DESCRIBE DETAIL/HISTORY`, `system.replication.states` |
| **9** | UC | **Grants & ownership** | Security: drift = broken or over-broad access | `(principal, privilege)` set per securable · **owner** parity · isolation mode | `information_schema.*_privileges`, `SHOW GRANTS` |
| **10** | UC | **Views & functions** | Consumption layer; cross-catalog views need owner perms in the secondary | existence · **DDL hash** · view-owner privileges on referenced objects | `information_schema.views/routines`, `SHOW CREATE` |

**Tier-2 (supported, lower priority):** external tables & external volumes (**metadata**-only parity;
volume *data* is not replicated), catalog isolation-mode/workspace bindings.

### 6.1 What we track & showcase per object (the metrics)

Every object gets a **status** — `IN_SYNC` · `LAGGING` (copied but behind RPO target) · `DRIFTED`
(present but a signal differs) · `MISSING` (in primary, absent in secondary) · `FAILED` (Managed DR
error) · `UNSUPPORTED-in-scope` (in scope but silently unreplicable) — plus **severity** and
**first_seen**. The specific signals per object type:

**Workspace-asset tier (priority):**

**1. Notebooks** — inventory matched by the **preserved workspace asset ID** (not just path, so a
move is not mistaken for a delete) + path; **content hash** (export + hash) for drift; language.
*Showcased:* missing/extra notebooks, content drift, path moves.

**2. Jobs / workflows** — **definition parity** (tasks, job clusters, libraries, parameters) from
`system.lakeflow.jobs`/Jobs API; **schedule present AND `PAUSED`** in the secondary (flag missing or
**unexpectedly active**); run-as/owner + **job ACLs**; referenced notebooks/clusters exist.
*Showcased:* definition diff, schedule state, missing job, ACL diff.

**3. SQL warehouses** — config parity (cluster size, type, channel, auto-stop, tags); **state expected
`STOPPED`**; permissions. *Showcased:* config drift, unexpected state, permission diff.

**4. Clusters** — config parity (policy, node types, DBR version, init scripts, libraries,
autoscaling); **state expected `TERMINATED`**; permissions. *Showcased:* config drift, unexpected
running state, permission diff.

**5. Draft AI/BI dashboards** — presence + **spec/definition hash**; referenced datasets/warehouse
resolvable in the secondary; explicitly flag **published dashboards** (not replicated — drafts only).
*Showcased:* missing draft, spec drift, published-only warning.

**6. Files & folders** — inventory (asset id + path) + **content hash**; folder-structure parity.
*Showcased:* missing/extra files, content drift.

**7. Workspace ACLs** — per-asset **`(principal, permission_level)` set** across all the above assets,
primary vs secondary → **added/removed** grants. *Showcased:* `+/- principal × permission` diff
(**high severity** — security/operability).

**Unity Catalog tier:**

**8. Managed tables** — group `replication_lag_ms` + per-table **last commit** (`DESCRIBE HISTORY`);
**commit version**, **row count** (or per-partition), **`numFiles`**/**`sizeInBytes`**
(`DESCRIBE DETAIL`); **schema hash** (`information_schema.columns`), partition cols, table features;
**row filters/column masks/ABAC** → `FAILED`; **owner** + isolation mode. *Showcased:* version/row/size/schema drift, lag vs target.

**9. Grants & ownership** — `(principal, privilege)` set per securable from
`information_schema.*_privileges`, primary vs secondary → **added/removed**; **owner** parity (flag
fallback to the DR service principal); isolation-mode/binding. *Showcased:* privilege diff, owner change.

**10. Views & functions** — presence + **DDL hash** (`information_schema.views`/`routines`,
`SHOW CREATE`); **cross-catalog** dependency resolvability (owner needs `USE CATALOG`/`USE SCHEMA`/
`SELECT` on referenced objects in the secondary — `DR_INVALID_CONFIGURATION.CROSS_CATALOG_VIEW_PERMISSION`).
*Showcased:* DDL diff, missing-dependency/invalid-view flag.

**Cross-cutting (all objects):** last-reconciled time, the raw primary/secondary signature pair
(for evidence/audit), and a link back to the blocking `error_class` from `system.replication.states`
when one applies. This is what makes the report **audit-ready** for DR-test sign-off.

## 7. Proposed architecture (what we're building)

- A scheduled **recon job** runs in a **primary/neutral workspace**. It (1) pulls **expected scope**
  from the DR API + `system.replication.states`, (2) reads the **primary** object inventory + parity
  signals from `information_schema` / Delta metadata / Jobs & Workspace APIs, (3) reads the **secondary**
  the same way via the **read-only monitor workspace**, (4) **diffs per object type** for the top-10
  objects → classifies each object `IN_SYNC` / `LAGGING` / `DRIFTED` / `MISSING` / `FAILED` /
  `UNSUPPORTED-in-scope`, and (5) writes to **recon control tables**
  (`dr_recon_runs`, `dr_recon_coverage`, `dr_recon_inventory`, `dr_recon_findings` — DDL in
  [`sql/dr_recon_tables.sql`](../../sql/dr_recon_tables.sql)).
- The **AI/BI dashboard reads ONLY the `dr_recon_*` tables** (the engine folds `replication_lag_ms`
  + `errors[]` into them), so the dashboard is decoupled from Managed-DR enrollment. It presents:
  workspace/failover-group **identity**, a **failover-readiness** headline (RAG), **RPO/RTO**,
  **failover/failback history + reconciliation**, a **coverage scorecard** across the top-10
  (workspace assets first), an **RPO trend**, **blocking errors** + a **silent-gap detector**, a
  **per-object drill-down**, and a **pre-failover readiness checklist**.

This turns Managed DR's coarse "pipeline is healthy, lag = N" into an **object-level, auditable,
shareable reconciliation report** — the piece Managed DR does not provide.

### 7.1 Deliverables in this folder

| File | Role |
|---|---|
| `research_and_gap_analysis.md` | This doc — research, gaps, top-10 objects, per-object signals, architecture. |
| `dr_reconciliation_dashboard.html` | **Sample dashboard** — extensive, interactive mockup of how the report looks (workspace identity, DR-event history, scorecard, RPO, errors, 10-object drill-down, readiness checklist). The visual spec for the AI/BI build. |
| `dr_reconciliation_design.html` | **Architecture & build spec** — implementation-grade: system context, end-to-end pipeline, per-object recon contract (source → signature → diff → status), classifier, control-table schemas, failover/failback behavior, runtime/permissions, build plan. Written so a developer agent can build from it. |
| `dr_reconciliation_architecture.excalidraw` | High-level editable architecture diagram. |
| `../../sql/dr_recon_tables.sql` | DDL for the four `dr_recon_*` control tables the dashboard reads. |

> **Productionization:** the live report is a **Databricks AI/BI (Lakeview) dashboard** over the
> `dr_recon_*` tables; the HTML is the sample/visual spec, not the runtime.

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
