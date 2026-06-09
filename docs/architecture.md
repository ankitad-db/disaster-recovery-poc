# Model DR — Architecture

Cross-region Disaster Recovery for Unity Catalog **models** between two
region-bound workspaces:

- **Primary (source):** `fe-sandbox-ps-dr-wp-us-west-2` (us-west-2)
- **Secondary (dest):** `fe-sandbox-ps-dr-wp-us-east-1` (us-east-1)

The recommended mechanism is a **cross-workspace pull**: a single job runs in the
**secondary** workspace, becomes the primary's identity (via a secret scope) to
*export*, then becomes its own identity to *import* into the local registry. No
S3 Cross-Region Replication (CRR) and no laptop/external host are required.

---

## 1. Why a pull (and not CRR)

The two regions have **independent Unity Catalog metastores**. A registered
model, its versions, aliases, experiments, and run metadata live in the
**metastore / UC control plane** — not only in S3.

- **CRR replicates S3 bytes only.** It copies `model.pkl`, `MLmodel`, etc. from
  the west bucket to the east bucket. It does **not** create the registered
  model, versions, or aliases in the east metastore.
- Therefore CRR **cannot** perform model DR on its own — an `import` step
  (`create_model_version`, alias assignment, run/experiment recreation) is
  always required. The cross-workspace pull performs that import.

| | Cross-workspace pull (current) | CRR alone | Split + CRR |
|---|---|---|---|
| Recreates UC registry/versions/aliases in east | yes (import) | never | yes (import) |
| Moves artifacts across regions | yes (direct pull) | yes | yes |
| Extra infra (replication rules, versioning, KMS) | none | yes | yes |
| Laptop / external host | none | none | none (if CRR) |
| Pieces to operate | 1 job + 1 secret scope | incomplete | 2 jobs + CRR |

**Verdict:** keep the cross-workspace pull as the core DR mechanism. The
split + CRR variant remains available (see `02a/02b` + `bridge`) for cases such
as a policy that forbids holding a cross-workspace token in east, very large
artifacts, or thousands of models — but it still needs the import step.

---

## 2. The one trick: identity switching

The pull job needs two identities and switches the process's *ambient* Databricks
identity between phases (`_ambient_identity` in `replicate.py`):

- **WEST identity** — host + SPN token read from an EAST **secret scope**. Used
  to read from the primary, including the `generate-temporary-credentials` call
  that downloads model artifacts (UC resolves artifact creds from the ambient
  identity, not the MlflowClient's registry profile).
- **EAST identity** — the cluster's own ambient identity. Used to import into the
  local registry.

The intermediate landing zone is the **EAST DBFS root bucket** — the export
writes straight there, so there is no second bucket-to-bucket copy.

```
WEST managed S3            EAST DBFS root bucket            EAST UC registry S3
(source artifacts)         (intermediate)                   (final)
        |                          |                                |
        |  EXPORT (west identity)  |                                |
        +---------- download ----->|                                |
                                   |  IMPORT (east ambient id)      |
                                   +----------- upload ------------>|
```

---

## 3. End-to-end flow (setup → seed → replicate → CDC)

```mermaid
flowchart TD
    subgraph SETUP[One-time setup - both metastores]
        S1[Create catalog + schemas<br/>dr_poc / ml / dr_control<br/><b>sql/01_uc_objects.sql</b>]
        S2[Create audit table<br/>dr_poc.dr_control.dr_replication_audit<br/><b>sql/02_audit_table.sql</b>]
        S1 --> S2
    end

    subgraph WEST[PRIMARY - WEST workspace]
        SEED[Seed model: train iris,<br/>log v1/v2/v3, aliases champion/challenger + tags<br/><b>notebooks/01_seed_primary.py</b><br/>modules/models/seed.py seed_primary]
    end

    subgraph EAST[SECONDARY - EAST workspace]
        REP[Baseline replicate - pull from WEST<br/><b>notebooks/02_replicate_secondary.py</b><br/>module.py replicate / replicate.py run_replicate]
        CDC[Scheduled incremental sync<br/><b>notebooks/03_cdc.py</b><br/>cdc.py run_cdc]
    end

    S2 --> SEED
    SEED --> REP
    REP --> CDC
    CDC -. "new version in WEST? watermark-gated re-pull" .-> CDC

    SEED -.records.-> AUDIT[(Audit table - shared<br/>observability + watermark<br/><b>common/audit.py</b>)]
    REP -.records.-> AUDIT
    CDC -.records.-> AUDIT
```

| Order | Notebook | Runs in | Purpose | Backing module |
|---|---|---|---|---|
| 0 | `sql/01_uc_objects.sql` | both | Create `dr_poc` catalog + `ml` / `dr_control` schemas | — |
| 0 | `sql/02_audit_table.sql` | both | Create the audit/watermark table | — |
| 1 | `notebooks/01_seed_primary.py` | **WEST** | Create iris model: v1/v2/v3, aliases, tags, runs, experiment | `modules/models/seed.py` |
| 2 | `notebooks/02_replicate_secondary.py` | **EAST** | Baseline cross-workspace pull | `module.py` -> `replicate` |
| 3 | `notebooks/03_cdc.py` | **EAST** | Scheduled incremental sync, watermark-gated | `cdc.py` -> `run_cdc` |

> In production the seed is replaced by the normal ML training pipeline producing
> models in the primary; seeding is the POC's stand-in for "models already exist".

---

## 4. Baseline replicate (detail)

```mermaid
flowchart TD
    Start([Job starts in EAST<br/><b>notebooks/02_replicate_secondary.py</b><br/>notebooks/_bootstrap.py]) --> Ctx[Build RunContext: cfg, direction,<br/>AuditLog, dbutils<br/><b>common/config.py / common/audit.py</b>]
    Ctx --> Call[ModelsDRModule.replicate<br/><b>modules/models/module.py</b>]
    Call --> Run[run_replicate<br/><b>modules/models/replicate.py</b>]

    Run --> Creds[_remote_creds: WEST host+token<br/>from secret scope<br/><b>replicate.py + config/dr_config.yaml</b>]

    Creds --> ExpPhase{{EXPORT PHASE<br/>_ambient_identity = WEST<br/><b>replicate.py</b>}}
    ExpPhase --> Paths[Dynamic export path<br/>new_timestamp/rel_export_dir/dbfs_path<br/><b>common/storage.py</b>]
    Paths --> Resolve[resolve_models in scope<br/><b>modules/models/_selection.py</b>]
    Resolve --> AuditE[Insert EXPORT row<br/><b>common/audit.py</b>]
    AuditE --> Export[export_model -> mlflow-export-import<br/><b>common/engine.py</b>]
    Export --> Pull[(Download v1/v2/v3 from WEST S3)]
    Pull --> Land[(Write to EAST DBFS bucket<br/>/dbfs/dr/primary/exports/&lt;ts&gt;)]
    Land --> RecE[update_status EXPORT SUCCESS<br/><b>common/audit.py</b>]

    RecE --> ImpPhase{{IMPORT PHASE<br/>_ambient_identity = EAST<br/><b>replicate.py</b>}}
    ImpPhase --> AuditI[Insert IMPORT row<br/><b>common/audit.py</b>]
    AuditI --> Import[import_model import_source_tags=False<br/><b>common/engine.py</b>]
    Import --> Register[recreate runs/logged models,<br/>create_model_version x3, re-apply aliases<br/><b>mlflow-export-import via engine.py</b>]
    Register --> Verify{_verify_import<br/>east has 1,2,3?<br/><b>replicate.py</b>}

    Verify -- No --> Fail[update_status FAILED + raise<br/><b>common/audit.py</b>]
    Verify -- Yes --> Done[update_status SUCCESS source=3/target=3<br/><b>common/audit.py</b>]

    Done --> Grants[replicate_grants WEST->EAST non-fatal<br/><b>modules/models/grants.py</b><br/>workspace_client_from_creds in common/clients.py]
    Grants --> End([East = warm mirror])
    Fail --> End2([Audit FAILED -> alert])
```

---

## 5. CDC (incremental, scheduled)

```mermaid
flowchart TD
    Sched([Scheduled job in EAST<br/><b>notebooks/03_cdc.py</b>]) --> RunCdc[run_cdc<br/><b>modules/models/cdc.py</b>]
    RunCdc --> Become[_ambient_identity = WEST via _remote_creds<br/><b>replicate.py + dr_config.yaml</b>]
    Become --> Max[_max_version per model in WEST<br/>search_model_versions<br/><b>cdc.py + common/clients.py</b>]
    Max --> WM[watermark from audit table<br/>AuditLog.watermark<br/><b>common/audit.py</b>]
    WM --> Cmp{source max &gt; watermark?<br/><b>cdc.py</b>}

    Cmp -- No --> Skip[Skip - already mirrored]
    Cmp -- Yes --> Rep[run_replicate models_override,<br/>delete_model=True = exact mirror<br/><b>modules/models/replicate.py</b>]
    Rep --> Adv[Insert VERIFY row, advance watermark<br/><b>cdc.py + common/audit.py</b>]

    Skip --> EndC([Nothing to do])
    Adv --> EndC2([East updated - RPO = schedule interval])
```

**RPO** = the CDC schedule interval. At an actual regional disaster you do **not**
pull (the primary may be down); the secondary is already a warm mirror, so you
**promote** it (see failover).

---

## 6. Component → script map

| Component | File · function |
|---|---|
| Entry (baseline) | `notebooks/02_replicate_secondary.py` |
| Entry (CDC) | `notebooks/03_cdc.py` |
| Entry (seed) | `notebooks/01_seed_primary.py` |
| Path bootstrap | `notebooks/_bootstrap.py` |
| Orchestration / lifecycle | `modules/models/module.py` (`replicate`, `cdc`, `seed`, …) |
| Pull engine + identity switch | `modules/models/replicate.py` (`run_replicate`, `_ambient_identity`, `_remote_creds`, `_verify_import`) |
| Incremental gating | `modules/models/cdc.py` (`run_cdc`, `_max_version`) |
| Model scope resolution | `modules/models/_selection.py` (`resolve_models`) |
| Export/import adapter | `common/engine.py` (`export_model`, `import_model`) |
| Dynamic paths | `common/storage.py` (`new_timestamp`, `rel_export_dir`, `dbfs_path`) |
| Audit + watermark | `common/audit.py` (`AuditLog.insert/update_status/watermark`) |
| Clients / identities | `common/clients.py` (`make_mlflow_client`, `workspace_client_from_creds`) |
| Consumer grants | `modules/models/grants.py` (`replicate_grants`) |
| Config (secrets, scope, audit table) | `config/dr_config.yaml` |

---

## 7. What gets replicated

- Registered model (name, model-level tags, description)
- All versions and their artifacts (`model.pkl`, `MLmodel`, env files)
- MLflow 3.x logged models
- Per-version runs, params, metrics, and the parent experiment
- Aliases (e.g. `champion`, `challenger`)
- Consumer-facing UC grants on the catalog/schema (`replicate_grants`)

Provenance for source tags lives in the **audit table** rather than as
model-version tags (UC forbids `.` in tag keys, which the engine's source tags
use), so `import_source_tags=False` is set deliberately.

---

## 8. Engine vs. framework responsibilities

`mlflow-export-import` is a **stateless, additive, directory-based serializer**
for MLflow objects. This framework treats it as the *engine* (the export/import
primitive, called via `common/engine.py`) and wraps it with the DR semantics it
lacks. The tool is pinned as a dependency and called through an adapter rather
than forked: upstream owns object serialization, we own DR behavior.

### What the engine does

- Walks MLflow objects via the REST API (`MlflowClient`), writes JSON manifests +
  downloads artifacts to a directory, then re-creates them on the target.
- **Single-object** tools (`model/`, `model_version/`, `run/`, `experiment/`,
  `logged_model/`, `prompt/`, `evaluation_dataset/`) — used here.
- **Bulk** tools (`bulk/`: `export_models`/`import_models`/`export_all`) — often
  threaded; the bulk import path can swallow per-version errors.
- **Copy** tools (`copy/`: `copy_model_version`) — direct server-to-server, but
  `copy_model_version` does **not** work cross-metastore for UC (the limitation
  that requires export/import).
- Replicates: model + tags + description, versions + artifacts, MLflow 3.x logged
  models, runs/params/metrics/experiments, aliases, optional registered-model
  permissions, optional source tags.

### What the engine does NOT do (built in this framework)

| Concern | mlflow-export-import | This framework |
|---|---|---|
| Cross-region transport of the export dir | out of scope | export written directly into east bucket via identity switch |
| Cross-workspace auth | one tracking URI at a time | `_ambient_identity` swaps WEST/EAST mid-run |
| Incremental / CDC | no — additive every run | watermark-gated re-pull (`cdc.py`) |
| Idempotency / no duplicates | additive | `delete_model=True` -> exact mirror |
| Verification | bulk path swallows errors | `_verify_import` fails loudly |
| Scheduling & observability | none | audit table + scheduled job |
| Catalog/schema grants | model-level perms only | `replicate_grants` mirrors USE CATALOG/SCHEMA/EXECUTE |
| Failover / failback | none | direction resolver + `failover.py` |

---

## 9. Production hardening (no architecture change)

- Replace the secret-scope PAT with a **service principal OAuth (M2M)** token; rotate it.
- **Schedule** the CDC job (sets RPO) and alert on `FAILED` audit rows.
- Ensure the SPN has UC read in primary and write in secondary.
