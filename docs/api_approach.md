# DR — Native API Approach (Technical Reference)

End-to-end, script-by-script reference for the model-DR engine. This is the
implementation companion to [architecture.md](architecture.md) (flows/diagrams) and
the [README](../README.md) (operator runbook). Everything here is **first-party code on
the public MLflow client + Databricks SDK** — there is no third-party replication tool.

- **Topology:** primary = **east** (`fe-sandbox-krish-us-eat-1-sandbox`, us-east-1),
  secondary = **west** (`fe-sandbox-ankita-dr-wp-us-west-2`, us-west-2). Direction is
  resolved at runtime from `dr_state`, never hardcoded.
- **Pattern:** cross-workspace **pull** — the DR job runs in the destination and reads
  the source registry via a secret scope. No laptop, no cross-region S3 bridge.

---

## 1. Big picture — how one model travels

```
SOURCE workspace (identity A)                 DEST workspace (identity B)
  registry: catalog.schema.model                 registry: catalog.schema.model
        │                                                 ▲
        │  export_model()  (read APIs)                    │  import_model() (write APIs)
        ▼                                                 │
  ┌───────────────┐   write bundle    ┌────────────────────────────┐
  │ native/export │ ───────────────►  │  dr_staging UC Volume (S3)  │ ──► native/import
  └───────────────┘                   │  /Volumes/.../<ts>/models/  │
                                       └────────────────────────────┘
        └── manifest.json + versions/ + runs/ + logged_models/ + genai ──┘
```

A single DR job process does **both** halves by switching its *ambient Databricks
identity* between phases (see `replicate.py`). The export phase becomes the remote
source (so UC artifact-credential vending resolves there); the import phase restores
the local identity and writes into the local registry. The bundle lands on the
destination's `dr_staging` Volume, so there is no bucket-to-bucket copy.

**Identity ↔ ID note.** Names are stable across workspaces (model name, experiment
name, version numbers, alias names). The numeric **`experiment_id` and `run_id` are
workspace-local and differ** after import — by design. The engine records the
source→target correspondence in `dr_id_mapping` (see §6).

---

## 2. Repository map

```
src/databricks_dr/
  cli.py                     # `python -m databricks_dr <module> <action>` entry point
  core/
    base.py                  # BaseDRModule ABC + RunContext (cfg, direction, audit, spark, dbutils)
    registry.py              # @register decorator + module lookup
  common/
    config.py                # Config + Direction resolver (active_primary_key, mapping_table, …)
    clients.py               # make_mlflow_client / local_or_profile_uri
    engine.py                # FACADE: export_model/import_model/... -> native engine
    audit.py                 # AuditLog + AuditRow; IdMappingLog + rows_from_import_result
    state.py                 # dr_state read/write (active-primary role)
    storage.py               # staging paths (UC Volume / DBFS), optional S3 bridge
    managed_dr.py            # Managed-DR alignment seam (no-op hooks)
    logging.py               # get_logger
    native/                  # ← the engine (pure MLflow client + SDK)
      manifest.py            #   bundle schema (dataclasses) + read/write JSON
      export.py              #   export_model(s)/export_model_version -> bundle
      import_.py             #   import_model(s)/import_model_version -> registry; ImportResult
      changefeed.py          #   CDC detection (system.access.audit scan + registry diff)
      _artifacts.py          #   download/upload artifacts, signature detect, byte sizing
      _notebooks.py          #   notebook-revision export/import (SOURCE/HTML/JUPYTER/DBC)
      _permissions.py        #   UC grants / WS registry ACL snapshot + apply
      _genai.py              #   MLflow 3 prompts / eval datasets / traces (version-gated)
      _scale.py              #   map_bounded (bounded threads) + paginated search_*
  modules/models/            # the models DR module (one file per concern)
    module.py                #   ModelsDRModule: wires phases to the lifecycle
    seed.py                  #   POC seeding (multi-model, multi-version)
    baseline.py              #   split export / import (run_export, run_import, run_baseline)
    replicate.py             #   cross-workspace pull (run_replicate) + ID-mapping persist
    cdc.py                   #   incremental sync (run_cdc) via changefeed
    failover.py              #   run_failover / run_failback (role flip + endpoint scale)
    grants.py                #   UC grants mirroring
    endpoints.py             #   serving-endpoint mirror / activate
    dependencies.py          #   per-model dependency validation
    health.py                #   drift/failure check (raises to fail a job)
    _selection.py            #   resolve_models(include) -> concrete names
config/dr_config.yaml        # regions, UC names, secrets, models.*, storage.staging_volume
notebooks/                   # thin Databricks wrappers (00..06 + drills)
resources/dr_models_jobs.yml # Asset Bundle job definitions
sql/                         # control-plane DDL (UC objects, audit, state)
```

---

## 3. The engine (`common/native/`) — API-level detail

### 3.1 `manifest.py` — the bundle contract

A native export writes a **self-describing bundle**; `manifest.json` is the index the
importer reads. Layout:

```
<bundle>/
  manifest.json                      # schema_version, engine, mlflow_version, exported_at, + records
  versions/<v>/  version.json + model/        # per-version resolved model artifacts
  runs/<run_id>/ run.json + artifacts/        # backing run payload (+ artifacts/notebooks/)
  logged_models/<id>/ logged_model.json + artifacts/   # MLflow 3 logged models
  prompts/ · evaluation_datasets/ · traces/   # GenAI (when in scope)
```

Dataclasses: `Manifest` (top index) → `RegisteredModelRec` (name, description, tags,
**aliases-by-version**, permissions), `VersionRec`, `RunRec` (params/metrics/tags +
`NotebookRec`s), `ExperimentRec`, `LoggedModelRec`, `PromptRec`,
`EvaluationDatasetRec`, `TraceRec`. `SCHEMA_VERSION = "2.0"`; `from_dict` tolerates 1.0
bundles. The tree is addressed only through relative paths in the manifest, so a bundle
can move between buckets/workspaces with no path rewriting.

### 3.2 `export.py` — serialize a model (SOURCE identity, read-only)

`export_model(model, output_dir, registry_uri, …)`:
1. `client.get_registered_model(model)` → `RegisteredModelRec` (incl. aliases, tags,
   description, timestamps; permissions via `_permissions.export_permissions`).
2. `_scale.search_all_model_versions(client, model)` → all versions (paginated).
3. Per version (`_export_version`, bounded by `max_workers`): `_artifacts.download_model_version`
   into `versions/<v>/model/`, detect signature, size bytes → `VersionRec`.
4. Dedup backing runs by `run_id`; per run (`_export_run`): `client.get_run`,
   `_artifacts.download_run_artifacts`, `_notebooks.export_run_notebooks`, capture the
   parent experiment via `client.get_experiment` → `RunRec` + `ExperimentRec`.
5. MLflow 3 logged models via `client.search_logged_models(filter_string="source_run_id='…'")`
   + `mlflow.artifacts.download_artifacts("models:/<id>")` (gated; skipped on 2.x).
6. GenAI (`_genai`): prompts, evaluation datasets, traces (each version-gated).
7. `manifest.write_manifest(output_dir, man)` and return the `Manifest`.

Variants: `export_models("all"|csv, …)` (bulk, one bundle per model under `models/<name>`)
and `export_model_version(model, version, …)` (single-version, parity with the low-level verb).

### 3.3 `import_.py` — rebuild a model (DEST identity, writes) → `ImportResult`

`import_model(model, input_dir, experiment_name, registry_uri, …)`:
1. `mlflow.set_registry_uri(registry_uri)`; read the manifest.
2. (optional) `_delete_registered_model` for a clean restore point; `_ensure_registered_model`
   (`create_registered_model` if absent).
3. `_ensure_experiment` — `get_experiment_by_name` or `create_experiment` (all backing
   runs land in this single per-model dest experiment).
4. **Recreate runs** (`_recreate_run`, bounded by `max_workers`): `client.create_run`,
   `client.log_batch` (params/metrics/tags, **batched ≤90**), `client.log_artifacts`,
   notebooks via `_notebooks.import_run_notebook`, `client.set_terminated`. Builds
   `run_id_map: {src_run → dst_run}`.
5. **Register versions in ascending source order** (`_register_version`): stage the
   bundled model files under the dest run (`client.log_artifacts(..., artifact_path)`)
   to form a `runs:/<dst_run>/…` URI, then **`mlflow.register_model(runs_uri, name)`**.
   Ascending order makes UC re-assign the **same version numbers**. Poll
   `get_model_version` until `READY` (`_wait_ready`), then `update_model_version`
   (description) + `set_model_version_tag` (UC forbids `.` in tag keys → skipped).
6. **Registered-model metadata** (`_apply_registered_model_metadata`):
   `update_registered_model`, `set_registered_model_tag`, and
   **`set_registered_model_alias`** (aliases reference the preserved version numbers).
7. Permissions (`_permissions.import_permissions`) + GenAI import (`_genai`).
8. Return an **`ImportResult`** (model, is_uc, `dest_experiment_id/name`,
   `source_experiments`, `run_id_map`, `run_experiment`, `version_map`, `run_version`) —
   the raw material for `dr_id_mapping`.

Variants: `import_models(input_dir, …)` → `List[ImportResult]` (bulk), and
`copy_model_version(...)` which uses `client.copy_model_version` for the **same-metastore**
UC→UC case (it does *not* work cross-metastore — the reason export/import exists).

### 3.4 `changefeed.py` — what changed (CDC)

`detect_changes(client, models, watermark_fn, spark, catalog, schema, since_iso)` returns
a `DetectResult(changed={model: source_max_version}, events, detector)`. Two combined
detectors:
- **Audit-event scan (trigger):** `system.access.audit` for UC model actions
  (`createModelVersion`, `finalizeModelVersion`, `setRegisteredModelAlias`,
  `deleteModelVersion`, …) newer than the scan watermark, scoped to the in-scope
  catalog/schema. Best-effort: any failure (missing schema, perms) silently falls back.
- **Registry diff (authoritative):** for each model, `max(search_model_versions)` vs the
  per-model audit watermark. Workspace/account-agnostic; this is the correctness guarantee.
  A metadata-only event (alias/tag with no new version) still triggers a metadata resync.

### 3.5 Support modules

- **`_artifacts.py`** — `download_model_version` (`models:/<model>/<v>` via
  `mlflow.artifacts.download_artifacts`), `download_run_artifacts`, `find_model_subdir`,
  `signature_present`, `dir_bytes`.
- **`_notebooks.py`** — export via `WorkspaceClient.workspace.export(path, format=…)`
  (SOURCE/HTML/JUPYTER/DBC, read from each run's `mlflow.source.name`/revision tags),
  import via `ws.workspace.import_` into an optional `notebook_dest_dir`.
- **`_permissions.py`** — UC: `ws.grants.get(securable_type=FUNCTION, full_name=model)` /
  `ws.grants.update(...)`. Workspace registry: `ws.model_registry.get_permissions` /
  set. Always best-effort (warn, never abort).
- **`_genai.py`** — feature-detects `mlflow.genai.register_prompt`/`load_prompt`,
  `create_dataset`/`get_dataset`, `search_traces`; degrades to `SKIPPED` when the runtime
  lacks the API, so the same code runs on 2.x and 3.x.
- **`_scale.py`** — `map_bounded(fn, items, max_workers, label)` (sequential when
  `max_workers=1`, else a bounded `ThreadPoolExecutor`) and paginated
  `search_all_model_versions` / `search_all_registered_models`.

---

## 4. The facade and module wiring

### 4.1 `common/engine.py`
The **only** module the DR modules call for transport. Thin pass-through to
`native/export` + `native/import_`; keeps a `backend=` kwarg purely for call-site
compatibility (all values resolve to the native engine). `import_model`/`import_models`
now return the `ImportResult`(s) so callers can persist ID mappings. `engine_version()`
returns `native-<mlflow_version>` for the audit table.

### 4.2 `modules/models/module.py` — `ModelsDRModule`
Maps the `BaseDRModule` lifecycle to the per-concern files:

| Lifecycle method | Delegates to | Notes |
|---|---|---|
| `seed()` | `seed.seed_models` | POC only; multi-model from `models.seed`. |
| `baseline()` | `baseline.run_baseline` (+ `grants`) | split export/import path. |
| `replicate()` | `replicate.run_replicate` + `_replicate_extras` | recommended pull path. |
| `cdc()` | `cdc.run_cdc` + `_replicate_extras` | incremental. |
| `export()`/`import_()`/`bridge()` | `baseline.run_export`/`run_import`, `storage.bridge_prefix` | split-workflow halves. |
| `validate()` | `dependencies.replicate_dependencies` | per-model deps. |
| `health()` | `health.run_health_check` | raises on problems. |
| `failover()`/`failback()` | `failover.run_failover`/`run_failback` | role flip. |

`_replicate_extras` runs the consumer-facing extras (grants, serving endpoints) after
models land — each non-fatal so one hiccup never fails an otherwise-good replication.

---

## 5. The models module — script-by-script

### `seed.py` — POC test material (PRIMARY only)
`seed_models(cfg)` reads `models.seed` (or falls back to `models.include`) and calls
`_seed_one_model` per entry: trains a small sklearn model on the chosen dataset
(`iris`/`wine`/`breast_cancer`), logs `n_versions` runs (one backing run per version),
sets `Champion`/`Challenger` aliases + tags, in a stable `/Shared/dr/experiments/<model>`
experiment. `seed_primary` is retained for back-compat. In production this is replaced by
your real training pipeline.

### `baseline.py` — one-time full history (split)
- `run_export(ctx)` — resolve in-scope models on the **source**, `storage.new_timestamp()`
  + `rel_export_dir`, `engine.export_models(...)` to the staging path, `write_latest_pointer`.
  Audited as `EXPORT`.
- `run_import(ctx, rel)` — read `_latest.txt`, `engine.import_models(...)` into the
  **dest**, then `_persist_id_mappings(...)` (writes `dr_id_mapping`). Audited as `IMPORT`.
- `run_baseline(ctx)` — export → (optional bridge) → import in one process (laptop/CLI).

### `replicate.py` — cross-workspace pull (recommended)
`run_replicate(ctx, full, delete_model, models_override)`:
- `_remote_creds(ctx)` reads host/token from the source's secret scope (`cfg.secrets[src]`).
- **EXPORT phase** under `_ambient_identity(host, token)`: for each model,
  `engine.export_model(...)` into `<staging>/<ts>/models/<model>`; record source versions
  + bytes; audit `EXPORT`.
- **IMPORT phase** under `_ambient_identity(None, None)` (local identity): per model,
  `engine.import_model(...)` → `ImportResult`, `_verify_import` (fails loudly if any
  expected version is missing — a partial import is never logged `SUCCESS`),
  `_persist_id_mapping(ctx, result, audit_id)`; audit `IMPORT`.
- `models_override` lets CDC replicate only changed models.

### `cdc.py` — incremental sync
`run_cdc(ctx)`: compute the last scan watermark; under the source identity,
`changefeed.detect_changes(...)`; if nothing changed → log and return; else
`replicate.run_replicate(full=False, delete_model=True, models_override=changed)` and
write a per-model `VERIFY` row (carrying `source_event_time` for RPO). Idempotent.
`cdc_use_system_tables` (default `false`) decides whether the audit-event scan runs.

### `failover.py` — role flip
- `run_failover(ctx)` — runs IN the secondary; **does not pull** (primary may be down —
  the secondary is already a warm mirror). Scales up endpoints, writes a `FAILOVER` audit
  marker, and `state.set_active_primary(dest)` so scheduled jobs honour the flip.
- `run_failback(ctx)` — after a reverse-direction CDC (driven with `failback=True`), scale
  up endpoints, write a `FAILBACK` marker, and reset `dr_state` to the **home** primary.

### `grants.py` / `endpoints.py` / `dependencies.py` / `health.py`
Consumer-facing extras + verification: UC grants mirroring (remote-read, local-apply),
serving-endpoint mirror (standby, scale-to-zero) and `activate_endpoints` on failover,
per-model dependency replication, and `run_health_check` (confirms each model is present
and at/above its watermark, scans recent `FAILED` rows, **raises** so a job task fails).

---

## 6. ID mapping — `dr_id_mapping`

**Why.** Experiment/run **names match** across workspaces but the **IDs are
workspace-local**. To stitch lineage (and answer "what is the west run for this east
run?") the engine persists the correspondence after every successful import.

**Where it comes from.** `import_model` returns an `ImportResult`; the modules call
`audit.rows_from_import_result(result, direction_label, source_workspace,
target_workspace, audit_id)` to build rows, then `IdMappingLog.insert_many(rows)`. The
write is **non-fatal** — the audit table remains the source of truth if it fails.

**Table** `dr_poc.dr_control.dr_id_mapping` (created in `00_setup.py` / `sql/02_audit_table.sql`):

| column | meaning |
|---|---|
| `mapping_id` | UUID for the row |
| `event_time` | when recorded (UTC) |
| `id_type` | `experiment` \| `run` \| `model_version` |
| `model_name` | registered model this mapping was produced for |
| `object_name` | stable name (e.g. experiment name; same on both sides) |
| `source_id` | ID in the **source** workspace |
| `target_id` | ID in the **destination** workspace (workspace-local) |
| `source_version` | model version the run/version backs (when applicable) |
| `source_workspace` / `target_workspace` | workspace identifiers |
| `direction` | e.g. `us-east-1->us-west-2` |
| `audit_id` | the `IMPORT` audit row that produced this mapping |

Captured per import: every source **experiment** → the single per-model dest experiment;
every source **run** → its recreated dest run (with the version it backs); every source
**version** → dest version.

**View** `v_dr_id_mapping_latest` returns the newest mapping per `(id_type, model_name, source_id)`
(`model_name` is in the key because `model_version` source_ids like `1`/`2`/`3` repeat across models).

```sql
-- east run_id -> west run_id for a model
SELECT source_id AS east_run, target_id AS west_run, source_version
FROM dr_poc.dr_control.v_dr_id_mapping_latest
WHERE id_type='run' AND model_name='dr_poc.ml.iris_dr_model';

-- east experiment_id -> west experiment_id (names are identical)
SELECT object_name AS experiment_name, source_id AS east_exp, target_id AS west_exp
FROM dr_poc.dr_control.v_dr_id_mapping_latest
WHERE id_type='experiment' AND model_name='dr_poc.ml.iris_dr_model';
```

---

## 7. Control plane (Delta tables in `dr_poc.dr_control`)

| Table / view | Written by | Purpose |
|---|---|---|
| `dr_replication_audit` | `AuditLog` (every phase) | source of truth: one row per EXPORT/IMPORT/VERIFY/GRANTS/ENDPOINT/FAILOVER/FAILBACK; backs the CDC watermark. |
| `dr_id_mapping` | `IdMappingLog` (after import) | source↔dest experiment/run/version IDs. |
| `dr_object_inventory` | reconciliation | desired-state snapshot per object (last source version, alias map). |
| `dr_state` | `state.py` (failover/failback) | single-row active-primary role; the direction source of truth. |
| `v_dr_watermark` | view | `MAX(source_version)` synced per model. |
| `v_dr_failures` | view | recent `FAILED` rows. |
| `v_dr_id_mapping_latest` | view | newest ID mapping per `(id_type, model_name, source_id)`. |

`system.access.audit` is the **only** system table read, and only as a CDC trigger signal
(see `changefeed.py`); the registry diff is the correctness guarantee.

---

## 8. Configuration knobs (`config/dr_config.yaml`)

- `regions.{east,west}` — `role`, `host`, `workspace`, `registry_uri`, `external_location_url`.
- `uc` — `catalog`, `schema`, `control_schema`, `audit_table`, `state_table`, `mapping_table`.
- `storage.staging_volume` — 3-level UC Volume for bundles (unset → DBFS root fallback).
- `secrets.{east,west}` — secret scope (in the *other* workspace) holding host + SPN token.
- `models` — `include` (replication scope), `seed` (POC seeding spec), `export_*`,
  `replicate_grants`, `replicate_serving_endpoints`, `max_workers`, `notebook_formats`,
  `prompts`/`evaluation_datasets`/`replicate_traces` (GenAI), `cdc_use_system_tables`.

---

## 9. Notebooks & CLI (entry points)

Notebooks (`notebooks/`, thin wrappers — each `%run ./_bootstrap` then call a module fn):
`00_setup` (control plane + staging volume + grants), `01_seed_primary`, `02_replicate_secondary`,
`02a_export_primary` / `02b_import_secondary` (split halves), `03_cdc`,
`04_failover_failback` (`action` widget), `05_health_check`, `06_test_endpoints`,
`drill_failover` / `drill_failback` (self-asserting rehearsal pair).

CLI: `python -m databricks_dr models <seed|baseline|replicate|cdc|failover|failback|health>`
(`cli.py` builds the `RunContext` and dispatches via the module registry).

---

## 10. Fidelity guarantees & known limits

**Preserved:** model + experiment **names**, **version numbers**, params/metrics/tags,
aliases, stages (WS registry), signatures, artifacts, notebook revisions, UC grants,
MLflow 3 logged models; GenAI prompts/eval-datasets/traces when the runtime supports them.

**Workspace-local (differ by design, mapped in `dr_id_mapping`):** `experiment_id`,
`run_id`.

**Limits:** `copy_model_version` is same-metastore only (hence export/import);
`system.access.audit` is per-account so the CDC scan is most useful source-side or in a
shared account (registry diff covers the rest); GenAI objects degrade to `SKIPPED` on
runtimes without the API.
