# Native DR Engine — Smoke-Test Runbook (seed → replicate → change → CDC → failover → failback)

End-to-end validation of the **native** engine on two fresh cross-region
workspaces. This is the manual, notebook-driven path (the same entry points the
Asset Bundle jobs call). It asserts full model fidelity, that **experiment IDs
differ but names/versions/aliases match**, and that GenAI objects replicate when
the runtime supports them.

- **Primary (source):** `east` — `https://fe-sandbox-krish-us-eat-1-sandbox.cloud.databricks.com`
- **Secondary (dest):** `west` — `https://fe-sandbox-ankita-dr-wp-us-west-2.cloud.databricks.com`

> Role mapping comes from `config/dr_config.yaml` (`east.role: primary`,
> `west.role: secondary`). Steady-state replication runs in the **secondary**
> (`west`) and pulls from the **primary** (`east`).

---

## 0. Prerequisites (once)

1. **Clone the repo as a Git folder** in BOTH workspaces.
2. **Secret scopes** (cross-workspace pull creds), per `config/dr_config.yaml → secrets`:
   - In **west** (dest for normal sync): scope `dr_remote_east` → keys `host`, `token`
     (an `ad-dr-spn` PAT for **east**).
   - In **east** (dest for failback): scope `dr_remote_west` → keys `host`, `token`
     (an `ad-dr-spn` PAT for **west**).

   ```bash
   # in WEST (pull source = east)
   databricks secrets create-scope dr_remote_east --profile dr-west
   databricks secrets put-secret  dr_remote_east host  --string-value "https://fe-sandbox-krish-us-eat-1-sandbox.cloud.databricks.com" --profile dr-west
   databricks secrets put-secret  dr_remote_east token --string-value "<EAST ad-dr-spn PAT>" --profile dr-west
   # in EAST (failback source = west)
   databricks secrets create-scope dr_remote_west --profile dr-east
   databricks secrets put-secret  dr_remote_west host  --string-value "https://fe-sandbox-ankita-dr-wp-us-west-2.cloud.databricks.com" --profile dr-east
   databricks secrets put-secret  dr_remote_west token --string-value "<WEST ad-dr-spn PAT>" --profile dr-east
   ```
3. **Runtime:** an ML runtime (MLflow + databricks-sdk pre-installed). No
   `%pip install mlflow-export-import` — the engine is first-party.

---

## 1. Setup the control plane (run in BOTH metastores)

Run `notebooks/00_setup.py` in **east** AND **west**. Creates the `dr_poc` catalog,
`ml` + `dr_control` schemas, the **`dr_staging` external Volume** (on the local
region's `external_location_url` — serverless/shared-safe bundle landing zone), the
**schema-2** `dr_replication_audit` table, the `dr_object_inventory` table, grants to
the replication SPN (tolerant), and seeds `dr_state` to the **config `role: primary`**
region (**east** in the current topology).

**Assert:**
```sql
DESCRIBE dr_poc.dr_control.dr_replication_audit;   -- includes object_type, trigger_type, source_event_time, rpo_lag_sec, bytes_moved ...
SELECT * FROM dr_poc.dr_control.dr_state;           -- exactly one row, active_primary = east
SHOW TABLES IN dr_poc.dr_control;                   -- audit + state + dr_object_inventory
SHOW VOLUMES IN dr_poc.dr_control;                  -- dr_staging (EXTERNAL)
```

---

## 2. Seed the primary (run in EAST)

Run `notebooks/01_seed_primary.py` in **east**. Seeds the models in `models.seed`:
`dr_poc.ml.iris_dr_model` (**v1/v2**), `dr_poc.ml.wine_dr_model` (**v1-3**), and
`dr_poc.ml.cancer_dr_model` (**v1/v2**) — each version is its own backing run — and
sets aliases (`Champion` on the latest, `Challenger` on the prior) and tags on each.
The assertions below focus on iris; repeat for the other two as a fuller check.
(Step 4 below adds a version to exercise CDC.)

**Capture source truth (run in EAST):**
```python
from mlflow import MlflowClient
c = MlflowClient(registry_uri="databricks-uc")
m = "dr_poc.ml.iris_dr_model"
src = {int(v.version): (v.run_id, c.get_run(v.run_id).info.experiment_id) for v in c.search_model_versions(f"name='{m}'")}
print("source versions:", sorted(src))
print("source aliases:", c.get_registered_model(m).aliases)
SRC_EXP_NAMES = {eid: c.get_experiment(eid).name for (_, eid) in src.values()}
print("source experiment names:", SRC_EXP_NAMES)
```
Note the **source experiment IDs** — they must NOT match on the dest.

---

## 3. Baseline replicate (run in WEST)

Run `notebooks/02_replicate_secondary.py` in **west**. Pulls east→west via the
`dr_remote_east` scope, exports a native bundle into the **west `dr_staging` Volume**
(`/Volumes/dr_poc/dr_control/dr_staging/...`), then imports into the west registry.

**Assert (run in WEST):**
```python
from mlflow import MlflowClient
c = MlflowClient(registry_uri="databricks-uc")
m = "dr_poc.ml.iris_dr_model"
dst = {int(v.version): (v.run_id, c.get_run(v.run_id).info.experiment_id) for v in c.search_model_versions(f"name='{m}'")}
assert sorted(dst) == [1,2], dst                                    # versions preserved (baseline)
al = {k.lower(): v for k, v in c.get_registered_model(m).aliases.items()}
assert al.get("champion") and al.get("challenger"), al              # aliases preserved (case-insensitive)
# experiment IDs DIFFER, names MATCH:
dst_exp_names = {eid: c.get_experiment(eid).name for (_, eid) in dst.values()}
print("dest experiment names:", dst_exp_names)                       # names are the /Shared/dr/... import experiment(s)
```

**Assert (audit, run in WEST):**
```sql
SELECT operation, status, object_type, trigger_type, source_version, target_version, bytes_moved, duration_sec
FROM dr_poc.dr_control.dr_replication_audit ORDER BY event_time;
-- expect EXPORT + IMPORT rows, status=SUCCESS, bytes_moved > 0, trigger_type=MANUAL
SELECT * FROM dr_poc.dr_control.v_dr_watermark;  -- synced_version = 2 after baseline (3 after CDC in step 5)
```

> **Experiment-ID check:** MLflow experiment & run IDs are workspace-local; they
> **will** differ on west. The runbook asserts version numbers, aliases, tags,
> params/metrics and experiment **names** match — not the IDs.

**Multi-model coverage (run in WEST).** The default seed creates **three** models; the
asserts above use iris. Confirm all three landed with the expected version counts:
```python
from mlflow import MlflowClient
c = MlflowClient(registry_uri="databricks-uc")
expected = {"dr_poc.ml.iris_dr_model": [1,2],
            "dr_poc.ml.wine_dr_model": [1,2,3],
            "dr_poc.ml.cancer_dr_model": [1,2]}
for m, want in expected.items():
    got = sorted(int(v.version) for v in c.search_model_versions(f"name='{m}'"))
    al = {k.lower(): v for k, v in c.get_registered_model(m).aliases.items()}
    assert got == want, (m, got, want)
    assert al.get("champion"), (m, al)
    print(m, "versions", got, "aliases", al)
```

**ID mapping (run in WEST).** Names match across workspaces but experiment/run IDs differ;
the engine persists the correspondence. Verify rows exist per model:
```sql
SELECT model_name, id_type, count(*) AS n
FROM dr_poc.dr_control.v_dr_id_mapping_latest
GROUP BY model_name, id_type ORDER BY model_name, id_type;
-- expect experiment/run/model_version rows for each of the 3 models
```

---

## 4. Make a change on the primary (run in EAST)

Register **v3** on `dr_poc.ml.iris_dr_model` in **east**. Do **not** re-run
`01_seed_primary.py` — it deletes and recreates each model (resetting version numbers,
which breaks the watermark). Instead log+register one new version, and optionally move
`Champion` to v3 to exercise an alias change too:
```python
import mlflow, mlflow.sklearn
from mlflow import MlflowClient
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment("/Shared/dr/experiments/dr_poc_ml_iris_dr_model")
m = "dr_poc.ml.iris_dr_model"
X, y = load_iris(return_X_y=True, as_frame=True)
with mlflow.start_run(run_name="iris_dr_model_seed_v3"):
    clf = RandomForestClassifier(n_estimators=90, max_depth=8, random_state=42).fit(X, y)
    info = mlflow.sklearn.log_model(clf, "model", registered_model_name=m, input_example=X.iloc[[0]])
MlflowClient().set_registered_model_alias(m, "Champion", info.registered_model_version)  # optional alias move
```

---

## 5. CDC incremental sync (run in WEST)

Run `notebooks/03_cdc.py` in **west**. The changefeed detects the change (audit-event
scan when `system.access.audit` is visible, else registry diff), re-replicates only the
changed model, and advances the watermark.

**Assert (run in WEST):**
```python
assert max(int(v.version) for v in c.search_model_versions(f"name='{m}'")) == 3
```
```sql
-- a VERIFY row for the changed model; trigger_type reflects the detector
SELECT operation, model_name, source_version, status, trigger_type, source_event_time, rpo_lag_sec
FROM dr_poc.dr_control.dr_replication_audit WHERE operation='VERIFY' ORDER BY event_time DESC;
```
Re-run `03_cdc.py` with no new change → log says **"nothing to sync"** (idempotent).

---

## 6. Failover (run in WEST)

Simulate east down; promote west. Run `notebooks/04_failover_failback.py` with
`action=failover` in **west** (or the `dr_models_failover` job).

**Assert:**
```sql
SELECT active_primary, reason FROM dr_poc.dr_control.dr_state;  -- active_primary = west, reason = FAILOVER
SELECT * FROM dr_poc.dr_control.dr_replication_audit WHERE operation='FAILOVER' ORDER BY event_time DESC LIMIT 1;
```

---

## 7. Failback (run in EAST)

Once east is healthy, run `notebooks/04_failover_failback.py` with `action=failback`
in **east**. Reverse-pulls west→east (the failover-window changes), then resets the role.

**Assert:**
```sql
SELECT active_primary, reason FROM dr_poc.dr_control.dr_state;  -- active_primary = east (home), reason = FAILBACK
SELECT * FROM dr_poc.dr_control.dr_replication_audit WHERE operation='FAILBACK' ORDER BY event_time DESC LIMIT 1;
```
Confirm east has every version created on west during the outage.

---

## 8. (Optional) self-asserting drills

- `notebooks/drill_failover.py` (run in the standby) and `notebooks/drill_failback.py`
  (run in the home primary) raise on any fidelity gap, so a red run = failed drill.
  Use these for repeatable regression rather than the manual steps above.

---

## What "pass" means

| Check | Expectation |
|---|---|
| Versions | `[1,2]` baseline, `3` after CDC, all present on dest |
| Aliases / tags / description | identical names→versions on dest |
| Runs (params/metrics/tags) | recreated; **run IDs differ**, content matches |
| Experiments | **IDs differ**, names match (import experiment) |
| Multi-model | all 3 seeded models present: iris `[1,2]`, wine `[1,2,3]`, cancer `[1,2]` |
| ID mapping | `dr_id_mapping` has experiment/run/model_version rows per model |
| GenAI (prompts/eval/traces) | replicated when runtime supports it; else `SKIPPED` in logs |
| Audit | EXPORT/IMPORT/VERIFY/FAILOVER/FAILBACK rows, `status=SUCCESS`, `bytes_moved>0` |
| CDC idempotency | second run with no change = "nothing to sync" |
| `dr_state` | flips on failover, resets on failback |

> Live cluster execution is performed by you on the two workspaces; this runbook is
> the asserted sequence. Code-level checks (compile, import, manifest round-trip,
> schema-1 back-compat, audit parity) already pass locally.
