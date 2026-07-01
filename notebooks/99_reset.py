# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Reset (start from scratch)
# MAGIC Wipes the DR POC material in the **local** workspace so you can re-run the whole
# MAGIC flow cleanly. **Identity-local** — run it once in EAST *and* once in WEST.
# MAGIC
# MAGIC What it clears (all scoped to the config's DR models / control plane only):
# MAGIC 1. the DR **registered models** (all versions + aliases)
# MAGIC 2. all **runs** in each DR experiment `/Shared/dr/experiments/<model>` (stops
# MAGIC    orphan-run buildup). The experiment shell is kept to avoid MLflow
# MAGIC    "deleted-state name" conflicts on the next seed/import.
# MAGIC 3. the **control tables** (`dr_replication_audit`, `dr_id_mapping`,
# MAGIC    `dr_object_inventory`, `dr_state`) — truncated
# MAGIC 4. the **staging volume** export bundles
# MAGIC
# MAGIC It does **not** drop the catalog/schema/volume or touch grants (those survive a
# MAGIC reset by design). After running this in both workspaces, restart the flow with
# MAGIC `00_setup` → `01_seed_primary` (EAST) → `02_replicate_secondary` (WEST).
# MAGIC
# MAGIC > Safety: this notebook only acts when the `confirm` widget is set to `yes`.

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
dbutils.widgets.dropdown("confirm", "no", ["no", "yes"], "Type yes to actually delete")  # noqa: F821

# COMMAND ----------
from databricks_dr.common.config import load_config
from databricks_dr.modules.models._selection import resolve_models

cfg = load_config(CONFIG_PATH)  # noqa: F821 (from _bootstrap)
CONFIRM = dbutils.widgets.get("confirm") == "yes"  # noqa: F821

catalog = cfg.uc["catalog"]
schema = cfg.uc["schema"]
control = cfg.uc["control_schema"]

# DR model names come from config (seed specs + explicit include names), unioned with
# any live models matching an include wildcard/all. Independent of current registry state.
model_names = set()
for s in (cfg.models.get("seed") or []):
    model_names.add(s.get("name") or f"{catalog}.{schema}.{s['model']}")
for m in cfg.models.get("include", []):
    if m and m != "all" and not m.endswith("*"):
        model_names.add(m)
try:
    model_names.update(resolve_models("databricks-uc", cfg.models.get("include", [])))
except Exception as e:  # noqa: BLE001 - registry may be empty already
    print("registry expand skipped:", e)
model_names = sorted(model_names)

control_tables = [
    cfg.audit_table,
    cfg.mapping_table,
    cfg.state_table,
    f"{catalog}.{control}.dr_object_inventory",
]
staging_path = f"/Volumes/{catalog}/{control}/dr_staging/{cfg.storage.get('base_path', 'dr')}"

print("Local workspace :", spark.conf.get("spark.databricks.workspaceUrl"))  # noqa: F821
print("DR models       :", model_names)
print("Control tables  :", control_tables)
print("Staging path    :", staging_path)
print("CONFIRM         :", CONFIRM)
if not CONFIRM:
    print("\nDRY RUN — set the `confirm` widget to `yes` and re-run to execute the reset.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1) Delete registered models + purge experiment runs

# COMMAND ----------
import mlflow
from mlflow import MlflowClient
from mlflow.entities import ViewType

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

for m in model_names:
    if not CONFIRM:
        print("[dry-run] would delete model:", m); continue
    try:
        client.delete_registered_model(m)
        print("deleted model:", m)
    except Exception as e:  # noqa: BLE001
        print("no model:", m, "|", e)

for m in model_names:
    exp_path = f"/Shared/dr/experiments/{m.replace('.', '_')}"
    exp = client.get_experiment_by_name(exp_path)
    if not exp:
        print("no experiment:", exp_path); continue
    if not CONFIRM:
        print("[dry-run] would purge runs in:", exp_path); continue
    n, token = 0, None
    while True:
        page = client.search_runs([exp.experiment_id], run_view_type=ViewType.ACTIVE_ONLY,
                                  max_results=1000, page_token=token)
        for r in page:
            client.delete_run(r.info.run_id); n += 1
        token = getattr(page, "token", None)
        if not token:
            break
    print(f"purged {n} run(s) from {exp_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2) Truncate control tables

# COMMAND ----------
for t in control_tables:
    if not CONFIRM:
        print("[dry-run] would truncate:", t); continue
    try:
        spark.sql(f"TRUNCATE TABLE {t}")  # noqa: F821
        print("truncated:", t)
    except Exception as e:  # noqa: BLE001 - table may not exist yet
        print("skip (missing?):", t, "|", e)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3) Clear the staging volume

# COMMAND ----------
if not CONFIRM:
    print("[dry-run] would remove:", staging_path)
else:
    try:
        dbutils.fs.rm(staging_path, recurse=True)  # noqa: F821
        print("cleared staging path:", staging_path)
    except Exception as e:  # noqa: BLE001 - path may not exist
        print("skip (missing?):", staging_path, "|", e)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Done
# MAGIC `dr_state` was truncated — re-run **`00_setup`** to recreate the views and re-seed
# MAGIC the active-primary row, then `01_seed_primary` (EAST) and `02_replicate_secondary`
# MAGIC (WEST). Run this reset in the **other** workspace too if you haven't yet.

# COMMAND ----------
print("Reset complete." if CONFIRM else "Dry run complete — nothing was deleted.")
