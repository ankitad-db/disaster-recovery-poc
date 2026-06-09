# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — UC control tables  (run once per metastore: WEST **and** EAST)
# MAGIC Applies the DDL in `sql/` against the **local** metastore so the DR namespace
# MAGIC + control plane exist before any replication:
# MAGIC `01_uc_objects.sql` (catalog/schemas) → `02_audit_table.sql` (audit + views)
# MAGIC → `03_state_table.sql` (active-role state, seeded to west).
# MAGIC
# MAGIC Idempotent (`CREATE … IF NOT EXISTS`), so it's safe to re-run. Orchestrated by
# MAGIC the `dr_models_bootstrap` job: `databricks bundle run dr_models_bootstrap -t east`
# MAGIC (and `-t west`). No `%pip` needed — this only runs SQL.

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
import os

SQL_FILES = ["01_uc_objects.sql", "02_audit_table.sql", "03_state_table.sql"]


def _statements(text: str):
    """Yield executable statements from a .sql file.

    Strips ``--`` line comments first (a comment may legitimately contain ``;``),
    then splits on the statement terminator. Our DDL has no ``--`` or ``;`` inside
    string literals, so this is safe.
    """
    no_comments = "\n".join(ln.split("--", 1)[0] for ln in text.splitlines())
    for chunk in no_comments.split(";"):
        stmt = chunk.strip()
        if stmt:
            preview = " ".join(stmt.split())[:80]
            yield stmt, preview


for fname in SQL_FILES:
    path = os.path.join(REPO_ROOT, "sql", fname)  # noqa: F821 (REPO_ROOT from _bootstrap)
    print(f"==> {fname}")
    with open(path) as f:
        text = f.read()
    for stmt, preview in _statements(text):
        spark.sql(stmt)  # noqa: F821 (Databricks-provided)
        print("   ok:", preview)

print("Setup complete for the local metastore.")

# COMMAND ----------
# MAGIC %md ## Verify
# COMMAND ----------
from databricks_dr.common.config import load_config

cfg = load_config(CONFIG_PATH)  # noqa: F821
print("audit rows:", spark.sql(f"SELECT count(*) FROM {cfg.audit_table}").collect()[0][0])  # noqa: F821
display(spark.sql(f"SELECT * FROM {cfg.state_table}"))  # noqa: F821 (expect one row, active_primary=west)
