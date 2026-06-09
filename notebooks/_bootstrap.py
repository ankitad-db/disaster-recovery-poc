# Databricks notebook source
# MAGIC %md
# MAGIC # _bootstrap (helper, %run from other notebooks)
# MAGIC Makes notebooks self-locating inside a **Git folder** so nothing is hardcoded.
# MAGIC Derives the repo root from this notebook's own path, puts `src/` on `sys.path`,
# MAGIC and exposes `REPO_ROOT` + `CONFIG_PATH`. The replication **engine**
# MAGIC (`mlflow-export-import`) must be pip-installed by the *caller* before `%run`,
# MAGIC because `%pip` + `restartPython` has to run in the caller's own first cell.

# COMMAND ----------
import os
import sys

# Git folders are backed by WSFS, which forbids writing __pycache__ next to the
# source files. Disable bytecode writes so importing the package doesn't error.
sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"


def _repo_root() -> str:
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    nb_path = ctx.notebookPath().get()           # /Users/<me>/<repo>/notebooks/<name>
    # strip "/notebooks/<name>" -> repo root, then map to the /Workspace FUSE mount
    parts = nb_path.split("/")
    root = "/".join(parts[:-2])
    return "/Workspace" + root if not root.startswith("/Workspace") else root


REPO_ROOT = _repo_root()
CONFIG_PATH = f"{REPO_ROOT}/config/dr_config.yaml"
_src = f"{REPO_ROOT}/src"
if _src not in sys.path:
    sys.path.insert(0, _src)

print("REPO_ROOT  :", REPO_ROOT)
print("CONFIG_PATH:", CONFIG_PATH)
print("src on path:", _src)
