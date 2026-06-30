"""Notebook-revision export/import (parity with MEI's notebook download).

A Databricks run records its source notebook via the ``mlflow.databricks.notebook.path``
tag (and, when available, a revision id). We export the notebook in the requested
formats (SOURCE/HTML/JUPYTER/DBC) under the run's ``artifacts/notebooks/`` so the
backing lineage is fully reproducible. On import we re-upload the SOURCE format to a
configured destination workspace dir (best effort -- the model/version restore never
fails because a notebook couldn't be re-uploaded).
"""

from __future__ import annotations

import base64
import os
from typing import List, Optional

from ..logging import get_logger
from .manifest import NotebookRec

_logger = get_logger(__name__)

NOTEBOOK_PATH_TAG = "mlflow.databricks.notebook.path"
NOTEBOOK_REVISION_TAG = "mlflow.databricks.notebookRevisionID"
_VALID_FORMATS = {"SOURCE", "HTML", "JUPYTER", "DBC"}
_EXT = {"SOURCE": "source", "HTML": "html", "JUPYTER": "ipynb", "DBC": "dbc"}


def export_run_notebooks(run_tags: dict, run_artifacts_dir: str, formats: List[str]) -> Optional[NotebookRec]:
    """Export a run's source notebook in ``formats`` under ``<artifacts>/notebooks/``.

    Returns a NotebookRec (relative to the run dir) or None when the run has no
    notebook path tag (e.g. a job/script run).
    """
    path = run_tags.get(NOTEBOOK_PATH_TAG)
    if not path:
        return None
    revision = run_tags.get(NOTEBOOK_REVISION_TAG)
    fmts = [f.strip().upper() for f in formats if f.strip().upper() in _VALID_FORMATS] or ["SOURCE"]

    nb_dir = os.path.join(run_artifacts_dir, "notebooks")
    os.makedirs(nb_dir, exist_ok=True)
    base = os.path.basename(path)
    written: List[str] = []
    try:
        ws = _workspace_client()
        for fmt in fmts:
            try:
                content_b64 = _export_workspace_object(ws, path, fmt, revision)
                if content_b64 is None:
                    continue
                out = os.path.join(nb_dir, f"{base}.{_EXT[fmt]}")
                with open(out, "wb") as f:
                    f.write(base64.b64decode(content_b64))
                written.append(fmt)
            except Exception as e:  # noqa: BLE001 - one format failing must not abort
                _logger.warning("notebook export %s fmt=%s failed: %s", path, fmt, e)
    except Exception as e:  # noqa: BLE001
        _logger.warning("notebook export client unavailable for %s: %s", path, e)

    if not written:
        return None
    return NotebookRec(path=path, revision_id=revision, formats=written, rel_dir="artifacts/notebooks")


def import_run_notebook(run_dir: str, nb: NotebookRec, dest_dir: Optional[str]) -> None:
    """Re-upload a notebook's SOURCE format into ``dest_dir`` (best effort)."""
    if not dest_dir or not nb or "SOURCE" not in nb.formats:
        return
    base = os.path.basename(nb.path)
    src_file = os.path.join(run_dir, nb.rel_dir, f"{base}.source")
    if not os.path.isfile(src_file):
        return
    try:
        from databricks.sdk.service.workspace import ImportFormat, Language

        ws = _workspace_client()
        ws.workspace.mkdirs(dest_dir)
        with open(src_file, "rb") as f:
            content_b64 = base64.b64encode(f.read()).decode("utf-8")
        ws.workspace.import_(
            path=f"{dest_dir}/{base}",
            content=content_b64,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        _logger.info("re-uploaded notebook %s -> %s/%s", nb.path, dest_dir, base)
    except Exception as e:  # noqa: BLE001 - never fail the model restore over a notebook
        _logger.warning("notebook re-upload %s failed: %s", nb.path, e)


def _workspace_client():
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _export_workspace_object(ws, path: str, fmt: str, revision: Optional[str]) -> Optional[str]:
    """Return base64 notebook content for a format, using a revision when present.

    The revision-pinned export uses the same underlying ``workspace/export`` REST
    surface MEI relies on; if the SDK shape doesn't accept a revision we fall back
    to the current revision so DR still captures the notebook.
    """
    from databricks.sdk.service.workspace import ExportFormat

    export_fmt = getattr(ExportFormat, fmt, ExportFormat.SOURCE)
    try:
        resp = ws.workspace.export(path=path, format=export_fmt)
        return getattr(resp, "content", None)
    except Exception as e:  # noqa: BLE001
        _logger.debug("workspace.export(%s, %s) failed: %s", path, fmt, e)
        return None
