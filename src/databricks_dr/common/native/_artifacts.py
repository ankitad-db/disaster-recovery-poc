"""Artifact download/upload helpers for the native engine.

All artifact movement goes through the public ``mlflow.artifacts`` API and the
``MlflowClient``, which resolve credentials from the *ambient* Databricks identity
(``DATABRICKS_HOST``/``TOKEN``). That is exactly the identity the replicate module
swaps per phase, so downloads hit the source workspace and uploads hit the dest.
"""

from __future__ import annotations

import os
from typing import Optional

from ..logging import get_logger

_logger = get_logger(__name__)


def download_model_version(model: str, version: str, dst_dir: str) -> bool:
    """Download a UC model version's resolved artifacts into ``dst_dir``.

    Uses the ``models:/<name>/<version>`` URI so UC issues temporary artifact
    credentials for the *ambient* identity. Returns True if anything landed.
    """
    import mlflow

    os.makedirs(dst_dir, exist_ok=True)
    uri = f"models:/{model}/{version}"
    try:
        mlflow.artifacts.download_artifacts(artifact_uri=uri, dst_path=dst_dir)
        return _has_files(dst_dir)
    except Exception as e:  # noqa: BLE001 - fall back to the backing run below
        _logger.warning("download model artifacts %s failed: %s", uri, e)
        return False


def download_run_artifacts(client, run_id: str, dst_dir: str) -> bool:
    """Download a run's full artifact tree into ``dst_dir``. Returns True if any."""
    os.makedirs(dst_dir, exist_ok=True)
    try:
        client.download_artifacts(run_id, "", dst_dir)
    except Exception as e:  # noqa: BLE001
        _logger.warning("download run artifacts run=%s failed: %s", run_id, e)
        return False
    return _has_files(dst_dir)


def upload_run_artifacts(client, run_id: str, local_dir: str, artifact_path: Optional[str] = None) -> None:
    """Upload a local artifact tree under a (re-created) destination run."""
    if not _has_files(local_dir):
        return
    client.log_artifacts(run_id, local_dir, artifact_path=artifact_path)


def find_model_subdir(version_model_dir: str) -> Optional[str]:
    """Locate the directory holding an ``MLmodel`` file within a downloaded tree.

    ``models:/`` downloads may nest the model one level down depending on how it
    was logged; the importer needs the dir that actually contains ``MLmodel``.
    """
    for root, _dirs, files in os.walk(version_model_dir):
        if "MLmodel" in files:
            return root
    return version_model_dir if _has_files(version_model_dir) else None


def _has_files(path: str) -> bool:
    for _root, _dirs, files in os.walk(path):
        if files:
            return True
    return False


def dir_bytes(path: str) -> int:
    """Total bytes of files under ``path`` (0 if missing). For RPO/throughput audit."""
    total = 0
    for root, _dirs, files in os.walk(path):
        for fn in files:
            try:
                total += os.path.getsize(os.path.join(root, fn))
            except OSError:
                pass
    return total


def signature_present(model_dir: str) -> bool:
    """True if a downloaded model tree's MLmodel declares a signature."""
    root = find_model_subdir(model_dir)
    if not root:
        return False
    mlmodel = os.path.join(root, "MLmodel")
    if not os.path.isfile(mlmodel):
        return False
    try:
        with open(mlmodel, encoding="utf-8") as f:
            return "signature" in f.read()
    except OSError:
        return False
