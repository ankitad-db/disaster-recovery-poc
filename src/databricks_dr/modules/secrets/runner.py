"""Thin entry points for the secrets DR flow.

Callable from notebooks (``from databricks_dr.modules.secrets import runner``),
from a Databricks job, or locally:

    python -m databricks_dr.modules.secrets.runner export
    python -m databricks_dr.modules.secrets.runner import --region secondary

Locally, auth comes from the workspace ``profile`` in secrets_dr_config.yaml (or
``DATABRICKS_HOST``/``DATABRICKS_TOKEN``); AWS creds come from the standard boto3
chain. On a cluster, the ambient identity + a Spark session are used.
"""

from __future__ import annotations

import argparse
from typing import Any, Dict, Optional

from ...common.logging import get_logger
from .config import load_config
from .export import run_export
from .import_ import run_import

_logger = get_logger(__name__)


def _spark_if_available():
    try:
        from pyspark.sql import SparkSession
        return SparkSession.getActiveSession()
    except Exception:  # noqa: BLE001
        return None


def export(config_path: Optional[str] = None, *, force_full: bool = False) -> Dict[str, Any]:
    cfg = load_config(config_path)
    return run_export(cfg, spark=_spark_if_available(), force_full=force_full)


def import_(config_path: Optional[str] = None, *, region_key: str = "secondary") -> Dict[str, Any]:
    cfg = load_config(config_path)
    return run_import(cfg, region_key=region_key, spark=_spark_if_available())


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Workspace secrets DR")
    sub = ap.add_subparsers(dest="cmd", required=True)
    pe = sub.add_parser("export", help="detect + export changed secrets (run in PRIMARY)")
    pe.add_argument("--config")
    pe.add_argument("--full", action="store_true", help="force a full export (ignore watermark)")
    pi = sub.add_parser("import", help="import latest bundle (run in PROMOTED workspace)")
    pi.add_argument("--config")
    pi.add_argument("--region", default="secondary", choices=["primary", "secondary"])
    args = ap.parse_args(argv)

    if args.cmd == "export":
        print(export(args.config, force_full=args.full))
    elif args.cmd == "import":
        print(import_(args.config, region_key=args.region))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
