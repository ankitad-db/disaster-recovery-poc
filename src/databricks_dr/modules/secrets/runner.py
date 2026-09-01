"""Entry points for direct cross-workspace secret replication.

Callable from notebooks (``from databricks_dr.modules.secrets import runner``),
from a Databricks job, or locally:

    python -m databricks_dr.modules.secrets.runner replicate            # primary -> secondary
    python -m databricks_dr.modules.secrets.runner failback             # secondary -> primary

Auth for each workspace comes from its ``profile`` in secrets_dr_config.yaml (or the
ambient identity on a cluster). No object storage, no AWS: values move directly over
the Secrets API.
"""

from __future__ import annotations

import argparse
from typing import Any, Dict, Optional

from ...common.logging import get_logger
from .config import load_config
from .replicate import run_replicate

_logger = get_logger(__name__)


def _spark_if_available():
    try:
        from pyspark.sql import SparkSession
        return SparkSession.getActiveSession()
    except Exception:  # noqa: BLE001
        return None


def replicate(config_path: Optional[str] = None, *, source_key: str = "primary",
              dest_key: str = "secondary") -> Dict[str, Any]:
    """Reconcile ``dest_key`` to ``source_key`` (default primary -> secondary)."""
    cfg = load_config(config_path)
    return run_replicate(cfg, source_key=source_key, dest_key=dest_key, spark=_spark_if_available())


def failback(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Reverse direction: secondary -> primary."""
    return replicate(config_path, source_key="secondary", dest_key="primary")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Direct cross-workspace secrets DR")
    sub = ap.add_subparsers(dest="cmd", required=True)
    pr = sub.add_parser("replicate", help="replicate source -> dest (default primary -> secondary)")
    pr.add_argument("--config")
    pr.add_argument("--source", default="primary", choices=["primary", "secondary"])
    pr.add_argument("--dest", default="secondary", choices=["primary", "secondary"])
    pb = sub.add_parser("failback", help="reverse replicate secondary -> primary")
    pb.add_argument("--config")
    args = ap.parse_args(argv)

    if args.cmd == "replicate":
        print(replicate(args.config, source_key=args.source, dest_key=args.dest))
    elif args.cmd == "failback":
        print(failback(args.config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
