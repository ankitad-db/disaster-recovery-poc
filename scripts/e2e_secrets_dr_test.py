#!/usr/bin/env python3
"""End-to-end live test for Workspace Secrets DR (primary=east, secondary=west).

Exercises the full lifecycle against the two sandbox workspaces + the real S3/KMS/CRR
assets, printing a labelled log that the test report is built from:

  1. control tables in both workspaces          (audit backend)
  2. seed sample secrets in the PRIMARY
  3. baseline export  east -> S3 -> CRR -> west  + verify bundle has no plaintext
  4. failover import  into west (destination-aware) + verify hashes match east
  5. re-import (idempotency)                     -> expect in_sync / all skipped
  6. incremental: rotate one key + delete one    -> export -> import
                                                     expect updated=1, deleted=1, skipped=rest
  7. failback: export west -> CRR -> east -> import into east (roles swapped)
  8. dump the audit tables

SAFETY: scopes.include is pinned to the seed scopes only, so mirror-mode import never
touches unrelated sandbox secrets.

Run:  AWS creds in env + dr-east/dr-west profiles authed.
      .venv/bin/python scripts/e2e_secrets_dr_test.py
"""
from __future__ import annotations

import base64
import copy
import hashlib
import sys
import time

sys.path.insert(0, "src")

from databricks.sdk import WorkspaceClient  # noqa: E402

from databricks_dr.common.sql import SqlExecutor, rows  # noqa: E402
from databricks_dr.modules.secrets import changefeed, control, export as exportmod  # noqa: E402
from databricks_dr.modules.secrets import import_ as importmod, seed as seedmod  # noqa: E402
from databricks_dr.modules.secrets.config import load_config  # noqa: E402

EAST_WH = "63af3d742ebd95ab"
WEST_WH = "4ca723533b34106a"
SEED_SCOPES = ["dr_app_prod", "dr_app_analytics"]


def hr(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}", flush=True)


def value_hashes(wc, include, exclude):
    """{(scope,key): sha256hex} for the live in-scope secrets in a workspace."""
    out = {}
    live = changefeed.live_state(wc, include, exclude)
    for scope, info in live.items():
        for key in info["keys"]:
            try:
                v = wc.secrets.get_secret(scope=scope, key=key).value
                out[(scope, key)] = hashlib.sha256(base64.b64decode(v)).hexdigest()[:16]
            except Exception as e:  # noqa: BLE001
                out[(scope, key)] = f"ERR:{str(e)[:40]}"
    return out


def wait_latest(s3, bucket, prefix, expected_bid, tries=48, delay=5):
    """Poll until the bucket's _latest.txt equals expected_bid (CRR pointer caught up)."""
    for _ in range(tries):
        try:
            cur = s3.get_object(Bucket=bucket, Key=f"{prefix}/_latest.txt")["Body"].read().decode().strip()
            if cur == expected_bid:
                return True
        except Exception:  # noqa: BLE001
            pass
        time.sleep(delay)
    return False


def wait_warehouse(wc, wid, label):
    for _ in range(30):
        st = wc.warehouses.get(id=wid).state
        if str(st).endswith("RUNNING"):
            print(f"  warehouse {label} RUNNING")
            return True
        print(f"  warehouse {label} {st}; waiting...")
        time.sleep(10)
    return False


def setup_control(ex, label):
    """Create dr_poc.dr_control control tables. Returns True if the backend works."""
    ddl = [
        "CREATE CATALOG IF NOT EXISTS dr_poc",
        "CREATE SCHEMA IF NOT EXISTS dr_poc.dr_control COMMENT 'DR control plane'",
        """CREATE TABLE IF NOT EXISTS dr_poc.dr_control.dr_secrets_inventory (
            scope STRING NOT NULL, secret_key STRING NOT NULL, value_hash STRING,
            acl_signature STRING, source_last_updated TIMESTAMP, last_synced_at TIMESTAMP,
            bundle_id STRING, status STRING, updated_at TIMESTAMP) USING DELTA""",
        """CREATE TABLE IF NOT EXISTS dr_poc.dr_control.dr_secrets_audit (
            audit_id STRING NOT NULL, event_time TIMESTAMP, operation STRING, direction STRING,
            scope STRING, item_count INT, status STRING, bundle_id STRING, duration_sec DOUBLE,
            detail STRING, actor STRING) USING DELTA""",
    ]
    try:
        for s in ddl:
            ex.execute(s)
        print(f"  [{label}] control tables ready")
        return True
    except Exception as e:  # noqa: BLE001
        print(f"  [{label}] control-table setup FAILED (auditing disabled): {str(e)[:160]}")
        return False


def dump_audit(ex, label):
    if not ex.available:
        print(f"  [{label}] (no audit backend)")
        return
    try:
        res = ex.execute(
            "SELECT operation, status, direction, item_count, bundle_id, detail "
            "FROM dr_poc.dr_control.dr_secrets_audit ORDER BY event_time"
        )
        cols = ["operation", "status", "direction", "item_count", "bundle_id", "detail"]
        for r in rows(res, cols):
            print(f"  [{label}] {r['operation']:<7} {r['status']:<8} {r['direction']:<20} "
                  f"n={r['item_count']} {r['detail']}")
    except Exception as e:  # noqa: BLE001
        print(f"  [{label}] audit dump error: {str(e)[:120]}")


def main() -> int:
    cfg = load_config("config/secrets_dr_config.yaml")
    # SAFETY: bound the test to the seed scopes only.
    cfg.raw["scopes"] = {"include": SEED_SCOPES, "exclude": []}

    east = WorkspaceClient(profile="dr-east")
    west = WorkspaceClient(profile="dr-west")

    hr("0. WAREHOUSES")
    wait_warehouse(east, EAST_WH, "east")
    wait_warehouse(west, WEST_WH, "west")
    ex_east = SqlExecutor(workspace_client=east, warehouse_id=EAST_WH, wait_timeout="50s")
    ex_west = SqlExecutor(workspace_client=west, warehouse_id=WEST_WH, wait_timeout="50s")

    hr("1. CONTROL TABLES (both workspaces)")
    audit_east = setup_control(ex_east, "east")
    audit_west = setup_control(ex_west, "west")
    if not audit_east:
        ex_east = SqlExecutor(workspace_client=east)  # no-op backend
    if not audit_west:
        ex_west = SqlExecutor(workspace_client=west)

    hr("2. SEED sample secrets in PRIMARY (east)")
    print(" ", seedmod.run_seed(cfg, wc=east))
    src = value_hashes(east, SEED_SCOPES, [])
    print("  east secrets:", {f"{s}/{k}": h for (s, k), h in sorted(src.items())})

    hr("3. BASELINE EXPORT (east -> S3 -> CRR -> west bucket)")
    r = exportmod.run_export(cfg, wc=east, ex=ex_east, force_full=True)
    print("  export:", r)
    import boto3
    from databricks_dr.modules.secrets import store
    s3e = boto3.client("s3", region_name="us-east-2")
    bkt_e = cfg.storage["primary_bucket"].replace("s3://", "")
    bid = s3e.get_object(Bucket=bkt_e, Key="secrets/_latest.txt")["Body"].read().decode().strip()
    body = s3e.get_object(Bucket=bkt_e, Key=f"secrets/{bid}/bundle.json")["Body"].read().decode()
    # Real plaintext-leak check: no live secret VALUE should appear in the bundle.
    plain_vals = []
    for (s, k) in src:
        try:
            plain_vals.append(base64.b64decode(east.secrets.get_secret(scope=s, key=k).value).decode())
        except Exception:  # noqa: BLE001
            pass
    has_plain = any(pv and pv in body for pv in plain_vals)
    snapshot_full = '"snapshot": "full"' in body
    print(f"  bundle_id={bid}  bytes={len(body)}  plaintext_leak={has_plain}  snapshot_full={snapshot_full}")

    hr("3b. WAIT FOR CRR replication to WEST bucket")
    s3w = boto3.client("s3", region_name="us-west-2")
    bkt_w = cfg.storage["secondary_bucket"].replace("s3://", "")
    replicated = False
    for _ in range(30):
        try:
            s3w.head_object(Bucket=bkt_w, Key=f"secrets/{bid}/bundle.json")
            replicated = True
            break
        except Exception:  # noqa: BLE001
            time.sleep(5)
    rs = s3e.head_object(Bucket=bkt_e, Key=f"secrets/{bid}/bundle.json").get("ReplicationStatus")
    ptr_ok = wait_latest(s3w, bkt_w, "secrets", bid)
    print(f"  replicated_to_west={replicated}  east_object_ReplicationStatus={rs}  west_latest_pointer_matches={ptr_ok}")

    hr("4. FAILOVER IMPORT into WEST (destination-aware)")
    r = importmod.run_import(cfg, region_key="secondary", wc=west, ex=ex_west)
    print("  import:", r)
    dst = value_hashes(west, SEED_SCOPES, [])
    print("  west secrets:", {f"{s}/{k}": h for (s, k), h in sorted(dst.items())})
    print(f"  HASHES MATCH east==west: {src == dst}")

    hr("5. IDEMPOTENCY — re-import (expect in_sync / all skipped)")
    r = importmod.run_import(cfg, region_key="secondary", wc=west, ex=ex_west)
    print("  re-import:", r)

    hr("6. INCREMENTAL — rotate db_password + delete service_url in EAST")
    east.secrets.put_secret(scope="dr_app_prod", key="db_password",
                            string_value="rotated-" + str(int(time.time())))
    try:
        east.secrets.delete_secret(scope="dr_app_prod", key="service_url")
    except Exception as e:  # noqa: BLE001
        print("  (delete service_url:", str(e)[:80], ")")
    src2 = value_hashes(east, SEED_SCOPES, [])
    print("  east now:", {f"{s}/{k}": h for (s, k), h in sorted(src2.items())})
    r = exportmod.run_export(cfg, wc=east, ex=ex_east, force_full=False)
    print("  export:", r)
    if r.get("bundle_id"):
        print("  west_latest_pointer_matches:", wait_latest(s3w, bkt_w, "secrets", r["bundle_id"]))
    r = importmod.run_import(cfg, region_key="secondary", wc=west, ex=ex_west)
    print("  import:", r, " <- expect updated=1, deleted=1, skipped=rest")
    dst2 = value_hashes(west, SEED_SCOPES, [])
    print("  west now:", {f"{s}/{k}": h for (s, k), h in sorted(dst2.items())})
    print(f"  HASHES MATCH after incremental: {src2 == dst2}")

    hr("7. FAILBACK — export WEST -> CRR -> EAST -> import into EAST (roles swapped)")
    # Make a change in WEST (simulating work while it was the active primary).
    west.secrets.put_secret(scope="dr_app_prod", key="api_token",
                            string_value="westside-" + str(int(time.time())))
    fb = copy.deepcopy(cfg.raw)
    fb["workspaces"]["primary"], fb["workspaces"]["secondary"] = (
        fb["workspaces"]["secondary"], fb["workspaces"]["primary"])
    fb["storage"]["primary_bucket"], fb["storage"]["secondary_bucket"] = (
        fb["storage"]["secondary_bucket"], fb["storage"]["primary_bucket"])
    from databricks_dr.modules.secrets.config import SecretsConfig
    cfg_fb = SecretsConfig(raw=fb, path=cfg.path).validate()
    r = exportmod.run_export(cfg_fb, wc=west, ex=ex_west, force_full=True)
    print("  export(west):", r)
    if r.get("bundle_id"):  # failback destination is east (cfg_fb secondary bucket == east bucket)
        print("  east_latest_pointer_matches:", wait_latest(s3e, bkt_e, "secrets", r["bundle_id"]))
    r = importmod.run_import(cfg_fb, region_key="secondary", wc=east, ex=ex_east)  # secondary==east here
    print("  import(east):", r)
    src3 = value_hashes(east, SEED_SCOPES, [])
    dst3 = value_hashes(west, SEED_SCOPES, [])
    print(f"  east api_token now == west api_token: "
          f"{src3.get(('dr_app_prod','api_token')) == dst3.get(('dr_app_prod','api_token'))}")

    hr("8. AUDIT TABLES")
    dump_audit(ex_east, "east")
    dump_audit(ex_west, "west")

    hr("DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
