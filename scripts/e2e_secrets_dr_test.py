#!/usr/bin/env python3
"""End-to-end live test for DIRECT cross-workspace Secrets DR (primary=east, secondary=west).

No S3 / CRR / KMS. Exercises the full lifecycle against the two sandbox workspaces:

  1. control tables in both workspaces          (audit backend)
  2. seed sample secrets in the PRIMARY
  3. replicate primary -> secondary (direct)     + verify hashes match
  4. re-replicate (idempotency)                  -> expect in_sync / all skipped
  5. incremental: rotate one key + delete one    -> replicate
                                                    expect updated=1, deleted=1, skipped=rest
  6. failback: change in WEST -> replicate west -> east + verify east converges
  7. dump the audit tables (both workspaces)

SAFETY: scopes.include is pinned to the seed scopes only, so mirror-mode never touches
unrelated sandbox secrets.

Run:  dr-east/dr-west profiles authed.  .venv/bin/python scripts/e2e_secrets_dr_test.py
"""
from __future__ import annotations

import base64
import hashlib
import sys
import time

sys.path.insert(0, "src")

from databricks.sdk import WorkspaceClient  # noqa: E402

from databricks_dr.common.sql import SqlExecutor, rows  # noqa: E402
from databricks_dr.modules.secrets import changefeed, control, replicate as repl, seed as seedmod  # noqa: E402
from databricks_dr.modules.secrets.config import load_config  # noqa: E402

EAST_WH = "63af3d742ebd95ab"
WEST_WH = "4ca723533b34106a"
SEED_SCOPES = ["dr_app_prod", "dr_app_analytics"]


def hr(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}", flush=True)


def value_hashes(wc):
    out = {}
    for scope, info in changefeed.live_state(wc, SEED_SCOPES, []).items():
        for key in info["keys"]:
            try:
                v = wc.secrets.get_secret(scope=scope, key=key).value
                out[(scope, key)] = hashlib.sha256(base64.b64decode(v)).hexdigest()[:16]
            except Exception as e:  # noqa: BLE001
                out[(scope, key)] = f"ERR:{str(e)[:30]}"
    return out


def wait_warehouse(wc, wid, label):
    for _ in range(40):
        if str(wc.warehouses.get(id=wid).state).endswith("RUNNING"):
            print(f"  warehouse {label} RUNNING")
            return
        time.sleep(10)


def setup_control(ex, region_key, cfg, label):
    cat = cfg.control_catalog_for(region_key)
    sch = cfg.control["schema"]
    ddl = ([f"CREATE CATALOG IF NOT EXISTS {cat}"] if cat == cfg.control["catalog"] else []) + [
        f"CREATE SCHEMA IF NOT EXISTS {cat}.{sch} COMMENT 'DR control plane'",
        f"""CREATE TABLE IF NOT EXISTS {cat}.{sch}.dr_secrets_inventory (
            scope STRING NOT NULL, secret_key STRING NOT NULL, value_hash STRING,
            acl_signature STRING, source_last_updated TIMESTAMP, last_synced_at TIMESTAMP,
            bundle_id STRING, status STRING, updated_at TIMESTAMP) USING DELTA""",
        f"""CREATE TABLE IF NOT EXISTS {cat}.{sch}.dr_secrets_audit (
            audit_id STRING NOT NULL, event_time TIMESTAMP, operation STRING, direction STRING,
            scope STRING, item_count INT, status STRING, bundle_id STRING, duration_sec DOUBLE,
            detail STRING, actor STRING) USING DELTA""",
    ]
    for s in ddl:
        ex.execute(s)
    print(f"  [{label}] control tables ready in {cat}.{sch}")


def dump_audit(ex, table, label):
    cols = ["operation", "status", "direction", "item_count", "detail"]
    for r in rows(ex.execute(f"SELECT operation,status,direction,item_count,detail FROM {table} ORDER BY event_time"), cols):
        print(f"  [{label}] {r['operation']:<9} {r['status']:<8} {str(r['direction']):<20} n={r['item_count']}  {r['detail']}")


def main() -> int:
    cfg = load_config("config/secrets_dr_config.yaml")
    cfg.raw["scopes"] = {"include": SEED_SCOPES, "exclude": []}   # SAFETY: bound the test

    east = WorkspaceClient(profile="dr-east")
    west = WorkspaceClient(profile="dr-west")

    hr("0. WAREHOUSES (for control tables)")
    wait_warehouse(east, EAST_WH, "east")
    wait_warehouse(west, WEST_WH, "west")
    ex_east = SqlExecutor(workspace_client=east, warehouse_id=EAST_WH, wait_timeout="50s")
    ex_west = SqlExecutor(workspace_client=west, warehouse_id=WEST_WH, wait_timeout="50s")

    hr("1. CONTROL TABLES (both workspaces)")
    setup_control(ex_east, "primary", cfg, "east")
    setup_control(ex_west, "secondary", cfg, "west")

    hr("2. SEED sample secrets in PRIMARY (east)")
    print(" ", seedmod.run_seed(cfg, wc=east))
    print("  east:", {f"{s}/{k}": h for (s, k), h in sorted(value_hashes(east).items())})

    hr("3. REPLICATE primary -> secondary (direct)")
    r = repl.run_replicate(cfg, source_key="primary", dest_key="secondary",
                           src_wc=east, dst_wc=west, ex=ex_east)
    print("  replicate:", r)
    src, dst = value_hashes(east), value_hashes(west)
    print("  west:", {f"{s}/{k}": h for (s, k), h in sorted(dst.items())})
    print(f"  HASHES MATCH east==west: {src == dst}")

    hr("4. IDEMPOTENCY — re-replicate (expect in_sync / all skipped)")
    print("  replicate:", repl.run_replicate(cfg, src_wc=east, dst_wc=west, ex=ex_east))

    hr("5. INCREMENTAL — rotate db_password + delete service_url in EAST, then replicate")
    east.secrets.put_secret(scope="dr_app_prod", key="db_password", string_value="rot-" + str(int(time.time())))
    try:
        east.secrets.delete_secret(scope="dr_app_prod", key="service_url")
    except Exception as e:  # noqa: BLE001
        print("  (delete service_url:", str(e)[:60], ")")
    r = repl.run_replicate(cfg, src_wc=east, dst_wc=west, ex=ex_east)
    print("  replicate:", r, " <- expect updated=1, deleted=1, skipped=rest")
    src, dst = value_hashes(east), value_hashes(west)
    print(f"  HASHES MATCH after incremental: {src == dst}")

    hr("6. FAILBACK — change api_token in WEST, replicate WEST -> EAST")
    west.secrets.put_secret(scope="dr_app_prod", key="api_token", string_value="west-" + str(int(time.time())))
    r = repl.run_replicate(cfg, source_key="secondary", dest_key="primary",
                           src_wc=west, dst_wc=east, ex=ex_west)
    print("  replicate(failback):", r)
    src, dst = value_hashes(east), value_hashes(west)
    print(f"  east api_token == west api_token: "
          f"{src.get(('dr_app_prod','api_token')) == dst.get(('dr_app_prod','api_token'))}")
    print(f"  HASHES MATCH after failback: {src == dst}")

    hr("7. AUDIT TABLES")
    dump_audit(ex_east, cfg.audit_table_for("primary"), "east")
    dump_audit(ex_west, cfg.audit_table_for("secondary"), "west")

    hr("DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
