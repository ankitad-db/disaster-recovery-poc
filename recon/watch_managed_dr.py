#!/usr/bin/env python3
"""Watch the Managed DR failover group until it reaches ACTIVE (replication complete).

Production-style monitoring via the DR REST API + system.replication.states. Logs each poll
and exits 0 the moment the group is ACTIVE (Managed DR bootstrap complete), or 2 on timeout.

  .venv/bin/python recon/watch_managed_dr.py            # poll every 5 min, up to 6h
"""
from __future__ import annotations
import json, subprocess, sys, time
from databricks.sdk import WorkspaceClient

ACCT = "0d26daa6-5e44-4c97-a497-ef015f91254a"
FG = "fg-dr-recon"
WH = "cc56ad79aa69c967"
POLL_S = 300
MAX_MIN = 360


def fg_state():
    r = subprocess.run(["databricks", "api", "get",
        f"/api/disaster-recovery/v1/accounts/{ACCT}/failover-groups/{FG}", "--profile", "dr2-acct"],
        capture_output=True, text=True)
    try:
        d = json.loads(r.stdout)
        return d.get("state"), d.get("effective_primary_region")
    except Exception:
        return "UNKNOWN", None


def latest_lag(w):
    try:
        r = w.statement_execution.execute_statement(warehouse_id=WH, wait_timeout="50s",
            statement=f"SELECT replication_lag_ms, size(errors) FROM system.replication.states "
                      f"WHERE failover_group_name LIKE '%/{FG}' ORDER BY event_time DESC LIMIT 1")
        d = r.result.data_array
        if d: return d[0][0], d[0][1]
    except Exception:
        pass
    return None, None


def main():
    w = WorkspaceClient(profile="dr2-east")
    t0 = time.time()
    prev = None
    while (time.time() - t0) / 60 < MAX_MIN:
        state, primary = fg_state()
        lag, errs = latest_lag(w)
        line = f"[{time.strftime('%H:%M:%S')}] state={state} primary={primary} lag_ms={lag} errors={errs}"
        if line[11:] != (prev or "")[11:]:      # emit on change (ignore timestamp)
            print(line, flush=True)
            prev = line
        if state == "ACTIVE":
            print(f"ACTIVE — Managed DR replication complete (lag_ms={lag}). Ready for recon.", flush=True)
            return 0
        time.sleep(POLL_S)
    print(f"TIMEOUT after {MAX_MIN}m — still not ACTIVE; re-arm the watcher.", flush=True)
    return 2


if __name__ == "__main__":
    sys.exit(main())
