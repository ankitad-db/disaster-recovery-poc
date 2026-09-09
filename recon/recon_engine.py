#!/usr/bin/env python3
"""DR Reconciliation engine — workspace assets (Managed DR supported).

Reconciles the Managed-DR SECONDARY against the PRIMARY, object by object, for the 7
workspace-asset types, and writes results + an incremental audit trail to dr_recon.*.

Common skeleton + one reader/differ per asset (see docs/dr_reconciliation). Reads are
control-plane REST (no compute on the secondary). Direction is resolved from the
failover group's effective_primary_region.

  .venv/bin/python recon/recon_engine.py --primary dr2-east --secondary dr2-west \
      --warehouse cc56ad79aa69c967 --catalog dr_recon --schema control \
      --account 0d26daa6-5e44-4c97-a497-ef015f91254a --failover-group fg-dr-recon
"""
from __future__ import annotations
import argparse, hashlib, json, time, uuid
from datetime import datetime, timezone

SEED_PREFIX = "/Workspace/Shared/dr_poc_seed"     # scope: only reconcile our seeded assets
SCHEDULE_MUST_BE_PAUSED = True

STATUS_SEVERITY = {"IN_SYNC": "INFO", "LAGGING": "WARNING", "DRIFTED": "WARNING",
                   "MISSING": "CRITICAL", "FAILED": "CRITICAL", "UNSUPPORTED": "INFO"}


def sha(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:16]


def now():
    return datetime.now(timezone.utc)


# ---------- per-asset readers: return {asset_key: {"fqn":..., "sig":..., "meta":{}}} ----------
def read_notebooks(w):
    out = {}
    try:
        for o in w.workspace.list(SEED_PREFIX, recursive=True):
            if str(o.object_type) .endswith("NOTEBOOK"):
                try:
                    src = w.workspace.export(path=o.path, format="SOURCE")
                    content = src.content or ""
                except Exception:
                    content = ""
                key = str(o.object_id or o.path)
                out[key] = {"fqn": o.path, "sig": sha({"lang": str(o.language), "c": content}),
                            "meta": {"path": o.path, "language": str(o.language)}}
    except Exception as e:
        print("  ! notebooks read:", str(e)[:100])
    return out


def read_files(w):
    out = {}
    try:
        for o in w.workspace.list(SEED_PREFIX, recursive=True):
            if str(o.object_type).endswith("FILE"):
                try:
                    src = w.workspace.export(path=o.path, format="AUTO"); content = src.content or ""
                except Exception:
                    content = ""
                key = str(o.object_id or o.path)
                out[key] = {"fqn": o.path, "sig": sha(content), "meta": {"path": o.path}}
    except Exception as e:
        print("  ! files read:", str(e)[:100])
    return out


def _norm_job(s):
    d = s.as_dict() if hasattr(s, "as_dict") else {}
    tasks = [{k: t.get(k) for k in ("task_key", "notebook_task", "depends_on")} for t in d.get("tasks", [])]
    sched = d.get("schedule", {})
    return {"tasks": tasks, "schedule": {"cron": sched.get("quartz_cron_expression"),
            "pause": sched.get("pause_status")}, "run_as": d.get("run_as")}


def read_jobs(w):
    out = {}
    try:
        for j in w.jobs.list(expand_tasks=True):
            nm = j.settings.name if j.settings else None
            if not nm or not nm.startswith("dr-poc"):
                continue
            full = w.jobs.get(job_id=j.job_id)
            norm = _norm_job(full.settings)
            acls = _acls(w, "jobs", str(j.job_id))
            out[nm] = {"fqn": f"job:{nm}", "sig": sha({"def": norm, "acls": acls}),
                       "meta": {"schedule": norm["schedule"], "acls": acls}}
    except Exception as e:
        print("  ! jobs read:", str(e)[:100])
    return out


def read_warehouses(w):
    out = {}
    try:
        for wh in w.warehouses.list():
            if not (wh.name or "").startswith("dr-poc"):
                continue
            cfg = {"size": wh.cluster_size, "type": str(wh.warehouse_type),
                   "auto_stop": wh.auto_stop_mins, "max": wh.max_num_clusters,
                   "channel": str(getattr(wh, "channel", None))}
            out[wh.name] = {"fqn": f"warehouse:{wh.name}", "sig": sha(cfg),
                            "meta": {"state": str(wh.state), "cfg": cfg}}
    except Exception as e:
        print("  ! warehouses read:", str(e)[:100])
    return out


def read_clusters(w):
    out = {}
    try:
        for c in w.clusters.list():
            if not (c.cluster_name or "").startswith("dr-poc"):
                continue
            cfg = {"spark": c.spark_version, "node": c.node_type_id, "policy": c.policy_id,
                   "workers": c.num_workers, "autotermination": c.autotermination_minutes}
            out[c.cluster_name] = {"fqn": f"cluster:{c.cluster_name}", "sig": sha(cfg),
                                   "meta": {"state": str(c.state), "cfg": cfg}}
    except Exception as e:
        print("  ! clusters read:", str(e)[:100])
    return out


def read_dashboards(w):
    out = {}
    try:
        for d in w.lakeview.list():
            if not (d.display_name or "").startswith("dr-poc"):
                continue
            full = w.lakeview.get(dashboard_id=d.dashboard_id)
            out[d.display_name] = {"fqn": f"dashboard:{d.display_name}",
                                   "sig": sha(full.serialized_dashboard or ""),
                                   "meta": {"published": False}}
    except Exception as e:
        print("  ! dashboards read:", str(e)[:100])
    return out


def _acls(w, obj_type, obj_id):
    try:
        p = w.permissions.get(request_object_type=obj_type, request_object_id=obj_id)
        return sorted(f"{a.group_name or a.user_name or a.service_principal_name}:{al.permission_level}"
                      for a in (p.access_control_list or []) for al in (a.all_permissions or []))
    except Exception:
        return []


READERS = {"notebooks": read_notebooks, "files": read_files, "jobs": read_jobs,
           "warehouses": read_warehouses, "clusters": read_clusters, "dashboards": read_dashboards}


# ---------- differ / classifier (common) ----------
# Managed DR brings replicated compute up DORMANT in the secondary: SQL warehouses STOPPED,
# clusters TERMINATED, and job schedules PAUSED. A different secondary state = drift.
EXPECTED_SECONDARY_STATE = {"warehouses": "STOPPED", "clusters": "TERMINATED"}


def classify(p, s, obj_type, meta):
    # Silent gap: object is in scope but of a type/feature Managed DR does not replicate
    # (e.g. published dashboards, materialized views). Flag so it is never mistaken for replicated.
    if (p or {}).get("meta", {}).get("unsupported") or (s or {}).get("meta", {}).get("unsupported"):
        return "UNSUPPORTED"
    if p and not s:
        return "MISSING"
    if s and not p:
        return "DRIFTED"           # extra in secondary
    if p["sig"] != s["sig"]:
        return "DRIFTED"
    # secondary must be in the expected dormant state
    exp = EXPECTED_SECONDARY_STATE.get(obj_type)
    if exp:
        st = (s["meta"].get("state") or "").split(".")[-1].upper()
        if st and st != exp:
            return "DRIFTED"
    # jobs: secondary schedule must be PAUSED
    if obj_type == "jobs" and SCHEDULE_MUST_BE_PAUSED:
        sp = (s["meta"].get("schedule") or {}).get("pause")
        if sp and sp != "PAUSED":
            return "DRIFTED"
    return "IN_SYNC"


def reconcile(primary_w, secondary_w, object_types):
    rows = []
    for t in object_types:
        P = READERS[t](primary_w)
        S = READERS[t](secondary_w)
        for key in set(P) | set(S):
            p, s = P.get(key), S.get(key)
            st = classify(p, s, t, (p or s))
            fqn = (p or s)["fqn"]
            detail = ""
            exp = EXPECTED_SECONDARY_STATE.get(t)
            sec_state = (s or {}).get("meta", {}).get("state", "").split(".")[-1].upper() if s else ""
            if st == "UNSUPPORTED": detail = "in scope but Managed DR does not replicate this (silent gap)"
            elif st == "MISSING": detail = "present in primary, absent in secondary"
            elif st == "DRIFTED" and not p: detail = "extra in secondary (not in primary)"
            elif st == "DRIFTED" and exp and sec_state and sec_state != exp:
                detail = f"secondary state {sec_state}, expected {exp}"
            elif st == "DRIFTED" and t == "jobs" and (s or {}).get("meta", {}).get("schedule", {}).get("pause") not in (None, "PAUSED"):
                detail = "secondary job schedule not PAUSED"
            elif st == "DRIFTED": detail = "signature differs (content/config/ACL)"
            rows.append({"object_type": t, "asset_key": key, "fqn": fqn, "status": st,
                         "severity": STATUS_SEVERITY[st],
                         "primary_sig": (p or {}).get("sig", ""), "secondary_sig": (s or {}).get("sig", ""),
                         "detail": detail})
    return rows


# ---------- persistence + incremental audit ----------
def lit(v):
    if v is None: return "NULL"
    if isinstance(v, bool): return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)): return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--primary", default="dr2-east")
    ap.add_argument("--secondary", default="dr2-west")
    ap.add_argument("--warehouse", default="cc56ad79aa69c967")
    ap.add_argument("--catalog", default="dr_recon")
    ap.add_argument("--schema", default="control")
    ap.add_argument("--account", default="0d26daa6-5e44-4c97-a497-ef015f91254a")
    ap.add_argument("--failover-group", default="fg-dr-recon")
    ap.add_argument("--rpo-target-ms", type=int, default=900000)  # 15 min
    ap.add_argument("--job-mode", action="store_true",
                    help="run on-cluster: ambient primary, secondary from a secret scope, Spark SQL")
    ap.add_argument("--peer-scope", default="dr_recon", help="secret scope holding the peer host+token")
    args = ap.parse_args()
    from databricks.sdk import WorkspaceClient

    spark = None
    if args.job_mode:
        # On-cluster: primary = ambient identity; secondary = peer host+token from a secret scope;
        # SQL via Spark; DR state from system.replication.states (no account API / laptop profiles).
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        except Exception:
            spark = None
        pw = WorkspaceClient()

        import base64 as _b64
        def _sec(k):
            # dbutils returns plaintext on-cluster; the SDK get_secret returns base64.
            try:
                return pw.dbutils.secrets.get(args.peer_scope, k)
            except Exception:
                return _b64.b64decode(pw.secrets.get_secret(args.peer_scope, k).value).decode()

        sw = WorkspaceClient(host=_sec("peer_host"), token=_sec("peer_token"))
        # Fresh failover-group STATE comes from the DR REST API (account-scoped, no ~3h
        # system-table lag). If account OAuth (M2M) creds are in the secret scope, build an
        # AccountClient to call it; otherwise fall back to system.replication.states below.
        aw = None
        try:
            from databricks.sdk import AccountClient
            aw = AccountClient(host=_sec("acct_host"), account_id=_sec("account_id"),
                               client_id=_sec("acct_client_id"), client_secret=_sec("acct_client_secret"))
        except Exception as e:
            print("  ! account creds not in scope (state from system table):", str(e)[:80])
            aw = None
    else:
        pw = WorkspaceClient(profile=args.primary)
        sw = WorkspaceClient(profile=args.secondary)
        aw = WorkspaceClient(profile="dr2-acct")
    C = f"{args.catalog}.{args.schema}"

    def run(s):
        if spark is not None:
            return spark.sql(s)
        r = pw.statement_execution.execute_statement(warehouse_id=args.warehouse, statement=s, wait_timeout="50s")
        st = str(getattr(r.status, "state", ""))
        if st.endswith(("FAILED", "CANCELED", "CLOSED")):
            raise RuntimeError(f"SQL {st}: {getattr(getattr(r.status,'error',None),'message','')}\n{s[:160]}")
        return r

    def rows_of(r):
        if spark is not None:
            return [list(x) for x in r.collect()]
        return r.result.data_array or []

    # ---- Managed DR signals: DR REST API (if account auth) else system.replication.states ----
    fg_state, primary_region, rpo_lag = "UNKNOWN", "us-east-1", None
    if aw is not None:
        try:
            fg = aw.api_client.do("GET", f"/api/disaster-recovery/v1/accounts/{args.account}/failover-groups/{args.failover_group}")
            fg_state = fg.get("state", "UNKNOWN"); primary_region = fg.get("effective_primary_region", "us-east-1")
        except Exception as e:
            print("  ! DR API:", str(e)[:100])
    try:
        r = run(f"SELECT replication_state, effective_primary_region, replication_lag_ms "
                f"FROM system.replication.states WHERE failover_group_name LIKE '%/{args.failover_group}' "
                f"ORDER BY event_time DESC LIMIT 1")
        d = rows_of(r)
        if d:
            if fg_state == "UNKNOWN" and d[0][0]: fg_state = d[0][0]
            if d[0][1]: primary_region = d[0][1]
            if d[0][2] is not None: rpo_lag = int(d[0][2])
    except Exception as e:
        print("  ! replication.states:", str(e)[:100])

    # ---- surface Managed DR's OWN blocking errors (native signal we must not miss) ----
    # errors[] on the latest event lists what Managed DR itself cannot replicate (missing
    # dependency, unsupported feature, bad storage mapping, ...). These are real regardless
    # of our BASELINE/ASSURANCE mode, so we always fold them into findings.
    mdr_errors = []
    try:
        r = run(f"SELECT e.error.error_class AS ec, e.error.message AS msg "
                f"FROM (SELECT explode(errors) AS e FROM system.replication.states "
                f"WHERE failover_group_name LIKE '%/{args.failover_group}' "
                f"AND event_time = (SELECT max(event_time) FROM system.replication.states "
                f"WHERE failover_group_name LIKE '%/{args.failover_group}'))")
        for row in rows_of(r):
            mdr_errors.append({"error_class": row[0] or "DR_ERROR", "detail": (row[1] or "")[:400]})
    except Exception as e:
        print("  ! errors[] parse:", str(e)[:100])

    # ---- reconcile (direction follows effective primary) ----
    if primary_region.startswith("us-west"):
        pw, sw = sw, pw
    rows = reconcile(pw, sw, list(READERS))

    run_id = "run_" + now().strftime("%Y%m%dT%H%M%SZ")
    ok = sum(1 for r in rows if r["status"] == "IN_SYNC")
    att = len(rows) - ok
    findings = [r for r in rows if r["status"] in ("MISSING", "FAILED", "DRIFTED", "LAGGING")]
    blocking = sum(1 for r in rows if r["status"] in ("MISSING", "FAILED")) + len(mdr_errors)

    # State-aware: only treat drift as real once Managed DR is ACTIVE (steady state).
    # During INITIAL_REPLICATION the secondary is legitimately mid-copy -> BASELINE mode:
    # record inventory + audit, but suppress findings/alarms (readiness = BOOTSTRAP).
    mode = "ASSURANCE" if fg_state == "ACTIVE" else "BASELINE"
    if mode == "ASSURANCE":
        readiness = "CRITICAL" if blocking else ("AT_RISK" if att else "GREEN")
    else:
        readiness = "BOOTSTRAP"          # informational; not a failure signal
        findings = []                    # do not raise findings/alerts during bootstrap

    run(f"INSERT INTO {C}.dr_recon_runs (run_id, run_ts, failover_group, effective_primary_region, "
        f"rpo_lag_ms, rpo_target_ms, readiness, objects_in_scope, objects_ok, objects_attention, "
        f"blocking_errors, replication_state, mode) VALUES ({lit(run_id)}, current_timestamp(), "
        f"{lit(args.failover_group)}, {lit(primary_region)}, {lit(rpo_lag)}, {lit(args.rpo_target_ms)}, "
        f"{lit(readiness)}, {lit(len(rows))}, {lit(ok)}, {lit(att)}, {lit(blocking)}, "
        f"{lit(fg_state)}, {lit(mode)})")

    # coverage rollup
    cov = {}
    for r in rows: cov[(r["object_type"], r["status"])] = cov.get((r["object_type"], r["status"]), 0) + 1
    if cov:
        run(f"INSERT INTO {C}.dr_recon_coverage VALUES " +
            ", ".join(f"({lit(run_id)},{lit(t)},{lit(s)},{lit(c)})" for (t, s), c in cov.items()))

    # inventory
    if rows:
        run(f"INSERT INTO {C}.dr_recon_inventory VALUES " + ", ".join(
            f"({lit(run_id)},{lit(r['object_type'])},{lit(r['asset_key'])},{lit(r['fqn'])},TRUE,"
            f"{lit(r['status'])},{lit(r['severity'])},{lit(r['primary_sig'])},{lit(r['secondary_sig'])},"
            f"{lit(r['detail'])},current_timestamp())" for r in rows))

    # findings = our per-object findings (gated by mode) + Managed DR's own errors[] (always)
    finding_rows = [(r["object_type"], r["fqn"], r["status"], "RECON." + r["status"],
                     r["detail"], r["severity"]) for r in findings]
    finding_rows += [("managed_dr", "fg-dr-recon", "FAILED", e["error_class"], e["detail"], "CRITICAL")
                     for e in mdr_errors]
    if finding_rows:
        run(f"INSERT INTO {C}.dr_recon_findings VALUES " + ", ".join(
            f"({lit(run_id)},{lit(ot)},{lit(fqn)},{lit(dk)},{lit(ec)},{lit(dt)},{lit(sev)},current_timestamp())"
            for (ot, fqn, dk, ec, dt, sev) in finding_rows))

    # ---- incremental audit: diff this run's per-object status vs the previous run ----
    prev = {}
    try:
        r = run(f"WITH last AS (SELECT run_id FROM {C}.dr_recon_runs WHERE run_id <> {lit(run_id)} "
                f"ORDER BY run_ts DESC LIMIT 1) "
                f"SELECT fqn, status, primary_sig FROM {C}.dr_recon_inventory "
                f"WHERE run_id IN (SELECT run_id FROM last)")
        for row in rows_of(r): prev[row[0]] = (row[1], row[2])
    except Exception:
        pass
    audit = []
    cur = {r["fqn"]: (r["status"], r["primary_sig"]) for r in rows}
    for fqn, (stt, sig) in cur.items():
        if fqn not in prev:
            audit.append((fqn, "NEW", None, stt, None, sig))
        elif prev[fqn][0] != stt or prev[fqn][1] != sig:
            audit.append((fqn, "CHANGED", prev[fqn][0], stt, prev[fqn][1], sig))
    for fqn, (stt, sig) in prev.items():
        if fqn not in cur:
            audit.append((fqn, "REMOVED", stt, None, sig, None))
    if audit:
        run(f"INSERT INTO {C}.dr_recon_audit VALUES " + ", ".join(
            f"({lit(uuid.uuid4().hex)},current_timestamp(),{lit(run_id)},"
            f"{lit(fqn.split(':')[0] if ':' in fqn else 'notebooks')},{lit(fqn)},{lit(ct)},"
            f"{lit(ps)},{lit(ns)},{lit(psig)},{lit(nsig)},{lit(primary_region)},{lit('')})"
            for (fqn, ct, ps, ns, psig, nsig) in audit))

    print(json.dumps({"run_id": run_id, "fg_state": fg_state, "mode": mode,
                       "primary_region": primary_region, "rpo_lag_ms": rpo_lag,
                       "readiness": readiness, "objects": len(rows), "in_sync": ok,
                       "attention": att, "blocking": blocking, "mdr_errors": len(mdr_errors),
                       "findings": len(finding_rows), "audit_changes": len(audit)}, indent=2))
    for r in sorted(rows, key=lambda x: (x["object_type"], x["fqn"])):
        print(f"  {r['status']:<9} {r['object_type']:<11} {r['fqn']}  {r['detail']}")


if __name__ == "__main__":
    main()
