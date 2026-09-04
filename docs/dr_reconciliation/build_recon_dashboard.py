#!/usr/bin/env python3
"""Build (and optionally deploy) the DR Reconciliation AI/BI (Lakeview) dashboard.

The dashboard reads ONLY the dr_recon_* control tables, so it renders on real data once
those tables exist + are seeded — no Managed DR enrollment or AWS needed.

  # write the serialized dashboard JSON to the repo:
  python docs/dr_reconciliation/build_recon_dashboard.py

  # also create + seed the tables and create+publish the dashboard in the workspace:
  python docs/dr_reconciliation/build_recon_dashboard.py --deploy \
      --profile dr-east --warehouse 63af3d742ebd95ab --catalog dr_poc --schema dr_control

Self-contained: emits the Lakeview widget JSON directly (no external builder import).
"""
from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path

CATALOG = "dr_poc"
SCHEMA = "dr_control"

# ---- semantic colors (status → color), reused by charts -------------------
STATUS_COLORS = [
    {"value": "IN_SYNC", "color": "#00A972"},
    {"value": "LAGGING", "color": "#FFAB00"},
    {"value": "DRIFTED", "color": "#E8590C"},
    {"value": "MISSING", "color": "#FF3621"},
    {"value": "FAILED", "color": "#B91C1C"},
    {"value": "UNSUPPORTED", "color": "#8B5CF6"},
]


def _id() -> str:
    return uuid.uuid4().hex[:8]


# ---- widget factories ------------------------------------------------------
def counter(ds, field, title, agg="SUM"):
    expr = "COUNT(`*`)" if agg == "COUNT" else f"{agg}(`{field}`)"
    name = f"{agg.lower()}_{field}"
    return {"name": _id(), "queries": [{"name": "main_query", "query": {
        "datasetName": ds, "fields": [{"name": name, "expression": expr}], "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": "counter",
                 "encodings": {"value": {"fieldName": name, "displayName": title}},
                 "frame": {"showTitle": True, "title": title}}}


def counter_str(ds, field, title):
    """Counter that shows a string value (latest single-row dataset)."""
    return {"name": _id(), "queries": [{"name": "main_query", "query": {
        "datasetName": ds, "fields": [{"name": field, "expression": f"`{field}`"}], "disaggregated": True}}],
        "spec": {"version": 2, "widgetType": "counter",
                 "encodings": {"value": {"fieldName": field, "displayName": title}},
                 "frame": {"showTitle": True, "title": title}}}


def stacked_bar(ds, x, y, color, title, sort_y=False, color_map=None):
    xs = {"type": "categorical"}
    if sort_y:
        xs["sort"] = {"by": "y-reversed"}
    cscale = {"type": "categorical"}
    if color_map:
        cscale["mappings"] = color_map
    return {"name": _id(), "queries": [{"name": "main_query", "query": {
        "datasetName": ds, "fields": [
            {"name": x, "expression": f"`{x}`"},
            {"name": f"sum_{y}", "expression": f"SUM(`{y}`)"},
            {"name": color, "expression": f"`{color}`"}], "disaggregated": False}}],
        "spec": {"version": 3, "widgetType": "bar",
                 "encodings": {"x": {"fieldName": x, "scale": xs, "displayName": x},
                               "y": {"fieldName": f"sum_{y}", "scale": {"type": "quantitative"}, "displayName": "objects"},
                               "color": {"fieldName": color, "scale": cscale, "displayName": "status"}},
                 "frame": {"showTitle": True, "title": title}}}


def line(ds, x, y, y2, title):
    return {"name": _id(), "queries": [{"name": "main_query", "query": {
        "datasetName": ds, "fields": [
            {"name": x, "expression": f"`{x}`"},
            {"name": f"avg_{y}", "expression": f"AVG(`{y}`)"},
            {"name": f"avg_{y2}", "expression": f"AVG(`{y2}`)"}], "disaggregated": False}}],
        "spec": {"version": 3, "widgetType": "line",
                 "encodings": {"x": {"fieldName": x, "scale": {"type": "temporal"}, "displayName": "run time"},
                               "y": {"fieldName": f"avg_{y}", "scale": {"type": "quantitative"}, "displayName": "RPO lag (s)"},
                               "y2": {"fieldName": f"avg_{y2}", "scale": {"type": "quantitative"}}},
                 "frame": {"showTitle": True, "title": title}}}


def bar(ds, x, y, title, sort_y=True):
    xs = {"type": "categorical"}
    if sort_y:
        xs["sort"] = {"by": "y-reversed"}
    return {"name": _id(), "queries": [{"name": "main_query", "query": {
        "datasetName": ds, "fields": [
            {"name": x, "expression": f"`{x}`"},
            {"name": f"sum_{y}", "expression": f"SUM(`{y}`)"}], "disaggregated": False}}],
        "spec": {"version": 3, "widgetType": "bar",
                 "encodings": {"x": {"fieldName": x, "scale": xs, "displayName": x},
                               "y": {"fieldName": f"sum_{y}", "scale": {"type": "quantitative"}, "displayName": y},
                               "label": {"show": True}},
                 "frame": {"showTitle": True, "title": title}}}


def table(ds, cols, title):
    fields, enc = [], []
    for i, c in enumerate(cols):
        fields.append({"name": c["f"], "expression": f"`{c['f']}`"})
        enc.append({"fieldName": c["f"], "type": c.get("t", "string"),
                    "displayAs": "string", "title": c["title"], "order": i, "alignContent": "left"})
    return {"name": _id(), "queries": [{"name": "main_query", "query": {
        "datasetName": ds, "fields": fields, "disaggregated": True}}],
        "spec": {"version": 1, "widgetType": "table", "encodings": {"columns": enc},
                 "frame": {"showTitle": True, "title": title}}}


def flt(ds, field, title, multi=True):
    qn = f"flt_{_id()}_{field}"
    return {"name": _id(), "queries": [{"name": qn, "query": {
        "datasetName": ds, "fields": [
            {"name": field, "expression": f"`{field}`"},
            {"name": f"{field}_assoc", "expression": "COUNT_IF(`associative_filter_predicate_group`)"}],
        "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": "filter-multi-select" if multi else "filter-single-select",
                 "encodings": {"fields": [{"fieldName": field, "displayName": title, "queryName": qn}]},
                 "frame": {"showTitle": True, "title": title}}}


def text(md):
    return {"name": _id(), "spec": {"version": 2, "widgetType": "markdown",
            "encodings": {}, "frame": {"showTitle": False}}, "textbox_spec": md}


def lw(widget, x, y, w, h):
    return {"widget": widget, "position": {"x": x, "y": y, "width": w, "height": h}}


def build_serialized(cat, sch):
    fq = f"{cat}.{sch}"
    latest = f"(SELECT run_id FROM {fq}.dr_recon_runs ORDER BY run_ts DESC LIMIT 1)"
    datasets = [
        {"name": "summary", "displayName": "latest run summary", "queryLines": [
            f"SELECT rpo_lag_ms/1000.0 AS rpo_lag_s, rpo_target_ms/1000.0 AS rpo_target_s, readiness, "
            f"objects_in_scope, objects_ok, objects_attention, blocking_errors, "
            f"ROUND(100.0*objects_ok/objects_in_scope,1) AS pct_in_sync, effective_primary_region, failover_group "
            f"FROM {fq}.dr_recon_runs WHERE run_id = {latest}"]},
        {"name": "coverage", "displayName": "coverage", "queryLines": [
            f"SELECT object_type, status, cnt FROM {fq}.dr_recon_coverage WHERE run_id = {latest}"]},
        {"name": "rpo", "displayName": "rpo trend", "queryLines": [
            f"SELECT run_ts, rpo_lag_ms/1000.0 AS rpo_lag_s, rpo_target_ms/1000.0 AS rpo_target_s "
            f"FROM {fq}.dr_recon_runs ORDER BY run_ts"]},
        {"name": "errors", "displayName": "blocking errors", "queryLines": [
            f"SELECT error_class, COUNT(*) AS cnt FROM {fq}.dr_recon_findings WHERE run_id = {latest} "
            f"AND severity <> 'INFO' GROUP BY error_class"]},
        {"name": "inventory", "displayName": "per-object", "queryLines": [
            f"SELECT object_type, fqn, status, severity, detail FROM {fq}.dr_recon_inventory WHERE run_id = {latest}"]},
    ]
    layout = [
        lw(text("## DR Reconciliation — failover readiness\nObject-level reconciliation of the Managed DR secondary vs the primary. Reads `dr_recon_*` only."), 0, 0, 6, 1),
        # KPI row
        lw(counter_str("summary", "readiness", "Failover readiness"), 0, 1, 1, 2),
        lw(counter("summary", "rpo_lag_s", "RPO lag (s)", "SUM"), 1, 1, 1, 2),
        lw(counter("summary", "pct_in_sync", "% in sync", "SUM"), 2, 1, 1, 2),
        lw(counter("summary", "objects_attention", "Need attention", "SUM"), 3, 1, 1, 2),
        lw(counter("summary", "blocking_errors", "Blocking errors", "SUM"), 4, 1, 1, 2),
        lw(counter("summary", "objects_in_scope", "In scope", "SUM"), 5, 1, 1, 2),
        # scorecard + rpo
        lw(stacked_bar("coverage", "object_type", "cnt", "status", "Coverage by object type", color_map=STATUS_COLORS), 0, 3, 4, 6),
        lw(line("rpo", "run_ts", "rpo_lag_s", "rpo_target_s", "RPO trend (lag vs target)"), 4, 3, 2, 6),
        # status dist + errors
        lw(stacked_bar("coverage", "status", "cnt", "status", "Objects by status", sort_y=True, color_map=STATUS_COLORS), 0, 9, 2, 5),
        lw(bar("errors", "error_class", "cnt", "Blocking errors by class"), 2, 9, 4, 5),
        # filters + inventory
        lw(flt("inventory", "object_type", "Object type"), 0, 14, 2, 1),
        lw(flt("inventory", "status", "Status"), 2, 14, 2, 1),
        lw(table("inventory", [
            {"f": "object_type", "title": "Object type"}, {"f": "fqn", "title": "Object"},
            {"f": "status", "title": "Status"}, {"f": "severity", "title": "Severity"},
            {"f": "detail", "title": "Detail"}], "Per-object reconciliation"), 0, 15, 6, 8),
    ]
    return json.dumps({"datasets": datasets,
                       "pages": [{"name": _id(), "displayName": "DR Reconciliation",
                                  "pageType": "PAGE_TYPE_CANVAS", "layout": layout}],
                       "uiSettings": {"theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"}}}, indent=2)


# ---- seed data (representative; matches the sample HTML story) -------------
def seed_statements(cat, sch):
    fq = f"{cat}.{sch}"
    runs = []  # 14 historical points for the RPO trend
    lags = [55, 61, 48, 72, 66, 40, 52, 88, 44, 58, 51, 63, 47, 42]
    for i, lag in enumerate(lags):
        rid = f"run_{i:02d}"
        latest = i == len(lags) - 1
        ready = "AT_RISK" if latest else "GREEN"
        insc, ok, att, be = (1816, 1804, 12, 3) if latest else (1816, 1816, 0, 0)
        runs.append(
            f"('{rid}', current_timestamp() - INTERVAL {len(lags)-1-i} HOURS, 'fg-dr-prod', 'us-east-2', "
            f"{lag*1000}, 300000, '{ready}', {insc}, {ok}, {att}, {be})")
    runs_sql = (f"INSERT INTO {fq}.dr_recon_runs (run_id, run_ts, failover_group, effective_primary_region, "
                f"rpo_lag_ms, rpo_target_ms, readiness, objects_in_scope, objects_ok, objects_attention, blocking_errors) "
                f"VALUES {', '.join(runs)}")

    L = "run_13"  # latest
    cov = [
        ("notebooks", "IN_SYNC", 408), ("notebooks", "DRIFTED", 2), ("notebooks", "MISSING", 1),
        ("jobs", "IN_SYNC", 74), ("jobs", "DRIFTED", 2),
        ("warehouses", "IN_SYNC", 22), ("warehouses", "DRIFTED", 1),
        ("clusters", "IN_SYNC", 46), ("clusters", "DRIFTED", 1),
        ("dashboards", "IN_SYNC", 28), ("dashboards", "DRIFTED", 1), ("dashboards", "UNSUPPORTED", 7),
        ("files", "IN_SYNC", 405), ("files", "DRIFTED", 3),
        ("ws_acls", "IN_SYNC", 96), ("ws_acls", "DRIFTED", 4),
        ("uc_tables", "IN_SYNC", 214), ("uc_tables", "LAGGING", 1), ("uc_tables", "DRIFTED", 1),
        ("uc_tables", "MISSING", 1), ("uc_tables", "FAILED", 1),
        ("uc_grants", "IN_SYNC", 978), ("uc_grants", "DRIFTED", 2),
        ("uc_views_fns", "IN_SYNC", 130), ("uc_views_fns", "DRIFTED", 1), ("uc_views_fns", "FAILED", 1),
    ]
    cov_sql = (f"INSERT INTO {fq}.dr_recon_coverage (run_id, object_type, status, cnt) VALUES "
               + ", ".join(f"('{L}','{t}','{s}',{c})" for t, s, c in cov))

    inv = [
        ("notebooks", "/Repos/prod/ml/train", "DRIFTED", "WARNING", "content hash differs"),
        ("notebooks", "/Workspace/prod/adhoc/backfill", "MISSING", "CRITICAL", "absent in secondary"),
        ("jobs", "hourly_ingest", "DRIFTED", "WARNING", "schedule ABSENT in secondary"),
        ("jobs", "ml_retrain", "DRIFTED", "WARNING", "task added in primary; ACL diff"),
        ("warehouses", "adhoc-analytics", "DRIFTED", "WARNING", "size L->M; auto_stop 10->60"),
        ("clusters", "ml-gpu", "DRIFTED", "WARNING", "init script path differs"),
        ("dashboards", "Ops Health", "DRIFTED", "WARNING", "spec hash differs"),
        ("dashboards", "Board KPIs", "UNSUPPORTED", "INFO", "published dashboard — not replicated"),
        ("files", "/Workspace/prod/config/params.json", "DRIFTED", "WARNING", "content hash differs"),
        ("ws_acls", "dashboard:Exec Revenue", "DRIFTED", "WARNING", "- bi_team:CAN_VIEW in secondary"),
        ("uc_tables", "dr_poc.sales.dim_customer", "LAGGING", "WARNING", "lag 4m10s > target; +12904 rows"),
        ("uc_tables", "dr_poc.mart.rev_by_region", "DRIFTED", "WARNING", "schema +1 col"),
        ("uc_tables", "dr_poc.mart.orders_masked", "FAILED", "CRITICAL", "column mask — Managed DR cannot replicate"),
        ("uc_tables", "dr_poc.raw.ext_clickstream", "MISSING", "CRITICAL", "no storage mapping"),
        ("uc_grants", "SCHEMA dr_poc.sales", "DRIFTED", "WARNING", "+analysts:SELECT / +etl_sp:MODIFY"),
        ("uc_grants", "TABLE dr_poc.sales.fact_orders", "DRIFTED", "WARNING", "owner svc -> user@co"),
        ("uc_views_fns", "VIEW dr_poc.mart.v_orders_enriched", "FAILED", "CRITICAL", "cross-catalog owner perms missing"),
        ("uc_views_fns", "FUNCTION dr_poc.sales.fn_fx_rate", "DRIFTED", "WARNING", "DDL hash differs"),
    ]
    def esc(s): return s.replace("'", "''")
    inv_sql = (f"INSERT INTO {fq}.dr_recon_inventory (run_id, object_type, catalog, schema_name, fqn, in_scope, "
               f"status, severity, primary_sig, secondary_sig, detail, last_reconciled) VALUES "
               + ", ".join(f"('{L}','{t}','{CATALOG}','','{esc(f)}',true,'{s}','{sev}','','','{esc(d)}',current_timestamp())"
                           for t, f, s, sev, d in inv))

    fnd = [
        ("uc_tables", "dr_poc.raw.ext_clickstream", "MISSING", "DR_INVALID_CONFIGURATION.MISSING_LOCATION_MAPPING", "s3://.../raw/ has no storage mapping", "CRITICAL"),
        ("uc_tables", "dr_poc.mart.orders_masked", "FAILED", "DR_UNSUPPORTED_FEATURE.COLUMN_MASK", "column mask on ssn", "CRITICAL"),
        ("uc_views_fns", "dr_poc.mart.v_orders_enriched", "FAILED", "DR_INVALID_CONFIGURATION.CROSS_CATALOG_VIEW_PERMISSION", "owner lacks SELECT on referenced table", "CRITICAL"),
        ("dashboards", "Board KPIs", "UNSUPPORTED", "SILENT_GAP.PUBLISHED_DASHBOARD", "published dashboards are not replicated", "INFO"),
        ("uc_tables", "dr_poc.mart.daily_sales_mv", "UNSUPPORTED", "SILENT_GAP.MATERIALIZED_VIEW", "materialized views are not replicated", "INFO"),
    ]
    fnd_sql = (f"INSERT INTO {fq}.dr_recon_findings (run_id, object_type, fqn, drift_kind, error_class, detail, severity, first_seen) VALUES "
               + ", ".join(f"('{L}','{t}','{esc(f)}','{dk}','{ec}','{esc(d)}','{sev}',current_timestamp())"
                           for t, f, dk, ec, d, sev in fnd))
    return [runs_sql, cov_sql, inv_sql, fnd_sql]


def ddl_statements(cat, sch):
    p = Path(__file__).resolve().parents[2] / "sql" / "dr_recon_tables.sql"
    # Drop full-line comments first (they contain semicolons), THEN split on ';'.
    body = "\n".join(ln for ln in p.read_text().splitlines() if not ln.lstrip().startswith("--"))
    stmts = [s.strip() for s in body.split(";") if s.strip()]
    # ensure catalog/schema exist first
    return [f"CREATE CATALOG IF NOT EXISTS {cat}", f"CREATE SCHEMA IF NOT EXISTS {cat}.{sch}"] + stmts


def deploy(profile, warehouse, cat, sch, serialized):
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(profile=profile)

    def run_sql(stmt):
        r = w.statement_execution.execute_statement(warehouse_id=warehouse, statement=stmt, wait_timeout="50s")
        st = str(getattr(r.status, "state", ""))
        if st.endswith(("FAILED", "CANCELED", "CLOSED")):
            raise RuntimeError(f"SQL {st}: {getattr(getattr(r.status,'error',None),'message','')}\n{stmt[:200]}")
        return r

    print("== DDL ==")
    for s in ddl_statements(cat, sch):
        run_sql(s); print("  ok:", " ".join(s.split())[:70])
    print("== truncate + seed ==")
    for t in ("dr_recon_runs", "dr_recon_coverage", "dr_recon_inventory", "dr_recon_findings"):
        run_sql(f"TRUNCATE TABLE {cat}.{sch}.{t}")
    for s in seed_statements(cat, sch):
        run_sql(s); print("  seeded:", s.split("VALUES")[0].split("INTO")[1].strip()[:48])

    me = w.current_user.me().user_name
    print("== create dashboard ==")
    body = {"display_name": "DR Reconciliation", "warehouse_id": warehouse,
            "parent_path": f"/Users/{me}", "serialized_dashboard": serialized}
    resp = w.api_client.do("POST", "/api/2.0/lakeview/dashboards", body=body)
    did = resp["dashboard_id"]
    w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{did}/published",
                    body={"warehouse_id": warehouse, "embed_credentials": True})
    host = w.config.host.rstrip("/")
    print(f"\nDASHBOARD_ID={did}")
    print(f"DRAFT_URL={host}/sql/dashboardsv3/{did}")
    print(f"PUBLISHED_URL={host}/sql/dashboardsv3/{did}/published")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--deploy", action="store_true")
    ap.add_argument("--profile", default="dr-east")
    ap.add_argument("--warehouse", default="63af3d742ebd95ab")
    ap.add_argument("--catalog", default=CATALOG)
    ap.add_argument("--schema", default=SCHEMA)
    args = ap.parse_args()

    serialized = build_serialized(args.catalog, args.schema)
    out = Path(__file__).with_name("dr_reconciliation.lvdash.json")
    out.write_text(serialized)
    print("wrote", out)
    if args.deploy:
        deploy(args.profile, args.warehouse, args.catalog, args.schema, serialized)


if __name__ == "__main__":
    main()
