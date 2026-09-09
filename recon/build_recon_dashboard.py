#!/usr/bin/env python3
"""Build + deploy the DR Reconciliation AI/BI (Lakeview) dashboard over dr_recon.control.

Rich, use-case-driven layout aligned with docs/dr_reconciliation/dr_reconciliation_dashboard.html:
identity + readiness/RPO KPIs, coverage scorecard, RPO trend, findings + silent gaps, the
incremental change feed (dr_recon_audit), failover/failback events (dr_recon_events), and a
per-object drill-down with filters.

  .venv/bin/python recon/build_recon_dashboard.py --deploy   # update the existing dashboard in place
"""
from __future__ import annotations
import argparse, json, uuid
from pathlib import Path

CAT, SCH = "dr_recon", "control"
FQ = f"{CAT}.{SCH}"
DASHBOARD_ID = "01f1abfaf70418bca73da438028036ba"      # existing published dashboard (keep URL)
WAREHOUSE = "cc56ad79aa69c967"
PROFILE = "dr2-east"

STATUS_COLORS = [
    {"value": "IN_SYNC", "color": "#00A972"}, {"value": "LAGGING", "color": "#FFAB00"},
    {"value": "DRIFTED", "color": "#E8590C"}, {"value": "MISSING", "color": "#FF3621"},
    {"value": "FAILED", "color": "#B91C1C"}, {"value": "UNSUPPORTED", "color": "#8B5CF6"},
]
CHANGE_COLORS = [
    {"value": "NEW", "color": "#00A972"}, {"value": "CHANGED", "color": "#FFAB00"},
    {"value": "REMOVED", "color": "#FF3621"},
]


def _id():
    return uuid.uuid4().hex[:8]


def ds(name, disp, sql):
    return {"name": name, "displayName": disp, "queryLines": [sql]}


def counter(dsn, field, title, disaggregated=True):
    return {"name": _id(), "queries": [{"name": "main_query", "query": {
        "datasetName": dsn, "fields": [{"name": field, "expression": f"`{field}`"}],
        "disaggregated": disaggregated}}],
        "spec": {"version": 2, "widgetType": "counter",
                 "encodings": {"value": {"fieldName": field, "displayName": title}},
                 "frame": {"showTitle": True, "title": title}}}


def stacked_bar(dsn, x, y, color, title, cmap):
    return {"name": _id(), "queries": [{"name": "main_query", "query": {
        "datasetName": dsn, "fields": [
            {"name": x, "expression": f"`{x}`"},
            {"name": f"sum_{y}", "expression": f"SUM(`{y}`)"},
            {"name": color, "expression": f"`{color}`"}], "disaggregated": False}}],
        "spec": {"version": 3, "widgetType": "bar",
                 "encodings": {"x": {"fieldName": x, "scale": {"type": "categorical"}, "displayName": x},
                               "y": {"fieldName": f"sum_{y}", "scale": {"type": "quantitative"}, "displayName": "objects"},
                               "color": {"fieldName": color, "scale": {"type": "categorical", "mappings": cmap}, "displayName": "status"}},
                 "frame": {"showTitle": True, "title": title}}}


def pie(dsn, angle, color, title, cmap):
    return {"name": _id(), "queries": [{"name": "main_query", "query": {
        "datasetName": dsn, "fields": [
            {"name": f"sum_{angle}", "expression": f"SUM(`{angle}`)"},
            {"name": color, "expression": f"`{color}`"}], "disaggregated": False}}],
        "spec": {"version": 3, "widgetType": "pie",
                 "encodings": {"angle": {"fieldName": f"sum_{angle}", "scale": {"type": "quantitative"}, "displayName": "objects"},
                               "color": {"fieldName": color, "scale": {"type": "categorical", "mappings": cmap}, "displayName": "status"}},
                 "frame": {"showTitle": True, "title": title}}}


def line(dsn, x, ys, title):
    fields = [{"name": x, "expression": f"`{x}`"}] + [{"name": y, "expression": f"AVG(`{y}`)"} for y in ys]
    enc = {"x": {"fieldName": x, "scale": {"type": "temporal"}, "displayName": "run time"},
           "y": {"fieldName": ys[0], "scale": {"type": "quantitative"}, "displayName": "seconds"}}
    if len(ys) > 1:
        enc["y2"] = {"fieldName": ys[1], "scale": {"type": "quantitative"}}
    return {"name": _id(), "queries": [{"name": "main_query", "query": {
        "datasetName": dsn, "fields": fields, "disaggregated": False}}],
        "spec": {"version": 3, "widgetType": "line", "encodings": enc,
                 "frame": {"showTitle": True, "title": title}}}


def table(dsn, cols, title):
    fields = [{"name": c["f"], "expression": f"`{c['f']}`"} for c in cols]
    enc = [{"fieldName": c["f"], "type": c.get("t", "string"), "displayAs": c.get("t", "string"),
            "title": c["title"], "order": i, "alignContent": "left"} for i, c in enumerate(cols)]
    return {"name": _id(), "queries": [{"name": "main_query", "query": {
        "datasetName": dsn, "fields": fields, "disaggregated": True}}],
        "spec": {"version": 1, "widgetType": "table", "encodings": {"columns": enc},
                 "frame": {"showTitle": True, "title": title}}}


def flt(dsn, field, title):
    qn = f"flt_{_id()}_{field}"
    return {"name": _id(), "queries": [{"name": qn, "query": {
        "datasetName": dsn, "fields": [
            {"name": field, "expression": f"`{field}`"},
            {"name": f"{field}_assoc", "expression": "COUNT_IF(`associative_filter_predicate_group`)"}],
        "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": "filter-multi-select",
                 "encodings": {"fields": [{"fieldName": field, "displayName": title, "queryName": qn}]},
                 "frame": {"showTitle": True, "title": title}}}


def md(text):
    return {"name": _id(), "spec": {"version": 2, "widgetType": "markdown",
            "encodings": {}, "frame": {"showTitle": False}}, "textbox_spec": text}


def W(widget, x, y, w, h):
    return {"widget": widget, "position": {"x": x, "y": y, "width": w, "height": h}}


def build():
    latest = f"(SELECT run_id FROM {FQ}.dr_recon_runs ORDER BY run_ts DESC LIMIT 1)"
    datasets = [
        ds("summary", "latest run", f"""SELECT readiness, mode, replication_state,
             rpo_lag_ms/1000.0 AS rpo_lag_s, rpo_target_ms/1000.0 AS rpo_target_s,
             ROUND(100.0*objects_ok/NULLIF(objects_in_scope,0),1) AS pct_in_sync,
             objects_in_scope, objects_ok, objects_attention, blocking_errors,
             effective_primary_region, failover_group
           FROM {FQ}.dr_recon_runs WHERE run_id = {latest}"""),
        ds("coverage", "coverage", f"SELECT object_type, status, cnt FROM {FQ}.dr_recon_coverage WHERE run_id = {latest}"),
        ds("status_dist", "status distribution", f"SELECT status, cnt FROM {FQ}.dr_recon_coverage WHERE run_id = {latest}"),
        ds("rpo", "rpo + activity trend", f"""SELECT run_ts, rpo_lag_ms/1000.0 AS rpo_lag_s,
             rpo_target_ms/1000.0 AS rpo_target_s, objects_attention FROM {FQ}.dr_recon_runs ORDER BY run_ts"""),
        ds("findings", "blocking findings", f"""SELECT object_type, fqn, error_class, severity, detail
           FROM {FQ}.dr_recon_findings WHERE run_id = {latest}"""),
        ds("audit", "incremental change feed", f"""SELECT event_time, change_type, object_type, fqn,
             prev_status, new_status FROM {FQ}.dr_recon_audit ORDER BY event_time DESC LIMIT 200"""),
        ds("events", "failover / failback", f"""SELECT event_time, event_type, direction, from_region,
             to_region, data_loss_window_ms/1000.0 AS data_loss_s, objects_reconciled, objects_total, outcome
           FROM {FQ}.dr_recon_events ORDER BY event_time DESC"""),
        ds("inventory", "per-object", f"""SELECT object_type, fqn, status, severity, detail, last_reconciled
           FROM {FQ}.dr_recon_inventory WHERE run_id = {latest}"""),
    ]

    L = []
    # --- header / identity ---
    L.append(W(md("# 🛡️ DR Reconciliation — Workspace Assets\n"
                  "Object-level failover readiness for **Databricks Managed DR** · failover group "
                  "**fg-dr-recon** · primary **us-east-1 (ankita-ps-dr-wp)** → secondary **us-west-2**. "
                  "State-aware: **BASELINE** while replicating, **ASSURANCE** (alerts) once ACTIVE. "
                  "Reads `dr_recon.control.*`; refreshed by the 15-min recon Workflow."), 0, 0, 6, 2))

    # --- KPI row ---
    L.append(W(counter("summary", "readiness", "Failover readiness"), 0, 2, 1, 3))
    L.append(W(counter("summary", "replication_state", "Replication state"), 1, 2, 1, 3))
    L.append(W(counter("summary", "rpo_lag_s", "RPO lag (s)"), 2, 2, 1, 3))
    L.append(W(counter("summary", "pct_in_sync", "% in sync"), 3, 2, 1, 3))
    L.append(W(counter("summary", "objects_attention", "Need attention"), 4, 2, 1, 3))
    L.append(W(counter("summary", "blocking_errors", "Blocking errors"), 5, 2, 1, 3))

    # --- coverage ---
    L.append(W(md("## Coverage — object-by-object parity\nPer workspace-asset type: in-sync vs "
                  "lagging/drifted vs missing/failed vs unsupported."), 0, 5, 6, 1))
    L.append(W(stacked_bar("coverage", "object_type", "cnt", "status", "Coverage by object type", STATUS_COLORS), 0, 6, 4, 6))
    L.append(W(pie("status_dist", "cnt", "status", "Objects by status", STATUS_COLORS), 4, 6, 2, 6))

    # --- RPO / activity trend ---
    L.append(W(md("## RPO & recon activity\nReplication lag vs target over time; objects needing "
                  "attention per run."), 0, 12, 6, 1))
    L.append(W(line("rpo", "run_ts", ["rpo_lag_s", "rpo_target_s"], "RPO lag vs target (s)"), 0, 13, 3, 5))
    L.append(W(line("rpo", "run_ts", ["objects_attention"], "Objects needing attention (per run)"), 3, 13, 3, 5))

    # --- findings + incremental change feed ---
    L.append(W(md("## Findings & incremental changes\nBlocking findings (raised only in ASSURANCE) and "
                  "the change feed: what was added/changed/removed across runs."), 0, 18, 6, 1))
    L.append(W(table("findings", [
        {"f": "object_type", "title": "Type"}, {"f": "fqn", "title": "Object"},
        {"f": "error_class", "title": "Class"}, {"f": "severity", "title": "Severity"},
        {"f": "detail", "title": "Detail"}], "Blocking findings (latest run)"), 0, 19, 3, 6))
    L.append(W(table("audit", [
        {"f": "event_time", "title": "When", "t": "datetime"}, {"f": "change_type", "title": "Change"},
        {"f": "object_type", "title": "Type"}, {"f": "fqn", "title": "Object"},
        {"f": "prev_status", "title": "From"}, {"f": "new_status", "title": "To"}],
        "Incremental change feed (dr_recon_audit)"), 3, 19, 3, 6))

    # --- DR events ---
    L.append(W(md("## Failover / failback history\nEvery promotion, its data-loss window, and the "
                  "post-event reconciliation outcome."), 0, 25, 6, 1))
    L.append(W(table("events", [
        {"f": "event_time", "title": "When", "t": "datetime"}, {"f": "event_type", "title": "Event"},
        {"f": "direction", "title": "Direction"}, {"f": "data_loss_s", "title": "Data-loss (s)", "t": "number"},
        {"f": "objects_reconciled", "title": "Reconciled", "t": "number"},
        {"f": "objects_total", "title": "Total", "t": "number"}, {"f": "outcome", "title": "Outcome"}],
        "Failover / failback events"), 0, 26, 6, 4))

    # --- per-object drill-down ---
    L.append(W(md("## Per-object drill-down\nFilter by type/status; every in-scope object with its "
                  "primary-vs-secondary verdict."), 0, 30, 6, 1))
    L.append(W(flt("inventory", "object_type", "Object type"), 0, 31, 2, 1))
    L.append(W(flt("inventory", "status", "Status"), 2, 31, 2, 1))
    L.append(W(table("inventory", [
        {"f": "object_type", "title": "Type"}, {"f": "fqn", "title": "Object"},
        {"f": "status", "title": "Status"}, {"f": "severity", "title": "Severity"},
        {"f": "detail", "title": "Detail"}, {"f": "last_reconciled", "title": "Reconciled", "t": "datetime"}],
        "Per-object reconciliation"), 0, 32, 6, 8))

    return json.dumps({"datasets": datasets,
                       "pages": [{"name": _id(), "displayName": "DR Reconciliation",
                                  "pageType": "PAGE_TYPE_CANVAS", "layout": L}],
                       "uiSettings": {"theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"}}}, indent=2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--deploy", action="store_true")
    args = ap.parse_args()
    serialized = build()
    out = Path(__file__).with_name("dr_reconciliation.lvdash.json")
    out.write_text(serialized)
    print("wrote", out)
    if args.deploy:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient(profile=PROFILE); host = w.config.host.rstrip("/")
        # update existing dashboard in place (keep the URL), then re-publish
        w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{DASHBOARD_ID}",
                        body={"serialized_dashboard": serialized, "display_name": "DR Reconciliation",
                              "warehouse_id": WAREHOUSE})
        w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{DASHBOARD_ID}/published",
                        body={"warehouse_id": WAREHOUSE, "embed_credentials": True})
        print("updated + published:", f"{host}/sql/dashboardsv3/{DASHBOARD_ID}/published")


if __name__ == "__main__":
    main()
