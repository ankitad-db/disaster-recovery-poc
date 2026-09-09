#!/usr/bin/env python3
"""Seed representative WORKSPACE ASSETS in the primary (dr2-east) for Managed DR recon testing.

Creates one of each Managed-DR-supported workspace-asset type so the failover group has real
content to replicate and the recon engine has something to reconcile:
  notebooks, files/folders, a job (paused schedule), a SQL warehouse, a cluster, a draft AI/BI
  dashboard, and ACLs on some of them.

Idempotent-ish: uses stable names/paths and overwrites where possible.
Run:  .venv/bin/python recon/seed_primary_assets.py --profile dr2-east
"""
from __future__ import annotations
import argparse, io, json, time

BASE = "/Workspace/Shared/dr_poc_seed"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="dr2-east")
    args = ap.parse_args()
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import workspace as wsvc, jobs as jsvc, sql as sqlsvc, compute as csvc, iam
    w = WorkspaceClient(profile=args.profile)
    me = w.current_user.me().user_name
    created = {}

    # ---- 1. folder + 2 notebooks + 1 file ----------------------------------
    w.workspace.mkdirs(BASE)
    nb1 = f"{BASE}/etl_bronze"
    nb2 = f"{BASE}/ml_train"
    for path, code in [(nb1, "# Databricks notebook source\nprint('bronze etl')\n"),
                       (nb2, "# Databricks notebook source\nprint('train model')\n")]:
        w.workspace.upload(path, io.BytesIO(code.encode()), format=wsvc.ImportFormat.SOURCE,
                           language=wsvc.Language.PYTHON, overwrite=True)
    # a workspace file
    try:
        w.workspace.upload(f"{BASE}/params.json", io.BytesIO(b'{"env":"prod","region":"us-east-1"}'),
                           format=wsvc.ImportFormat.AUTO, overwrite=True)
    except Exception as e:
        print("  file upload note:", str(e)[:80])
    created["notebooks"] = [nb1, nb2]
    print("  notebooks + folder + file: ok")

    # ---- 2. SQL warehouse (create then stop -> STOPPED asset) --------------
    try:
        whs = {x.name: x for x in w.warehouses.list()}
        if "dr-poc-wh" in whs:
            wh_id = whs["dr-poc-wh"].id
        else:
            wh = w.warehouses.create(name="dr-poc-wh", cluster_size="2X-Small",
                                     max_num_clusters=1, auto_stop_mins=10,
                                     enable_serverless_compute=True,
                                     warehouse_type=sqlsvc.CreateWarehouseRequestWarehouseType.PRO).result()
            wh_id = wh.id
        try: w.warehouses.stop(id=wh_id)
        except Exception: pass
        created["warehouse"] = wh_id
        print("  sql warehouse dr-poc-wh:", wh_id, "(stopped)")
    except Exception as e:
        print("  warehouse note:", str(e)[:120])

    # ---- 3. cluster (create then terminate -> TERMINATED asset) ------------
    try:
        existing = {c.cluster_name: c.cluster_id for c in w.clusters.list()}
        if "dr-poc-cluster" in existing:
            cid = existing["dr-poc-cluster"]
        else:
            sv = w.clusters.select_spark_version(long_term_support=True)
            nt = w.clusters.select_node_type(local_disk=True, min_memory_gb=8)
            c = w.clusters.create(cluster_name="dr-poc-cluster", spark_version=sv, node_type_id=nt,
                                  num_workers=0,
                                  spark_conf={"spark.databricks.cluster.profile": "singleNode",
                                              "spark.master": "local[*]"},
                                  custom_tags={"ResourceClass": "SingleNode"},
                                  autotermination_minutes=10).result()
            cid = c.cluster_id
        try: w.clusters.delete(cluster_id=cid)  # 'delete' = terminate (keeps config)
        except Exception: pass
        created["cluster"] = cid
        print("  cluster dr-poc-cluster:", cid, "(terminating)")
    except Exception as e:
        print("  cluster note:", str(e)[:120])

    # ---- 4. job with a PAUSED schedule -------------------------------------
    try:
        jname = "dr-poc-nightly-etl"
        existing = [j for j in w.jobs.list() if j.settings and j.settings.name == jname]
        if existing:
            job_id = existing[0].job_id
        else:
            job = w.jobs.create(
                name=jname,
                tasks=[jsvc.Task(task_key="bronze",
                                 notebook_task=jsvc.NotebookTask(notebook_path=nb1),
                                 description="seed etl task")],
                schedule=jsvc.CronSchedule(quartz_cron_expression="0 0 3 * * ?",
                                           timezone_id="UTC",
                                           pause_status=jsvc.PauseStatus.PAUSED))
            job_id = job.job_id
        created["job"] = job_id
        print("  job dr-poc-nightly-etl:", job_id, "(schedule paused)")
    except Exception as e:
        print("  job note:", str(e)[:160])
        job_id = None

    # ---- 5. draft AI/BI (Lakeview) dashboard -------------------------------
    try:
        from databricks.sdk.service import dashboards as dsvc
        ser = json.dumps({"datasets": [], "pages": [{"name": "p1", "displayName": "DR POC",
                          "layout": [], "pageType": "PAGE_TYPE_CANVAS"}]})
        dash = w.lakeview.create(dashboard=dsvc.Dashboard(
            display_name="dr-poc-draft-dashboard", serialized_dashboard=ser,
            warehouse_id=created.get("warehouse")))
        created["dashboard"] = dash.dashboard_id
        print("  draft dashboard dr-poc-draft-dashboard:", dash.dashboard_id)
    except Exception as e:
        print("  dashboard note:", str(e)[:160])

    # ---- 6. ACLs on the job + notebook -------------------------------------
    try:
        if job_id:
            w.jobs.set_permissions(job_id=str(job_id), access_control_list=[
                iam.JobAccessControlRequest(group_name="users",
                                            permission_level=iam.JobPermissionLevel.CAN_VIEW)])
        # notebook ACL via workspace object permissions
        obj = w.workspace.get_status(nb1)
        w.workspace.set_permissions(workspace_object_type="notebooks",
                                    workspace_object_id=str(obj.object_id),
                                    access_control_list=[
            iam.WorkspaceObjectAccessControlRequest(group_name="users",
                permission_level=iam.WorkspaceObjectPermissionLevel.CAN_READ)])
        print("  ACLs set on job + notebook (group 'users')")
    except Exception as e:
        print("  ACL note:", str(e)[:160])

    print("\nSEED SUMMARY:", json.dumps(created, indent=2, default=str))


if __name__ == "__main__":
    main()
