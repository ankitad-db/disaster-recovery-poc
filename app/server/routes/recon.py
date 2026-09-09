"""Read-only reconciliation monitoring endpoints."""
from fastapi import APIRouter, HTTPException, Query

from .. import queries
from ..config import GENIE_SPACE_ID, RECON_JOB_ID, TOPOLOGY

router = APIRouter()


@router.get("/overview")
def overview():
    """Everything the operator console needs for the summary view, in one call."""
    try:
        run = queries.latest_run()
        if not run:
            return {"topology": TOPOLOGY, "run": None, "coverage": [], "findings": [],
                    "audit": [], "events": [], "history": [], "acks": []}
        run_id = run["run_id"]
        return {
            "topology": TOPOLOGY,
            "genie_configured": bool(GENIE_SPACE_ID),
            "recon_job_id": RECON_JOB_ID,
            "run": run,
            "coverage": queries.coverage(run_id),
            "findings": queries.findings(run_id),
            "audit": queries.audit_feed(120),
            "events": queries.events(50),
            "history": queries.run_history(40),
            "acks": queries.acks(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/inventory")
def inventory(
    object_type: str | None = Query(None),
    status: str | None = Query(None),
):
    """Per-object drill-down for the latest run, with optional filters."""
    try:
        run = queries.latest_run()
        if not run:
            return {"run_id": None, "items": []}
        items = queries.inventory(run["run_id"])
        if object_type and object_type != "ALL":
            items = [i for i in items if i["object_type"] == object_type]
        if status and status != "ALL":
            items = [i for i in items if i["status"] == status]
        return {"run_id": run["run_id"], "items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
