"""Operator actions: trigger recon, acknowledge findings, export sign-off."""
import json
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from .. import queries
from ..config import RECON_JOB_ID, TOPOLOGY, get_workspace_client

router = APIRouter()


def _invoking_user(request: Request) -> str:
    for h in ("X-Forwarded-Email", "X-Forwarded-Preferred-Username", "X-Forwarded-User"):
        v = request.headers.get(h)
        if v:
            return v
    try:
        return get_workspace_client().current_user.me().user_name or "operator"
    except Exception:
        return "operator"


# ---- Run recon now --------------------------------------------------------

@router.post("/run-recon")
def run_recon():
    try:
        w = get_workspace_client()
        run = w.jobs.run_now(job_id=int(RECON_JOB_ID))
        return {"run_id": run.run_id, "job_id": RECON_JOB_ID,
                "message": "Reconciliation run triggered."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/run-recon/{run_id}")
def run_recon_status(run_id: int):
    try:
        w = get_workspace_client()
        r = w.jobs.get_run(run_id=run_id)
        state = r.state
        return {
            "run_id": run_id,
            "life_cycle_state": str(state.life_cycle_state) if state else None,
            "result_state": str(state.result_state) if state and state.result_state else None,
            "state_message": state.state_message if state else None,
            "run_page_url": r.run_page_url,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---- Acknowledge findings -------------------------------------------------

class AckBody(BaseModel):
    fqn: str
    note: str = ""


@router.post("/acknowledge")
def acknowledge(body: AckBody, request: Request):
    try:
        user = _invoking_user(request)
        queries.acknowledge(body.fqn, user, body.note)
        return {"ok": True, "fqn": body.fqn, "ack_by": user}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/unacknowledge")
def unacknowledge(body: AckBody):
    try:
        queries.unacknowledge(body.fqn)
        return {"ok": True, "fqn": body.fqn}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---- Export DR-test sign-off ----------------------------------------------

def _build_signoff() -> dict:
    run = queries.latest_run()
    if not run:
        raise RuntimeError("No reconciliation run available to export.")
    run_id = run["run_id"]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "failover_group": TOPOLOGY["failover_group"],
        "topology": TOPOLOGY,
        "run": run,
        "coverage": queries.coverage(run_id),
        "findings": queries.findings(run_id),
        "events": queries.events(50),
        "acknowledgements": queries.acks(),
    }


@router.get("/export/signoff.json")
def export_json():
    try:
        return _build_signoff()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/export/signoff.md", response_class=PlainTextResponse)
def export_md():
    try:
        d = _build_signoff()
        r = d["run"]
        lag_ms = r.get("rpo_lag_ms")
        tgt_ms = r.get("rpo_target_ms")
        lag = "null (never replicated)" if lag_ms in (None, "null") else f"{int(lag_ms)/1000:.0f}s"
        tgt = f"{int(tgt_ms)/1000/60:.0f} min" if tgt_ms else "—"
        lines = [
            "# DR Reconciliation — Failover Readiness Sign-off",
            "",
            f"- **Generated:** {d['generated_at']}",
            f"- **Failover group:** {d['failover_group']}",
            f"- **Run:** `{r['run_id']}` at {r['run_ts']}",
            f"- **Effective primary:** {r.get('effective_primary_region')}  "
            f"→ secondary {d['topology']['secondary']['region']}",
            f"- **Readiness:** **{r.get('readiness')}**",
            f"- **Replication state / mode:** {r.get('replication_state')} / {r.get('mode')}",
            f"- **RPO lag vs target:** {lag} vs {tgt}",
            f"- **Objects:** {r.get('objects_ok')}/{r.get('objects_in_scope')} in sync · "
            f"{r.get('objects_attention')} need attention · {r.get('blocking_errors')} blocking",
            "",
            "## Coverage by object type",
            "",
            "| Object type | Status | Count |",
            "|---|---|---|",
        ]
        for c in d["coverage"]:
            lines.append(f"| {c['object_type']} | {c['status']} | {c['cnt']} |")
        lines += ["", "## Findings", "", "| Severity | Object | FQN | Kind | Ack |",
                  "|---|---|---|---|---|"]
        for f in d["findings"]:
            ack = f.get("ack_by") or ""
            lines.append(f"| {f.get('severity')} | {f.get('object_type')} | "
                         f"{f.get('fqn')} | {f.get('drift_kind')} | {ack} |")
        lines += ["", "## Failover / failback events", ""]
        if d["events"]:
            lines += ["| When | Event | Direction | Outcome |", "|---|---|---|---|"]
            for e in d["events"]:
                lines.append(f"| {e.get('event_time')} | {e.get('event_type')} | "
                             f"{e.get('from_region')}→{e.get('to_region')} | {e.get('outcome')} |")
        else:
            lines.append("_No failover/failback events recorded (steady-state)._")
        lines += ["", "> Failover/failback is triggered out-of-band via the account-scoped "
                  "Managed DR API under change control; this report is evidence only."]
        return "\n".join(lines)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
