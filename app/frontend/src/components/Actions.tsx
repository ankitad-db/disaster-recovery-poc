import { useEffect, useRef, useState } from "react";
import { api } from "../api";

export function ActionsBar({ onTriggered }: { onTriggered: () => void }) {
  const [running, setRunning] = useState(false);
  const [runId, setRunId] = useState<number | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [toast, setToast] = useState<{ msg: string; sem: string } | null>(null);
  const poll = useRef<number | null>(null);

  useEffect(() => () => { if (poll.current) window.clearInterval(poll.current); }, []);

  async function runRecon() {
    setRunning(true); setStatus(null); setToast(null);
    try {
      const r = await api.runRecon();
      setRunId(r.run_id);
      setToast({ msg: `Reconciliation run #${r.run_id} triggered.`, sem: "s-info" });
      setStatus("PENDING");
      poll.current = window.setInterval(async () => {
        try {
          const s = await api.runStatus(r.run_id);
          const life = (s.life_cycle_state || "").replace(/RunLifeCycleState\./, "");
          const res = (s.result_state || "").replace(/RunResultState\./, "");
          setStatus(res ? `${life} · ${res}` : life);
          if (["TERMINATED", "SKIPPED", "INTERNAL_ERROR"].some(x => life.includes(x))) {
            if (poll.current) window.clearInterval(poll.current);
            setRunning(false);
            const ok = res.includes("SUCCESS");
            setToast({ msg: ok ? "Recon complete — refreshing." : `Recon finished: ${res}`, sem: ok ? "s-ok" : "s-warn" });
            if (ok) onTriggered();
          }
        } catch { /* keep polling */ }
      }, 5000);
    } catch (e) {
      setRunning(false);
      setToast({ msg: "Trigger failed: " + (e as Error).message, sem: "s-crit" });
    }
  }

  return (
    <section>
      <div className="sechead">
        <span className="eyebrow">Operator actions</span>
        <h2>Run reconciliation &amp; export sign-off</h2>
        <span className="note">this is why it is an app, not a dashboard</span>
      </div>
      <div className="card">
        <div className="btn-row">
          <button className="btn primary" onClick={runRecon} disabled={running}>
            {running ? "⏳ Running…" : "▶ Run recon now"}
          </button>
          <a className="btn" href="/api/export/signoff.md" target="_blank" rel="noreferrer">⇩ Export sign-off (Markdown)</a>
          <a className="btn" href="/api/export/signoff.json" target="_blank" rel="noreferrer">⇩ Export sign-off (JSON)</a>
          {runId && <span className="mono" style={{ fontSize: 12, color: "var(--muted)" }}>run #{runId}: {status}</span>}
        </div>
        {toast && <div className={`toast ${toast.sem}`}>{toast.msg}</div>}
        <div className="note-box">
          <b>Run recon now</b> triggers workflow job <span className="mono">805214227604364</span> and polls its run
          state. <b>Acknowledge</b> (on the Findings tab) writes to
          <span className="mono"> dr_recon.control.dr_recon_ack</span> so the finding is muted here.
          <b> Export sign-off</b> renders the current readiness, coverage, findings and events as a
          downloadable Markdown/JSON DR-test record. Failover / failback itself stays operator-run and out-of-band.
        </div>
      </div>
    </section>
  );
}
