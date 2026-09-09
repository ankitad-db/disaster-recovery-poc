import { useState } from "react";
import { api, Audit, DrEvent, Finding } from "../api";
import { changeSem, fmtTs, NUM, severitySem, StatusTag } from "../lib";

// ---- blocking findings (with acknowledge) --------------------------------

export function Findings({ findings, onChange }:
  { findings: Finding[]; onChange: () => void }) {
  const [busy, setBusy] = useState<string | null>(null);
  const [showAcked, setShowAcked] = useState(false);

  const visible = showAcked ? findings : findings.filter(f => !f.ack_by);
  const ackedCount = findings.filter(f => f.ack_by).length;

  async function ack(fqn: string) {
    const note = window.prompt(`Acknowledge finding for:\n${fqn}\n\nOptional note:`, "") ?? null;
    if (note === null) return; // cancelled
    setBusy(fqn);
    try { await api.acknowledge(fqn, note); onChange(); }
    catch (e) { alert("Acknowledge failed: " + (e as Error).message); }
    finally { setBusy(null); }
  }
  async function unack(fqn: string) {
    setBusy(fqn);
    try { await api.unacknowledge(fqn); onChange(); }
    catch (e) { alert("Un-acknowledge failed: " + (e as Error).message); }
    finally { setBusy(null); }
  }

  return (
    <section>
      <div className="sechead">
        <span className="eyebrow">Native signal</span>
        <h2>Blocking findings</h2>
        <span className="note">objects holding failover readiness · acknowledged findings are muted</span>
        {ackedCount > 0 && (
          <button className="linkbtn" style={{ marginLeft: "auto" }}
            onClick={() => setShowAcked(s => !s)}>
            {showAcked ? "hide" : "show"} {ackedCount} acknowledged
          </button>
        )}
      </div>
      <div className="tablewrap">
        <table>
          <thead><tr>
            <th>Severity</th><th>Object</th><th>FQN</th><th>Kind</th>
            <th>Error class</th><th>Detail</th><th>Ack</th><th></th>
          </tr></thead>
          <tbody>
            {visible.length === 0 && (
              <tr><td colSpan={8} className="empty">No blocking findings 🎉</td></tr>
            )}
            {visible.map((f, i) => (
              <tr key={i} className={f.ack_by ? "acked" : ""}>
                <td><span className={`tag s-${severitySem(f.severity)}`}><span className="dot" />{f.severity}</span></td>
                <td className="obj">{f.object_type}</td>
                <td className="obj">{f.fqn}</td>
                <td><StatusTag status={f.drift_kind} /></td>
                <td className="obj">{f.error_class}</td>
                <td style={{ whiteSpace: "normal", maxWidth: 320, color: "var(--muted)", fontSize: 12 }}>{f.detail}</td>
                <td className="obj">{f.ack_by ? `${f.ack_by}` : "—"}</td>
                <td>
                  {f.ack_by ? (
                    <button className="linkbtn" disabled={busy === f.fqn} onClick={() => unack(f.fqn)}>un-ack</button>
                  ) : (
                    <button className="linkbtn" disabled={busy === f.fqn} onClick={() => ack(f.fqn)}>acknowledge</button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

// ---- incremental change feed ---------------------------------------------

export function ChangeFeed({ audit }: { audit: Audit[] }) {
  return (
    <section>
      <div className="sechead">
        <span className="eyebrow">Delta</span>
        <h2>Incremental change feed</h2>
        <span className="note">NEW / CHANGED / REMOVED across runs · newest first</span>
      </div>
      <div className="tablewrap">
        <table>
          <thead><tr>
            <th>When (UTC)</th><th>Change</th><th>Object</th><th>FQN</th>
            <th>Prev → New status</th><th>Detail</th>
          </tr></thead>
          <tbody>
            {audit.length === 0 && (
              <tr><td colSpan={6} className="empty">No changes recorded yet.</td></tr>
            )}
            {audit.map((a, i) => (
              <tr key={i}>
                <td className="num">{fmtTs(a.event_time)}</td>
                <td><span className={`tag s-${changeSem(a.change_type)}`}><span className="dot" />{a.change_type}</span></td>
                <td className="obj">{a.object_type}</td>
                <td className="obj">{a.fqn}</td>
                <td className="obj">{(a.prev_status ?? "∅")} → {(a.new_status ?? "∅")}</td>
                <td style={{ whiteSpace: "normal", maxWidth: 280, color: "var(--muted)", fontSize: 12 }}>{a.detail ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

// ---- failover / failback timeline ----------------------------------------

function outcomeSem(o: string) {
  const u = (o || "").toUpperCase();
  if (u.includes("PASS") && u.includes("LATE")) return "warn";
  if (u.includes("PASS") || u.includes("SUCCESS")) return "ok";
  if (u.includes("FAIL")) return "crit";
  return "info";
}

export function EventsTimeline({ events }: { events: DrEvent[] }) {
  return (
    <section>
      <div className="sechead">
        <span className="eyebrow">DR events</span>
        <h2>Failover / failback history</h2>
        <span className="note">promotions, data-loss window vs RPO, and post-event reconciliation outcome</span>
      </div>
      <div className="card">
        <div className="note-box">
          Failover / failback is triggered <b>out-of-band</b> via the account-scoped Managed DR
          API under change control — it is intentionally not an action in this console. This log is
          read-only evidence of past events.
        </div>
        {events.length === 0 ? (
          <div className="empty" style={{ marginTop: 8 }}>
            No failover or failback events recorded — steady-state since the failover group was created.
          </div>
        ) : (
          <div className="timeline" style={{ marginTop: 14 }}>
            {events.map((e, i) => (
              <div key={i} className={`tl-item ${outcomeSem(e.outcome)}`}>
                <div className="tl-when">{fmtTs(e.event_time)}</div>
                <div className="tl-head">
                  <span className="tl-title">{e.event_type}</span>
                  <span className="mono" style={{ fontSize: 12, color: "var(--muted)" }}>
                    {e.from_region} → {e.to_region}
                  </span>
                  <span className={`tag s-${outcomeSem(e.outcome)}`}><span className="dot" />{e.outcome}</span>
                </div>
                <div className="tl-detail">
                  trigger {e.trigger} · duration {e.duration_sec}s · data-loss window{" "}
                  {e.data_loss_window_ms != null ? `${(e.data_loss_window_ms / 1000).toFixed(0)}s` : "—"} ·{" "}
                  reconciled {NUM(e.objects_reconciled)}/{NUM(e.objects_total)}
                  {e.detail ? ` · ${e.detail}` : ""}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </section>
  );
}
