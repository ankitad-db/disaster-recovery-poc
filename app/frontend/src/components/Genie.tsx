import { useRef, useState } from "react";
import { api, GenieAnswer } from "../api";

interface Turn { role: "user" | "genie"; text: string; ans?: GenieAnswer; }

const SAMPLES = [
  "Which objects are MISSING in the latest run?",
  "How many objects of each type need attention?",
  "What changed since the previous run?",
  "Show the RPO lag trend across runs.",
];

export function GenieTab({ configured }: { configured: boolean }) {
  const [turns, setTurns] = useState<Turn[]>([]);
  const [q, setQ] = useState("");
  const [busy, setBusy] = useState(false);
  const convId = useRef<string | null>(null);

  async function ask(question: string) {
    const text = question.trim();
    if (!text || busy) return;
    setQ("");
    setTurns(t => [...t, { role: "user", text }]);
    setBusy(true);
    try {
      const ans = await api.askGenie(text, convId.current);
      convId.current = ans.conversation_id;
      setTurns(t => [...t, { role: "genie", text: ans.answer, ans }]);
    } catch (e) {
      setTurns(t => [...t, { role: "genie", text: "Error: " + (e as Error).message }]);
    } finally {
      setBusy(false);
    }
  }

  return (
    <section>
      <div className="sechead">
        <span className="eyebrow">Ask Genie</span>
        <h2>Natural-language Q&amp;A over the reconciliation tables</h2>
        <span className="note">Genie space scoped to dr_recon.control · powered by the Conversation API</span>
      </div>
      <div className="card chat">
        {!configured && (
          <div className="note-box" style={{ borderLeftColor: "var(--warn)" }}>
            Genie is not configured (DR_GENIE_SPACE_ID unset). Set the Genie space id in app.yaml and redeploy.
          </div>
        )}
        <div className="chat-log">
          {turns.length === 0 && (
            <div>
              <div style={{ color: "var(--muted)", fontSize: 13, marginBottom: 10 }}>
                Ask about reconciliation status, drift, RPO, or DR events. Try:
              </div>
              <div className="chips">
                {SAMPLES.map(s => (
                  <button key={s} className="chip" onClick={() => ask(s)} disabled={!configured}>{s}</button>
                ))}
              </div>
            </div>
          )}
          {turns.map((t, i) => (
            <div key={i} className={`bubble ${t.role}`}>
              {t.role === "genie" ? <GenieBubble turn={t} /> : <span>{t.text}</span>}
            </div>
          ))}
          {busy && <div className="bubble genie"><span className="mono" style={{ color: "var(--muted)" }}>Genie is thinking…</span></div>}
        </div>
        <form className="askform" onSubmit={e => { e.preventDefault(); ask(q); }}>
          <input value={q} onChange={e => setQ(e.target.value)}
            placeholder="Ask Genie about the DR reconciliation data…" disabled={!configured || busy} />
          <button className="btn primary" type="submit" disabled={!configured || busy || !q.trim()}>Ask</button>
        </form>
      </div>
    </section>
  );
}

function GenieBubble({ turn }: { turn: Turn }) {
  const ans = turn.ans;
  return (
    <div>
      <div className="ans">{turn.text}</div>
      {ans?.sql && (
        <div className="sqlbox"><pre>{ans.sql}</pre></div>
      )}
      {ans && ans.columns.length > 0 && (
        <div className="tablewrap" style={{ marginTop: 10 }}>
          <table style={{ minWidth: 0 }}>
            <thead><tr>{ans.columns.map((c, i) => <th key={i}>{c}</th>)}</tr></thead>
            <tbody>
              {ans.rows.slice(0, 50).map((r, i) => (
                <tr key={i}>{r.map((v, j) => <td key={j} className="num">{v === null ? "—" : String(v)}</td>)}</tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {ans && ans.suggestions.length > 0 && (
        <div className="chips" style={{ marginTop: 10 }}>
          {ans.suggestions.slice(0, 4).map((s, i) => <span key={i} className="chip" style={{ cursor: "default" }}>{s}</span>)}
        </div>
      )}
    </div>
  );
}
