import { useEffect, useState } from "react";
import { api, InventoryItem } from "../api";
import { fmtTs, severitySem, StatusTag } from "../lib";

const STATUSES = ["ALL", "IN_SYNC", "LAGGING", "DRIFTED", "MISSING", "FAILED", "UNSUPPORTED"];

export function DrillDown({ objectTypes }: { objectTypes: string[] }) {
  const [ot, setOt] = useState("ALL");
  const [st, setSt] = useState("ALL");
  const [items, setItems] = useState<InventoryItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    setLoading(true);
    api.inventory(ot, st)
      .then(r => { if (live) { setItems(r.items); setErr(null); } })
      .catch(e => { if (live) setErr((e as Error).message); })
      .finally(() => { if (live) setLoading(false); });
    return () => { live = false; };
  }, [ot, st]);

  return (
    <section>
      <div className="sechead">
        <span className="eyebrow">Drill-down</span>
        <h2>Per-object reconciliation — primary vs secondary</h2>
        <span className="note">latest run · filter by object type and status</span>
      </div>
      <div className="filters">
        <label className="eyebrow">object type</label>
        <select value={ot} onChange={e => setOt(e.target.value)}>
          <option value="ALL">ALL</option>
          {objectTypes.map(t => <option key={t} value={t}>{t}</option>)}
        </select>
        <label className="eyebrow">status</label>
        <select value={st} onChange={e => setSt(e.target.value)}>
          {STATUSES.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
        <span className="mono" style={{ marginLeft: "auto", fontSize: 12, color: "var(--muted)" }}>
          {items.length} object{items.length === 1 ? "" : "s"}
        </span>
      </div>
      <div className="tablewrap">
        <table>
          <thead><tr>
            <th>Type</th><th>FQN</th><th>Asset key</th><th>Severity</th>
            <th>Primary sig</th><th>Secondary sig</th><th>Detail</th>
            <th>Last reconciled</th><th>Status</th>
          </tr></thead>
          <tbody>
            {loading && <tr><td colSpan={9} className="empty">Loading…</td></tr>}
            {err && <tr><td colSpan={9} className="empty" style={{ color: "var(--crit)" }}>{err}</td></tr>}
            {!loading && !err && items.length === 0 &&
              <tr><td colSpan={9} className="empty">No objects match these filters.</td></tr>}
            {!loading && !err && items.map((it, i) => (
              <tr key={i}>
                <td className="obj">{it.object_type}</td>
                <td className="obj">{it.fqn}</td>
                <td className="obj" style={{ color: "var(--muted)" }}>{it.asset_key}</td>
                <td><span className={`tag s-${severitySem(it.severity)}`}><span className="dot" />{it.severity}</span></td>
                <td className="num">{it.primary_sig ?? "—"}</td>
                <td className="num">{it.secondary_sig ?? "∅"}</td>
                <td style={{ whiteSpace: "normal", maxWidth: 280, color: "var(--muted)", fontSize: 12 }}>{it.detail ?? "—"}</td>
                <td className="num">{fmtTs(it.last_reconciled)}</td>
                <td><StatusTag status={it.status} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
