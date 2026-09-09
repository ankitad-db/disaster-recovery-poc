import { useCallback, useEffect, useState } from "react";
import { api, Overview } from "./api";
import { fmtTs } from "./lib";
import { ActionsBar } from "./components/Actions";
import { CoverageAndTrend, Hero, Kpis, TopologyStrip } from "./components/Overview";
import { DrillDown } from "./components/DrillDown";
import { GenieTab } from "./components/Genie";
import { ChangeFeed, EventsTimeline, Findings } from "./components/Signals";

type Tab = "summary" | "findings" | "events" | "drilldown" | "genie";
const TABS: { id: Tab; label: string }[] = [
  { id: "summary", label: "Summary" },
  { id: "findings", label: "Findings & changes" },
  { id: "events", label: "Failover history" },
  { id: "drilldown", label: "Per-object drill-down" },
  { id: "genie", label: "Ask Genie" },
];

export default function App() {
  const [data, setData] = useState<Overview | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState<Tab>("summary");
  const [theme, setTheme] = useState<string | null>(null);

  const load = useCallback(() => {
    setLoading(true);
    api.overview()
      .then(d => { setData(d); setErr(null); })
      .catch(e => setErr((e as Error).message))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => { load(); }, [load]);

  function toggleTheme() {
    const root = document.documentElement;
    const dark = root.getAttribute("data-theme") === "dark" ||
      (!root.getAttribute("data-theme") && matchMedia("(prefers-color-scheme:dark)").matches);
    const next = dark ? "light" : "dark";
    root.setAttribute("data-theme", next);
    setTheme(next);
  }

  if (loading && !data) {
    return <div className="center"><div className="spin" />loading reconciliation state…</div>;
  }
  if (err && !data) {
    return <div className="center" style={{ color: "var(--crit)" }}>
      <div>Failed to load: {err}</div>
      <button className="btn" onClick={load}>Retry</button>
    </div>;
  }

  const d = data!;
  const run = d.run;
  const objectTypes = [...new Set(d.coverage.map(c => c.object_type))].sort();

  return (
    <div className="wrap">
      <div className="topbar">
        <div className="brand">
          <div className="glyph">🛡️</div>
          <div>
            <h1>DR Reconciliation Console</h1>
            <div className="sub">Object-level failover readiness for Databricks Managed DR · AWS us-east-1 → us-west-2</div>
          </div>
        </div>
        <div className="selector">failover group: {d.topology.failover_group}</div>
        <div className="meta">
          last recon <span className="mono">{run ? fmtTs(run.run_ts) : "—"}</span><br />
          run <span className="mono">{run ? run.run_id : "—"}</span>
        </div>
        <button className="toggle" onClick={toggleTheme} title="Toggle theme" aria-label="Toggle light/dark theme">◐</button>
      </div>

      <nav className="nav" role="tablist">
        {TABS.map(t => (
          <button key={t.id} role="tab" aria-selected={tab === t.id} onClick={() => setTab(t.id)}>
            {t.label}
          </button>
        ))}
      </nav>

      {!run && (
        <section><div className="card"><div className="empty">No reconciliation runs found in dr_recon.control.dr_recon_runs.</div></div></section>
      )}

      {run && tab === "summary" && (
        <>
          <Hero run={run} />
          <TopologyStrip topo={d.topology} run={run} />
          <Kpis run={run} />
          <CoverageAndTrend coverage={d.coverage} history={d.history} run={run} />
          <ActionsBar onTriggered={load} />
        </>
      )}

      {run && tab === "findings" && (
        <>
          <Findings findings={d.findings} onChange={load} />
          <ChangeFeed audit={d.audit} />
        </>
      )}

      {run && tab === "events" && <EventsTimeline events={d.events} />}

      {run && tab === "drilldown" && <DrillDown objectTypes={objectTypes} />}

      {tab === "genie" && <GenieTab configured={d.genie_configured} />}

      <div className="foot">
        Reconciles the Databricks Managed DR secondary against the primary, object by object — the
        per-object, audit-ready operator view that complements the AI/BI dashboard.<br />
        Reads &amp; writes via the app service principal over warehouse cc56ad79aa69c967 · data in
        <span className="mono"> dr_recon.control.*</span>
      </div>
    </div>
  );
}
