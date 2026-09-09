import { Coverage, HistoryRow, Run, Topology } from "../api";
import { lagStr, NUM, Pill, Sem, targetStr } from "../lib";

// ---- readiness verdict ----------------------------------------------------

function verdict(run: Run): { sem: Sem; sub: string } {
  const r = (run.readiness || "").toUpperCase();
  if (r === "GREEN" || r === "READY")
    return { sem: "ok", sub: "Secondary is a faithful replica of primary — failover is safe." };
  if (r === "BOOTSTRAP")
    return {
      sem: "info",
      sub: "Initial replication in progress — the secondary is still bootstrapping. " +
        "Objects reported MISSING are expected until the baseline copy completes.",
    };
  if (r === "AT_RISK")
    return { sem: "warn", sub: `${run.objects_attention} object(s) need attention before failover can be relied on.` };
  if (r === "CRITICAL")
    return { sem: "crit", sub: `${run.blocking_errors} blocking error(s) — failover is not safe until resolved.` };
  return { sem: "idle", sub: "Reconciliation status." };
}

const SEMVAR: Record<Sem, string> = {
  ok: "var(--ok)", warn: "var(--warn)", crit: "var(--crit)", info: "var(--info)", idle: "var(--idle)",
};

// ---- RPO gauge (semicircle) ----------------------------------------------

function Gauge({ run }: { run: Run }) {
  const lag = run.rpo_lag_ms;
  const tgt = run.rpo_target_ms || 900000;
  const frac = lag === null || lag === undefined ? null : Math.min(lag / tgt, 1.15);
  const sem: Sem = lag === null || lag === undefined ? "warn" : frac! <= 1 ? "ok" : "crit";
  // semicircle from 180deg to 0deg
  const R = 90, CX = 110, CY = 110;
  const pt = (t: number) => {
    const a = Math.PI * (1 - t);
    return [CX + R * Math.cos(a), CY - R * Math.sin(a)];
  };
  const [ex, ey] = pt(Math.min(frac ?? 0, 1));
  const [sx, sy] = pt(0);
  const large = (frac ?? 0) > 0.5 ? 1 : 0;
  return (
    <div className="card gauge">
      <div className="gwrap">
        <svg viewBox="0 0 220 140" aria-label="RPO lag vs target gauge">
          <path d={`M ${sx} ${sy} A ${R} ${R} 0 1 1 ${CX + R} ${CY}`}
            fill="none" stroke="var(--grid)" strokeWidth="14" strokeLinecap="round" />
          {frac !== null && frac > 0 && (
            <path d={`M ${sx} ${sy} A ${R} ${R} 0 ${large} 1 ${ex} ${ey}`}
              fill="none" stroke={SEMVAR[sem]} strokeWidth="14" strokeLinecap="round" />
          )}
        </svg>
        <div className="gcenter">
          <div className="gval" style={{ color: SEMVAR[sem] }}>
            {lagStr(lag)}{lag !== null && lag !== undefined && <small></small>}
          </div>
          <div className="glab">RPO lag</div>
        </div>
      </div>
      <div className="gtarget">
        target ≤ <b className="mono">{targetStr(tgt)}</b> ·{" "}
        {lag === null || lag === undefined
          ? "no replication point yet (bootstrap)"
          : lag <= tgt ? "within target" : "exceeds target"}
      </div>
    </div>
  );
}

export function Hero({ run }: { run: Run }) {
  const v = verdict(run);
  return (
    <section>
      <div className="grid hero">
        <div className="card verdict">
          <div className="rail" style={{ background: SEMVAR[v.sem] }} />
          <div style={{ paddingLeft: 8 }}>
            <div className="vlab">Failover readiness · {run.failover_group}</div>
            <div className="vbig" style={{ color: SEMVAR[v.sem] }}>{run.readiness}</div>
            <div className="vsub">{v.sub}</div>
            <div className="vmeta">
              <div>Replication state<b>{run.replication_state}</b></div>
              <div>Mode<b>{run.mode}</b></div>
              <div>Effective primary<b>{run.effective_primary_region}</b></div>
              <div>Objects in sync<b>{run.objects_ok}/{run.objects_in_scope}</b></div>
            </div>
          </div>
        </div>
        <Gauge run={run} />
      </div>
    </section>
  );
}

// ---- topology strip -------------------------------------------------------

export function TopologyStrip({ topo, run }: { topo: Topology; run: Run }) {
  return (
    <section>
      <div className="sechead">
        <span className="eyebrow">Topology</span>
        <h2>Failover group — workspaces &amp; replication</h2>
        <span className="note">who is primary, and how the secondary is tracking</span>
      </div>
      <div className="card" style={{ padding: 0 }}>
        <div className="topo">
          <div className="node prim">
            <div className="role">{topo.primary.role}</div>
            <div className="wsname">{topo.primary.name}</div>
            <div className="kv">
              workspace id&nbsp; <b>{topo.primary.workspace_id}</b><br />
              region&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; <b>{topo.primary.region}</b><br />
              state&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{" "}
              <span className="pill s-ok" style={{ padding: "1px 8px" }}>
                <span className="dot" />{topo.primary.state}</span>
            </div>
          </div>
          <div className="link">
            <div className="lab">Managed DR<br />failover group<br /><b>{run.mode}</b></div>
            <div className="flow">⇢</div>
            <div className="repl">
              {run.replication_state === "INITIAL_REPLICATION"
                ? "initial replication\n(baseline copy)" : "continuous replication"}
              <br />RPO target {targetStr(run.rpo_target_ms)}
            </div>
          </div>
          <div className="node sec">
            <div className="role">{topo.secondary.role}</div>
            <div className="wsname">{topo.secondary.name}</div>
            <div className="kv">
              region&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; <b>{topo.secondary.region}</b><br />
              state&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{" "}
              <span className="pill s-info" style={{ padding: "1px 8px" }}>
                <span className="dot" />{topo.secondary.state}</span>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

// ---- KPI tiles ------------------------------------------------------------

export function Kpis({ run }: { run: Run }) {
  const pctInSync = run.objects_in_scope
    ? ((run.objects_ok / run.objects_in_scope) * 100).toFixed(1) : "0.0";
  const tiles: { label: string; big: JSX.Element; foot: string; color?: string; rail?: boolean }[] = [
    { label: "Readiness", rail: true,
      big: <span>{run.readiness}</span>, foot: `${run.objects_attention} need attention` },
    { label: "Replication state",
      big: <span style={{ fontSize: 18 }}>{run.replication_state}</span>, foot: `mode ${run.mode}` },
    { label: "RPO — lag",
      big: <span>{lagStr(run.rpo_lag_ms)}</span>,
      foot: `target ≤ ${targetStr(run.rpo_target_ms)}` },
    { label: "Objects in sync",
      big: <span>{pctInSync}<small> %</small></span>,
      foot: `${run.objects_ok} of ${run.objects_in_scope} in scope` },
    { label: "Need attention", color: "var(--warn)",
      big: <span>{run.objects_attention}</span>, foot: "drifted / lagging / missing" },
    { label: "Blocking errors", color: "var(--crit)",
      big: <span>{run.blocking_errors}</span>, foot: "holding failover readiness" },
  ];
  return (
    <section>
      <div className="grid kpis">
        {tiles.map((t, i) => (
          <div className={`card kpi${t.rail ? " accent-rail" : ""}`} key={i}>
            <div className="label">{t.label}</div>
            <div className="big" style={t.color ? { color: t.color } : undefined}>{t.big}</div>
            <div className="foot">{t.foot}</div>
          </div>
        ))}
      </div>
    </section>
  );
}

// ---- coverage scorecard + RPO trend --------------------------------------

const ORDER = ["IN_SYNC", "LAGGING", "DRIFTED", "MISSING", "FAILED", "UNSUPPORTED"];
const SEGCLASS: Record<string, string> = {
  IN_SYNC: "b-ok", LAGGING: "b-warn", DRIFTED: "b-warn",
  MISSING: "b-crit", FAILED: "b-crit", UNSUPPORTED: "b-info",
};

function ScoreRow({ type, rows }: { type: string; rows: Coverage[] }) {
  const total = rows.reduce((s, r) => s + r.cnt, 0) || 1;
  const ok = rows.filter(r => r.status === "IN_SYNC").reduce((s, r) => s + r.cnt, 0);
  const sorted = [...rows].sort((a, b) => ORDER.indexOf(a.status) - ORDER.indexOf(b.status));
  return (
    <div className="score-row">
      <div className="name">{type}<span>{rows.map(r => `${r.cnt} ${r.status.toLowerCase()}`).join(" · ")}</span></div>
      <div className="bar">
        {sorted.map((r, i) => (
          <i key={i} className={SEGCLASS[r.status] || "b-info"}
            style={{ width: `${(r.cnt / total * 100).toFixed(1)}%` }} title={`${r.status}: ${r.cnt}`} />
        ))}
      </div>
      <div className="pct">{(ok / total * 100).toFixed(0)}%<small> ok</small></div>
    </div>
  );
}

export function CoverageAndTrend({ coverage, history, run }:
  { coverage: Coverage[]; history: HistoryRow[]; run: Run }) {
  const byType = new Map<string, Coverage[]>();
  for (const c of coverage) {
    if (!byType.has(c.object_type)) byType.set(c.object_type, []);
    byType.get(c.object_type)!.push(c);
  }
  const types = [...byType.keys()].sort();
  return (
    <section>
      <div className="sechead">
        <span className="eyebrow">Coverage</span>
        <h2>Reconciliation scorecard — by object type</h2>
        <span className="note">latest run · segments show status mix</span>
      </div>
      <div className="grid two">
        <div className="card">
          {types.length === 0 && <div className="empty">No coverage rows for this run.</div>}
          {types.map(t => <ScoreRow key={t} type={t} rows={byType.get(t)!} />)}
          <div className="legend">
            <span><i className="sw b-ok" />in sync</span>
            <span><i className="sw b-warn" />lagging / drifted</span>
            <span><i className="sw b-crit" />missing / failed</span>
            <span><i className="sw b-info" />unsupported</span>
          </div>
        </div>
        <RpoTrend history={history} run={run} />
      </div>
    </section>
  );
}

function RpoTrend({ history, run }: { history: HistoryRow[]; run: Run }) {
  const W = 320, H = 92;
  const pts = history.map(h => (h.rpo_lag_ms === null ? null : h.rpo_lag_ms));
  const known = pts.filter((p): p is number => p !== null);
  const tgt = run.rpo_target_ms || 900000;
  const max = Math.max(tgt, ...(known.length ? known : [tgt])) * 1.1;
  const n = Math.max(pts.length - 1, 1);
  const x = (i: number) => (i / n) * W;
  const y = (v: number) => H - (v / max) * (H - 8) - 4;
  const targetY = y(tgt);
  const linePts = pts.map((p, i) => (p === null ? null : `${x(i)},${y(p)}`));
  const path = linePts.filter(Boolean).join(" L ");
  const allNull = known.length === 0;
  return (
    <div className="card">
      <div className="spark-head">
        <div>
          <div className="eyebrow">RPO trend · rpo_lag vs target</div>
          <div className="spark-val" style={{ marginTop: 8 }}>{lagStr(run.rpo_lag_ms)}</div>
        </div>
        <Pill text={allNull ? "no lag data" : (run.rpo_lag_ms ?? 0) <= tgt ? "within target" : "over target"}
          sem={allNull ? "warn" : (run.rpo_lag_ms ?? 0) <= tgt ? "ok" : "crit"} />
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" style={{ marginTop: 12, height: 92 }}
        aria-label="RPO lag across runs">
        <line x1="0" y1={targetY} x2={W} y2={targetY} stroke="var(--warn)" strokeDasharray="3 4" opacity="0.7" />
        {path && <path d={`M ${path}`} fill="none" stroke="var(--accent)" strokeWidth="2" strokeLinejoin="round" />}
        {pts.map((p, i) => p !== null && (
          <circle key={i} cx={x(i)} cy={y(p)} r="2.5" fill="var(--accent)" />
        ))}
      </svg>
      <div className="foot" style={{ fontSize: 11.5, color: "var(--muted)", marginTop: 8 }}>
        {allNull
          ? <>no replication point recorded yet — <span className="mono">null</span> lag during initial replication (expected in bootstrap)</>
          : <>dashed line = {targetStr(tgt)} RPO target · {history.length} runs</>}
      </div>
      <div style={{ borderTop: "1px solid var(--grid)", marginTop: 12, paddingTop: 10 }}>
        <div className="eyebrow" style={{ marginBottom: 6 }}>Replication health</div>
        <div className="row" style={{ padding: "5px 0" }}>
          <span className="cls">state</span>
          <span className="count">{run.replication_state}</span>
        </div>
        <div className="row" style={{ padding: "5px 0" }}>
          <span className="cls">effective primary</span>
          <span className="count">{run.effective_primary_region}</span>
        </div>
      </div>
    </div>
  );
}
