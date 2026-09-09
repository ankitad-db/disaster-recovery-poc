// Shared formatting + status helpers for the DR console.

export type Sem = "ok" | "warn" | "crit" | "info" | "idle";

// Map reconciliation status to a semantic color class.
export function statusSem(status: string): Sem {
  switch ((status || "").toUpperCase()) {
    case "IN_SYNC": return "ok";
    case "LAGGING":
    case "DRIFTED": return "warn";
    case "MISSING":
    case "FAILED": return "crit";
    case "UNSUPPORTED": return "info";
    default: return "idle";
  }
}

export function severitySem(sev: string): Sem {
  switch ((sev || "").toUpperCase()) {
    case "CRITICAL": return "crit";
    case "HIGH": return "warn";
    case "MEDIUM": return "warn";
    case "LOW": return "info";
    default: return "idle";
  }
}

export function changeSem(kind: string): Sem {
  switch ((kind || "").toUpperCase()) {
    case "NEW": return "info";
    case "CHANGED": return "warn";
    case "REMOVED": return "crit";
    default: return "idle";
  }
}

export function StatusTag({ status }: { status: string }) {
  return (
    <span className={`tag s-${statusSem(status)}`}>
      <span className="dot" />{status}
    </span>
  );
}

export function Pill({ text, sem }: { text: string; sem: Sem }) {
  return (
    <span className={`pill s-${sem}`}><span className="dot" />{text}</span>
  );
}

const NUM = (n: number | null | undefined) =>
  n === null || n === undefined ? "—" : Number(n).toLocaleString();

export { NUM };

// milliseconds -> human string; null means "never replicated".
export function lagStr(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return "null";
  const s = ms / 1000;
  if (s < 90) return `${s.toFixed(0)}s`;
  if (s < 5400) return `${(s / 60).toFixed(1)}m`;
  return `${(s / 3600).toFixed(1)}h`;
}

export function targetStr(ms: number | null | undefined): string {
  if (!ms) return "—";
  const m = ms / 1000 / 60;
  return m >= 1 ? `${m.toFixed(0)}m` : `${(ms / 1000).toFixed(0)}s`;
}

// UTC timestamp -> "2026-09-09 06:45 UTC"
export function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  const d = new Date(ts);
  if (isNaN(d.getTime())) return ts;
  const p = (n: number) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())} ` +
    `${p(d.getUTCHours())}:${p(d.getUTCMinutes())} UTC`;
}

export function fmtDate(ts: string | null | undefined): string {
  if (!ts) return "—";
  const d = new Date(ts);
  if (isNaN(d.getTime())) return ts;
  const p = (n: number) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}`;
}
