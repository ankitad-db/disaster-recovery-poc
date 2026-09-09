export interface Run {
  run_id: string; run_ts: string; failover_group: string;
  effective_primary_region: string; rpo_lag_ms: number | null; rpo_target_ms: number | null;
  readiness: string; objects_in_scope: number; objects_ok: number; objects_attention: number;
  blocking_errors: number; replication_state: string; mode: string;
}
export interface Coverage { object_type: string; status: string; cnt: number; }
export interface Finding {
  run_id: string; object_type: string; fqn: string; drift_kind: string;
  error_class: string; detail: string; severity: string; first_seen: string;
  ack_by: string | null; ack_ts: string | null; ack_note: string | null;
}
export interface Audit {
  event_time: string; run_id: string; object_type: string; fqn: string;
  change_type: string; prev_status: string | null; new_status: string | null;
  prev_sig: string | null; new_sig: string | null; direction: string | null; detail: string | null;
}
export interface DrEvent {
  event_time: string; event_type: string; direction: string; from_region: string;
  to_region: string; trigger: string; duration_sec: number; data_loss_window_ms: number;
  objects_reconciled: number; objects_total: number; outcome: string; detail: string;
}
export interface HistoryRow {
  run_id: string; run_ts: string; rpo_lag_ms: number | null; rpo_target_ms: number | null;
  readiness: string; objects_in_scope: number; objects_ok: number;
  objects_attention: number; blocking_errors: number;
}
export interface Topology {
  failover_group: string;
  primary: { name: string; region: string; workspace_id: string; role: string; state: string };
  secondary: { name: string; region: string; role: string; state: string };
}
export interface Overview {
  topology: Topology; genie_configured: boolean; recon_job_id: string;
  run: Run | null; coverage: Coverage[]; findings: Finding[]; audit: Audit[];
  events: DrEvent[]; history: HistoryRow[]; acks: { fqn: string; ack_by: string; ack_ts: string; note: string }[];
}
export interface InventoryItem {
  object_type: string; asset_key: string; fqn: string; in_scope: boolean;
  status: string; severity: string; primary_sig: string | null;
  secondary_sig: string | null; detail: string | null; last_reconciled: string | null;
}
export interface GenieAnswer {
  conversation_id: string; message_id: string; answer: string;
  sql: string | null; columns: string[]; rows: any[][]; suggestions: string[]; status: string | null;
}

async function j<T>(url: string, init?: RequestInit): Promise<T> {
  const r = await fetch(url, init);
  if (!r.ok) {
    let msg = `${r.status}`;
    try { msg = (await r.json()).detail || msg; } catch {}
    throw new Error(msg);
  }
  return r.json();
}

export const api = {
  overview: () => j<Overview>("/api/overview"),
  inventory: (ot: string, st: string) =>
    j<{ run_id: string | null; items: InventoryItem[] }>(
      `/api/inventory?object_type=${encodeURIComponent(ot)}&status=${encodeURIComponent(st)}`),
  runRecon: () => j<{ run_id: number; message: string }>("/api/run-recon", { method: "POST" }),
  runStatus: (id: number) => j<any>(`/api/run-recon/${id}`),
  acknowledge: (fqn: string, note: string) =>
    j("/api/acknowledge", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fqn, note }),
    }),
  unacknowledge: (fqn: string) =>
    j("/api/unacknowledge", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fqn, note: "" }),
    }),
  askGenie: (question: string, conversation_id: string | null) =>
    j<GenieAnswer>("/api/genie/ask", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question, conversation_id }),
    }),
};
