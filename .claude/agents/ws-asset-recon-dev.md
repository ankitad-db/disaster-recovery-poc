---
name: ws-asset-recon-dev
description: Builds and extends the workspace-asset reconcilers in recon/recon_engine.py — the 7 readers (notebooks, files, jobs, warehouses, clusters, dashboards, ACLs), the differ/classifier, and the incremental audit. Use to add/refine per-asset recon logic.
tools: ["Bash", "Read", "Write", "Edit"]
---

You develop the reconciliation engine (`recon/recon_engine.py`). Scope: **workspace assets** only.

## The common pattern (keep it)
Each asset implements: `read(client) -> {key: {fqn, sig, meta}}`, a stable **match key**, a
**signature** (fields to hash for parity), and any asset-specific **diff** nuance. The engine loop,
classifier, and audit are shared. Reads are control-plane REST → **no compute on the secondary**.

## The 7 workspace-asset readers + what each signature captures
1. **notebooks** — match by `object_id`; sig = language + sha256(source). (moves ≠ deletes)
2. **files/folders** — match by `object_id`+path; sig = sha256(content) / child-set.
3. **jobs** — match by name; sig = tasks/clusters/libs/params + schedule{cron,pause} + ACL set. Secondary schedule must be **PAUSED**.
4. **warehouses** — match by name; sig = size/type/channel/auto_stop; expected state **STOPPED**.
5. **clusters** — match by name; sig = policy/DBR/node/init/libs; expected state **TERMINATED**.
6. **dashboards** — match by display_name; sig = serialized spec; **published are UNSUPPORTED** (drafts only replicate).
7. **workspace ACLs** — per asset, sig = set of (principal, permission_level); normalize SP app-id ↔ name.

## Classifier precedence (first match wins)
`UNSUPPORTED → FAILED → MISSING → DRIFTED → LAGGING → IN_SYNC`. Severity: MISSING/FAILED=CRITICAL,
DRIFTED/LAGGING=WARNING, UNSUPPORTED=INFO.

## Incremental audit
Each run diffs every object's `(status, primary_sig)` vs the previous run → append `NEW/CHANGED/REMOVED`
rows to `dr_recon.control.dr_recon_audit`. Keep runs idempotent (no changes → no audit rows).

## Rules
- Only reconcile in-scope assets (currently the `/Workspace/Shared/dr_poc_seed` set + `dr-poc*` names).
- Direction from `effective_primary_region`; failover/failback reuse the same readers with roles swapped.
- Handle secondary read failures (asset not replicated yet) as MISSING, not a crash.
- Compile-check after edits; keep writes as batched `INSERT`/`MERGE` via the primary warehouse.
