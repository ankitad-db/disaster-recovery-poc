# Workspace Secrets DR — Test Report (live end-to-end)

**Date:** 2026-09-01 · **Executed by:** automated driver `scripts/e2e_secrets_dr_test.py`
**Result:** ✅ PASS — replicate PRIMARY→SECONDARY, incremental diff-and-apply, and failback
all verified against the live sandbox workspaces + real S3/KMS/CRR assets.

This is the actual runbook that was followed, with observations and findings. For the
reusable procedure see [secrets_dr_runbook.md](secrets_dr_runbook.md); for architecture,
[secrets_dr_architecture.excalidraw](secrets_dr_architecture.excalidraw).

---

## Environment tested

| | Primary (active) | Secondary (passive) |
|---|---|---|
| Workspace | `fe-sandbox-…-ps-dr-wp-us-east-2` (id 7474657900994867) | `fe-sandbox-…-ps-dr-wp-us-west-2` (id 7474651252878032) |
| Region | us-east-2 | us-west-2 |
| S3 bucket | `dr-secrets-us-east-2-332745928618` | `dr-secrets-us-west-2-332745928618` |
| SQL warehouse | `63af3d742ebd95ab` | `4ca723533b34106a` |

**KMS:** `alias/dr-secrets-us-east-2` and `alias/dr-secrets-us-west-2` both resolve to a
single **Multi-Region Key** `mrk-837ba5a7e3fb49569e666446a7b0992d` (PRIMARY in east,
REPLICA in west) — see Finding 1. **CRR:** bidirectional on the `secrets/` prefix, replica
key = the MRK. AWS account `332745928618`.

**How it was run:** locally, SDK auth via the `dr-east`/`dr-west` CLI profiles, AWS via the
boto3 chain. The test is scoped to the seed scopes only (`dr_app_prod`, `dr_app_analytics`)
so mirror-mode never touches unrelated sandbox secrets.

---

## What was executed, and the result of each step

| # | Step | Result | Evidence |
|---|---|---|---|
| 1 | Control tables created in **both** workspaces | ✅ | `dr_poc.dr_control.{dr_secrets_inventory,dr_secrets_audit}` created in east + west |
| 2 | Seed 5 sample secrets in PRIMARY (2 scopes, 1 ACL) | ✅ | `dr_app_prod`{db_password,api_token,service_url} + `dr_app_analytics`{warehouse_token,s3_access_key} |
| 3 | **Baseline export** east → S3 | ✅ | bundle 5 items, 3,676 bytes, **plaintext_leak=False**, **snapshot=full**, 26.5 s |
| 3b | **CRR** east bucket → west bucket | ✅ | `ReplicationStatus=COMPLETED`, `_latest` pointer replicated |
| 4 | **Failover import** into WEST (destination-aware) | ✅ | `added=5, acls=1`; **east & west sha256 identical for all 5 keys** |
| 5 | **Idempotency** — re-import with no changes | ✅ | `in_sync=True, skipped=5` (nothing re-decrypted/rewritten) |
| 6 | **Incremental** — rotate `db_password` + delete `service_url`, export, import | ✅ | export `changed=4, deleted=1`; import **`updated=1, deleted=1, skipped=3`**; hashes match |
| 7 | **Failback** — change `api_token` in WEST, export WEST → CRR → import into EAST (roles swapped) | ✅ | import `updated=1, skipped=3`; **east `api_token` == west `api_token`** |
| 8 | Audit tables | ✅ east / ⚠️ west | east records EXPORT×4 + IMPORT (failback); west blocked — Finding 7 |

### Step 6 detail — the diff logic (the important one)

- The `system.access.audit` scan returned **0 changes** for the just-made rotation/delete
  (audit-log ingestion latency — see Finding 4).
- The **full state-diff recon** (the safety net) correctly detected `changed=4, deleted=1`
  by comparing live values/hashes against the inventory — so the export was correct anyway.
- The destination-aware **import applied only the delta**: `updated=1` (rotated
  `db_password`), `deleted=1` (`service_url` tombstone), `skipped=3` (unchanged — not even
  decrypted). West end-state hashes matched east exactly. This is the core "diff across
  both workspaces, then apply" behaviour, working as designed.

### Step 7 detail — failback (symmetric)

To prove failback, `api_token` was changed in **WEST** (simulating work done while WEST
was the active primary), then the flow was run with the **roles swapped** (config primary↔
secondary and the bucket mapping swapped): export from WEST → CRR west→east → import into
EAST. The import was destination-aware: `added=0, updated=1 (api_token), deleted=0,
skipped=3`, and **east's `api_token` hash then equalled west's**. Steady state is
restored symmetrically — the same code runs both directions.

### Audit trail (east control tables)

```
EXPORT  SUCCESS  us-east-2->us-west-2  n=5  snapshot items=5 deleted=0 scopes=2   (baseline, ×3 test runs)
EXPORT  SUCCESS  us-east-2->us-west-2  n=4  snapshot items=4 deleted=1 scopes=2   (incremental: rotate + delete)
IMPORT  SUCCESS  us-west-2->us-east-2  n=1  added=0 updated=1 deleted=0 skipped=3 mode=mirror   (failback)
```
`dr_secrets_inventory` (east) ends with `service_url = DELETED`, the other four `IN_SYNC` —
matching the live workspace state.

---

## Findings & fixes

**Finding 1 — Cross-region envelope decryption requires KMS Multi-Region Keys.** *(blocker, fixed)*
The first import failed with `kms:Decrypt … resource does not exist in this Region`. The
envelope data key was wrapped by the **east** key, but KMS keys are regional, so the west
importer couldn't unwrap it locally — and in a real east-region outage, reaching the east
key would be impossible anyway. **Fix:** migrated both regional keys to a single **Multi-Region
Key** (primary in east, replica in west); both region aliases now point at it, so the promoted
secondary decrypts **locally**. No application code changed — `kms.decrypt` in the local region
just works once the replica exists. *This is the correct DR design and the provisioner should
create MRKs from the start.*

**Finding 2 — CRR needs KMS permission on the MRK.** *(fixed)*
After the MRK migration, CRR went to `ReplicationStatus=FAILED`: the `dr-secrets-crr-role`
policy and the rule's `ReplicaKmsKeyID` still referenced the old regional keys. **Fix:**
updated the CRR role policy to allow `Decrypt/Encrypt/GenerateDataKey` on the MRK ARNs and
set each rule's `ReplicaKmsKeyID` to the MRK. Replication returned to `COMPLETED`.

**Finding 3 — `_latest` pointer replication race.** *(fixed in test harness)*
The importer reads `_latest.txt` then the bundle it names. Right after an export, the bundle
object can replicate before the pointer, so a naive import can read a **stale** pointer.
**Fix:** the test waits until the destination `_latest.txt` equals the new bundle id before
importing. *Recommendation:* in production, either import on a slight delay or have the
importer verify the pointed-at bundle exists locally and fall back to polling.

**Finding 4 — `system.access.audit` has ingestion latency.** *(by design; validated)*
A secret mutation was **not** visible in `system.access.audit` seconds later, so the
audit-based detector saw nothing. The **full state-diff recon** caught it. Conclusion: treat
the audit scan as an optimization for *what changed*, and keep `full_recon: true` as the
authority for correctness. RPO must not assume the audit stream is instantaneous.

**Finding 5 — CRR tail latency is variable.** *(observation)*
CRR completed in **seconds** for the baseline, but one incremental round took **~6.5 minutes**
for the pointer to replicate. S3 CRR is asynchronous/best-effort (SLA is 15 min for the
default tier). **RPO = export cadence + CRR tail**, not just the export schedule. For a tighter,
bounded RPO consider **S3 Replication Time Control (RTC)** (15-min SLA with metrics/alarms).

**Finding 6 — the off-cluster `SqlExecutor` swallowed failed statements.** *(bug, fixed)*
The SDK statement-execution path returned the `StatementResponse` without checking its
status, so a **FAILED** statement (e.g. control-table DDL that errored) returned quietly and
callers saw empty data. The test even printed *"[west] control tables ready"* while the DDL
had actually failed. **Fix:** `SqlExecutor.execute` now inspects the status and **raises** on
`FAILED/CANCELED/CLOSED` with the error message. This immediately surfaced Finding 7.
*Lesson: the control plane was failing silently; only the pure-SDK data path was truly
verified until this was fixed.*

**Finding 7 — the WEST metastore can't host the control tables yet.** *(environment gap)*
With Finding 6 fixed, creating a managed Delta table in a fresh west `dr_poc` catalog fails:
`EXTERNAL_LOCATION_DOES_NOT_EXIST … ankita-dr-wp-us-west-2-ext-s3-…/dr_managed/… does not
exist`. The catalog/schema (metadata) create fine, but the metastore's **managed storage
location doesn't exist**, so no table can be created. **This does not affect the DR data path**
(secret replication is pure SDK and is fully verified); it only blocks the audit/inventory
tables on the passive side. **Options:** (a) point `control.catalog` at an existing west catalog
that has valid managed storage, or (b) create the west `dr_poc` catalog with an explicit
`MANAGED LOCATION` on a real external location. Since the passive side runs no compute in
steady state, west audit only matters post-failover, when the promoted workspace should already
have working storage.

---

## Repo changes made as a result

- **KMS → Multi-Region Key** (live) + `scripts/provision_secrets_dr_aws.sh` rewritten to
  create an MRK (primary in east, replica in west) and to scope CRR/compute KMS grants to it.
- **CRR** role policy + `ReplicaKmsKeyID` updated (live) to the MRK.
- **`SqlExecutor.execute` now raises on failed statements** (`src/databricks_dr/common/sql.py`)
  — control-plane failures are loud instead of silent (Finding 6).
- Test harness `scripts/e2e_secrets_dr_test.py` added (the exact steps above, re-runnable);
  it waits for the `_latest` pointer to replicate before importing (Finding 3).
- `config/secrets_dr_config.yaml`: real workspace ids filled in.

## Recommendations / next steps

1. Adopt **S3 RTC** if a bounded RPO is required; add CloudWatch alarms on
   `ReplicationLatency` / `OperationsFailedReplication`.
2. Run the same flow **from Databricks jobs** (Asset Bundle) as the DR service principal,
   using the `dr-secrets-uc-role` / instance profile, to validate the on-cluster identity +
   KMS grants end-to-end (this run used a local admin identity).
3. Wire the control-table **audit/inventory** into a health check + alert (the tables are
   populated; add `v_dr_secrets_failures` monitoring).
