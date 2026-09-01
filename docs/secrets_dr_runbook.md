# Workspace Secrets DR — Runbook & Test Playbook

A complete, self-contained playbook: the mental model, the concrete environment, a
step-by-step test procedure with **what to check at every stage**, and troubleshooting.
Work top-to-bottom the first time; later, jump to the section you need.

> Convention: **EAST = primary (active)**, **WEST = secondary (passive standby)**.
> Direction is symmetric — for failback, swap the roles.

---

## 1. What this is and why it exists

Databricks **Managed DR** replicates a lot of the workspace, but **not workspace
secrets** (secret scopes, secret values, and their ACLs). If EAST is lost, any job or
notebook in the promoted WEST workspace that reads `dbutils.secrets.get(...)` breaks
until those secrets are recreated. This project keeps an **encrypted, cross-region copy
of your secrets** so the promoted workspace can be made whole in minutes.

**Design principles (managed-DR best practice):**

| Principle | How it shows up here |
|---|---|
| **Active–passive** | EAST serves; WEST is a passive standby. |
| **No compute on the passive side** | All steady-state work runs in EAST. WEST is only *written to* — on failover, once it's promoted. |
| **Encryption end-to-end** | Values are envelope-encrypted (KMS + AES-GCM) **before** they touch S3. Plaintext never lands in object storage. |
| **Durable, self-contained recovery point** | Each export is a **full snapshot**, so a single bundle can rebuild a cold secondary. |
| **Idempotent, destination-aware** | Import diffs the bundle against WEST's live state and applies **only** the delta. Safe to re-run. |

---

## 2. The mental model (what actually happens)

```
                         STEADY STATE (EAST active)
  ┌──────────────────────────────── EAST (us-east-2) ─────────────────────────────┐
  │  EXPORT job (scheduled, run as DR service principal)                           │
  │                                                                                │
  │   system.access.audit ──(1 detect)──► changed scopes/keys since last export    │
  │                          + full state-diff recon (safety net)                  │
  │   secrets.get_secret ───(2 read)────► plaintext value  ──► sha256 (rotation)   │
  │   KMS GenerateDataKey ──(3 encrypt)─► AES-256-GCM(value), AAD = {scope,key}     │
  │   (4 snapshot) full desired state ──► s3://dr-secrets-us-east-2-…/secrets/<id>/ │
  │                                        bundle.json  (+ _latest.txt)  SSE-KMS    │
  └────────────────────────────────────────────────────────────────────────────┬─┘
                                                                     S3 CRR (bidir) │
  ┌────────────────────────────── WEST (us-west-2) ───────────────────────────────▼┐
  │  s3://dr-secrets-us-west-2-…/secrets/<id>/bundle.json   ← encrypted replica     │
  │  (no compute here in steady state)                                              │
  └─────────────────────────────────────────────────────────────────────────────┘

                         ON FAILOVER (WEST promoted, writable)
  ┌────────────────────────────── WEST (us-west-2) ────────────────────────────────┐
  │  IMPORT job (destination-aware reconcile)                                       │
  │   read _latest bundle from LOCAL bucket ─► decrypt on demand (KMS)              │
  │   read WEST live secrets ─► diff vs bundle by value_hash / ACL signature        │
  │   apply ONLY the delta:  create_scope · put_secret · put_acl · delete_secret    │
  └─────────────────────────────────────────────────────────────────────────────┘
```

**Why a full snapshot, not a delta stream?** So `_latest.txt` always points at one
object that fully describes the desired state. A cold WEST can be rebuilt from it in one
pass, and the importer can detect deletions (a key present in WEST but absent from the
snapshot) without replaying history.

**Why diff on import instead of blindly applying?** It makes import **idempotent** and
**incremental**: unchanged secrets aren't even decrypted or rewritten, only drift is
applied, and re-running is a no-op. It also catches drift on the WEST side.

---

## 3. The environment (concrete)

**Workspaces**

| Role | Region | Host | CLI profile |
|---|---|---|---|
| Primary (active) | us-east-2 | `https://fe-sandbox-fe-sandbox-ps-dr-wp-us-east-2.cloud.databricks.com` | `dr-east` |
| Secondary (passive) | us-west-2 | `https://fe-sandbox-fe-sandbox-ps-dr-wp-us-west-2.cloud.databricks.com` | `dr-west` |

**AWS assets** (account `332745928618`, created by `scripts/provision_secrets_dr_aws.sh`)

| Asset | East (us-east-2) | West (us-west-2) |
|---|---|---|
| S3 bucket | `dr-secrets-us-east-2-332745928618` | `dr-secrets-us-west-2-332745928618` |
| KMS alias | `alias/dr-secrets-us-east-2` | `alias/dr-secrets-us-west-2` |
| CRR rule | `dr-secrets-crr-east-to-west` | `dr-secrets-crr-west-to-east` |

> **KMS must be a Multi-Region Key.** Both region aliases resolve to one MRK (primary in
> east, replica in west) so the promoted secondary can decrypt the envelope data key
> **locally** — even if the primary region is down. Independent regional keys do **not**
> work for cross-region envelope decryption (see the test report, Finding 1).

IAM roles (for running on Databricks compute later): `dr-secrets-uc-role` (serverless/UC),
`dr-secrets-ec2-role` (+ instance profile `dr-secrets-instance-profile`, classic clusters),
`dr-secrets-crr-role` (S3 replication). Buckets are versioned, SSE-KMS, public access
blocked. CRR filter prefix = `secrets/`, delete-marker + KMS-object replication ON.

> **Verified end-to-end on 2026-09-01** — replicate east→west, incremental diff-and-apply,
> and failback west→east all passed against these live workspaces. See
> [secrets_dr_test_report.md](secrets_dr_test_report.md) for the run log, observations, and findings.

**Control tables** (per workspace, `dr_poc.dr_control`): `dr_secrets_inventory`
(per-secret desired state), `dr_secrets_audit` (operation history), and views
`v_dr_secrets_watermark`, `v_dr_secrets_failures`.

---

## 4. One-time setup

### 4.1 AWS assets

```bash
# Requires valid AWS creds (aws sts get-caller-identity should show account 332745928618).
bash scripts/provision_secrets_dr_aws.sh
```

**✅ Check** — assets exist and CRR is active:

```bash
aws s3api get-bucket-versioning  --bucket dr-secrets-us-east-2-332745928618 --region us-east-2   # Status: Enabled
aws s3api get-bucket-replication --bucket dr-secrets-us-east-2-332745928618 --region us-east-2 \
  --query 'ReplicationConfiguration.Rules[].{id:ID,status:Status,dest:Destination.Bucket}'        # rule Enabled -> west
aws s3api get-bucket-replication --bucket dr-secrets-us-west-2-332745928618 --region us-west-2 \
  --query 'ReplicationConfiguration.Rules[].{id:ID,status:Status,dest:Destination.Bucket}'        # rule Enabled -> east
aws kms describe-key --key-id alias/dr-secrets-us-east-2 --region us-east-2 --query KeyMetadata.Arn
```

### 4.2 Authenticate to both workspaces

```bash
databricks auth login https://fe-sandbox-fe-sandbox-ps-dr-wp-us-east-2.cloud.databricks.com --profile dr-east
databricks auth login https://fe-sandbox-fe-sandbox-ps-dr-wp-us-west-2.cloud.databricks.com --profile dr-west
```

**✅ Check:** `databricks current-user me --profile dr-east` and `--profile dr-west` both
return your user. (Optional: put each workspace id into `config/secrets_dr_config.yaml`
under `workspaces.*.workspace_id` — the export auto-derives it if left blank.)

### 4.3 Control tables — run in BOTH workspaces

Import the repo as a Databricks Git folder in each workspace, then run
`notebooks/secrets/00_setup_secrets.py` in **EAST** and **WEST**. If a workspace's metastore
has no managed storage for a fresh `dr_poc` catalog, set the notebook's **`catalog`** widget
to a catalog that already has managed storage (e.g. that workspace's default catalog) and add
the same under `control.catalog_by_workspace` in the config (west is configured this way).

**✅ Check** (either workspace):
```sql
SHOW TABLES IN dr_poc.dr_control;            -- dr_secrets_inventory, dr_secrets_audit
SELECT * FROM dr_poc.dr_control.dr_secrets_audit;   -- empty (no ops yet)
```

### 4.4 (POC only) Seed sample secrets in the PRIMARY

Run `notebooks/secrets/01_seed_secrets.py` in **EAST**. It creates the scopes/keys/ACLs
from the `seed` section of the config with random values. *Skip in production — your real
scopes already exist.*

**✅ Check** (EAST):
```bash
databricks secrets list-scopes --profile dr-east                 # dr_app_prod, dr_app_analytics
databricks secrets list-secrets dr_app_prod --profile dr-east    # db_password, api_token, service_url
databricks secrets list-acls dr_app_prod --profile dr-east
```

---

## 5. Test procedure (end-to-end)

Each step lists the action and **exactly what to verify**. Fill in the blank check-boxes
with anything else you want to confirm for your environment.

### Step 1 — Baseline export (EAST → S3 → CRR → WEST bucket)

Run `notebooks/secrets/10_export_secrets.py` in **EAST** with `full = true` (first run),
or locally: `python -m databricks_dr.modules.secrets.runner export --full`.

**What happens:** all in-scope secrets are read, hashed, envelope-encrypted, and written
as one snapshot bundle to the EAST bucket; `_latest.txt` is advanced. CRR copies it west.

**✅ Check — the run summary** prints `exported = <#keys>`, `deleted = 0`, a `bundle_id`.

**✅ Check — bundle in the EAST bucket:**
```bash
aws s3 cp s3://dr-secrets-us-east-2-332745928618/secrets/_latest.txt - --region us-east-2   # a bundle id
BID=$(aws s3 cp s3://dr-secrets-us-east-2-332745928618/secrets/_latest.txt - --region us-east-2)
aws s3 cp s3://dr-secrets-us-east-2-332745928618/secrets/$BID/bundle.json - --region us-east-2 | python -m json.tool | head -40
```
Confirm: `"snapshot": "full"`, `"encryption": "AES256-GCM"`, each item has `edk`/`nonce`/`ct`
and a `value_hash`, and **no plaintext appears anywhere** in the JSON.

**✅ Check — CRR replicated it to WEST** (may take seconds–minutes):
```bash
aws s3 ls s3://dr-secrets-us-west-2-332745928618/secrets/$BID/ --region us-west-2   # bundle.json present
aws s3api head-object --bucket dr-secrets-us-east-2-332745928618 --key secrets/$BID/bundle.json \
  --region us-east-2 --query ReplicationStatus                                       # COMPLETED
```

**✅ Check — control tables (EAST):**
```sql
SELECT operation, status, item_count, bundle_id, duration_sec FROM dr_poc.dr_control.dr_secrets_audit
 ORDER BY event_time DESC LIMIT 1;                              -- EXPORT SUCCESS, item_count>0
SELECT scope, secret_key, status, value_hash FROM dr_poc.dr_control.dr_secrets_inventory ORDER BY scope, secret_key;
```

- [ ] _your extra check:_ ______________________________________________

### Step 2 — Failover import (WEST promoted)

> In this sandbox both workspaces are independently writable, so you can import into WEST
> directly. With a true read-only Managed-DR standby you would promote WEST first; the
> importer's **write preflight** will otherwise fail fast with a clear message.

Run `notebooks/secrets/20_import_secrets.py` in **WEST** with `region = secondary`, or
locally: `python -m databricks_dr.modules.secrets.runner import --region secondary`.

**What happens:** WEST reads `_latest` from its **local** bucket, reads its own (empty)
live secrets, diffs, and applies everything as ADD.

**✅ Check — the run summary:** `added = <#keys>`, `updated = 0`, `deleted = 0`,
`skipped = 0` on the first import.

**✅ Check — secrets now exist in WEST:**
```bash
databricks secrets list-scopes --profile dr-west                 # same scopes as EAST
databricks secrets list-secrets dr_app_prod --profile dr-west    # same keys as EAST
databricks secrets list-acls dr_app_prod --profile dr-west       # ACLs match EAST
```

**✅ Check — values actually match** (compare hashes, not plaintext). In each workspace:
```python
import base64, hashlib
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(profile="dr-east")   # then repeat with dr-west
v = w.secrets.get_secret(scope="dr_app_prod", key="db_password").value
print("sha256:", hashlib.sha256(base64.b64decode(v)).hexdigest())
```
The EAST and WEST hashes for the same key must be identical.

**✅ Check — control tables (WEST):** an `IMPORT SUCCESS` row; `dr_secrets_inventory`
rows `IN_SYNC`.

**✅ Check — idempotency:** run the import **again**. Summary should be
`in_sync = true`, `added/updated/deleted = 0`, `skipped = <#keys>` (nothing rewritten).

- [ ] _your extra check:_ ______________________________________________

### Step 3 — Incremental change (rotation)

In **EAST**, rotate one secret:
```bash
databricks secrets put-secret dr_app_prod db_password --string-value "rotated-$(date +%s)" --profile dr-east
```
Re-run **export** (EAST, `full = false`), then **import** (WEST).

**✅ Check — export:** a new `bundle_id`; the rotated key's `value_hash` in
`dr_secrets_inventory` changed. (Because a change was detected, the snapshot is refreshed;
`_latest.txt` now points at the new bundle.)

**✅ Check — CRR:** the new bundle appears in the WEST bucket.

**✅ Check — import:** summary shows `updated = 1`, `skipped = <#unchanged>`,
`added = 0`, `deleted = 0`. The WEST hash for `db_password` now matches the new EAST hash.

- [ ] _your extra check:_ ______________________________________________

### Step 4 — Delete (mirror semantics)

`reconcile.mode: mirror` (default) means WEST is kept identical to EAST. In **EAST**:
```bash
databricks secrets delete-secret dr_app_prod service_url --profile dr-east
```
Re-run **export**, then **import**.

**✅ Check — export:** the deleted key appears in the bundle's `deletes[]`; its inventory
row is `DELETED`.

**✅ Check — import:** summary shows `deleted = 1`; the key is gone from WEST:
```bash
databricks secrets list-secrets dr_app_prod --profile dr-west   # service_url no longer listed
```
> If you set `reconcile.mode: additive`, deletes are **not** propagated — WEST keeps the
> key. Use `mirror` for a true replica.

- [ ] _your extra check:_ ______________________________________________

### Step 5 — Failback (symmetric, WEST → EAST)

Failback is the same flow with roles swapped: run **export** in WEST and **import** with
`region = primary` in EAST. (In the bidirectional CRR setup, a bundle written to the WEST
bucket replicates back to EAST.) Verify EAST converges to WEST's state with the same
checks as Steps 2–4.

- [ ] _your extra check:_ ______________________________________________

---

## 6. What "good" looks like (acceptance checklist)

- [ ] Both buckets: versioning **Enabled**, SSE-KMS default, public access **Blocked**.
- [ ] Both CRR rules **Enabled**; `head-object … ReplicationStatus` is `COMPLETED`.
- [ ] Bundle JSON contains **no plaintext** — only `edk`/`nonce`/`ct` + `value_hash`.
- [ ] After import, EAST and WEST return **identical sha256** for every in-scope key.
- [ ] ACLs match between EAST and WEST.
- [ ] Re-running import is a **no-op** (`in_sync = true`).
- [ ] Rotation propagates as `updated = 1`; unchanged keys are `skipped`.
- [ ] Delete propagates (mirror) / is withheld (additive), per config.
- [ ] `dr_secrets_audit` has EXPORT + IMPORT `SUCCESS` rows; `v_dr_secrets_failures` empty.

---

## 7. RPO / RTO

- **RPO** (data-loss window) = export cadence + CRR lag. With the export scheduled every
  15 min and CRR typically seconds, RPO ≈ minutes. Tune `export_schedule_cron`.
- **RTO** (time to recover) = time to promote WEST + run one import. The import is
  incremental and small, so RTO is minutes. Query recovery point:
  ```sql
  SELECT * FROM dr_poc.dr_control.v_dr_secrets_watermark;   -- last_export / last_import per scope
  ```

---

## 8. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Import raises *"target is not writable — promote the workspace"* | WEST is a read-only standby | Promote WEST (make it writable) before importing. Expected on a true Managed-DR standby. |
| `get_secret` fails on export | DR principal lacks **READ** on the scope | Grant the export identity READ ACL on in-scope scopes. |
| KMS `AccessDenied` / *"resource does not exist in this Region"* on decrypt | KMS key is **not** multi-region (data key wrapped by a regional key that the other region can't decrypt) | Use a **Multi-Region Key** (the provisioner creates one). Both aliases must resolve to the same MRK. |
| KMS `AccessDenied` on encrypt/decrypt | Key policy / role missing KMS grant | Confirm the running identity can `GenerateDataKey`/`Decrypt` on the region's `alias/dr-secrets-*`. |
| CRR stuck at `ReplicationStatus=FAILED` | `dr-secrets-crr-role` / rule `ReplicaKmsKeyID` don't reference the (MR) key | Grant the CRR role KMS on the MRK and set each rule's `ReplicaKmsKeyID` to it (Finding 2). |
| Control-table setup "succeeds" but tables are empty / `EXTERNAL_LOCATION_DOES_NOT_EXIST` | The target metastore has no valid managed storage for a fresh catalog | Set `control.catalog_by_workspace.<workspace>` to a catalog that has managed storage (e.g. the workspace default), or create `dr_poc` with an explicit `MANAGED LOCATION` (Finding 7). The DR data path is unaffected. |
| Bundle not in WEST bucket | CRR lag or objects predate the rule | Wait; check `head-object … ReplicationStatus`. CRR only replicates objects written **after** the rule was enabled. |
| `decrypt` fails with an auth/tag error | EncryptionContext mismatch | `{scope,key}` must match encrypt-time exactly; don't hand-edit bundles or move ciphertext between keys. |
| Export writes nothing, says "no changes" | Nothing changed since last watermark | Expected. Force a full snapshot with `--full` / `full=true`. |
| Auth error: *"stored credentials from older CLI versions…"* | Stale OAuth cache | Re-run `databricks auth login … --profile …` (see §4.2). |

---

## 9. Security notes

- **Client-side envelope encryption.** Each value is encrypted with a one-time KMS data
  key (AES-256-GCM). Only the KMS-wrapped data key + ciphertext are stored, so **plaintext
  never lands in S3**. `{scope,key}` is bound as AES-GCM associated data **and** the KMS
  EncryptionContext, so a ciphertext can't be silently relocated to a different key.
- **Defense in depth.** Bundles are also written SSE-KMS at rest; buckets block public
  access and are versioned. CRR re-encrypts at the destination with the destination-region
  key.
- **Least privilege.** The DR identity needs READ on in-scope scopes in the primary and
  MANAGE (write) in the promoted secondary; `provision_secrets_dr_aws.sh` scopes the IAM
  policies to just these buckets and KMS aliases.
- **Never commit** real secret values, `.env`, or AWS session tokens.

---

## 10. Command reference

```bash
# Local run (auth via profile in config; AWS via boto3 chain)
python -m databricks_dr.modules.secrets.runner export            # incremental snapshot
python -m databricks_dr.modules.secrets.runner export --full     # force full snapshot
python -m databricks_dr.modules.secrets.runner import --region secondary   # failover import
python -m databricks_dr.modules.secrets.runner import --region primary     # failback import

# Asset Bundle (scheduled export in primary, failover import in secondary)
databricks bundle deploy -t east
databricks bundle deploy -t west
databricks bundle run dr_secrets_export          -t east
databricks bundle run dr_secrets_failover_import -t west
databricks bundle deploy -t east --var export_pause_status=UNPAUSED   # go live
```
