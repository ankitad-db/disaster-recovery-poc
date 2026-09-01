# Workspace Secrets DR — Runbook & Test Playbook

Direct cross-workspace replication (no S3 / CRR / KMS). The mental model, the concrete
environment, a step-by-step test procedure with **what to check at every stage**, and
troubleshooting. Work top-to-bottom the first time; later, jump to the section you need.

> Convention: **EAST = primary (active)**, **WEST = secondary (passive standby)**.
> Direction is symmetric — for failback, swap the roles.

---

## 1. What this is and why it exists

Databricks **Managed DR** does not cover **workspace secrets** (secret scopes, secret
values, ACLs). If EAST is lost, jobs in the promoted WEST workspace that read
`dbutils.secrets.get(...)` break until those secrets exist there. This keeps WEST a
**warm mirror** of EAST's secrets so it can serve immediately on failover.

**Design principles:**

| Principle | How it shows up here |
|---|---|
| **Active–passive** | EAST serves; WEST is a passive standby. |
| **No compute on the passive side** | The `replicate` job runs only in the active workspace; it reads/writes WEST over the Secrets API (control plane) — no cluster on WEST. |
| **Direct & simple** | Values move EAST→WEST over TLS via the Secrets API. No object storage, no AWS, no envelope crypto to manage. Both secret stores are encrypted at rest by the platform. |
| **Idempotent, diff-driven** | Each run diffs source vs destination and applies **only** the delta. Safe to re-run. |
| **Symmetric** | Failover/failback is the same code with the roles swapped. |

**Trade-off to know:** there is no independent, versioned, offsite backup of the secrets —
the only copies live in the two workspaces' secret managers. For warm-standby DR that's
standard; if you also need an offline encrypted archive, that's a separate concern.

---

## 2. The mental model (what actually happens)

```
                         STEADY STATE (EAST active)
  ┌──────────────────────────── EAST (us-east-2) ─────────────────────────────┐
  │  replicate job (run as DR service principal)                              │
  │   read EAST secrets      get_secret -> sha256 ; list_acls                  │
  │   read WEST secrets      (cross-workspace Secrets API — no WEST compute)   │
  │   diff by value-hash + ACL signature -> ADD / UPDATE / DELETE / unchanged  │
  │   apply ONLY the delta into WEST:                                          │
  │      create_scope · put_secret · put_acl · delete_secret · delete_acl ─────┼──► WEST
  └───────────────────────────────────────────────────────────────────────────┘   (warm mirror)

                         ON FAILOVER
  Promote WEST (it's already a warm mirror — nothing to import). Repoint consumers.
  Failback later = run `replicate` WEST -> EAST.
```

**Why diff both sides instead of blindly pushing?** It makes replication **idempotent** and
**incremental** — unchanged secrets aren't rewritten, only drift is applied, and re-running
is a no-op. It also catches drift on the WEST side.

---

## 3. The environment (concrete)

| Role | Region | Host | CLI profile | SQL warehouse |
|---|---|---|---|---|
| Primary (active) | us-east-2 | `…-ps-dr-wp-us-east-2` (id 7474657900994867) | `dr-east` | `63af3d742ebd95ab` |
| Secondary (passive) | us-west-2 | `…-ps-dr-wp-us-west-2` (id 7474651252878032) | `dr-west` | `4ca723533b34106a` |

**Control tables** (per workspace): `<catalog>.dr_control.dr_secrets_{inventory,audit}` +
views. East uses catalog `dr_poc`; **west uses its default catalog**
`fe_sandbox_ps_dr_wp_us_west_2_catalog` (via `control.catalog_by_workspace`) because a fresh
`dr_poc` catalog there has no managed storage.

> **Verified end-to-end on 2026-09-02** — replicate east→west, incremental diff-and-apply,
> and failback west→east all passed. See
> [secrets_dr_test_report.md](secrets_dr_test_report.md) for the run log and findings.

---

## 4. One-time setup

### 4.1 Authenticate to both workspaces

```bash
databricks auth login --host https://fe-sandbox-fe-sandbox-ps-dr-wp-us-east-2.cloud.databricks.com --profile dr-east
databricks auth login --host https://fe-sandbox-fe-sandbox-ps-dr-wp-us-west-2.cloud.databricks.com --profile dr-west
```
**✅ Check:** `databricks current-user me --profile dr-east` and `--profile dr-west` both return you.

### 4.2 Control tables — run in BOTH workspaces

Run `notebooks/secrets/00_setup_secrets.py` in **EAST** and **WEST**. On a workspace whose
metastore lacks managed storage for a fresh `dr_poc` catalog, set the notebook's **`catalog`**
widget to a catalog that has managed storage (e.g. the workspace default) and mirror it under
`control.catalog_by_workspace` in the config (west is configured this way).

**✅ Check** (either workspace): `SHOW TABLES IN <catalog>.dr_control;` lists
`dr_secrets_inventory`, `dr_secrets_audit`.

### 4.3 (POC only) Seed sample secrets in the PRIMARY

Run `notebooks/secrets/01_seed_secrets.py` in **EAST**. *Skip in production — real scopes exist.*

**✅ Check** (EAST): `databricks secrets list-scopes --profile dr-east` → `dr_app_prod`, `dr_app_analytics`.

### 4.4 For the job/notebook path — a peer token

`10_replicate_secrets.py` reaches the peer workspace with a PAT held in a local secret scope
(default `dr_peer`/`token`). Create it in the active workspace:
```bash
databricks secrets create-scope dr_peer --profile dr-east
databricks secrets put-secret  dr_peer token --string-value "<west-PAT>" --profile dr-east
```
(For local CLI runs this isn't needed — the `dr-west` profile is used directly.)

---

## 5. Test procedure (end-to-end)

Each step lists the action and **what to verify**. Fill the blank check-boxes with your own.

### Step 1 — Replicate primary → secondary

`python -m databricks_dr.modules.secrets.runner replicate` (or `10_replicate_secrets.py`,
`direction=forward`).

**✅ Check — summary:** `added`/`updated` cover all in-scope keys, `deleted=0` on a fresh WEST.
**✅ Check — secrets now in WEST:** `databricks secrets list-secrets dr_app_prod --profile dr-west`.
**✅ Check — values match (hashes, not plaintext):** in each workspace,
```python
import base64, hashlib
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(profile="dr-east")   # then dr-west
v = w.secrets.get_secret(scope="dr_app_prod", key="db_password").value
print(hashlib.sha256(base64.b64decode(v)).hexdigest())
```
EAST and WEST hashes for the same key must be identical.
**✅ Check — audit (EAST):** a `REPLICATE SUCCESS` row in `<dr_poc>.dr_control.dr_secrets_audit`.

- [ ] _your extra check:_ ______________________________________________

### Step 2 — Idempotency

Run `replicate` again with no changes. **✅** Summary `in_sync=True`, `added/updated/deleted=0`,
`skipped=<#keys>` — nothing rewritten.

### Step 3 — Incremental (rotate + delete)

In EAST: `databricks secrets put-secret dr_app_prod db_password --string-value new --profile dr-east`
and `databricks secrets delete-secret dr_app_prod service_url --profile dr-east`. Re-run `replicate`.

**✅** Summary `updated=1, deleted=1, skipped=<rest>` (mirror mode); WEST hashes match EAST;
`service_url` is gone from WEST. *(With `reconcile.mode: additive`, deletes are not propagated.)*

- [ ] _your extra check:_ ______________________________________________

### Step 4 — Failback (symmetric)

Change a secret in WEST, then `python -m databricks_dr.modules.secrets.runner failback`
(WEST→EAST). **✅** EAST converges to WEST's value; a `REPLICATE` row appears in WEST's audit
table (direction `us-west-2->us-east-2`).

- [ ] _your extra check:_ ______________________________________________

---

## 6. What "good" looks like (acceptance checklist)

- [ ] After replicate, EAST and WEST return **identical sha256** for every in-scope key.
- [ ] ACLs match between EAST and WEST (`acls` applied; verified in the test).
- [ ] Re-running replicate is a **no-op** (`in_sync=True`).
- [ ] Rotation propagates as `updated`; unchanged keys are `skipped`.
- [ ] Delete propagates (mirror) / is withheld (additive), per config.
- [ ] Failback converges EAST to WEST and records a reverse-direction audit row.
- [ ] `dr_secrets_audit` has `REPLICATE SUCCESS` rows in both workspaces.

---

## 7. RPO / RTO

- **RPO** = replicate cadence (there is no async storage tail — the push is synchronous).
  With the job scheduled every 15 min, RPO ≈ 15 min. Tune `replicate_schedule_cron`.
- **RTO** = time to promote WEST + repoint consumers. WEST is already a warm mirror, so there
  is **no import step** — RTO is essentially the promotion itself.

---

## 8. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| *"destination … is not writable — promote it"* | Writing into a read-only/passive standby | Promote/failover the destination first. Expected on a true read-only Managed-DR standby. |
| `get_secret` fails / value unreadable | DR identity lacks **READ** on the scope | Grant the replicate identity READ ACL on in-scope scopes in both workspaces. |
| ACL not applied on the destination | The ACL **principal** doesn't exist in the destination workspace | Ensure users/groups/SPNs are federated/present in both workspaces; we log and continue. |
| Control-table setup / `EXTERNAL_LOCATION_DOES_NOT_EXIST` | Metastore has no managed storage for a fresh catalog | Set `control.catalog_by_workspace.<workspace>` to a catalog with managed storage (e.g. the default). DR data path is unaffected. |
| Peer auth fails in the notebook | Missing/invalid peer PAT | Check the `dr_peer`/`token` secret scope in the active workspace (§4.4). |

---

## 9. Security notes

- **In transit:** values move over the Secrets API (**TLS**). **At rest:** both workspaces'
  secret stores are encrypted by the platform. No plaintext is written to files or object storage.
- **Least privilege:** the DR identity needs READ on in-scope scopes in the source and MANAGE
  (write) in the destination.
- **No offsite backup:** copies exist only in the two workspaces' secret managers (see §1 trade-off).
- **Never commit** real secret values or PATs.

---

## 10. Command reference

```bash
python -m databricks_dr.modules.secrets.runner replicate            # primary -> secondary
python -m databricks_dr.modules.secrets.runner replicate --source secondary --dest primary
python -m databricks_dr.modules.secrets.runner failback             # secondary -> primary

# Asset Bundle (scheduled replicate in the active workspace)
databricks bundle deploy -t east
databricks bundle run dr_secrets_replicate -t east
databricks bundle deploy -t east --var replicate_pause_status=UNPAUSED   # go live
```
