# AWS assets request — Workspace Secrets DR

Provision the S3 + KMS + IAM assets that back the workspace-secrets DR flow
(export-in-primary, KMS-envelope-encrypted bundle, bidirectional S3 CRR,
import-on-failover). See `docs/diagrams/secrets_dr_architecture.excalidraw`.

> The architecture is not fully finalised yet, so this request intentionally asks
> for a superset (e.g. BOTH a UC service credential and an instance profile, and
> all four external locations). We will narrow it down once the design locks.

## Context / fixed facts
- **AWS account:** `049629455384`
- **Regions:** `us-east-2` (primary) and `us-west-2` (secondary)
- **Workspaces:** `fevm-fevm-ps-dr-us-east-2` (primary) / `fevm-krish-dr-fevm-us-west-2` (secondary)
- **DR service principal:** `ad-dr-spn` (`dc5781bc-c86f-4808-a00c-930062f91f17`)
- Secret values are envelope-encrypted client-side; the buckets are the encrypted-bundle transport.

## a) Two S3 buckets (one per region)
| Region | Bucket (suggested name) |
|--------|-------------------------|
| us-east-2 | `sec-dr-us-east-2-049629455384` |
| us-west-2 | `sec-dr-us-west-2-049629455384` |

- **Versioning: ENABLED** on both (required for CRR).
- **Default encryption: SSE-KMS** with the region CMK (section b).
- Block public access; bucket policy limited to the DR role(s) + replication role.

## b) KMS customer-managed keys (one per region)
- `alias/dr-secrets-us-east-2`, `alias/dr-secrets-us-west-2`.
- Key policy allows: the DR compute identity (`Encrypt`/`Decrypt`/`GenerateDataKey`)
  and the S3 replication role (`Decrypt` on source key, `Encrypt`/`GenerateDataKey`
  on destination key) so CRR can re-encrypt across regions.

## c) External locations (UC) — request all 4
Register both buckets in both workspaces (superset; strictly only the same-region
pairing is used by the current pull/local-read design, but request all 4 while the
design is open):
- EAST workspace → EL for `s3://sec-dr-us-east-2-049629455384/`
- EAST workspace → EL for `s3://sec-dr-us-west-2-049629455384/`
- WEST workspace → EL for `s3://sec-dr-us-east-2-049629455384/`
- WEST workspace → EL for `s3://sec-dr-us-west-2-049629455384/`

## d) IAM — provision BOTH credential types (classic + serverless)
Compute type isn't finalised, so set up both so either works:
1. **UC service credential** (IAM role registered in Unity Catalog) — for **serverless**
   jobs. Trust policy for the Databricks UC role; used by `boto3` at runtime.
2. **Instance profile** (IAM role + instance profile) — for **classic clusters**.
   Registered as a workspace instance profile.

Both roles need, **in both regions**:
- **S3:** `s3:GetObject`, `PutObject`, `ListBucket`, `DeleteObject`,
  `GetObjectVersion` on `arn:aws:s3:::sec-dr-*-049629455384` and `/*`.
- **KMS:** `kms:Encrypt`, `Decrypt`, `GenerateDataKey`, `DescribeKey` on both CMKs.

(Databricks-side, not IAM: grant `ad-dr-spn` **READ** on the in-scope secret scopes
in the primary so the export can call `get-secret`.)

## e) Bidirectional S3 CRR
- Rules: east bucket **→** west bucket **and** west bucket **→** east bucket, scoped
  to the `secrets/` prefix.
- Enable **SSE-KMS re-encryption** with the destination-region CMK.
- Replication IAM role: `s3:ReplicateObject`, `s3:ReplicateDelete`,
  `s3:GetObjectVersionForReplication`, `s3:ListBucket`, plus `kms:Decrypt` (source
  key) and `kms:Encrypt`/`GenerateDataKey` (destination key).

## After provisioning — update config
`config/secrets_dr_config.yaml`:
- `storage.primary_bucket` / `storage.secondary_bucket` (replace `REPLACE-…`)
- `storage.kms_key.us-east-2` / `storage.kms_key.us-west-2`
