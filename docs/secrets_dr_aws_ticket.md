# [Ticket] Provision AWS assets for Workspace Secrets DR

## Summary
Create the S3 + KMS + IAM + bidirectional CRR assets listed below. All names,
regions, and policies are fully specified 

## Details
- **AWS account:** `997819012307`
- **Regions:** `us-east-2` (primary), `us-west-2` (secondary)
- **Databricks workspaces the roles are used from:**
  - Primary (us-east-2): `fevm-fevm-ps-dr-us-east-2` — `https://fevm-fevm-ps-dr-us-east-2.cloud.databricks.com` (workspace id `7474660494970929`)
  - Secondary (us-west-2): `fevm-krish-dr-fevm-us-west-2` — `https://fevm-krish-dr-fevm-us-west-2.cloud.databricks.com` (workspace id `7474658261824919`)

---

## Step 1 — Create 2 S3 buckets

| Bucket name | Region | Versioning | Default encryption | Public access |
|-------------|--------|------------|--------------------|---------------|
| `dr-secrets-us-east-2-997819012307` | us-east-2 | **Enabled** | SSE-KMS → `alias/dr-secrets-us-east-2` | Block ALL |
| `dr-secrets-us-west-2-997819012307` | us-west-2 | **Enabled** | SSE-KMS → `alias/dr-secrets-us-west-2` | Block ALL |

Only the roles created in Steps 3 and 4 may access these buckets.

## Step 2 — Create 2 KMS customer-managed keys

| Alias | Region |
|-------|--------|
| `alias/dr-secrets-us-east-2` | us-east-2 |
| `alias/dr-secrets-us-west-2` | us-west-2 |

Each key's policy must allow the roles `dr-secrets-uc-role`, `dr-secrets-ec2-role`,
and `dr-secrets-crr-role` (Steps 3–4) to `Encrypt`, `Decrypt`, `GenerateDataKey`,
`DescribeKey`.

## Step 3 — Create 2 IAM roles for Databricks compute (create BOTH)

**3a. Role `dr-secrets-uc-role`** (serverless / Unity Catalog credential). Trust
policy below — the `sts:ExternalId` value will be provided separately:

```json
{ "Version": "2012-10-17", "Statement": [
  { "Effect": "Allow",
    "Principal": { "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL" },
    "Action": "sts:AssumeRole",
    "Condition": { "StringEquals": { "sts:ExternalId": "<PROVIDED-SEPARATELY>" } } },
  { "Effect": "Allow",
    "Principal": { "AWS": "arn:aws:iam::997819012307:role/dr-secrets-uc-role" },
    "Action": "sts:AssumeRole" } ] }
```

**3b. Role `dr-secrets-ec2-role`** + instance profile `dr-secrets-instance-profile`
(classic clusters). Trust policy:

```json
{ "Version": "2012-10-17", "Statement": [
  { "Effect": "Allow", "Principal": { "Service": "ec2.amazonaws.com" }, "Action": "sts:AssumeRole" } ] }
```

**Attach this same policy `dr-secrets-access-policy` to BOTH roles (3a and 3b):**

```json
{ "Version": "2012-10-17", "Statement": [
  { "Sid": "S3", "Effect": "Allow",
    "Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:GetObjectVersion",
               "s3:ListBucket","s3:GetBucketLocation"],
    "Resource": [
      "arn:aws:s3:::dr-secrets-us-east-2-997819012307",
      "arn:aws:s3:::dr-secrets-us-east-2-997819012307/*",
      "arn:aws:s3:::dr-secrets-us-west-2-997819012307",
      "arn:aws:s3:::dr-secrets-us-west-2-997819012307/*" ] },
  { "Sid": "KMS", "Effect": "Allow",
    "Action": ["kms:Encrypt","kms:Decrypt","kms:GenerateDataKey","kms:DescribeKey"],
    "Resource": [
      "arn:aws:kms:us-east-2:997819012307:key/*",
      "arn:aws:kms:us-west-2:997819012307:key/*" ],
    "Condition": { "StringEquals": { "kms:ResourceAliases": [
      "alias/dr-secrets-us-east-2","alias/dr-secrets-us-west-2" ] } } } ] }
```

## Step 4 — Create CRR role + enable bidirectional replication

**Role `dr-secrets-crr-role`**, trust `s3.amazonaws.com`:

```json
{ "Version": "2012-10-17", "Statement": [
  { "Effect": "Allow", "Principal": { "Service": "s3.amazonaws.com" }, "Action": "sts:AssumeRole" } ] }
```

**Policy `dr-secrets-crr-policy`** (attach to `dr-secrets-crr-role`):

```json
{ "Version": "2012-10-17", "Statement": [
  { "Sid": "SourceRead", "Effect": "Allow",
    "Action": ["s3:GetReplicationConfiguration","s3:ListBucket",
               "s3:GetObjectVersionForReplication","s3:GetObjectVersionAcl","s3:GetObjectVersionTagging"],
    "Resource": [
      "arn:aws:s3:::dr-secrets-us-east-2-997819012307","arn:aws:s3:::dr-secrets-us-east-2-997819012307/*",
      "arn:aws:s3:::dr-secrets-us-west-2-997819012307","arn:aws:s3:::dr-secrets-us-west-2-997819012307/*" ] },
  { "Sid": "DestWrite", "Effect": "Allow",
    "Action": ["s3:ReplicateObject","s3:ReplicateDelete","s3:ReplicateTags"],
    "Resource": [
      "arn:aws:s3:::dr-secrets-us-east-2-997819012307/*","arn:aws:s3:::dr-secrets-us-west-2-997819012307/*" ] },
  { "Sid": "KMS", "Effect": "Allow",
    "Action": ["kms:Decrypt","kms:Encrypt","kms:GenerateDataKey"],
    "Resource": ["arn:aws:kms:us-east-2:997819012307:key/*","arn:aws:kms:us-west-2:997819012307:key/*"],
    "Condition": { "StringEquals": { "kms:ResourceAliases": [
      "alias/dr-secrets-us-east-2","alias/dr-secrets-us-west-2" ] } } } ] }
```

**Replication rules** (filter prefix = `secrets/`, delete-marker replication ON,
"replicate KMS-encrypted objects" ON):

| Rule name | Source bucket | Destination bucket | Destination KMS key |
|-----------|---------------|--------------------|---------------------|
| `dr-secrets-crr-east-to-west` | `dr-secrets-us-east-2-997819012307` | `dr-secrets-us-west-2-997819012307` | `alias/dr-secrets-us-west-2` |
| `dr-secrets-crr-west-to-east` | `dr-secrets-us-west-2-997819012307` | `dr-secrets-us-east-2-997819012307` | `alias/dr-secrets-us-east-2` |

---

## Acceptance criteria
- Both S3 buckets exist with versioning + SSE-KMS default encryption as specified.
- Both KMS CMKs exist with the aliases above and the required key-policy grants.
- Roles `dr-secrets-uc-role`, `dr-secrets-ec2-role` (+ instance profile
  `dr-secrets-instance-profile`), and `dr-secrets-crr-role` exist with the exact
  policies above attached.
- Both CRR rules are active and replicating the `secrets/` prefix bidirectionally
  with KMS re-encryption at the destination.

