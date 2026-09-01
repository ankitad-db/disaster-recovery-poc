#!/usr/bin/env bash
# Provision AWS assets for Workspace Secrets DR.
#
# Idempotent: safe to re-run. Creates in the caller's AWS account:
#   - 2 S3 buckets (one per region): versioned, SSE-KMS, public access blocked
#   - 2 KMS customer-managed keys (one per region) + aliases
#   - 3 IAM roles (UC serverless credential, EC2 instance profile, S3 CRR) + inline policies
#   - bidirectional S3 CRR on the `secrets/` prefix with KMS re-encryption
#
# Requires: valid AWS credentials in the environment (verify with `aws sts get-caller-identity`).
set -euo pipefail

ACCOUNT_ID="${ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
EAST="${EAST_REGION:-us-east-2}"
WEST="${WEST_REGION:-us-west-2}"

BUCKET_EAST="dr-secrets-${EAST}-${ACCOUNT_ID}"
BUCKET_WEST="dr-secrets-${WEST}-${ACCOUNT_ID}"
ALIAS_EAST="alias/dr-secrets-${EAST}"
ALIAS_WEST="alias/dr-secrets-${WEST}"

UC_ROLE="dr-secrets-uc-role"
EC2_ROLE="dr-secrets-ec2-role"
INSTANCE_PROFILE="dr-secrets-instance-profile"
CRR_ROLE="dr-secrets-crr-role"
UC_MASTER_ROLE_ARN="arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
UC_EXTERNAL_ID="${UC_EXTERNAL_ID:-PLACEHOLDER-UPDATE-AFTER-STORAGE-CREDENTIAL}"

log() { printf '\n=== %s ===\n' "$*"; }

# ---------------------------------------------------------------------------
# KMS: create a CMK + alias in a region if the alias doesn't already resolve.
# Echoes the key ARN.
# ---------------------------------------------------------------------------
ensure_kms() {
  local region="$1" alias="$2"
  local arn
  if arn=$(aws kms describe-key --region "$region" --key-id "$alias" \
              --query KeyMetadata.Arn --output text 2>/dev/null); then
    echo "$arn"; return 0
  fi
  local key_id
  key_id=$(aws kms create-key --region "$region" \
             --description "Workspace Secrets DR ($region)" \
             --tags TagKey=project,TagValue=secrets-dr \
             --query KeyMetadata.KeyId --output text)
  aws kms create-alias --region "$region" --alias-name "$alias" --target-key-id "$key_id"
  aws kms describe-key --region "$region" --key-id "$key_id" --query KeyMetadata.Arn --output text
}

# ---------------------------------------------------------------------------
# IAM role helpers (idempotent).
# ---------------------------------------------------------------------------
ensure_role() {
  local name="$1" trust_file="$2"
  if aws iam get-role --role-name "$name" >/dev/null 2>&1; then
    aws iam update-assume-role-policy --role-name "$name" --policy-document "file://$trust_file"
  else
    aws iam create-role --role-name "$name" --assume-role-policy-document "file://$trust_file" \
      --description "Workspace Secrets DR" >/dev/null
  fi
}

main() {
  local TMP; TMP=$(mktemp -d)
  trap 'rm -rf "${TMP:-}"' EXIT
  log "Account $ACCOUNT_ID | east=$EAST west=$WEST"

  # ---- KMS keys first (needed for bucket encryption + policies) ----
  log "KMS keys"
  local KMS_EAST_ARN KMS_WEST_ARN
  KMS_EAST_ARN=$(ensure_kms "$EAST" "$ALIAS_EAST"); echo "east key: $KMS_EAST_ARN"
  KMS_WEST_ARN=$(ensure_kms "$WEST" "$ALIAS_WEST"); echo "west key: $KMS_WEST_ARN"

  # ---- IAM roles ----
  log "IAM roles"
  # UC self-referential trust is a two-phase problem: the role cannot list its own
  # ARN as a principal until it exists. Create with a bootstrap trust (UC master
  # only), then add the self-assume statement after the role is created.
  cat >"$TMP/trust-uc-bootstrap.json" <<JSON
{ "Version": "2012-10-17", "Statement": [
  { "Effect": "Allow",
    "Principal": { "AWS": "$UC_MASTER_ROLE_ARN" },
    "Action": "sts:AssumeRole",
    "Condition": { "StringEquals": { "sts:ExternalId": "$UC_EXTERNAL_ID" } } } ] }
JSON
  cat >"$TMP/trust-uc.json" <<JSON
{ "Version": "2012-10-17", "Statement": [
  { "Effect": "Allow",
    "Principal": { "AWS": "$UC_MASTER_ROLE_ARN" },
    "Action": "sts:AssumeRole",
    "Condition": { "StringEquals": { "sts:ExternalId": "$UC_EXTERNAL_ID" } } },
  { "Effect": "Allow",
    "Principal": { "AWS": "arn:aws:iam::${ACCOUNT_ID}:role/${UC_ROLE}" },
    "Action": "sts:AssumeRole" } ] }
JSON
  cat >"$TMP/trust-ec2.json" <<'JSON'
{ "Version": "2012-10-17", "Statement": [
  { "Effect": "Allow", "Principal": { "Service": "ec2.amazonaws.com" }, "Action": "sts:AssumeRole" } ] }
JSON
  cat >"$TMP/trust-crr.json" <<'JSON'
{ "Version": "2012-10-17", "Statement": [
  { "Effect": "Allow", "Principal": { "Service": "s3.amazonaws.com" }, "Action": "sts:AssumeRole" } ] }
JSON
  ensure_role "$UC_ROLE"  "$TMP/trust-uc-bootstrap.json"
  ensure_role "$EC2_ROLE" "$TMP/trust-ec2.json"
  ensure_role "$CRR_ROLE" "$TMP/trust-crr.json"
  # phase 2: role now exists -> add the self-assume statement
  aws iam update-assume-role-policy --role-name "$UC_ROLE" --policy-document "file://$TMP/trust-uc.json"

  # instance profile for the classic-cluster role
  if ! aws iam get-instance-profile --instance-profile-name "$INSTANCE_PROFILE" >/dev/null 2>&1; then
    aws iam create-instance-profile --instance-profile-name "$INSTANCE_PROFILE" >/dev/null
  fi
  if ! aws iam get-instance-profile --instance-profile-name "$INSTANCE_PROFILE" \
        --query 'InstanceProfile.Roles[].RoleName' --output text | grep -qw "$EC2_ROLE"; then
    aws iam add-role-to-instance-profile --instance-profile-name "$INSTANCE_PROFILE" --role-name "$EC2_ROLE"
  fi

  # ---- inline policies ----
  log "IAM inline policies"
  cat >"$TMP/access-policy.json" <<JSON
{ "Version": "2012-10-17", "Statement": [
  { "Sid": "S3", "Effect": "Allow",
    "Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:GetObjectVersion",
               "s3:ListBucket","s3:GetBucketLocation"],
    "Resource": [
      "arn:aws:s3:::${BUCKET_EAST}","arn:aws:s3:::${BUCKET_EAST}/*",
      "arn:aws:s3:::${BUCKET_WEST}","arn:aws:s3:::${BUCKET_WEST}/*" ] },
  { "Sid": "KMS", "Effect": "Allow",
    "Action": ["kms:Encrypt","kms:Decrypt","kms:GenerateDataKey","kms:DescribeKey"],
    "Resource": ["${KMS_EAST_ARN}","${KMS_WEST_ARN}"] } ] }
JSON
  cat >"$TMP/crr-policy.json" <<JSON
{ "Version": "2012-10-17", "Statement": [
  { "Sid": "SourceRead", "Effect": "Allow",
    "Action": ["s3:GetReplicationConfiguration","s3:ListBucket",
               "s3:GetObjectVersionForReplication","s3:GetObjectVersionAcl","s3:GetObjectVersionTagging"],
    "Resource": [
      "arn:aws:s3:::${BUCKET_EAST}","arn:aws:s3:::${BUCKET_EAST}/*",
      "arn:aws:s3:::${BUCKET_WEST}","arn:aws:s3:::${BUCKET_WEST}/*" ] },
  { "Sid": "DestWrite", "Effect": "Allow",
    "Action": ["s3:ReplicateObject","s3:ReplicateDelete","s3:ReplicateTags"],
    "Resource": ["arn:aws:s3:::${BUCKET_EAST}/*","arn:aws:s3:::${BUCKET_WEST}/*"] },
  { "Sid": "KMS", "Effect": "Allow",
    "Action": ["kms:Decrypt","kms:Encrypt","kms:GenerateDataKey"],
    "Resource": ["${KMS_EAST_ARN}","${KMS_WEST_ARN}"] } ] }
JSON
  aws iam put-role-policy --role-name "$UC_ROLE"  --policy-name dr-secrets-access-policy --policy-document "file://$TMP/access-policy.json"
  aws iam put-role-policy --role-name "$EC2_ROLE" --policy-name dr-secrets-access-policy --policy-document "file://$TMP/access-policy.json"
  aws iam put-role-policy --role-name "$CRR_ROLE" --policy-name dr-secrets-crr-policy    --policy-document "file://$TMP/crr-policy.json"

  # ---- S3 buckets ----
  create_bucket() {
    local bucket="$1" region="$2" kms_arn="$3"
    log "S3 bucket $bucket ($region)"
    if ! aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
      if [ "$region" = "us-east-1" ]; then
        aws s3api create-bucket --bucket "$bucket" --region "$region" >/dev/null
      else
        aws s3api create-bucket --bucket "$bucket" --region "$region" \
          --create-bucket-configuration LocationConstraint="$region" >/dev/null
      fi
    fi
    aws s3api put-bucket-versioning --bucket "$bucket" \
      --versioning-configuration Status=Enabled
    aws s3api put-public-access-block --bucket "$bucket" \
      --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
    aws s3api put-bucket-encryption --bucket "$bucket" \
      --server-side-encryption-configuration "{\"Rules\":[{\"ApplyServerSideEncryptionByDefault\":{\"SSEAlgorithm\":\"aws:kms\",\"KMSMasterKeyID\":\"${kms_arn}\"},\"BucketKeyEnabled\":true}]}"
  }
  create_bucket "$BUCKET_EAST" "$EAST" "$KMS_EAST_ARN"
  create_bucket "$BUCKET_WEST" "$WEST" "$KMS_WEST_ARN"

  # ---- bidirectional CRR ----
  put_replication() {
    local src="$1" dst_bucket="$2" dst_kms="$3" rule_id="$4"
    log "CRR rule $rule_id ($src -> $dst_bucket)"
    cat >"$TMP/repl.json" <<JSON
{ "Role": "arn:aws:iam::${ACCOUNT_ID}:role/${CRR_ROLE}",
  "Rules": [ {
    "ID": "${rule_id}",
    "Priority": 1,
    "Filter": { "Prefix": "secrets/" },
    "Status": "Enabled",
    "SourceSelectionCriteria": { "SseKmsEncryptedObjects": { "Status": "Enabled" } },
    "Destination": {
      "Bucket": "arn:aws:s3:::${dst_bucket}",
      "EncryptionConfiguration": { "ReplicaKmsKeyID": "${dst_kms}" } },
    "DeleteMarkerReplication": { "Status": "Enabled" } } ] }
JSON
    aws s3api put-bucket-replication --bucket "$src" --replication-configuration "file://$TMP/repl.json"
  }
  put_replication "$BUCKET_EAST" "$BUCKET_WEST" "$KMS_WEST_ARN" "dr-secrets-crr-east-to-west"
  put_replication "$BUCKET_WEST" "$BUCKET_EAST" "$KMS_EAST_ARN" "dr-secrets-crr-west-to-east"

  log "DONE"
  cat <<SUMMARY
Buckets : $BUCKET_EAST | $BUCKET_WEST
KMS     : $KMS_EAST_ARN | $KMS_WEST_ARN
Roles   : $UC_ROLE, $EC2_ROLE (+ $INSTANCE_PROFILE), $CRR_ROLE
CRR     : dr-secrets-crr-east-to-west, dr-secrets-crr-west-to-east (prefix secrets/)
NOTE    : UC role trust uses external id '$UC_EXTERNAL_ID' -- update after creating
          the UC storage credential in Databricks.
SUMMARY
}

main "$@"
