"""Workspace Secrets DR module.

A standalone disaster-recovery flow for Databricks *workspace secrets* (secret
scopes, secret values and their ACLs). It is deliberately independent of the
models module: secrets have their own mechanism (the Secrets API, not MLflow) and
their own constraints (values are only readable via the ``get-secret`` API, writes
are blocked on a read-only DR secondary until failover).

Design in one paragraph:

* **Export** runs in the PRIMARY workspace. It detects changed secrets
  (system-tables-first, with a full state-diff recon), reads their values with the
  ``get-secret`` API, envelope-encrypts them, and writes a bundle to the
  primary-region S3 bucket.
* **S3 Cross-Region Replication** (bidirectional) mirrors the bundle to the other
  region -- no secondary compute required in steady state.
* **Import** runs in the PROMOTED workspace on failover and is *destination-aware*:
  it reads the bundle from the local-region bucket, reads the promoted workspace's
  own live secrets, diffs the two, and applies **only the difference** (add / update
  / delete / ACL). The first failover into a cold secondary is a full rebuild; later
  failovers are incremental.

Everything is SDK-only (no ``dbutils``), so the flow runs identically in a
notebook, a Databricks job, or locally with a CLI profile.
"""

from .config import SecretsConfig, load_config

__all__ = ["SecretsConfig", "load_config"]
