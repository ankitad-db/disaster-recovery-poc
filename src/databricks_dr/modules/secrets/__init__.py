"""Workspace Secrets DR module.

A standalone disaster-recovery flow for Databricks *workspace secrets* (secret
scopes, secret values and their ACLs). It is deliberately independent of the
models module: secrets have their own mechanism (the Secrets API, not MLflow) and
their own constraints (values are only readable via the ``get-secret`` API, writes
are blocked on a read-only DR secondary until failover).

Design in one paragraph:

* A single **replicate** job runs in the ACTIVE workspace. It reads the SOURCE
  secrets (values via ``get-secret`` -> sha256, plus per-scope ACLs), reads the
  DESTINATION secrets **cross-workspace** via the Secrets API (reads/writes over the
  control plane spin up **no compute** on the passive side), **diffs** the two, and
  applies **only the difference** straight into the destination (``create_scope`` /
  ``put_secret`` / ``put_acl`` / ``delete_secret`` / ``delete_acl``).
* Values move source->destination **directly over TLS** — no object storage, no CRR,
  no KMS envelope. Both workspaces' secret stores are encrypted at rest by the platform.
* Direction is parameterised, so failover is just ``replicate`` with the roles
  swapped. The secondary is a **warm mirror**: on a real outage you promote it.

Everything is SDK-only (no ``dbutils``), so the flow runs identically in a
notebook, a Databricks job, or locally with CLI profiles.
"""

from .config import SecretsConfig, load_config

__all__ = ["SecretsConfig", "load_config"]
