"""POC seeding — create sample secret scopes/secrets/ACLs in the PRIMARY.

Gives the export flow something to protect. Secret VALUES are generated randomly
at runtime (never stored in config or git). Idempotent-ish: re-running creates any
missing scopes and refreshes the sample values. Not used in production, where the
real scopes already exist.
"""

from __future__ import annotations

import secrets as _pysecrets
from typing import Any, Dict

from ...common.logging import get_logger
from .config import SecretsConfig

_logger = get_logger(__name__)


def _rand_value() -> str:
    return "poc-" + _pysecrets.token_hex(20)


def _acl_permission(value: str):
    from databricks.sdk.service.workspace import AclPermission

    v = (value or "READ").split(".")[-1].upper()
    return getattr(AclPermission, v, AclPermission.READ)


def run_seed(cfg: SecretsConfig, *, wc=None) -> Dict[str, Any]:
    if wc is None:
        from databricks.sdk import WorkspaceClient
        prof = cfg.workspaces["primary"].profile
        wc = WorkspaceClient(profile=prof) if prof else WorkspaceClient()

    spec = cfg.seed
    if not spec:
        _logger.info("No seed.scopes in config; nothing to seed.")
        return {"scopes": []}

    existing = {s.name for s in wc.secrets.list_scopes()}
    created = []
    for sc in spec:
        name = sc["name"]
        if name not in existing:
            try:
                wc.secrets.create_scope(scope=name)
                existing.add(name)
            except Exception as e:  # noqa: BLE001
                if "already exists" not in str(e).lower():
                    raise
        for key in sc.get("keys", []):
            wc.secrets.put_secret(scope=name, key=key, string_value=_rand_value())
        for a in sc.get("acls", []):
            principal = a["principal"]
            # 'current_user' resolves to the running identity (always exists), so the
            # POC has a real ACL to exercise the export/import ACL path.
            if principal == "current_user":
                principal = wc.current_user.me().user_name
            try:
                wc.secrets.put_acl(scope=name, principal=principal,
                                   permission=_acl_permission(a.get("permission", "READ")))
            except Exception as e:  # noqa: BLE001
                # Principal may not exist in this workspace -- warn + continue so a
                # bad ACL entry never aborts seeding.
                _logger.warning("Skipped ACL %s on scope %s: %s",
                                principal, name, str(e)[:140])
        created.append({"scope": name, "keys": len(sc.get("keys", []))})

    _logger.info("Seeded %d scope(s): %s", len(created), [c["scope"] for c in created])
    return {"scopes": created}
