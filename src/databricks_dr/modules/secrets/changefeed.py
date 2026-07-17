"""Change detection for secrets DR.

Two strategies, system-tables-first (native):

1. **system_tables** (default): scan ``system.access.audit`` for secret mutation
   events on the PRIMARY workspace since the last export watermark. This is the
   native change feed -- Databricks already records every ``putSecret`` /
   ``deleteSecret`` / ``putAcl`` / scope event -- so no bespoke polling is needed.

2. **state_diff** (fallback / recon): enumerate the live scopes/keys with the SDK
   (``list_scopes`` / ``list_secrets`` / ``list_acls``) and diff against the
   inventory. Used as the safety net (``full_recon``) even in system-tables mode,
   because audit logs have latency/retention limits and can miss events.

Both return a normalised set of changed ``(scope, key)`` pairs plus deleted keys.
Reading the actual secret *values* happens later in ``export`` (via get-secret).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple

from ...common.logging import get_logger
from ...common.sql import SqlExecutor, rows

_logger = get_logger(__name__)

# system.access.audit action names that mutate secret state.
MUTATION_ACTIONS = (
    "putSecret", "deleteSecret",
    "createScope", "deleteScope",
    "putAcl", "deleteAcl",
)


@dataclass
class ChangeSet:
    changed: Set[Tuple[str, str]] = field(default_factory=set)   # (scope, key) to (re)export
    deleted: Set[Tuple[str, str]] = field(default_factory=set)   # (scope, key) removed
    scopes_touched: Set[str] = field(default_factory=set)        # scope-level or ACL changes
    full: bool = False                                           # True = full recon (export all)

    def summary(self) -> str:
        return (f"changed={len(self.changed)} deleted={len(self.deleted)} "
                f"scopes_touched={len(self.scopes_touched)} full={self.full}")


def detect_via_system_tables(
    ex: SqlExecutor, *, audit_table: str, service_name: str, workspace_id: str,
    since_iso: str | None, lookback_hours: int,
) -> ChangeSet:
    """Scan system.access.audit for secret mutations since the watermark."""
    cs = ChangeSet()
    if not ex.available:
        _logger.warning("No SQL backend for system-table scan; returning empty changeset")
        return cs
    since_pred = (
        f"event_time >= TIMESTAMP '{since_iso}'" if since_iso
        else f"event_time >= current_timestamp() - INTERVAL {int(lookback_hours)} HOURS"
    )
    ws_pred = f"AND workspace_id = '{workspace_id}'" if workspace_id else ""
    actions = ", ".join(f"'{a}'" for a in MUTATION_ACTIONS)
    sql = (
        "SELECT action_name, "
        "request_params.scope AS scope, "
        "request_params.key AS secret_key "
        f"FROM {audit_table} "
        f"WHERE service_name = '{service_name}' "
        f"AND action_name IN ({actions}) "
        f"{ws_pred} AND {since_pred}"
    )
    for r in rows(ex.execute(sql), ["action_name", "scope", "secret_key"]):
        scope = r.get("scope") or ""
        key = r.get("secret_key") or ""
        act = r.get("action_name")
        if not scope:
            continue
        cs.scopes_touched.add(scope)
        if act == "putSecret" and key:
            cs.changed.add((scope, key))
        elif act == "deleteSecret" and key:
            cs.deleted.add((scope, key))
        # createScope / deleteScope / put|deleteAcl are scope-level: handled by
        # re-reading the scope's keys + ACLs in the export step.
    _logger.info("system-table detection: %s", cs.summary())
    return cs


def live_state(wc, include: List[str], exclude: List[str]) -> Dict[str, Dict]:
    """Enumerate live scopes -> {keys: {key: last_updated}, acls: [...]} via the SDK.

    Values are NOT read here (that needs get-secret and only for changed keys).
    """
    state: Dict[str, Dict] = {}
    want_all = "all" in [s.lower() for s in include]
    scopes = wc.secrets.list_scopes()
    for sc in scopes:
        name = sc.name
        if name in exclude:
            continue
        if not want_all and name not in include:
            continue
        keys = {}
        for s in (wc.secrets.list_secrets(scope=name) or []):
            keys[s.key] = getattr(s, "last_updated_timestamp", None)
        acls = [(a.principal, str(a.permission)) for a in (wc.secrets.list_acls(scope=name) or [])]
        state[name] = {"keys": keys, "acls": sorted(acls)}
    return state


def detect_via_state_diff(live: Dict[str, Dict], inventory: Dict[Tuple[str, str], Dict]) -> ChangeSet:
    """Full reconciliation: diff live scope/key state against the inventory."""
    cs = ChangeSet(full=True)
    live_pairs: Set[Tuple[str, str]] = set()
    for scope, info in live.items():
        for key, last_updated in info["keys"].items():
            pair = (scope, key)
            live_pairs.add(pair)
            inv = inventory.get(pair)
            if inv is None or str(inv.get("source_last_updated")) != str(last_updated):
                cs.changed.add(pair)
            cs.scopes_touched.add(scope)
    # Keys present in inventory but no longer live -> deleted.
    for pair, inv in inventory.items():
        if pair not in live_pairs and inv.get("status") != "DELETED":
            cs.deleted.add(pair)
    _logger.info("state-diff detection: %s", cs.summary())
    return cs
