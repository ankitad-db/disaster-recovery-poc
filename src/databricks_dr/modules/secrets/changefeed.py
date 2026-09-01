"""Live secret-state reader.

Enumerates a workspace's in-scope scopes -> {keys, acls} with the Secrets SDK
(``list_scopes`` / ``list_secrets`` / ``list_acls``). Direct replication diffs the
SOURCE's live state against the DESTINATION's live state, so this is the single
source of truth for "what exists where". Values are read separately (only when a
diff needs them) via ``get_secret``.
"""

from __future__ import annotations

from typing import Dict, List

from ...common.logging import get_logger

_logger = get_logger(__name__)


def live_state(wc, include: List[str], exclude: List[str]) -> Dict[str, Dict]:
    """Enumerate live scopes -> {keys: {key: last_updated}, acls: [(principal, perm)]}.

    Values are NOT read here (that needs get-secret and only for changed keys).
    """
    state: Dict[str, Dict] = {}
    want_all = "all" in [s.lower() for s in include]
    for sc in wc.secrets.list_scopes():
        name = sc.name
        if name in exclude:
            continue
        if not want_all and name not in include:
            continue
        keys = {s.key: getattr(s, "last_updated_timestamp", None)
                for s in (wc.secrets.list_secrets(scope=name) or [])}
        acls = [(a.principal, str(a.permission)) for a in (wc.secrets.list_acls(scope=name) or [])]
        state[name] = {"keys": keys, "acls": sorted(acls)}
    return state
