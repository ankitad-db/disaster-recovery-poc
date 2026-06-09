"""Resolve which registered models are in DR scope on the source registry."""

from __future__ import annotations

from typing import List

from ...common.clients import make_mlflow_client
from ...common.logging import get_logger

_logger = get_logger(__name__)


def resolve_models(registry_uri: str, include: List[str]) -> List[str]:
    """Expand the config ``include`` list into concrete model names.

    Supports: exact names, a trailing ``*`` prefix, and the literal ``all``.
    """
    if not include:
        return []
    if include == ["all"] or "all" in include:
        return _all_models(registry_uri)

    exact = [m for m in include if not m.endswith("*")]
    prefixes = [m[:-1] for m in include if m.endswith("*")]
    if not prefixes:
        return exact

    resolved = list(exact)
    for name in _all_models(registry_uri):
        if any(name.startswith(p) for p in prefixes):
            resolved.append(name)
    return sorted(set(resolved))


def _all_models(registry_uri: str) -> List[str]:
    client = make_mlflow_client(registry_uri)
    names = [rm.name for rm in client.search_registered_models()]
    _logger.info("Found %d registered models on %s", len(names), registry_uri)
    return names
