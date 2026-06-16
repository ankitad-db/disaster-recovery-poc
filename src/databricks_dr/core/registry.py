"""Module registry: maps an object-type key to its BaseDRModule subclass.

New modules register here (or via entry points later). The CLI looks modules up
by key so adding a module requires no CLI changes.
"""

from __future__ import annotations

from typing import Dict, Type

from .base import BaseDRModule

_REGISTRY: Dict[str, Type[BaseDRModule]] = {}


def register(cls: Type[BaseDRModule]) -> Type[BaseDRModule]:
    """Class decorator to register a DR module by its ``object_type``."""
    key = cls.object_type
    if key in _REGISTRY and _REGISTRY[key] is not cls:
        raise ValueError(f"DR module '{key}' already registered to {_REGISTRY[key]}")
    _REGISTRY[key] = cls
    return cls


def get_module(object_type: str) -> Type[BaseDRModule]:
    _ensure_loaded()
    if object_type not in _REGISTRY:
        raise KeyError(f"No DR module '{object_type}'. Available: {sorted(_REGISTRY)}")
    return _REGISTRY[object_type]


def available() -> list[str]:
    _ensure_loaded()
    return sorted(_REGISTRY)


def _ensure_loaded() -> None:
    """Import built-in modules so their @register decorators run."""
    # Importing the package triggers registration of ModelsDRModule.
    from ..modules import models  # noqa: F401
