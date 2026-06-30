"""Managed DR integration seam (intentionally a no-op for the POC).

Databricks **Managed DR** owns cross-region replication + failover for the assets it
supports (UC metadata & grants, managed-table data, and a growing set of workspace
assets) and provides a stable workspace URL that survives a regional failover. It
does NOT cover ML models, serving endpoints, vector search, secrets, Delta shares or
volume *data* -- which is exactly the gap this DIY framework fills.

This DIY framework is deliberately **standalone**: failover/failback are driven by
our own ``dr_state`` control table (see ``modules/models/failover.py``), with no hard
dependency on any Managed DR API. That keeps it usable in workspaces that don't have
Managed DR at all, and avoids coupling to a private/evolving control surface.

The functions below are seams for the (optional) future where a customer runs Managed
DR and wants the DIY layer to react to the *same* failover signal instead of being
triggered separately. They are no-ops today and safe to call unconditionally; wire
real behavior here (e.g. read a Managed-DR failover event/region from a known table or
API) only when that integration is needed.

Intended sequence in a Managed-DR shop:
    1. Managed DR fails the workspace over to the secondary region (stable URL).
    2. ``on_failover(context)`` is invoked so the DIY layer can align its ``dr_state``
       and stop/redirect replication, then promote the standby models/endpoints.
    3. Steady-state CDC resumes in the (new) direction.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .logging import get_logger

_logger = get_logger(__name__)


@dataclass
class ManagedDRContext:
    """Minimal context a Managed-DR failover signal would carry."""

    active_region: Optional[str] = None  # region key Managed DR promoted to
    event_id: Optional[str] = None
    event_time: Optional[str] = None
    source: str = "manual"  # manual|managed_dr


def is_managed_dr_enabled() -> bool:
    """Whether this workspace is under Managed DR. No-op stub: always False for the POC."""
    return False


def correlate_failover_event() -> Optional[ManagedDRContext]:
    """Return the latest Managed-DR failover signal, if any.

    No-op stub. A real implementation would read the Managed-DR failover record
    (table/API) and return its region + event id/time so the DIY layer can align
    ``dr_state`` to the same active region rather than being triggered separately.
    """
    return None


def on_failover(context: ManagedDRContext) -> None:
    """Hook invoked after a (Managed-DR or manual) failover. No-op for the POC.

    The DIY failover/failback path remains fully functional without this; it exists
    so a future Managed-DR integration can converge both layers on one signal.
    """
    _logger.info(
        "managed_dr.on_failover seam invoked (no-op): active_region=%s source=%s event=%s",
        context.active_region, context.source, context.event_id,
    )
