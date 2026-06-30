"""Change detection for model CDC: system-tables event scan + registry-diff safety net.

This mirrors how Databricks Managed DR reasons about change: it watches the Unity
Catalog audit stream rather than polling every object. We do the same for the
objects Managed DR doesn't cover.

Two detectors, combined:

  1. **Audit-event scan (preferred trigger).** Query ``system.access.audit`` for UC
     model lifecycle actions (createModelVersion, finalizeModelVersion,
     setRegisteredModelAlias, deleteModelVersion, ...) newer than the last scan
     watermark, scoped to the in-scope catalog/schema. This gives near-real-time,
     low-cost "what changed" signals and the *reason* (action) for the audit table.
     Note: ``system.access.audit`` is per-account, so this scan sees events for the
     workspace whose identity is active. In the cross-account DR topology the scan
     is most useful run source-side (or when both workspaces share an account); when
     it returns nothing we fall through to (2).

  2. **Registry diff (authoritative safety net).** Compare each in-scope model's max
     source version (read live under the source identity) against the per-model
     watermark in the audit table. This is workspace/account agnostic and is what
     guarantees correctness even if the audit stream lags or is unavailable.

``detect_changes`` returns the changed set (model -> source max version) plus any
correlated audit events so the caller can record ``source_event_id`` /
``source_event_time`` / ``action`` for RPO accounting.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from ..logging import get_logger

_logger = get_logger(__name__)

# UC audit actions that mean "a registered model changed".
MODEL_AUDIT_ACTIONS = (
    "createModelVersion",
    "finalizeModelVersion",
    "updateModelVersion",
    "deleteModelVersion",
    "createRegisteredModel",
    "updateRegisteredModel",
    "deleteRegisteredModel",
    "setRegisteredModelAlias",
    "deleteRegisteredModelAlias",
    "setModelVersionTag",
)


@dataclass
class ChangeEvent:
    """A single correlated audit event behind a model change."""

    model: str
    action: str
    event_time: Optional[str] = None
    event_id: Optional[str] = None


@dataclass
class DetectResult:
    """Outcome of change detection for one CDC pass."""

    changed: Dict[str, int] = field(default_factory=dict)  # model -> source max version
    events: Dict[str, ChangeEvent] = field(default_factory=dict)  # model -> latest event
    detector: str = "registry_diff"  # which signal flagged the change set
    scan_watermark: Optional[str] = None  # latest audit event_time observed this pass


def detect_changes(
    *,
    client,
    models: List[str],
    watermark_fn: Callable[[str], int],
    spark=None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    since_iso: Optional[str] = None,
) -> DetectResult:
    """Return the set of in-scope models that changed since their watermark."""
    events = _scan_audit_events(spark, catalog, schema, since_iso) if spark is not None else {}
    scan_wm = max((e.event_time for e in events.values() if e.event_time), default=None)

    changed: Dict[str, int] = {}
    for model in models:
        source_max = _max_version(client, model)
        wm = watermark_fn(model)
        flagged_by_event = model in events
        if source_max > wm:
            changed[model] = source_max
            _logger.info("%s: source v%s > watermark v%s -> sync", model, source_max, wm)
        elif flagged_by_event:
            # Metadata-only change (alias/tag/description) with no new version: still
            # re-sync so aliases/tags converge. Carry the current source max.
            changed[model] = source_max
            _logger.info("%s: audit event %s with no new version -> resync metadata",
                         model, events[model].action)
        else:
            _logger.info("%s: up to date (v%s)", model, wm)

    detector = "audit+registry" if events else "registry_diff"
    return DetectResult(changed=changed, events=events, detector=detector, scan_watermark=scan_wm)


def _scan_audit_events(spark, catalog: Optional[str], schema: Optional[str],
                       since_iso: Optional[str]) -> Dict[str, ChangeEvent]:
    """Query system.access.audit for in-scope model events; {} on any failure.

    Best effort: a missing system schema, permissions gap, or schema drift must not
    break CDC -- the registry diff is the correctness guarantee.
    """
    actions = ", ".join(f"'{a}'" for a in MODEL_AUDIT_ACTIONS)
    where = [
        "service_name = 'unityCatalog'",
        f"action_name IN ({actions})",
    ]
    if since_iso:
        where.append(f"event_time > to_timestamp('{since_iso}')")
    else:
        where.append("event_time > current_timestamp() - INTERVAL 24 HOURS")
    # request_params is MAP<STRING,STRING> -> use bracket access (missing keys -> NULL).
    sql = f"""
        SELECT event_time, action_name, event_id,
               COALESCE(request_params['full_name_arg'], request_params['name'],
                        request_params['full_name'], request_params['model_name']) AS model_name
        FROM system.access.audit
        WHERE {' AND '.join(where)}
        ORDER BY event_time
    """
    try:
        rows = spark.sql(sql).collect()
    except Exception as e:  # noqa: BLE001
        _logger.info("audit-event scan unavailable (%s); using registry diff only", e)
        return {}

    prefix = f"{catalog}.{schema}." if catalog and schema else None
    events: Dict[str, ChangeEvent] = {}
    for r in rows:
        model = r["model_name"]
        if not model:
            continue
        if prefix and not str(model).startswith(prefix):
            continue
        events[model] = ChangeEvent(  # keep the latest (rows are ordered ascending)
            model=str(model),
            action=r["action_name"],
            event_time=str(r["event_time"]) if r["event_time"] is not None else None,
            event_id=str(r["event_id"]) if r["event_id"] is not None else None,
        )
    if events:
        _logger.info("audit scan flagged %d model(s): %s", len(events), ", ".join(events))
    return events


def _max_version(client, model: str) -> int:
    try:
        nums = [int(v.version) for v in client.search_model_versions(f"name='{model}'")]
        return max(nums) if nums else 0
    except Exception as e:  # noqa: BLE001
        _logger.error("cannot list versions for %s: %s", model, e)
        return 0
