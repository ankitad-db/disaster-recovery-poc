"""Change detection for model CDC: system-tables event scan + registry-diff safety net.

This mirrors how Databricks Managed DR reasons about change: it watches the Unity
Catalog audit stream rather than polling every object. We do the same for the
objects Managed DR doesn't cover.

Three detectors, combined (any one flags a model; correctness never depends on a
single signal):

  1. **Audit-event scan (optional trigger).** Query ``system.access.audit`` for UC
     model lifecycle actions (createModelVersion, finalizeModelVersion,
     setRegisteredModelAlias, deleteModelVersion, ...) newer than the last scan
     watermark, scoped to the in-scope catalog/schema. This gives near-real-time,
     low-cost "what changed" signals and the *reason* (action) for the audit table.
     Note: ``system.access.audit`` is per-account, so this scan sees events for the
     workspace whose identity is active. In the cross-account DR topology the scan
     is most useful run source-side (or when both workspaces share an account); when
     it returns nothing we fall through to (2)/(3).

  2. **Version diff (authoritative safety net).** Compare each in-scope model's max
     source version (read live under the source identity) against the per-model
     watermark in the audit table. Catches new versions even if the audit stream
     lags or is unavailable.

  3. **Metadata-signature drift (authoritative, account-agnostic).** A version diff
     misses changes that don't bump a version -- a new/re-pointed alias, an edited
     description, or added/changed tags. We compute a stable signature over the
     source model's aliases, tags, and per-version metadata and compare it to the
     signature stored on the last successful sync (``dr_object_inventory``). Any
     difference re-syncs the model so aliases/tags/descriptions converge, without
     depending on the audit stream. This is the key robustness guarantee in the
     cross-account pull topology where the local audit stream can't see the source.

``detect_changes`` returns the changed set (model -> source max version) plus any
correlated audit events (for ``source_event_id`` / ``source_event_time`` / ``action``
RPO accounting) and the freshly-computed per-model signatures + alias maps so the
caller can persist them to the desired-state inventory after a successful sync.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from ..logging import get_logger
from . import _scale

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
    signatures: Dict[str, str] = field(default_factory=dict)  # model -> live source signature
    alias_maps: Dict[str, Dict[str, str]] = field(default_factory=dict)  # model -> {alias: version}


@dataclass
class ModelState:
    """Live metadata snapshot of a source model (for detection + inventory)."""

    max_version: int = 0
    versions: List[int] = field(default_factory=list)
    alias_map: Dict[str, str] = field(default_factory=dict)  # {alias: version}
    signature: str = ""  # sha256 over aliases + tags + per-version metadata


def model_state(client, model: str) -> ModelState:
    """Compute a source model's version set, alias map, and metadata signature.

    The signature is a stable hash over exactly the metadata the replicator carries
    and must keep converged: the registered-model description + tags, the alias map,
    and each version's description + tags. Two source states hash equal iff a
    metadata resync would be a no-op, so a hash change is a reliable "resync me"
    signal even when no new version was created.

    Best-effort: any read failure yields an empty state (``max_version`` 0, empty
    signature). Detection then falls back to the version diff -- an empty signature
    never spuriously flags drift because the caller only compares non-empty hashes.
    """
    try:
        versions = _scale.search_all_model_versions(client, model)
    except Exception as e:  # noqa: BLE001
        _logger.error("cannot list versions for %s: %s", model, e)
        return ModelState()

    vnums = sorted(int(v.version) for v in versions)
    try:
        rm = client.get_registered_model(model)
        rm_desc = rm.description or ""
        rm_tags = dict(rm.tags or {})
        rm_aliases = dict(rm.aliases or {})
    except Exception as e:  # noqa: BLE001 - versions alone still give a usable signature
        _logger.debug("get_registered_model %s failed: %s", model, e)
        rm_desc, rm_tags, rm_aliases = "", {}, {}

    alias_map = {str(a): str(v) for a, v in rm_aliases.items()}
    payload = {
        "description": rm_desc,
        "tags": dict(sorted(rm_tags.items())),
        "aliases": dict(sorted(alias_map.items())),
        "versions": [
            {
                "version": int(v.version),
                "description": getattr(v, "description", "") or "",
                "tags": dict(sorted(dict(getattr(v, "tags", {}) or {}).items())),
                "aliases": sorted(str(a) for a in (getattr(v, "aliases", []) or [])),
            }
            for v in sorted(versions, key=lambda x: int(x.version))
        ],
    }
    signature = hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return ModelState(max_version=(vnums[-1] if vnums else 0), versions=vnums,
                      alias_map=alias_map, signature=signature)


def detect_changes(
    *,
    client,
    models: List[str],
    watermark_fn: Callable[[str], int],
    spark=None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    since_iso: Optional[str] = None,
    signature_fn: Optional[Callable[[str], Optional[str]]] = None,
    detect_metadata_drift: bool = True,
) -> DetectResult:
    """Return the set of in-scope models that changed since their watermark.

    ``signature_fn(model)`` returns the metadata signature stored at the last
    successful sync (``None`` when the model has never synced). When
    ``detect_metadata_drift`` is on and both the stored and live signatures are
    present and differ, the model is flagged even without a new version.
    """
    events = _scan_audit_events(spark, catalog, schema, since_iso) if spark is not None else {}
    scan_wm = max((e.event_time for e in events.values() if e.event_time), default=None)

    changed: Dict[str, int] = {}
    signatures: Dict[str, str] = {}
    alias_maps: Dict[str, Dict[str, str]] = {}
    drift_flagged = False

    for model in models:
        st = model_state(client, model)
        signatures[model] = st.signature
        alias_maps[model] = st.alias_map
        wm = watermark_fn(model)
        flagged_by_event = model in events

        stored_sig = signature_fn(model) if (detect_metadata_drift and signature_fn) else None
        # Only compare non-empty hashes: a missing stored signature (never synced) or a
        # failed live read (empty) must not masquerade as drift.
        metadata_drift = bool(
            detect_metadata_drift and stored_sig and st.signature and st.signature != stored_sig
        )

        if st.max_version > wm:
            changed[model] = st.max_version
            _logger.info("%s: source v%s > watermark v%s -> sync", model, st.max_version, wm)
        elif metadata_drift:
            # Alias/tag/description change with no new version: re-sync so metadata
            # converges. Carry the current source max as the (unchanged) watermark key.
            changed[model] = st.max_version
            drift_flagged = True
            _logger.info("%s: metadata signature changed (no new version) -> resync", model)
        elif flagged_by_event:
            changed[model] = st.max_version
            _logger.info("%s: audit event %s with no new version -> resync metadata",
                         model, events[model].action)
        else:
            _logger.info("%s: up to date (v%s)", model, wm)

    detectors = []
    if events:
        detectors.append("audit")
    if drift_flagged:
        detectors.append("signature")
    detectors.append("registry_diff")
    detector = "+".join(dict.fromkeys(detectors))
    return DetectResult(changed=changed, events=events, detector=detector, scan_watermark=scan_wm,
                        signatures=signatures, alias_maps=alias_maps)


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
    # The fully-qualified model name lives under different keys per action (verified
    # against real system.access.audit rows):
    #   setRegisteredModelAlias / finalizeModelVersion / deleteModelVersion:
    #       full_name_arg = 'catalog.schema.model'  (already qualified)
    #   createModelVersion:  catalog_name + schema_name + model_name (short)
    #   createRegisteredModel: catalog_name + schema_name + name (short)
    # So when full_name_arg is absent we rebuild the FQN from the parts; otherwise the
    # catalog/schema prefix filter below would drop new-version / new-model events.
    sql = f"""
        SELECT event_time, action_name, event_id,
               COALESCE(
                 request_params['full_name_arg'],
                 request_params['full_name'],
                 CASE
                   WHEN request_params['catalog_name'] IS NOT NULL
                        AND request_params['schema_name'] IS NOT NULL
                   THEN concat(request_params['catalog_name'], '.',
                               request_params['schema_name'], '.',
                               COALESCE(request_params['model_name'], request_params['name']))
                 END
               ) AS model_name
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
