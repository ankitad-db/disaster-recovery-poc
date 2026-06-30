"""Bundle layout + manifest schema for the native engine.

A native export writes a self-describing *bundle* to ``output_dir``:

    <output_dir>/
      manifest.json                      # this schema (the index for the importer)
      experiments/<exp_key>/             # (metadata only; runs carry the payload)
      runs/<run_id>/
        run.json                         # params, metrics, tags, lifecycle
        artifacts/...                    # full run artifact tree (incl. the model)
        artifacts/notebooks/...          # notebook revisions (SOURCE/HTML/JUPYTER/DBC)
      versions/<version>/
        version.json                     # number, run_id, aliases, tags, stage, source
        model/...                        # the version's resolved model artifacts
      logged_models/<logged_model_id>/   # MLflow 3 logged models
        logged_model.json
        artifacts/...
      prompts/<name>_v<version>/         # MLflow prompt registry (>=2.21)
        prompt.json
      evaluation_datasets/<id>/          # GenAI evaluation datasets (>=3.4)
        evaluation_dataset.json
      traces/<trace_id>/                 # MLflow traces + assessments (>=2.14)
        trace.json
        artifacts/...

``manifest.json`` is the single source of truth the importer reads; the directory
tree is addressed entirely through the relative paths recorded here, so the bundle
can be moved between buckets/workspaces without any path rewriting.

Schema is versioned (``SCHEMA_VERSION``). ``Manifest.from_dict`` tolerates older
(1.0) bundles by defaulting any fields that were added in 2.0.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

MANIFEST_NAME = "manifest.json"
SCHEMA_VERSION = "2.0"


@dataclass
class ExperimentRec:
    """A source experiment a backing run belonged to."""

    experiment_id: str
    name: str
    tags: Dict[str, str] = field(default_factory=dict)
    artifact_location: Optional[str] = None
    lifecycle_stage: Optional[str] = None


@dataclass
class NotebookRec:
    """A notebook revision exported alongside a backing run."""

    path: str
    revision_id: Optional[str] = None
    formats: List[str] = field(default_factory=list)  # SOURCE|HTML|JUPYTER|DBC
    rel_dir: str = ""  # bundle-relative dir under the run's artifacts/notebooks/


@dataclass
class RunRec:
    """A backing MLflow run (the lineage behind a model version)."""

    run_id: str
    experiment_id: str
    rel_dir: str  # bundle-relative dir holding run.json + artifacts/
    status: str = "FINISHED"
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    params: Dict[str, str] = field(default_factory=dict)
    metrics: Dict[str, float] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)
    lifecycle_stage: str = "active"
    has_artifacts: bool = False
    bytes: int = 0
    notebooks: List[NotebookRec] = field(default_factory=list)


@dataclass
class VersionRec:
    """A single registered-model version and the pointers it needs to be rebuilt."""

    version: str
    rel_dir: str  # bundle-relative dir holding version.json + model/
    run_id: Optional[str] = None
    source: Optional[str] = None
    run_link: Optional[str] = None
    status: Optional[str] = None
    current_stage: Optional[str] = None  # WS registry only (UC uses aliases)
    description: Optional[str] = None
    user_id: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    aliases: List[str] = field(default_factory=list)
    has_model_artifacts: bool = False
    signature_present: bool = False
    model_id: Optional[str] = None  # MLflow 3 logged-model source
    bytes: int = 0


@dataclass
class LoggedModelRec:
    """An MLflow 3 logged model captured for fidelity."""

    logged_model_id: str
    name: Optional[str]
    experiment_id: Optional[str]
    rel_dir: str
    source_run_id: Optional[str] = None
    model_type: Optional[str] = None
    status: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, str] = field(default_factory=dict)
    metrics: Dict[str, float] = field(default_factory=dict)
    has_artifacts: bool = False


@dataclass
class PromptRec:
    """An MLflow Prompt Registry entry/version (>=2.21)."""

    name: str
    version: str
    rel_dir: str
    template: Optional[str] = None
    commit_message: Optional[str] = None
    description: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class EvaluationDatasetRec:
    """A GenAI evaluation dataset (>=3.4)."""

    dataset_id: str
    name: Optional[str]
    rel_dir: str
    digest: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class TraceRec:
    """An MLflow trace (+ assessments) attached to a backing run/experiment (>=2.14)."""

    trace_id: str
    experiment_id: Optional[str]
    rel_dir: str
    request_id: Optional[str] = None
    has_artifacts: bool = False


@dataclass
class RegisteredModelRec:
    """The registered model's own metadata (aliases live here, by version)."""

    name: str
    description: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    aliases: Dict[str, str] = field(default_factory=dict)  # alias -> version
    creation_timestamp: Optional[int] = None
    last_updated_timestamp: Optional[int] = None
    permissions: Optional[Dict[str, Any]] = None  # WS ACLs or UC grants snapshot


@dataclass
class Manifest:
    """Top-level bundle index produced by export, consumed by import."""

    schema_version: str
    engine: str  # e.g. "native-3.1.0"
    mlflow_version: str
    exported_at: str
    source_registry_uri: str
    registered_model: RegisteredModelRec
    is_uc: bool = True
    versions: List[VersionRec] = field(default_factory=list)
    runs: List[RunRec] = field(default_factory=list)
    experiments: List[ExperimentRec] = field(default_factory=list)
    logged_models: List[LoggedModelRec] = field(default_factory=list)
    prompts: List[PromptRec] = field(default_factory=list)
    evaluation_datasets: List[EvaluationDatasetRec] = field(default_factory=list)
    traces: List[TraceRec] = field(default_factory=list)

    # ---- (de)serialization -------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Manifest":
        return cls(
            schema_version=d.get("schema_version", "1.0"),
            engine=d.get("engine", "native"),
            mlflow_version=d.get("mlflow_version", "unknown"),
            exported_at=d.get("exported_at", ""),
            source_registry_uri=d.get("source_registry_uri", ""),
            registered_model=RegisteredModelRec(**d["registered_model"]),
            is_uc=d.get("is_uc", True),
            versions=[VersionRec(**v) for v in d.get("versions", [])],
            runs=[_run_from_dict(r) for r in d.get("runs", [])],
            experiments=[ExperimentRec(**e) for e in d.get("experiments", [])],
            logged_models=[LoggedModelRec(**lm) for lm in d.get("logged_models", [])],
            prompts=[PromptRec(**p) for p in d.get("prompts", [])],
            evaluation_datasets=[EvaluationDatasetRec(**ed) for ed in d.get("evaluation_datasets", [])],
            traces=[TraceRec(**t) for t in d.get("traces", [])],
        )

    def run_by_id(self, run_id: str) -> Optional[RunRec]:
        return next((r for r in self.runs if r.run_id == run_id), None)

    def total_bytes(self) -> int:
        return sum(v.bytes for v in self.versions) + sum(r.bytes for r in self.runs)


def _run_from_dict(r: Dict[str, Any]) -> RunRec:
    notebooks = [NotebookRec(**nb) for nb in r.get("notebooks", [])]
    r = {k: v for k, v in r.items() if k != "notebooks"}
    return RunRec(notebooks=notebooks, **r)


def manifest_path(bundle_dir: str) -> str:
    return os.path.join(bundle_dir, MANIFEST_NAME)


def write_manifest(bundle_dir: str, manifest: "Manifest") -> str:
    os.makedirs(bundle_dir, exist_ok=True)
    path = manifest_path(bundle_dir)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest.to_dict(), f, indent=2, default=_json_default)
    return path


def read_manifest(bundle_dir: str) -> "Manifest":
    path = manifest_path(bundle_dir)
    with open(path, encoding="utf-8") as f:
        return Manifest.from_dict(json.load(f))


def _json_default(o: Any) -> Any:
    """Tolerate odd MLflow entity values (e.g. Metric objects) in tag/param maps."""
    for attr in ("to_dictionary", "to_dict"):
        if hasattr(o, attr):
            try:
                return getattr(o, attr)()
            except Exception:  # noqa: BLE001
                pass
    return str(o)
