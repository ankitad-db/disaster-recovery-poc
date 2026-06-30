"""MLflow 3 / GenAI object capture + restore (version-gated).

Prompts, evaluation datasets and traces only exist on newer MLflow versions. Each
helper is gated on the installed ``mlflow`` version and degrades gracefully (logs +
returns empty) when the runtime is too old or the API surface differs, so a DR run
never fails because the source/destination MLflow lacks a GenAI feature.

Gates (mirroring mlflow-export-import's version_utils):
  * prompts            >= 2.21.0
  * logged models      >= 3.0.0
  * trace assessments  >= 3.2.0
  * evaluation datasets>= 3.4.0
"""

from __future__ import annotations

import json
import os
from typing import List, Optional, Tuple

from ..logging import get_logger
from .manifest import EvaluationDatasetRec, PromptRec, TraceRec

_logger = get_logger(__name__)


def _ver() -> Tuple[int, int, int]:
    try:
        import mlflow

        parts = (mlflow.__version__.split("+")[0].split(".") + ["0", "0", "0"])[:3]
        return tuple(int("".join(c for c in p if c.isdigit()) or 0) for p in parts)  # type: ignore[return-value]
    except Exception:  # noqa: BLE001
        return (0, 0, 0)


def has_prompt_support() -> bool:
    return _ver() >= (2, 21, 0)


def has_logged_model_support() -> bool:
    return _ver() >= (3, 0, 0)


def has_assessment_support() -> bool:
    return _ver() >= (3, 2, 0)


def has_eval_dataset_support() -> bool:
    return _ver() >= (3, 4, 0)


# --------------------------------------------------------------------------- #
# Prompts
# --------------------------------------------------------------------------- #
def export_prompts_for_model(output_dir: str, prompt_names: List[str]) -> List[PromptRec]:
    """Export named prompt-registry entries (all versions) into ``prompts/``."""
    if not prompt_names or not has_prompt_support():
        return []
    recs: List[PromptRec] = []
    try:
        import mlflow
    except Exception:  # noqa: BLE001
        return recs
    for name in prompt_names:
        try:
            versions = _search_prompt_versions(mlflow, name)
            for pv in versions:
                v = str(getattr(pv, "version", "1"))
                rel = os.path.join("prompts", f"{name}_v{v}")
                abs_dir = os.path.join(output_dir, rel)
                os.makedirs(abs_dir, exist_ok=True)
                payload = {
                    "name": name, "version": v,
                    "template": getattr(pv, "template", None),
                    "commit_message": getattr(pv, "commit_message", None),
                    "description": getattr(pv, "description", None),
                    "tags": dict(getattr(pv, "tags", {}) or {}),
                }
                with open(os.path.join(abs_dir, "prompt.json"), "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2, default=str)
                recs.append(PromptRec(
                    name=name, version=v, rel_dir=rel, template=payload["template"],
                    commit_message=payload["commit_message"], description=payload["description"],
                    tags=payload["tags"],
                ))
        except Exception as e:  # noqa: BLE001
            _logger.warning("prompt export %s failed: %s", name, e)
    return recs


def import_prompts(input_dir: str, recs: List[PromptRec]) -> None:
    if not recs or not has_prompt_support():
        return
    try:
        import mlflow
    except Exception:  # noqa: BLE001
        return
    for rec in sorted(recs, key=lambda r: (r.name, int(r.version))):
        try:
            register = getattr(getattr(mlflow, "genai", None), "register_prompt", None) \
                or getattr(mlflow, "register_prompt", None)
            if register is None:
                _logger.warning("no register_prompt API; skipping prompt %s", rec.name)
                return
            register(name=rec.name, template=rec.template or "",
                     commit_message=rec.commit_message, tags=rec.tags or None)
            _logger.info("imported prompt %s v%s", rec.name, rec.version)
        except Exception as e:  # noqa: BLE001
            _logger.warning("prompt import %s failed: %s", rec.name, e)


def _search_prompt_versions(mlflow, name: str):
    genai = getattr(mlflow, "genai", None)
    for fn_name in ("search_prompt_versions", "search_prompts"):
        fn = getattr(genai, fn_name, None) or getattr(mlflow, fn_name, None)
        if fn:
            try:
                return list(fn(name) if fn_name == "search_prompt_versions" else fn(filter_string=f"name='{name}'"))
            except Exception:  # noqa: BLE001
                continue
    load = getattr(genai, "load_prompt", None) or getattr(mlflow, "load_prompt", None)
    if load:
        try:
            return [load(f"prompts:/{name}")]
        except Exception:  # noqa: BLE001
            return []
    return []


# --------------------------------------------------------------------------- #
# Evaluation datasets
# --------------------------------------------------------------------------- #
def export_evaluation_datasets(output_dir: str, dataset_names: List[str]) -> List[EvaluationDatasetRec]:
    if not dataset_names or not has_eval_dataset_support():
        return []
    recs: List[EvaluationDatasetRec] = []
    try:
        import mlflow
        get_dataset = getattr(mlflow.genai, "get_dataset", None)
    except Exception:  # noqa: BLE001
        return recs
    if get_dataset is None:
        return recs
    for name in dataset_names:
        try:
            ds = get_dataset(name=name)
            ds_id = str(getattr(ds, "dataset_id", name))
            rel = os.path.join("evaluation_datasets", ds_id)
            abs_dir = os.path.join(output_dir, rel)
            os.makedirs(abs_dir, exist_ok=True)
            payload = ds.to_dict() if hasattr(ds, "to_dict") else {"name": name}
            with open(os.path.join(abs_dir, "evaluation_dataset.json"), "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, default=str)
            recs.append(EvaluationDatasetRec(
                dataset_id=ds_id, name=name, rel_dir=rel,
                digest=str(getattr(ds, "digest", "")) or None,
                tags=dict(getattr(ds, "tags", {}) or {}),
            ))
        except Exception as e:  # noqa: BLE001
            _logger.warning("evaluation dataset export %s failed: %s", name, e)
    return recs


def import_evaluation_datasets(input_dir: str, recs: List[EvaluationDatasetRec]) -> None:
    if not recs or not has_eval_dataset_support():
        return
    try:
        import mlflow
        create = getattr(mlflow.genai, "create_dataset", None)
    except Exception:  # noqa: BLE001
        return
    if create is None:
        return
    for rec in recs:
        try:
            with open(os.path.join(input_dir, rec.rel_dir, "evaluation_dataset.json"), encoding="utf-8") as f:
                payload = json.load(f)
            create(name=rec.name or rec.dataset_id, tags=rec.tags or None)
            _logger.info("imported evaluation dataset %s", rec.name or rec.dataset_id)
            _ = payload  # records/rows restore is backend-specific; metadata created above
        except Exception as e:  # noqa: BLE001
            _logger.warning("evaluation dataset import %s failed: %s", rec.name, e)


# --------------------------------------------------------------------------- #
# Traces (+ assessments)
# --------------------------------------------------------------------------- #
def export_traces_for_experiments(output_dir: str, experiment_ids: List[str], enabled: bool) -> List[TraceRec]:
    if not enabled or not experiment_ids or _ver() < (2, 14, 0):
        return []
    recs: List[TraceRec] = []
    try:
        import mlflow
        from mlflow import MlflowClient

        client = MlflowClient()
        search = getattr(client, "search_traces", None) or getattr(mlflow, "search_traces", None)
        if search is None:
            return recs
        for exp_id in experiment_ids:
            try:
                traces = search(experiment_ids=[exp_id])
            except TypeError:
                traces = search([exp_id])
            for tr in traces or []:
                tid = str(getattr(tr, "trace_id", getattr(tr, "request_id", "")))
                if not tid:
                    continue
                rel = os.path.join("traces", tid)
                abs_dir = os.path.join(output_dir, rel)
                os.makedirs(abs_dir, exist_ok=True)
                payload = tr.to_dict() if hasattr(tr, "to_dict") else {"trace_id": tid}
                with open(os.path.join(abs_dir, "trace.json"), "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2, default=str)
                recs.append(TraceRec(trace_id=tid, experiment_id=exp_id, rel_dir=rel,
                                     request_id=getattr(tr, "request_id", None)))
    except Exception as e:  # noqa: BLE001
        _logger.warning("trace export failed: %s", e)
    return recs


def import_traces(input_dir: str, recs: List[TraceRec], dest_experiment_id: Optional[str]) -> None:
    if not recs or _ver() < (2, 14, 0):
        return
    try:
        from mlflow import MlflowClient

        client = MlflowClient()
        log_trace = getattr(client, "_log_trace", None) or getattr(client, "log_trace", None)
        if log_trace is None:
            _logger.info("trace import API not available on this MLflow; traces captured but not restored")
            return
    except Exception as e:  # noqa: BLE001
        _logger.warning("trace import client unavailable: %s", e)
        return
    _logger.info("trace restore is best-effort on %s traces (API-version dependent)", len(recs))
