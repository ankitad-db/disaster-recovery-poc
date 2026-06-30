"""Scale primitives for the native engine: pagination + bounded parallelism.

MLflow's ``search_*`` APIs are page-tokened; at scale (hundreds/thousands of
models, versions, runs) we must iterate every page rather than rely on a single
default-limited call. We also parallelize independent per-object work with a
bounded thread pool so a large registry replicates in wall-clock time
proportional to ``max_workers`` rather than serially.

Determinism note: ``max_workers=1`` (the POC default) makes execution fully
sequential and ordered, which keeps audit rows and logs reproducible. Raise it in
config (``models.max_workers``) once steady-state throughput matters.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterable, Iterator, List, Optional, TypeVar

from ..logging import get_logger

_logger = get_logger(__name__)

T = TypeVar("T")
R = TypeVar("R")


# --------------------------------------------------------------------------- #
# Pagination
# --------------------------------------------------------------------------- #
def paginate(search_fn: Callable[..., object], **kwargs) -> Iterator:
    """Yield every result across all pages of an MLflow ``search_*`` call.

    ``search_fn`` must accept ``page_token`` and return a ``PagedList`` (which
    exposes ``.token``). Works for ``search_registered_models``,
    ``search_model_versions``, ``search_runs``, ``search_logged_models``,
    ``search_traces`` and ``search_experiments``.
    """
    token: Optional[str] = None
    while True:
        page = search_fn(page_token=token, **kwargs) if token else search_fn(**kwargs)
        for item in page:
            yield item
        token = getattr(page, "token", None)
        if not token:
            return


def search_all_model_versions(client, model: str) -> List:
    """All versions of a model, paginated and sorted ascending by version number.

    UC ``search_model_versions`` omits aliases/tags from the summary, so each
    version is re-fetched with ``get_model_version`` for full fidelity.
    """
    summaries = list(paginate(client.search_model_versions, filter_string=f"name='{model}'"))
    full = []
    for mv in summaries:
        try:
            full.append(client.get_model_version(model, mv.version))
        except Exception as e:  # noqa: BLE001 - fall back to the (partial) summary
            _logger.debug("get_model_version %s v%s failed, using summary: %s", model, mv.version, e)
            full.append(mv)
    return sorted(full, key=lambda mv: int(mv.version))


def search_all_registered_models(client) -> List[str]:
    return [rm.name for rm in paginate(client.search_registered_models)]


# --------------------------------------------------------------------------- #
# Bounded parallelism
# --------------------------------------------------------------------------- #
def map_bounded(
    fn: Callable[[T], R],
    items: Iterable[T],
    *,
    max_workers: int = 1,
    label: str = "task",
) -> List[R]:
    """Apply ``fn`` to each item, parallelised up to ``max_workers``.

    ``max_workers <= 1`` runs sequentially in input order (deterministic). Any
    worker exception propagates after the pool drains, so a failure is never
    silently dropped. Results preserve input order.
    """
    items = list(items)
    if max_workers <= 1 or len(items) <= 1:
        return [fn(it) for it in items]

    results: List[Optional[R]] = [None] * len(items)
    errors: List[BaseException] = []
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=f"dr-{label}") as pool:
        futures = {pool.submit(fn, it): i for i, it in enumerate(items)}
        for fut in futures:
            idx = futures[fut]
            try:
                results[idx] = fut.result()
            except BaseException as e:  # noqa: BLE001 - collect, re-raise after drain
                errors.append(e)
                _logger.error("%s[%d] failed: %s", label, idx, e)
    if errors:
        raise errors[0]
    return results  # type: ignore[return-value]


def resolve_workers(cfg_models: dict, default: int = 1) -> int:
    try:
        return max(1, int(cfg_models.get("max_workers", default)))
    except (TypeError, ValueError):
        return default
