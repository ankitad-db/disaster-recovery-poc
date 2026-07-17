"""Bounded retry with exponential backoff for transient cross-workspace failures.

Cross-region model replication makes many remote calls -- artifact downloads, UC
temporary-credential vending, ``register_model``, run recreation -- any of which can
fail *transiently* under throttling, a 5xx, or a network blip. A production DR run
must not abort a whole batch because one call hit a rate limit; it should retry the
call a few times with backoff, and only surface a hard failure when the error is
non-transient or the attempts are exhausted.

Deliberately conservative: only errors whose message matches a known transient
marker are retried, so a genuine bug (bad name, permission denied, NOT_FOUND) fails
fast instead of being masked by pointless retries.
"""

from __future__ import annotations

import time
from typing import Callable, Tuple, TypeVar

from .logging import get_logger

_logger = get_logger(__name__)

T = TypeVar("T")

#: substrings (matched case-insensitively) that mark an error as worth retrying.
_TRANSIENT_MARKERS = (
    "rate limit", "too many requests", "429",
    "500", "502", "503", "504",
    "temporarily unavailable", "service unavailable", "unavailable",
    "timeout", "timed out", "read timed out", "deadline exceeded",
    "connection reset", "connection aborted", "connection refused",
    "econnreset", "broken pipe", "throttl",
)


def is_transient(exc: BaseException) -> bool:
    """True if ``exc``'s message looks like a retryable transient failure."""
    msg = str(exc).lower()
    return any(marker in msg for marker in _TRANSIENT_MARKERS)


def retry_call(
    fn: Callable[[], T],
    *,
    attempts: int = 3,
    base_delay: float = 2.0,
    max_delay: float = 30.0,
    retry_on: Callable[[BaseException], bool] = is_transient,
    label: str = "op",
) -> Tuple[T, int]:
    """Call ``fn()``; retry transient failures with exponential backoff.

    Returns ``(result, attempts_used)`` so the caller can record ``retry_count`` on
    its audit row. A non-transient error (per ``retry_on``) or the final attempt
    re-raises immediately -- retries never hide a deterministic failure.
    """
    attempts = max(1, int(attempts))
    for attempt in range(1, attempts + 1):
        try:
            return fn(), attempt
        except BaseException as e:  # noqa: BLE001 - decide retry vs re-raise below
            if attempt >= attempts or not retry_on(e):
                raise
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            _logger.warning(
                "%s failed (attempt %d/%d): %s -- retrying in %.1fs",
                label, attempt, attempts, e, delay,
            )
            time.sleep(delay)
    # Unreachable: the loop either returns or raises. Keeps type-checkers happy.
    raise RuntimeError(f"{label}: retry loop exited unexpectedly")
