"""Retry with exponential backoff and full jitter.

Three properties matter, and the usual naive loop has none of them:

**Exponential.** Delays double each attempt, so a source that is genuinely down
is not hammered. Linear retries against a struggling service are how a client
turns a partial outage into a full one.

**Jittered.** Every client that failed at the same moment would otherwise retry
at the same moment, and keep colliding. "Full jitter" -- a uniform draw over
``[0, delay]`` rather than ``delay`` itself -- is the variant AWS measured as
best in *Exponential Backoff and Jitter* (2015): it minimises both contention
and total completion time.

**Respectful.** When the server sends ``Retry-After``, that value wins over the
computed backoff. Ignoring it is how a client gets blocked rather than
throttled.

Only :class:`TransientSourceError` is retried. A 4xx, a missing file or a
contract violation will not become correct by being asked again; retrying those
wastes the budget and delays the real failure.

``sleeper`` and ``rng`` are injectable so the tests can assert on the delay
sequence without actually waiting.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

from helios.exceptions import RetryBudgetExhausted, TransientSourceError
from helios.logging_config import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    """How hard, and how politely, to retry."""

    max_attempts: int = 5
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay_seconds <= 0:
            raise ValueError("base_delay_seconds must be positive")
        if self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError("max_delay_seconds must be >= base_delay_seconds")

    def backoff_ceiling(self, attempt: int) -> float:
        """Upper bound of the delay before ``attempt`` (1-based), before jitter."""
        uncapped: float = self.base_delay_seconds * float(2 ** (attempt - 1))
        return min(uncapped, self.max_delay_seconds)


def compute_delay(
    policy: RetryPolicy,
    attempt: int,
    *,
    retry_after_seconds: float | None = None,
    rng: random.Random | None = None,
) -> float:
    """Delay to wait before the given attempt.

    Args:
        policy: The retry budget.
        attempt: 1-based attempt number that is about to be made.
        retry_after_seconds: The server's own instruction, which wins.
        rng: Injectable randomness, so tests are deterministic.

    Returns:
        Seconds to sleep.
    """
    if retry_after_seconds is not None and retry_after_seconds >= 0:
        return min(retry_after_seconds, policy.max_delay_seconds)

    generator = rng if rng is not None else random.Random()
    return generator.uniform(0.0, policy.backoff_ceiling(attempt))


def with_retry(
    operation: Callable[[], T],
    *,
    policy: RetryPolicy,
    description: str = "operation",
    source: str = "",
    sleeper: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
    on_retry: Callable[[int, float, TransientSourceError], None] | None = None,
) -> T:
    """Run ``operation``, retrying transient failures.

    Args:
        operation: A zero-argument callable. Must be safe to run more than
            once -- every retried operation in this pipeline is a read.
        policy: The retry budget.
        description: Used in logs.
        source: Source name, used in logs and in the raised exception.
        sleeper: Injected for tests.
        rng: Injected for tests.
        on_retry: Called with ``(attempt, delay, error)`` before each retry, so
            the caller can count retries for the run record.

    Returns:
        Whatever ``operation`` returns.

    Raises:
        RetryBudgetExhausted: Every attempt failed transiently.
        Exception: Any non-transient error is re-raised immediately.
    """
    last_error: TransientSourceError | None = None

    for attempt in range(1, policy.max_attempts + 1):
        try:
            return operation()
        except TransientSourceError as exc:
            last_error = exc
            if attempt == policy.max_attempts:
                break

            delay = compute_delay(
                policy, attempt, retry_after_seconds=exc.retry_after_seconds, rng=rng
            )
            logger.warning(
                "transient failure, retrying",
                extra={
                    "operation": description,
                    "source": source,
                    "attempt": attempt,
                    "max_attempts": policy.max_attempts,
                    "delay_seconds": round(delay, 3),
                    "status_code": exc.status_code,
                    "honoured_retry_after": exc.retry_after_seconds is not None,
                    "error": str(exc),
                },
            )
            if on_retry is not None:
                on_retry(attempt, delay, exc)
            sleeper(delay)

    raise RetryBudgetExhausted(
        f"{description} failed after {policy.max_attempts} attempt(s): {last_error}",
        source=source,
        attempts=policy.max_attempts,
    )
