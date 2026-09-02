"""Unit tests for the retry policy.

The delays are asserted against the formula, not against whatever the code
happens to produce, and the sleeper is injected so the suite does not actually
wait.
"""

from __future__ import annotations

import random

import pytest

from helios.exceptions import (
    PermanentSourceError,
    RetryBudgetExhausted,
    TransientSourceError,
)
from helios.sources.retry import RetryPolicy, compute_delay, with_retry


class TestRetryPolicy:
    def test_backoff_doubles(self) -> None:
        policy = RetryPolicy(base_delay_seconds=1.0, max_delay_seconds=100.0)
        assert [policy.backoff_ceiling(a) for a in (1, 2, 3, 4)] == [1.0, 2.0, 4.0, 8.0]

    def test_backoff_is_capped(self) -> None:
        policy = RetryPolicy(base_delay_seconds=1.0, max_delay_seconds=5.0)
        assert policy.backoff_ceiling(10) == 5.0

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"max_attempts": 0},
            {"base_delay_seconds": 0},
            {"base_delay_seconds": 10, "max_delay_seconds": 1},
        ],
    )
    def test_invalid_policies_are_refused(self, kwargs: dict[str, float]) -> None:
        with pytest.raises(ValueError):
            RetryPolicy(**kwargs)  # type: ignore[arg-type]


class TestComputeDelay:
    def test_full_jitter_stays_within_the_ceiling(self) -> None:
        # Full jitter draws uniformly over [0, ceiling] rather than sleeping
        # the ceiling itself, so clients that failed together do not retry
        # together.
        policy = RetryPolicy(base_delay_seconds=1.0, max_delay_seconds=64.0)
        rng = random.Random(0)
        for attempt in range(1, 6):
            for _ in range(50):
                delay = compute_delay(policy, attempt, rng=rng)
                assert 0.0 <= delay <= policy.backoff_ceiling(attempt)

    def test_jitter_actually_varies(self) -> None:
        policy = RetryPolicy(base_delay_seconds=4.0)
        rng = random.Random(1)
        delays = {compute_delay(policy, 3, rng=rng) for _ in range(20)}
        assert len(delays) > 1

    def test_retry_after_wins_over_backoff(self) -> None:
        # The server knows better than the client's guess. Ignoring it is how
        # a client gets blocked rather than throttled.
        policy = RetryPolicy(base_delay_seconds=0.5, max_delay_seconds=60.0)
        assert compute_delay(policy, 5, retry_after_seconds=2.0) == 2.0

    def test_retry_after_is_still_capped(self) -> None:
        policy = RetryPolicy(base_delay_seconds=0.5, max_delay_seconds=10.0)
        assert compute_delay(policy, 1, retry_after_seconds=9999.0) == 10.0


class TestWithRetry:
    def test_success_on_the_first_attempt(self) -> None:
        assert with_retry(lambda: "ok", policy=RetryPolicy(), sleeper=lambda _: None) == "ok"

    def test_transient_failure_is_retried_then_succeeds(self) -> None:
        attempts = {"n": 0}

        def flaky() -> str:
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise TransientSourceError("503", status_code=503)
            return "ok"

        slept: list[float] = []
        result = with_retry(
            flaky, policy=RetryPolicy(max_attempts=5), sleeper=slept.append, rng=random.Random(0)
        )
        assert result == "ok"
        assert attempts["n"] == 3
        assert len(slept) == 2

    def test_permanent_failure_is_not_retried(self) -> None:
        attempts = {"n": 0}

        def broken() -> str:
            attempts["n"] += 1
            raise PermanentSourceError("404", status_code=404)

        with pytest.raises(PermanentSourceError):
            with_retry(broken, policy=RetryPolicy(max_attempts=5), sleeper=lambda _: None)
        # A 4xx will not become a 200 by being asked again; burning the budget
        # on it only delays the real failure.
        assert attempts["n"] == 1

    def test_budget_exhaustion_raises(self) -> None:
        def always_down() -> str:
            raise TransientSourceError("down", status_code=503)

        with pytest.raises(RetryBudgetExhausted) as exc:
            with_retry(always_down, policy=RetryPolicy(max_attempts=3), sleeper=lambda _: None)
        assert exc.value.attempts == 3

    def test_no_sleep_after_the_final_attempt(self) -> None:
        slept: list[float] = []

        def always_down() -> str:
            raise TransientSourceError("down")

        with pytest.raises(RetryBudgetExhausted):
            with_retry(always_down, policy=RetryPolicy(max_attempts=3), sleeper=slept.append)
        # Three attempts, two gaps -- sleeping after the last one would just
        # delay the failure being reported.
        assert len(slept) == 2

    def test_on_retry_callback_counts_retries(self) -> None:
        counted: list[int] = []

        def flaky() -> str:
            if len(counted) < 2:
                raise TransientSourceError("503")
            return "ok"

        with_retry(
            flaky,
            policy=RetryPolicy(max_attempts=5),
            sleeper=lambda _: None,
            on_retry=lambda attempt, delay, error: counted.append(attempt),
        )
        assert counted == [1, 2]

    def test_retry_after_is_honoured_end_to_end(self) -> None:
        slept: list[float] = []
        state = {"n": 0}

        def rate_limited() -> str:
            state["n"] += 1
            if state["n"] == 1:
                raise TransientSourceError("429", status_code=429, retry_after_seconds=3.0)
            return "ok"

        with_retry(rate_limited, policy=RetryPolicy(), sleeper=slept.append)
        assert slept == [3.0]
