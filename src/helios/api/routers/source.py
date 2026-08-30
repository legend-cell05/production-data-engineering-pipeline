"""The simulated upstream metering API.

This router *is* the third-party system. It exists so the pipeline has a real
HTTP source to read -- with pagination, rate limiting and intermittent
failures -- instead of a mock that always succeeds. A retry policy that has
never actually retried anything is not a retry policy.

Failures are injected at configurable rates (``HELIOS_SOURCE_FAULT_RATE``,
``HELIOS_SOURCE_RATE_LIMIT_RATE``). Set both to 0 for a deterministic run;
leave them at their defaults to watch the backoff work.
"""

from __future__ import annotations

import datetime as dt
import random
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query, Response, status

from helios.api.dependencies import get_reading_store, get_source_settings
from helios.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/source", tags=["upstream source"])

#: Seeded separately from the data generator: fault injection must not change
#: the dataset, only the order in which the client manages to read it.
_fault_rng = random.Random(1_337)


def _maybe_fail() -> None:
    """Inject a transient failure, as a real API under load would."""
    cfg = get_source_settings()

    if _fault_rng.random() < cfg.source_rate_limit_rate:
        # A real rate limiter tells the client how long to wait. Honouring it
        # is the difference between being throttled and being blocked.
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="rate limit exceeded",
            headers={"Retry-After": "1"},
        )

    if _fault_rng.random() < cfg.source_fault_rate:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="upstream temporarily unavailable",
        )


@router.get(
    "/readings",
    summary="Paginated interval readings",
    response_description="A page of readings plus the cursor for the next one.",
)
def list_readings(
    updated_since: Annotated[
        dt.datetime | None,
        Query(
            description="Return records whose updated_at is at or after this instant (ISO-8601)."
        ),
    ] = None,
    cursor: Annotated[
        str | None, Query(description="Opaque cursor returned by the previous page.")
    ] = None,
    limit: Annotated[int, Query(ge=1, le=50_000, description="Maximum records per page.")] = 5_000,
) -> dict[str, Any]:
    """Return one page of meter readings.

    Pagination is cursor-based, not offset-based: records inserted upstream
    between two pages would shift an offset and silently skip a row.
    """
    _maybe_fail()

    store = get_reading_store()
    if not store.is_available():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="upstream store not generated yet; run `helios seed`",
        )

    try:
        page = store.page(updated_since=updated_since, cursor=cursor, limit=limit)
    except ValueError as exc:
        # A malformed cursor is the client's mistake, so 400 -- and 4xx is not
        # retried, which is exactly right here.
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"invalid cursor: {exc}"
        ) from exc

    return {
        "records": page.records,
        "next_cursor": page.next_cursor,
        "count": len(page.records),
        "total_matching": page.total_matching,
    }


@router.get("/readings/stats", summary="Store statistics")
def reading_stats() -> dict[str, Any]:
    """What the upstream store currently holds. Never fails on purpose."""
    return get_reading_store().stats()


@router.post(
    "/readings/reload",
    summary="Reload the store from disk",
    status_code=status.HTTP_200_OK,
)
def reload_store() -> dict[str, int]:
    """Re-read the upstream file, after regenerating it."""
    return {"records": get_reading_store().reload()}


@router.get("/health", summary="Upstream liveness")
def source_health(response: Response) -> dict[str, Any]:
    """Whether the simulated source has data to serve."""
    store = get_reading_store()
    available = store.is_available()
    if not available:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "ok" if available else "no_data", "store": str(store.path.name)}
