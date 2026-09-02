"""The REST API connector -- the incremental, high-volume source.

Cursor pagination, not offset pagination. With ``LIMIT/OFFSET``, a record
inserted upstream between page 3 and page 4 shifts every later row by one and
the client silently skips a record. A cursor on ``updated_at`` is stable under
concurrent writes, which is the only kind of pagination that is safe against a
live system.

The awkward case a cursor has to handle is a tie: several records sharing the
same ``updated_at`` straddling a page boundary. Advancing the cursor past them
would skip some; not advancing would loop forever. The API therefore returns an
explicit ``next_cursor`` that encodes both the timestamp and the last record
id, and the client simply follows it -- resolving the tie is the server's job,
because only the server knows its own ordering.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from typing import Any

import httpx

from helios.contracts.models import Contract
from helios.exceptions import PermanentSourceError, TransientSourceError
from helios.logging_config import get_logger
from helios.sources.base import FetchResult
from helios.sources.retry import RetryPolicy, with_retry

logger = get_logger(__name__)

#: Statuses worth retrying: the server is busy or briefly broken.
_RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


class ApiSource:
    """Reads paginated JSON from the upstream metering API."""

    def __init__(
        self,
        name: str,
        contract: Contract,
        client: httpx.Client,
        *,
        path: str,
        page_size: int = 5_000,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        """
        Args:
            client: An injected ``httpx.Client``. In production it points at
                the real service; in tests it wraps the FastAPI app through
                ``ASGITransport``, so the full pagination and retry paths are
                exercised without a server process or a network.
        """
        self._name = name
        self._contract = contract
        self._client = client
        self._path = path
        self._page_size = page_size
        self._retry_policy = retry_policy or RetryPolicy()

    @property
    def name(self) -> str:
        return self._name

    @property
    def contract(self) -> Contract:
        return self._contract

    @property
    def supports_incremental(self) -> bool:
        return True

    # -- Fetching -----------------------------------------------------------

    def fetch(self, since: dt.datetime | None, result: FetchResult) -> Iterator[dict[str, Any]]:
        """Walk the cursor until the API says there is nothing left."""
        params: dict[str, Any] = {"limit": self._page_size}
        if since is not None:
            params["updated_since"] = since.astimezone(dt.UTC).isoformat()

        cursor: str | None = None
        while True:
            page_params = dict(params)
            if cursor:
                page_params["cursor"] = cursor

            def fetch_page(params: dict[str, Any] = page_params) -> dict[str, Any]:
                # Bound as a default argument so each retry re-sends *this*
                # page's parameters, not whatever the loop variable holds by
                # the time the retry fires.
                return self._get_page(params)

            payload = with_retry(
                fetch_page,
                policy=self._retry_policy,
                description=f"GET {self._path}",
                source=self._name,
                on_retry=lambda *_: setattr(
                    result, "retries_performed", result.retries_performed + 1
                ),
            )

            records = payload.get("records", [])
            result.pages_fetched += 1

            for record in records:
                result.records_read += 1
                yield record

            cursor = payload.get("next_cursor")
            if not cursor or not records:
                break

        logger.info(
            "api fetch complete",
            extra={
                "source": self._name,
                "pages": result.pages_fetched,
                "records": result.records_read,
                "retries": result.retries_performed,
                "since": since.isoformat() if since else None,
            },
        )

    def _get_page(self, params: dict[str, Any]) -> dict[str, Any]:
        """One HTTP request, with failures classified transient or permanent."""
        try:
            response = self._client.get(self._path, params=params)
        except httpx.TimeoutException as exc:
            raise TransientSourceError(
                f"timeout calling {self._path}: {exc}", source=self._name
            ) from exc
        except httpx.TransportError as exc:
            # Connection reset, DNS failure, refused connection: the request
            # never reached a handler, so retrying is safe and usually works.
            raise TransientSourceError(
                f"transport error calling {self._path}: {exc}", source=self._name
            ) from exc

        if response.status_code in _RETRYABLE_STATUSES:
            raise TransientSourceError(
                f"{self._path} returned {response.status_code}",
                source=self._name,
                status_code=response.status_code,
                retry_after_seconds=_parse_retry_after(response.headers.get("Retry-After")),
            )

        if response.status_code >= 400:
            # 4xx means this request is wrong. Repeating it unchanged will
            # produce the same answer, so fail loudly instead of burning the
            # retry budget.
            raise PermanentSourceError(
                f"{self._path} returned {response.status_code}: {response.text[:200]}",
                source=self._name,
                status_code=response.status_code,
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise PermanentSourceError(
                f"{self._path} returned a body that is not JSON", source=self._name
            ) from exc

        if not isinstance(payload, dict) or "records" not in payload:
            raise PermanentSourceError(
                f"{self._path} response is missing the 'records' key", source=self._name
            )
        return payload


def _parse_retry_after(value: str | None) -> float | None:
    """Parse a ``Retry-After`` header expressed in seconds.

    The HTTP date form is also legal but is not emitted by this API; returning
    ``None`` for it falls back to exponential backoff, which is correct rather
    than clever.
    """
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None
