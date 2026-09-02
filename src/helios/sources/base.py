"""The source protocol.

Every connector -- REST API, CSV drop, nested JSON -- looks the same to the
ingestion runner: it has a name, a contract, and it yields raw dictionaries for
a given cursor position. Nothing above this layer knows whether a source speaks
HTTP or lives on a filesystem.

That is what makes adding a source a one-class change rather than a change to
the runner, the raw schema and the loader.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from helios.contracts.models import Contract


@dataclass
class FetchResult:
    """What a fetch produced, beyond the records themselves."""

    records_read: int = 0
    pages_fetched: int = 0
    retries_performed: int = 0
    #: Highest ``updated_at`` actually seen. The runner uses this rather than
    #: "now" so the watermark can never advance past data that was not read.
    max_source_updated_at: dt.datetime | None = None
    notes: dict[str, Any] = field(default_factory=dict)

    def observe(self, updated_at: dt.datetime) -> None:
        if self.max_source_updated_at is None or updated_at > self.max_source_updated_at:
            self.max_source_updated_at = updated_at


@runtime_checkable
class Source(Protocol):
    """A readable upstream system."""

    @property
    def name(self) -> str:
        """Stable source identifier. Also the raw partition name."""
        ...

    @property
    def contract(self) -> Contract:
        """The schema contract this source's records must satisfy."""
        ...

    @property
    def supports_incremental(self) -> bool:
        """Whether ``since`` narrows the read.

        ``False`` means every run re-reads everything -- correct for a small
        nightly snapshot, ruinous for telemetry. The runner still advances the
        watermark either way, so the distinction is about cost, not
        correctness.
        """
        ...

    def fetch(self, since: dt.datetime | None, result: FetchResult) -> Iterator[dict[str, Any]]:
        """Yield raw records updated at or after ``since``.

        Implementations must:

        * yield dictionaries, not validated records -- validation belongs to
          the contract, in one place;
        * call ``result.observe(updated_at)`` for every record, so the runner
          learns the true maximum without a second pass;
        * raise :class:`~helios.exceptions.TransientSourceError` for anything
          worth retrying and
          :class:`~helios.exceptions.PermanentSourceError` for anything that
          is not;
        * stream rather than materialise -- a source with ten million records
          must not require ten million records of memory.
        """
        ...
