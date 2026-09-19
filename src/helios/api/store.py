"""In-memory store backing the simulated upstream API.

Reads ``data/upstream/readings.ndjson`` once and serves cursor-paginated slices
of it. It stands in for the metering platform's own database; the point is not
the store, it is that the pipeline has to talk to it over HTTP, with
pagination, rate limits and intermittent failures.

Ordering is ``(updated_at, reading_id)``. The second key is what makes the
cursor safe: many records share an ``updated_at`` to the second, so a cursor on
the timestamp alone would either skip the rest of a tied group or return it
forever.
"""

from __future__ import annotations

import base64
import binascii
import datetime as dt
import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from helios.logging_config import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class Page:
    """One page of records plus the cursor that follows it."""

    records: list[dict[str, Any]]
    next_cursor: str | None
    total_matching: int


def encode_cursor(updated_at: str, reading_id: str) -> str:
    """Opaque cursor.

    Base64 because a cursor is the server's business: a client that parses one
    will eventually depend on its shape, and the shape will eventually change.
    """
    return base64.urlsafe_b64encode(f"{updated_at}|{reading_id}".encode()).decode()


def decode_cursor(cursor: str) -> tuple[str, str]:
    """Decode a cursor.

    Raises:
        ValueError: Malformed cursor -- the caller turns this into a 400, not a
            500: a bad cursor is the client's mistake.
    """
    try:
        decoded = base64.urlsafe_b64decode(cursor.encode()).decode()
    except (binascii.Error, UnicodeDecodeError) as exc:
        raise ValueError("cursor is not valid base64") from exc
    updated_at, separator, reading_id = decoded.partition("|")
    if not separator:
        raise ValueError("cursor is missing its separator")
    return updated_at, reading_id


class ReadingStore:
    """Thread-safe, lazily loaded reading store."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._records: list[dict[str, Any]] | None = None
        self._loaded_mtime: float | None = None
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def is_available(self) -> bool:
        return self._path.is_file()

    def reload(self) -> int:
        """Force a reload. Used after regenerating the upstream data."""
        with self._lock:
            self._records = None
            self._loaded_mtime = None
        return len(self.records)

    def _file_mtime(self) -> float | None:
        try:
            return self._path.stat().st_mtime
        except OSError:
            return None

    @property
    def records(self) -> list[dict[str, Any]]:
        """Every record, sorted by ``(updated_at, reading_id)``.

        The cache is invalidated when the file's modification time changes, so
        regenerating the upstream data does not require restarting the service.
        Without this, a second ``docker compose up`` would serve the previous
        dataset from memory while the pipeline reads the new one from disk.
        """
        if self._records is not None and self._loaded_mtime == self._file_mtime():
            return self._records

        with self._lock:
            current_mtime = self._file_mtime()
            if self._records is not None and self._loaded_mtime == current_mtime:
                return self._records  # another thread won the race

            if not self._path.is_file():
                logger.warning("reading store not found", extra={"path": str(self._path)})
                self._records = []
                return self._records

            records: list[dict[str, Any]] = []
            with self._path.open(encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))

            records.sort(key=lambda r: (str(r.get("updated_at", "")), str(r.get("reading_id", ""))))
            self._records = records
            self._loaded_mtime = current_mtime
            logger.info(
                "reading store loaded",
                extra={"records": len(records), "path": self._path.name},
            )
            return self._records

    # -- Querying -----------------------------------------------------------

    def page(
        self,
        *,
        updated_since: dt.datetime | None = None,
        cursor: str | None = None,
        limit: int = 5_000,
    ) -> Page:
        """Return one page of records at or after ``updated_since``.

        ``updated_since`` is inclusive. An exclusive bound would drop every
        record sharing the boundary timestamp with the last one already read --
        and at one-second resolution with sixty meters, that boundary is
        crowded. Re-sending a record the client already has costs nothing,
        because the content hash makes the re-insert a no-op; losing one costs
        a hole in the data that nobody notices for weeks.
        """
        records = self.records

        if updated_since is not None:
            bound = updated_since.astimezone(dt.UTC)
            records = [r for r in records if _parse(r.get("updated_at")) >= bound]

        total = len(records)

        if cursor:
            cursor_updated, cursor_id = decode_cursor(cursor)
            records = [
                r
                for r in records
                if (str(r.get("updated_at", "")), str(r.get("reading_id", "")))
                > (cursor_updated, cursor_id)
            ]

        window = records[:limit]
        next_cursor = None
        if len(records) > limit and window:
            last = window[-1]
            next_cursor = encode_cursor(
                str(last.get("updated_at", "")), str(last.get("reading_id", ""))
            )

        return Page(records=window, next_cursor=next_cursor, total_matching=total)

    def stats(self) -> dict[str, Any]:
        """Summary for the API's own status endpoint."""
        records = self.records
        if not records:
            return {"records": 0, "earliest_updated_at": None, "latest_updated_at": None}
        return {
            "records": len(records),
            "earliest_updated_at": records[0].get("updated_at"),
            "latest_updated_at": records[-1].get("updated_at"),
        }


def _parse(value: Any) -> dt.datetime:
    """Parse a stored timestamp, tolerating the ``+0000`` offset form."""
    if value is None:
        return dt.datetime.min.replace(tzinfo=dt.UTC)
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return dt.datetime.min.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC) if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)
