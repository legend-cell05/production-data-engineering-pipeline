"""Bulk loading into the raw layer with ``COPY``.

``COPY`` is several times faster than multi-row ``INSERT`` because it bypasses
the statement parser and the per-row protocol round trip entirely. At 170 000
records that is the difference between a pipeline that finishes in seconds and
one that finishes in minutes -- and minutes is what turns a fifteen-minute
schedule into a backlog.

``COPY`` has no ``ON CONFLICT``, though, and idempotency is the whole point of
this pipeline. The standard answer, used here, is two steps in one transaction:

1. ``COPY`` into an unlogged temporary table -- no indexes, no WAL, no
   constraint checks, as fast as PostgreSQL ingests anything;
2. ``INSERT ... SELECT ... ON CONFLICT DO NOTHING`` from that table into
   ``raw.record``, where the primary key on
   ``(source_name, natural_key, content_hash)`` silently absorbs everything
   already present.

The difference between the rows staged and the rows inserted is the duplicate
count -- which is free, and is exactly the number that shows the grace window
doing its job.

Records are flushed in chunks. Each chunk commits on its own, so a failure
halfway through leaves the earlier chunks durably landed. That is safe
precisely because the watermark is not advanced until the whole source
succeeds: a retry re-reads the same records and the primary key absorbs them.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from types import TracebackType

from helios.config import Settings, get_settings
from helios.contracts.models import ValidatedRecord
from helios.db.engine import raw_connection
from helios.exceptions import LoadError
from helios.logging_config import get_logger

logger = get_logger(__name__)

_STAGE_COLUMNS = (
    "source_name",
    "natural_key",
    "content_hash",
    "payload",
    "source_updated_at",
    "contract_version",
    "batch_id",
)

#: The payload crosses as TEXT and is cast to JSONB on the way into raw.
#: Handing psycopg a pre-serialised string with a JSONB column type would make
#: it encode that string *as* a JSON value -- a double-encoded payload that
#: looks right in the table and returns NULL for every key lookup.
_STAGE_TYPES = ["text", "text", "text", "text", "timestamptz", "text", "uuid"]


@dataclass
class CopyResult:
    """Outcome of a bulk load."""

    staged: int = 0
    inserted: int = 0
    duplicates: int = 0
    chunks: int = 0

    @property
    def duplicate_rate(self) -> float:
        return 0.0 if self.staged == 0 else self.duplicates / self.staged


class RawWriter:
    """Buffered ``COPY`` writer for ``raw.record``.

    Use as a context manager; the final partial chunk is flushed on exit.

        >>> with RawWriter(batch_id) as writer:      # doctest: +SKIP
        ...     for record in records:
        ...         writer.add(record)
        >>> writer.result.inserted                   # doctest: +SKIP
    """

    def __init__(
        self,
        batch_id: uuid.UUID,
        settings: Settings | None = None,
        *,
        chunk_size: int | None = None,
    ) -> None:
        self._cfg = settings or get_settings()
        self._batch_id = batch_id
        self._chunk_size = chunk_size or self._cfg.copy_batch_size
        self._buffer: list[ValidatedRecord] = []
        self.result = CopyResult()

    def __enter__(self) -> RawWriter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self.flush()

    def add(self, record: ValidatedRecord) -> None:
        """Buffer one record, flushing when the chunk is full."""
        self._buffer.append(record)
        if len(self._buffer) >= self._chunk_size:
            self.flush()

    def add_many(self, records: list[ValidatedRecord]) -> None:
        for record in records:
            self.add(record)

    def flush(self) -> None:
        """Write the buffer and clear it."""
        if not self._buffer:
            return

        chunk = self._buffer
        self._buffer = []
        staged, inserted = self._write_chunk(chunk)

        self.result.staged += staged
        self.result.inserted += inserted
        self.result.duplicates += staged - inserted
        self.result.chunks += 1

        logger.info(
            "raw chunk loaded",
            extra={
                "staged": staged,
                "inserted": inserted,
                "duplicates": staged - inserted,
                "chunk": self.result.chunks,
            },
        )

    def _write_chunk(self, chunk: list[ValidatedRecord]) -> tuple[int, int]:
        """COPY one chunk into a temp table, then upsert it into raw."""
        import json

        raw_schema = self._cfg.raw_schema
        try:
            with raw_connection(self._cfg) as conn, conn.cursor() as cur:
                # UNLOGGED via TEMP: no WAL, no indexes, no constraints. The
                # table disappears at COMMIT, so nothing is left behind even
                # if the process dies.
                cur.execute(
                    """
                    CREATE TEMP TABLE _stage_raw (
                        source_name       TEXT,
                        natural_key       TEXT,
                        content_hash      TEXT,
                        payload           TEXT,
                        source_updated_at TIMESTAMPTZ,
                        contract_version  TEXT,
                        batch_id          UUID
                    ) ON COMMIT DROP
                    """
                )

                columns = ", ".join(_STAGE_COLUMNS)
                with cur.copy(f"COPY _stage_raw ({columns}) FROM STDIN") as copy:
                    copy.set_types(_STAGE_TYPES)
                    for record in chunk:
                        copy.write_row(
                            (
                                record.source_name,
                                record.natural_key,
                                record.content_hash,
                                json.dumps(record.payload, default=str, ensure_ascii=False),
                                record.source_updated_at,
                                record.contract_version,
                                # A uuid.UUID, not its string form: `set_types` told
                                # psycopg this column is a UUID, and the adapter expects
                                # the native type.
                                self._batch_id,
                            )
                        )

                cur.execute("SELECT COUNT(*) FROM _stage_raw")
                row = cur.fetchone()
                staged = int(row[0]) if row else 0

                # DISTINCT ON collapses duplicates *within* the chunk; the
                # primary key collapses duplicates against what is already
                # stored. Without the first one, a chunk containing the same
                # record twice would abort with a cardinality violation.
                cur.execute(
                    f"""
                    INSERT INTO {raw_schema}.record (
                        source_name, natural_key, content_hash, payload,
                        source_updated_at, contract_version, batch_id
                    )
                    SELECT DISTINCT ON (source_name, natural_key, content_hash)
                        source_name, natural_key, content_hash,
                        CAST(payload AS JSONB),
                        source_updated_at, contract_version, batch_id
                    FROM _stage_raw
                    ORDER BY source_name, natural_key, content_hash, source_updated_at DESC
                    ON CONFLICT (source_name, natural_key, content_hash) DO NOTHING
                    """
                )
                inserted = cur.rowcount
        except Exception as exc:  # psycopg errors are not SQLAlchemy errors here
            raise LoadError(f"COPY into {raw_schema}.record failed: {exc}") from exc

        return staged, max(0, inserted)
