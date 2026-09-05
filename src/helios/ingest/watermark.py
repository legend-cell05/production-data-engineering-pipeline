"""Watermark management -- the state that makes ingestion incremental.

A watermark is the highest source-side ``updated_at`` this pipeline has
ingested for a source. The next run asks for everything at or after
``watermark - grace``.

**Why the grace window exists.** Records do not become available in the order
they were produced. A meter that loses connectivity buffers its readings and
flushes them hours later, with an old ``reading_ts`` but a new ``updated_at``.
That case is handled by the cursor itself. The case that is not is a source
whose ``updated_at`` is assigned at write time while the row becomes *visible*
slightly later -- a transaction that commits after a later one. Reading
strictly greater than the watermark loses exactly those rows, permanently and
silently. Re-reading a window of them costs a few duplicate hashes.

**What it costs.** Every run re-reads the grace window. At 90 minutes and one
record per meter per 15 minutes, that is six extra records per meter per run --
all of which are absorbed by the primary key. The duplicate count in the run
report is that cost, made visible.

**What it does not fix.** A record later than the grace window is still missed.
The generator produces some on purpose, and the completeness view is what
catches them. Widening the window trades cost for coverage; it never reaches
certainty, and pretending otherwise would be the real mistake.
"""

from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from helios.config import Settings, get_settings
from helios.db.engine import get_engine
from helios.exceptions import DatabaseError
from helios.logging_config import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class Watermark:
    """A source's ingestion cursor."""

    source_name: str
    value: dt.datetime | None
    grace_minutes: int
    records_seen: int
    last_success_at: dt.datetime | None

    @property
    def is_initial(self) -> bool:
        """True when this source has never been read."""
        return self.value is None

    def read_from(self, grace: dt.timedelta | None = None) -> dt.datetime | None:
        """Where the next read should start.

        Returns ``None`` for a first run, which means "read everything".
        """
        if self.value is None:
            return None
        window = grace if grace is not None else dt.timedelta(minutes=self.grace_minutes)
        return self.value - window


def read_watermark(source_name: str, settings: Settings | None = None) -> Watermark:
    """Load a source's watermark, or an empty one if it has never run."""
    cfg = settings or get_settings()
    engine = get_engine(cfg)
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    f"""SELECT watermark_value, grace_minutes, records_seen, last_success_at
                        FROM {cfg.meta_schema}.source_watermark
                        WHERE source_name = :source"""
                ),
                {"source": source_name},
            ).one_or_none()
    except SQLAlchemyError as exc:
        raise DatabaseError(f"cannot read the watermark for {source_name}: {exc}") from exc

    if row is None:
        return Watermark(
            source_name=source_name,
            value=None,
            grace_minutes=cfg.late_arrival_grace_minutes,
            records_seen=0,
            last_success_at=None,
        )
    return Watermark(
        source_name=source_name,
        value=row.watermark_value,
        grace_minutes=int(row.grace_minutes),
        records_seen=int(row.records_seen),
        last_success_at=row.last_success_at,
    )


def advance_watermark(
    source_name: str,
    new_value: dt.datetime | None,
    *,
    batch_id: uuid.UUID,
    records_seen: int,
    settings: Settings | None = None,
) -> dt.datetime | None:
    """Move a source's watermark forward.

    The watermark is set to the highest ``updated_at`` **actually observed**,
    never to "now". Using the wall clock would advance the cursor past records
    the source had not yet produced, and they would never be read.

    It is also monotonic: ``GREATEST`` keeps the stored value if the new one is
    older. A backfill re-reads an old window and must not wind the cursor
    backwards, or the next normal run would re-ingest everything since.

    Returns:
        The stored watermark after the update.
    """
    cfg = settings or get_settings()
    engine = get_engine(cfg)
    try:
        with engine.begin() as conn:
            stored = conn.execute(
                text(
                    f"""
                    INSERT INTO {cfg.meta_schema}.source_watermark
                        (source_name, watermark_value, grace_minutes, records_seen,
                         last_batch_id, last_success_at, updated_at)
                    VALUES (:source, :value, :grace, :seen, :batch, now(), now())
                    ON CONFLICT (source_name) DO UPDATE
                    SET watermark_value = GREATEST(
                            {cfg.meta_schema}.source_watermark.watermark_value,
                            EXCLUDED.watermark_value
                        ),
                        grace_minutes   = EXCLUDED.grace_minutes,
                        records_seen    = {cfg.meta_schema}.source_watermark.records_seen
                                          + EXCLUDED.records_seen,
                        last_batch_id   = EXCLUDED.last_batch_id,
                        last_success_at = now(),
                        updated_at      = now()
                    RETURNING watermark_value
                    """
                ),
                {
                    "source": source_name,
                    "value": new_value,
                    "grace": cfg.late_arrival_grace_minutes,
                    "seen": records_seen,
                    "batch": str(batch_id),
                },
            ).scalar_one()
    except SQLAlchemyError as exc:
        raise DatabaseError(f"cannot advance the watermark for {source_name}: {exc}") from exc

    stored_value: dt.datetime | None = stored
    logger.info(
        "watermark advanced",
        extra={
            "source": source_name,
            "watermark": stored_value.isoformat() if stored_value else None,
            "records_seen": records_seen,
        },
    )
    return stored_value


def reset_watermark(source_name: str, settings: Settings | None = None) -> None:
    """Clear a source's watermark so the next run reads everything.

    Deliberately not called by anything automatic. A reset re-reads the entire
    source, which is exactly right after a contract change and exactly wrong as
    a reflex when a run fails.
    """
    cfg = settings or get_settings()
    engine = get_engine(cfg)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"""UPDATE {cfg.meta_schema}.source_watermark
                        SET watermark_value = NULL, updated_at = now()
                        WHERE source_name = :source"""
                ),
                {"source": source_name},
            )
    except SQLAlchemyError as exc:
        raise DatabaseError(f"cannot reset the watermark for {source_name}: {exc}") from exc
    logger.warning("watermark reset", extra={"source": source_name})


def all_watermarks(settings: Settings | None = None) -> list[Watermark]:
    """Every stored watermark, for the CLI and the API."""
    cfg = settings or get_settings()
    engine = get_engine(cfg)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"""SELECT source_name, watermark_value, grace_minutes,
                               records_seen, last_success_at
                        FROM {cfg.meta_schema}.source_watermark
                        ORDER BY source_name"""
                )
            ).all()
    except SQLAlchemyError as exc:
        raise DatabaseError(f"cannot list watermarks: {exc}") from exc

    return [
        Watermark(
            source_name=row.source_name,
            value=row.watermark_value,
            grace_minutes=int(row.grace_minutes),
            records_seen=int(row.records_seen),
            last_success_at=row.last_success_at,
        )
        for row in rows
    ]
