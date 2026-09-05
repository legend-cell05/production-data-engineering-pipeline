"""The ingestion runner.

One function drives every source, whatever it speaks:

    read watermark -> fetch since (watermark - grace) -> validate
      -> COPY the valid ones into raw -> park the invalid ones
      -> advance the watermark to the highest updated_at actually observed

The ordering of the last two steps is the part that matters. The watermark is
advanced **after** the data has landed and **only** to a timestamp that was
genuinely seen. Advance it first and a crash loses records forever; advance it
to "now" and records the source had not yet produced are skipped forever. Both
failures are silent, which is what makes them expensive.
"""

from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass

from helios.config import Settings, get_settings
from helios.exceptions import ContractViolation
from helios.ingest.deadletter import DeadLetterBuffer
from helios.ingest.watermark import Watermark, advance_watermark, read_watermark
from helios.load.copy_loader import RawWriter
from helios.logging_config import get_logger
from helios.run_record import RunCounters
from helios.sources.base import FetchResult, Source

logger = get_logger(__name__)


@dataclass
class IngestReport:
    """What one source's ingestion did."""

    source_name: str
    records_read: int = 0
    records_ingested: int = 0
    records_duplicate: int = 0
    records_dead_lettered: int = 0
    retries_performed: int = 0
    pages_fetched: int = 0
    watermark_before: dt.datetime | None = None
    watermark_after: dt.datetime | None = None
    was_initial_load: bool = False

    @property
    def duplicate_rate(self) -> float:
        """Share of records already held. This is the grace window's cost."""
        return 0.0 if self.records_read == 0 else self.records_duplicate / self.records_read

    def to_counters(self) -> RunCounters:
        return RunCounters(
            records_read=self.records_read,
            records_ingested=self.records_ingested,
            records_duplicate=self.records_duplicate,
            records_dead_lettered=self.records_dead_lettered,
            retries_performed=self.retries_performed,
            watermark_before=self.watermark_before,
            watermark_after=self.watermark_after,
        )


def ingest_source(
    source: Source,
    batch_id: uuid.UUID,
    settings: Settings | None = None,
    *,
    since_override: dt.datetime | None = None,
    ignore_watermark: bool = False,
) -> IngestReport:
    """Ingest one source into the raw layer.

    Args:
        source: Any connector satisfying the :class:`Source` protocol.
        batch_id: Identifies this run; stamped on every row written.
        settings: Configuration.
        since_override: Read from this instant instead of the watermark. Used
            by ``helios backfill`` to re-read a bounded window.
        ignore_watermark: Read everything, ignoring the stored cursor.

    Returns:
        An :class:`IngestReport`.

    Raises:
        Whatever the source raises for a *transport* failure. Those must
        propagate: the watermark then stays where it was and the next run
        picks up from the same place.
    """
    cfg = settings or get_settings()
    watermark: Watermark = read_watermark(source.name, cfg)

    if ignore_watermark:
        since = None
    elif since_override is not None:
        since = since_override
    else:
        since = watermark.read_from(cfg.grace_window)

    report = IngestReport(
        source_name=source.name,
        watermark_before=watermark.value,
        was_initial_load=watermark.is_initial,
    )

    logger.info(
        "ingest started",
        extra={
            "source": source.name,
            "since": since.isoformat() if since else None,
            "watermark": watermark.value.isoformat() if watermark.value else None,
            "grace_minutes": cfg.late_arrival_grace_minutes,
            "incremental": source.supports_incremental,
        },
    )

    fetch_result = FetchResult()
    dead_letters = DeadLetterBuffer(batch_id, cfg)

    with RawWriter(batch_id, cfg) as writer:
        for record in source.fetch(since, fetch_result):
            try:
                validated = source.contract.validate(record)
            except ContractViolation as exc:
                # Permanent for this record: park it and carry on. One bad row
                # must never stop a hundred thousand good ones.
                dead_letters.add(source.name, record, exc)
                continue

            fetch_result.observe(validated.source_updated_at)
            writer.add(validated)

    report.records_read = fetch_result.records_read
    report.pages_fetched = fetch_result.pages_fetched
    report.retries_performed = fetch_result.retries_performed
    report.records_ingested = writer.result.inserted
    report.records_duplicate = writer.result.duplicates
    report.records_dead_lettered = dead_letters.flush()

    # Only advance once everything above has succeeded. `GREATEST` inside the
    # update keeps this monotonic even when a backfill re-reads an old window.
    if fetch_result.max_source_updated_at is not None:
        report.watermark_after = advance_watermark(
            source.name,
            fetch_result.max_source_updated_at,
            batch_id=batch_id,
            records_seen=report.records_ingested,
            settings=cfg,
        )
    else:
        # Nothing came back. That is a normal quiet run, not a failure -- but
        # the watermark must not move, or a source that is merely silent would
        # look like a source that is up to date.
        report.watermark_after = watermark.value
        logger.info("no records returned; watermark unchanged", extra={"source": source.name})

    logger.info(
        "ingest finished",
        extra={
            "source": source.name,
            "read": report.records_read,
            "ingested": report.records_ingested,
            "duplicate": report.records_duplicate,
            "dead_lettered": report.records_dead_lettered,
            "retries": report.retries_performed,
            "pages": report.pages_fetched,
            "duplicate_rate_pct": round(100 * report.duplicate_rate, 2),
            "watermark_after": (
                report.watermark_after.isoformat() if report.watermark_after else None
            ),
        },
    )
    return report
