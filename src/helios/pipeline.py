"""End-to-end orchestration.

Each function here is one *task* a scheduler would call. They are deliberately
separate rather than one monolithic `run()`, because that is how they are
actually used: an orchestrator runs `ingest` for each source on its own
schedule, `promote` after them, `refresh_marts` once the readings have landed,
and `quality` last. ``run_full_pipeline`` simply calls them in order for local
use and for CI.

Every task opens its own row in ``meta.pipeline_run``, so the history shows
what ran, when, and with what result -- even when the tasks are spread across a
DAG rather than a single process.
"""

from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass, field

import httpx

from helios.config import Settings, get_settings
from helios.contracts.registry import get_contract
from helios.exceptions import ContractViolation, HeliosError, SourceError
from helios.ingest.deadletter import DeadLetterBuffer, list_pending, mark_resolved
from helios.ingest.runner import IngestReport, ingest_source
from helios.ingest.watermark import reset_watermark
from helios.load.copy_loader import RawWriter
from helios.load.promote import promote_readings, promote_reference, refresh_consumption
from helios.logging_config import get_logger
from helios.quality.checks import CheckResult, run_quality_checks
from helios.run_record import RunRecord
from helios.sources.registry import DEFAULT_SOURCE_ORDER, build_sources

logger = get_logger(__name__)


@dataclass
class PipelineResult:
    """Summary of a full pipeline execution."""

    batch_id: uuid.UUID
    status: str = "RUNNING"
    reports: list[IngestReport] = field(default_factory=list)
    core_counts: dict[str, int] = field(default_factory=dict)
    readings_in_core: int = 0
    mart: dict[str, int] = field(default_factory=dict)
    quality: list[CheckResult] = field(default_factory=list)
    failed_sources: dict[str, str] = field(default_factory=dict)
    duration_seconds: float = 0.0

    @property
    def records_read(self) -> int:
        return sum(r.records_read for r in self.reports)

    @property
    def records_ingested(self) -> int:
        return sum(r.records_ingested for r in self.reports)

    @property
    def records_duplicate(self) -> int:
        return sum(r.records_duplicate for r in self.reports)

    @property
    def records_dead_lettered(self) -> int:
        return sum(r.records_dead_lettered for r in self.reports)

    @property
    def retries(self) -> int:
        return sum(r.retries_performed for r in self.reports)

    @property
    def blocking_failures(self) -> list[CheckResult]:
        return [r for r in self.quality if not r.passed and r.check.severity == "BLOCKING"]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.quality if not r.passed and r.check.severity == "WARNING"]


# ---------------------------------------------------------------------------
# Individual tasks
# ---------------------------------------------------------------------------


def ingest(
    source_names: list[str] | None = None,
    settings: Settings | None = None,
    *,
    client: httpx.Client,
    batch_id: uuid.UUID | None = None,
    since_override: dt.datetime | None = None,
    ignore_watermark: bool = False,
    continue_on_error: bool = True,
) -> tuple[list[IngestReport], dict[str, str]]:
    """Ingest one or more sources into the raw layer.

    Args:
        continue_on_error: When ``True``, a source that fails is recorded and
            the others still run. That is almost always what you want: a
            metering API being down should not stop the weather feed, and the
            failed source's watermark simply stays put until the next run.

    Returns:
        The reports, and a mapping of source name to error for the failures.
    """
    cfg = settings or get_settings()
    batch = batch_id or uuid.uuid4()
    sources = build_sources(cfg, client=client)
    names = source_names or list(DEFAULT_SOURCE_ORDER)

    reports: list[IngestReport] = []
    failures: dict[str, str] = {}

    for name in names:
        source = sources.get(name)
        if source is None:
            failures[name] = "no connector configured"
            continue

        with RunRecord("ingest", source_name=name, batch_id=uuid.uuid4(), settings=cfg) as run:
            try:
                report = ingest_source(
                    source,
                    batch,
                    cfg,
                    since_override=since_override,
                    ignore_watermark=ignore_watermark,
                )
            except (SourceError, HeliosError) as exc:
                failures[name] = f"{type(exc).__name__}: {exc}"
                logger.error("source failed", extra={"source": name, "error": str(exc)})
                if not continue_on_error:
                    raise
                continue

            reports.append(report)
            run.counters = report.to_counters()

    return reports, failures


def promote(
    batch_id: uuid.UUID | None = None, settings: Settings | None = None
) -> tuple[dict[str, int], int]:
    """Promote raw into core: reference first, then readings.

    Order matters. A reading whose meter is not yet in core is skipped by the
    promotion join and stays in raw; promoting reference data first means a new
    meter and its first readings land in the same pass.
    """
    cfg = settings or get_settings()
    with RunRecord("promote", batch_id=uuid.uuid4(), settings=cfg) as run:
        counts = promote_reference(batch_id, cfg)
        readings = promote_readings(batch_id, cfg)
        run.counters.rows_promoted = readings
    return counts, readings


def refresh_marts(
    from_date: dt.date | None = None,
    to_date: dt.date | None = None,
    settings: Settings | None = None,
) -> dict[str, int]:
    """Rebuild the consumption mart for a window (default: everything)."""
    cfg = settings or get_settings()
    with RunRecord("refresh-marts", batch_id=uuid.uuid4(), settings=cfg) as run:
        result = refresh_consumption(from_date, to_date, cfg)
        run.counters.rows_promoted = result["rows"]
    return result


def check_quality(
    batch_id: uuid.UUID | None = None,
    settings: Settings | None = None,
    *,
    raise_on_blocking: bool = True,
) -> list[CheckResult]:
    """Run the post-load checks against a batch id."""
    cfg = settings or get_settings()
    return run_quality_checks(batch_id or uuid.uuid4(), cfg, raise_on_blocking=raise_on_blocking)


# ---------------------------------------------------------------------------
# Composite operations
# ---------------------------------------------------------------------------


def run_full_pipeline(
    settings: Settings | None = None,
    *,
    client: httpx.Client,
    fail_on_quality: bool = True,
) -> PipelineResult:
    """Ingest every source, promote, refresh the marts and check quality."""
    cfg = settings or get_settings()
    batch = uuid.uuid4()
    result = PipelineResult(batch_id=batch)

    with RunRecord("run", batch_id=batch, settings=cfg) as run:
        result.reports, result.failed_sources = ingest(None, cfg, client=client, batch_id=batch)

        result.core_counts, result.readings_in_core = promote(batch, cfg)
        result.mart = refresh_consumption(settings=cfg)
        result.quality = run_quality_checks(batch, cfg, raise_on_blocking=fail_on_quality)

        run.counters.records_read = result.records_read
        run.counters.records_ingested = result.records_ingested
        run.counters.records_duplicate = result.records_duplicate
        run.counters.records_dead_lettered = result.records_dead_lettered
        run.counters.retries_performed = result.retries
        run.counters.rows_promoted = result.readings_in_core

        if result.failed_sources:
            run.mark_partial("source(s) failed: " + ", ".join(sorted(result.failed_sources)))
    # Read after the context manager exits: that is where the run record is
    # closed and the duration computed.
    result.duration_seconds = run.duration_seconds
    result.status = run.status
    return result


def backfill(
    source_name: str,
    from_ts: dt.datetime,
    to_ts: dt.datetime | None = None,
    settings: Settings | None = None,
    *,
    client: httpx.Client,
) -> IngestReport:
    """Re-read a bounded window of a source.

    Used after fixing a transformation bug, or when a source announces it
    re-published a period. Safe by construction: the content hash absorbs
    records that have not changed, the upsert replaces the ones that have, and
    ``GREATEST`` keeps the watermark from being wound backwards -- so a
    backfill of last week does not cause a re-read of everything since.

    Only the ingestion is bounded. Promotion and the mart refresh must be run
    afterwards; ``helios backfill`` does that for you.
    """
    cfg = settings or get_settings()
    sources = build_sources(cfg, client=client)
    if source_name not in sources:
        raise HeliosError(f"cannot backfill unknown source {source_name!r}")

    batch = uuid.uuid4()
    logger.warning(
        "backfill started",
        extra={
            "source": source_name,
            "from": from_ts.isoformat(),
            "to": to_ts.isoformat() if to_ts else None,
        },
    )

    with RunRecord("backfill", source_name=source_name, batch_id=batch, settings=cfg) as run:
        report = ingest_source(sources[source_name], batch, cfg, since_override=from_ts)
        run.counters = report.to_counters()
    return report


def replay_dead_letters(
    source_name: str | None = None,
    settings: Settings | None = None,
    *,
    limit: int = 1000,
) -> dict[str, int]:
    """Re-validate parked records against the current contracts.

    Records that now pass are ingested exactly as a fresh read would ingest
    them and marked resolved; records that still fail have their attempt count
    incremented, and are abandoned once the budget is spent.

    This is what makes the dead-letter queue a queue rather than a bin: fix the
    upstream, widen the contract, replay, and the data is recovered.
    """
    cfg = settings or get_settings()
    pending = list_pending(source_name, limit=limit, settings=cfg)
    if not pending:
        logger.info("dead-letter queue is empty", extra={"source": source_name})
        return {"examined": 0, "recovered": 0, "still_failing": 0}

    batch = uuid.uuid4()
    recovered_ids: list[int] = []
    failures = DeadLetterBuffer(batch, cfg)

    with RunRecord("dlq-replay", source_name=source_name, batch_id=batch, settings=cfg) as run:
        with RawWriter(batch, cfg) as writer:
            for entry in pending:
                contract = get_contract(entry.source_name)
                try:
                    validated = contract.validate(entry.payload)
                except ContractViolation as exc:
                    failures.add(entry.source_name, entry.payload, exc)
                    continue
                writer.add(validated)
                recovered_ids.append(entry.dlq_id)

        mark_resolved(recovered_ids, cfg)
        still_failing = failures.flush()

        run.counters.records_read = len(pending)
        run.counters.records_ingested = writer.result.inserted
        run.counters.records_duplicate = writer.result.duplicates
        run.counters.records_dead_lettered = still_failing

    summary = {
        "examined": len(pending),
        "recovered": len(recovered_ids),
        "still_failing": still_failing,
    }
    logger.info("dead-letter replay complete", extra=summary)
    return summary


def reset_source(source_name: str, settings: Settings | None = None) -> None:
    """Clear a source's watermark so the next run re-reads it in full."""
    reset_watermark(source_name, settings)
