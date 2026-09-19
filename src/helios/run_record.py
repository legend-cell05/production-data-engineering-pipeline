"""Recording what the pipeline did.

Every command opens a row in ``meta.pipeline_run`` **before** doing any work
and closes it afterwards, whatever the outcome. A run that crashes therefore
appears as ``FAILED`` with its error message, rather than as a row that never
appeared -- the difference between "the pipeline broke at 02:14" and "nobody
knows whether it ran".
"""

from __future__ import annotations

import datetime as dt
import time
import uuid
from dataclasses import dataclass, field
from types import TracebackType

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from helios import __version__
from helios.config import Settings, get_settings
from helios.db.engine import get_engine
from helios.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class RunCounters:
    """The numbers a run reports when it finishes."""

    records_read: int = 0
    records_ingested: int = 0
    records_duplicate: int = 0
    records_dead_lettered: int = 0
    rows_promoted: int = 0
    retries_performed: int = 0
    watermark_before: dt.datetime | None = None
    watermark_after: dt.datetime | None = None
    notes: dict[str, object] = field(default_factory=dict)

    def merge(self, other: RunCounters) -> None:
        """Fold another run's counters into this one, for multi-source runs."""
        self.records_read += other.records_read
        self.records_ingested += other.records_ingested
        self.records_duplicate += other.records_duplicate
        self.records_dead_lettered += other.records_dead_lettered
        self.rows_promoted += other.rows_promoted
        self.retries_performed += other.retries_performed


class RunRecord:
    """Context manager that opens and closes a ``meta.pipeline_run`` row.

        >>> with RunRecord("ingest", source_name="weather") as run:   # doctest: +SKIP
        ...     run.counters.records_read = 42

    On an exception the row is closed as ``FAILED`` with the message, and the
    exception is re-raised: recording a failure must never swallow it.
    """

    def __init__(
        self,
        command: str,
        *,
        source_name: str | None = None,
        batch_id: uuid.UUID | None = None,
        settings: Settings | None = None,
    ) -> None:
        self._cfg = settings or get_settings()
        self.command = command
        self.source_name = source_name
        self.batch_id = batch_id or uuid.uuid4()
        self.counters = RunCounters()
        self.status = "RUNNING"
        self.error_message: str | None = None
        self.duration_seconds = 0.0
        self._started = 0.0

    def __enter__(self) -> RunRecord:
        self._started = time.perf_counter()
        self._open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        # Returns None rather than False: an __exit__ that never suppresses
        # should say so in its type, not return a constant.
        self.duration_seconds = time.perf_counter() - self._started
        if exc is not None:
            self.status = "FAILED"
            self.error_message = f"{type(exc).__name__}: {exc}"[:2000]
        elif self.status == "RUNNING":
            self.status = "SUCCESS"
        self._close()

    def mark_partial(self, reason: str) -> None:
        """Finish as ``PARTIAL``: useful work was done, but not all of it.

        Used when some sources of a multi-source run succeeded and others did
        not. Reporting that as SUCCESS would hide a broken source; reporting it
        as FAILED would hide the data that did land.
        """
        self.status = "PARTIAL"
        self.error_message = reason[:2000]

    # -- Persistence --------------------------------------------------------

    def _open(self) -> None:
        cfg = self._cfg
        try:
            with get_engine(cfg).begin() as conn:
                conn.execute(
                    text(
                        f"""INSERT INTO {cfg.meta_schema}.pipeline_run
                            (batch_id, command, source_name, status, pipeline_version)
                            VALUES (CAST(:batch AS UUID), :command, :source, 'RUNNING', :version)"""
                    ),
                    {
                        "batch": str(self.batch_id),
                        "command": self.command,
                        "source": self.source_name,
                        "version": __version__,
                    },
                )
        except SQLAlchemyError as exc:  # pragma: no cover
            logger.error("could not open the run record", extra={"error": str(exc)})

        logger.info(
            "run started",
            extra={
                "batch_id": str(self.batch_id),
                "command": self.command,
                "source": self.source_name,
            },
        )

    def _close(self) -> None:
        cfg = self._cfg
        c = self.counters
        try:
            with get_engine(cfg).begin() as conn:
                conn.execute(
                    text(
                        f"""UPDATE {cfg.meta_schema}.pipeline_run
                            SET finished_at = now(),
                                status = :status,
                                records_read = :read,
                                records_ingested = :ingested,
                                records_duplicate = :duplicate,
                                records_dead_lettered = :dead,
                                rows_promoted = :promoted,
                                retries_performed = :retries,
                                watermark_before = :wm_before,
                                watermark_after = :wm_after,
                                duration_seconds = :duration,
                                error_message = :error
                            WHERE batch_id = CAST(:batch AS UUID)"""
                    ),
                    {
                        "status": self.status,
                        "read": c.records_read,
                        "ingested": c.records_ingested,
                        "duplicate": c.records_duplicate,
                        "dead": c.records_dead_lettered,
                        "promoted": c.rows_promoted,
                        "retries": c.retries_performed,
                        "wm_before": c.watermark_before,
                        "wm_after": c.watermark_after,
                        "duration": round(self.duration_seconds, 3),
                        "error": self.error_message,
                        "batch": str(self.batch_id),
                    },
                )
        except SQLAlchemyError as exc:  # pragma: no cover
            logger.error("could not close the run record", extra={"error": str(exc)})

        log = logger.error if self.status == "FAILED" else logger.info
        log(
            "run finished",
            extra={
                "batch_id": str(self.batch_id),
                "command": self.command,
                "source": self.source_name,
                "status": self.status,
                "records_read": c.records_read,
                "records_ingested": c.records_ingested,
                "records_duplicate": c.records_duplicate,
                "records_dead_lettered": c.records_dead_lettered,
                "rows_promoted": c.rows_promoted,
                "retries": c.retries_performed,
                "duration_seconds": round(self.duration_seconds, 2),
                "error": self.error_message,
            },
        )
