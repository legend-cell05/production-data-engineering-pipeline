"""The dead-letter queue.

A record that violates its contract is parked here with its payload and the
field that failed, and the run continues.

The alternative -- failing the batch -- sounds rigorous and is not. One
malformed record out of a hundred thousand would block a hundred thousand good
ones, at 3 a.m., and the pipeline would be disabled within a week by whoever is
on call. Parking the bad record keeps the good data flowing and turns the
problem into a queue someone can work through in office hours.

The queue is not a bin. Every entry keeps the original payload, so once the
upstream problem is fixed ``helios dlq replay`` re-validates them against the
current contract and the ones that now pass are ingested. Records that keep
failing stop being retried after ``HELIOS_DLQ_MAX_ATTEMPTS`` and are marked
``ABANDONED`` -- visible, counted, and no longer pretending they will fix
themselves.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from helios.config import Settings, get_settings
from helios.db.engine import get_engine
from helios.exceptions import ContractViolation, DatabaseError
from helios.logging_config import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class DeadLetter:
    """One parked record."""

    dlq_id: int
    source_name: str
    natural_key: str
    payload: dict[str, Any]
    error_type: str
    error_message: str
    failed_field: str | None
    attempts: int
    status: str


class DeadLetterBuffer:
    """Accumulates failures and writes them in one statement.

    A per-record INSERT would turn a batch with a few thousand rejects into a
    few thousand round trips, which is how a pipeline ends up slower on bad
    data than on good data.
    """

    def __init__(self, batch_id: uuid.UUID, settings: Settings | None = None) -> None:
        self._cfg = settings or get_settings()
        self._batch_id = batch_id
        self._rows: list[dict[str, Any]] = []

    def __len__(self) -> int:
        return len(self._rows)

    def add(self, source_name: str, record: dict[str, Any], error: Exception) -> None:
        """Park one failing record."""
        failed_field = getattr(error, "field", None) or None
        natural_key = getattr(error, "natural_key", "") or ""

        self._rows.append(
            {
                "source_name": source_name,
                "natural_key": natural_key or f"unkeyed-{len(self._rows)}",
                "payload": json.dumps(record, default=str, ensure_ascii=False),
                "error_type": type(error).__name__,
                "error_message": str(error)[:2000],
                "failed_field": failed_field,
                "batch_id": str(self._batch_id),
            }
        )

    def flush(self) -> int:
        """Write the buffered failures. Returns how many were written."""
        if not self._rows:
            return 0

        cfg = self._cfg
        statement = text(
            f"""
            INSERT INTO {cfg.meta_schema}.dead_letter (
                source_name, natural_key, payload, error_type, error_message,
                failed_field, first_batch_id, last_batch_id
            )
            VALUES (
                :source_name, :natural_key, CAST(:payload AS JSONB), :error_type,
                :error_message, :failed_field, CAST(:batch_id AS UUID), CAST(:batch_id AS UUID)
            )
            ON CONFLICT (source_name, natural_key) DO UPDATE
            SET payload       = EXCLUDED.payload,
                error_type    = EXCLUDED.error_type,
                error_message = EXCLUDED.error_message,
                failed_field  = EXCLUDED.failed_field,
                -- The same record failing again is one entry with a higher
                -- attempt count, not a second entry. Otherwise a permanently
                -- broken upstream fills the table with copies of one problem.
                attempts       = {cfg.meta_schema}.dead_letter.attempts + 1,
                last_batch_id  = EXCLUDED.last_batch_id,
                last_failed_at = now(),
                status         = CASE
                    WHEN {cfg.meta_schema}.dead_letter.attempts + 1 >= :max_attempts
                    THEN 'ABANDONED' ELSE 'PENDING'
                END
            """
        )
        rows = [{**row, "max_attempts": cfg.dlq_max_attempts} for row in self._rows]
        count = len(rows)
        self._rows = []

        try:
            with get_engine(cfg).begin() as conn:
                conn.execute(statement, rows)
        except SQLAlchemyError as exc:
            raise DatabaseError(f"cannot write to the dead-letter queue: {exc}") from exc

        logger.warning("records dead-lettered", extra={"records": count})
        return count


def list_pending(
    source_name: str | None = None,
    *,
    limit: int = 500,
    settings: Settings | None = None,
) -> list[DeadLetter]:
    """Pending dead letters, oldest failure first."""
    cfg = settings or get_settings()
    try:
        with get_engine(cfg).connect() as conn:
            rows = conn.execute(
                text(
                    f"""SELECT dlq_id, source_name, natural_key, payload, error_type,
                               error_message, failed_field, attempts, status
                        FROM {cfg.meta_schema}.dead_letter
                        WHERE status = 'PENDING'
                          AND (CAST(:source AS TEXT) IS NULL OR source_name = :source)
                        ORDER BY first_failed_at
                        LIMIT :limit"""
                ),
                {"source": source_name, "limit": limit},
            ).all()
    except SQLAlchemyError as exc:
        raise DatabaseError(f"cannot read the dead-letter queue: {exc}") from exc

    return [
        DeadLetter(
            dlq_id=int(row.dlq_id),
            source_name=row.source_name,
            natural_key=row.natural_key,
            payload=row.payload,
            error_type=row.error_type,
            error_message=row.error_message,
            failed_field=row.failed_field,
            attempts=int(row.attempts),
            status=row.status,
        )
        for row in rows
    ]


def mark_resolved(dlq_ids: list[int], settings: Settings | None = None) -> int:
    """Mark dead letters as resolved after a successful replay."""
    if not dlq_ids:
        return 0
    cfg = settings or get_settings()
    try:
        with get_engine(cfg).begin() as conn:
            result = conn.execute(
                text(
                    f"""UPDATE {cfg.meta_schema}.dead_letter
                        SET status = 'RESOLVED', resolved_at = now()
                        WHERE dlq_id = ANY(:ids)"""
                ),
                {"ids": dlq_ids},
            )
    except SQLAlchemyError as exc:
        raise DatabaseError(f"cannot resolve dead letters: {exc}") from exc
    logger.info("dead letters resolved", extra={"records": result.rowcount})
    return int(result.rowcount)


def summary(settings: Settings | None = None) -> list[dict[str, Any]]:
    """Counts by source, cause and status."""
    cfg = settings or get_settings()
    try:
        with get_engine(cfg).connect() as conn:
            rows = (
                conn.execute(
                    text(f"SELECT * FROM {cfg.mart_schema}.v_dlq_overview ORDER BY records DESC")
                )
                .mappings()
                .all()
            )
    except SQLAlchemyError as exc:
        raise DatabaseError(f"cannot summarise the dead-letter queue: {exc}") from exc
    return [dict(row) for row in rows]


def is_permanent(error: Exception) -> bool:
    """Whether an error means 'this record will never be valid'.

    Only contract violations are permanent at record level. Anything else --
    a dropped connection, a timeout -- is about the *transport*, and parking
    the record would hide an infrastructure problem as a data problem.
    """
    return isinstance(error, ContractViolation)
