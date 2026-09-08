"""Promotion raw -> core, and refresh of the consumption mart.

All of this is set-based SQL executed inside the database. No data crosses the
network: moving 170 000 rows into Python to type them and send them back would
be slower by an order of magnitude and would add nothing.

Every operation is idempotent -- upserts on natural keys, and a
delete-then-insert per day for the mart -- so a re-run, a backfill and a
dead-letter replay all converge on the same result.
"""

from __future__ import annotations

import datetime as dt
import uuid

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from helios.config import Settings, get_settings
from helios.db.engine import get_engine
from helios.db.schema import ensure_reading_partitions
from helios.db.sql_files import load_rendered_sql, split_statements
from helios.exceptions import LoadError
from helios.logging_config import get_logger

logger = get_logger(__name__)


def promote_reference(
    batch_id: uuid.UUID | None = None, settings: Settings | None = None
) -> dict[str, int]:
    """Promote sites, meters, tariffs and weather from raw into core.

    Args:
        batch_id: Promote only this batch, or ``None`` to rebuild core from the
            whole of raw. Both give the same result; scoping is an optimisation.

    Returns:
        Row counts per core table after promotion.
    """
    cfg = settings or get_settings()
    engine = get_engine(cfg)
    sql = load_rendered_sql("queries/promote_reference.sql", cfg)
    parameter = {"batch_id": str(batch_id) if batch_id else None}

    try:
        with engine.begin() as conn:
            for statement in split_statements(sql):
                conn.execute(text(statement), parameter)
            counts = {
                table: int(
                    conn.execute(
                        text(f"SELECT COUNT(*) FROM {cfg.core_schema}.{table}")
                    ).scalar_one()
                )
                for table in (
                    "site",
                    "meter",
                    "tariff",
                    "tariff_band",
                    "site_tariff",
                    "weather_station",
                    "weather_daily",
                )
            }
    except SQLAlchemyError as exc:
        raise LoadError(f"reference promotion failed: {exc}") from exc

    logger.info("reference promoted", extra=counts)
    return counts


def promote_readings(batch_id: uuid.UUID | None = None, settings: Settings | None = None) -> int:
    """Promote meter readings from raw into the partitioned core table.

    Partitions are created first, for exactly the months the batch covers. A
    partitioned table whose partitions are created by hand breaks at midnight
    on the first of the month, every month, until someone automates it.

    Returns:
        Rows now present in ``core.meter_reading``.
    """
    cfg = settings or get_settings()
    engine = get_engine(cfg)

    span = _reading_span(batch_id, cfg)
    if span is None:
        logger.info("no readings in raw to promote", extra={"batch_id": str(batch_id)})
        return _reading_count(cfg)

    ensure_reading_partitions(span[0], span[1], cfg)

    sql = load_rendered_sql("queries/promote_readings.sql", cfg)
    try:
        with engine.begin() as conn:
            for statement in split_statements(sql):
                conn.execute(text(statement), {"batch_id": str(batch_id) if batch_id else None})
    except SQLAlchemyError as exc:
        raise LoadError(f"reading promotion failed: {exc}") from exc

    total = _reading_count(cfg)
    logger.info(
        "readings promoted",
        extra={"rows_in_core": total, "from": str(span[0]), "to": str(span[1])},
    )
    return total


def _reading_span(batch_id: uuid.UUID | None, cfg: Settings) -> tuple[dt.date, dt.date] | None:
    """Earliest and latest reading timestamp waiting in raw."""
    engine = get_engine(cfg)
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    f"""SELECT MIN((payload ->> 'reading_ts')::TIMESTAMPTZ) AS lo,
                               MAX((payload ->> 'reading_ts')::TIMESTAMPTZ) AS hi
                        FROM {cfg.raw_schema}.record
                        WHERE source_name = 'meter_readings'
                          AND (CAST(:batch AS UUID) IS NULL OR batch_id = CAST(:batch AS UUID))"""
                ),
                {"batch": str(batch_id) if batch_id else None},
            ).one()
    except SQLAlchemyError as exc:
        raise LoadError(f"cannot determine the reading span: {exc}") from exc

    if row.lo is None or row.hi is None:
        return None
    return row.lo.date(), row.hi.date()


def _reading_count(cfg: Settings) -> int:
    with get_engine(cfg).connect() as conn:
        return int(
            conn.execute(text(f"SELECT COUNT(*) FROM {cfg.core_schema}.meter_reading")).scalar_one()
        )


def refresh_consumption(
    from_date: dt.date | None = None,
    to_date: dt.date | None = None,
    settings: Settings | None = None,
) -> dict[str, int]:
    """Rebuild ``mart.consumption_interval`` for a date window.

    Defaults to the full span held in core. Passing a narrow window is what
    makes an incremental refresh cheap: yesterday's marts can be rebuilt
    without touching three months of history.

    Returns:
        Rows in the mart, and how many carry a flag other than ``ok``.
    """
    cfg = settings or get_settings()
    engine = get_engine(cfg)

    if from_date is None or to_date is None:
        span = _core_reading_span(cfg)
        if span is None:
            logger.info("no readings in core; nothing to refresh")
            return {"rows": 0, "flagged": 0}
        from_date = from_date or span[0]
        to_date = to_date or span[1]

    sql = load_rendered_sql("queries/refresh_consumption.sql", cfg)
    parameters = {
        "from_date": from_date,
        "to_date": to_date,
        "interval_minutes": cfg.interval_minutes,
    }

    try:
        with engine.begin() as conn:
            for statement in split_statements(sql):
                conn.execute(text(statement), parameters)
            rows = int(
                conn.execute(
                    text(f"SELECT COUNT(*) FROM {cfg.mart_schema}.consumption_interval")
                ).scalar_one()
            )
            flagged = int(
                conn.execute(
                    text(
                        f"""SELECT COUNT(*) FROM {cfg.mart_schema}.consumption_interval
                            WHERE delta_flag <> 'ok'"""
                    )
                ).scalar_one()
            )
    except SQLAlchemyError as exc:
        raise LoadError(f"consumption refresh failed: {exc}") from exc

    logger.info(
        "consumption refreshed",
        extra={"from": str(from_date), "to": str(to_date), "rows": rows, "flagged": flagged},
    )
    return {"rows": rows, "flagged": flagged}


def _core_reading_span(cfg: Settings) -> tuple[dt.date, dt.date] | None:
    """Date range covered by core, in the business timezone."""
    with get_engine(cfg).connect() as conn:
        row = conn.execute(
            text(
                f"""SELECT MIN((reading_ts AT TIME ZONE 'Europe/Paris')::DATE) AS lo,
                           MAX((reading_ts AT TIME ZONE 'Europe/Paris')::DATE) AS hi
                    FROM {cfg.core_schema}.meter_reading"""
            )
        ).one()
    if row.lo is None:
        return None
    return row.lo, row.hi
