"""DDL application and partition management.

The numbered files under ``sql/schema`` and ``sql/marts`` are executed in order,
each inside its own transaction so a partial schema is never left behind.

Unlike the reference-data project this repository is a sibling of, the DDL here
is written to be **additive**: every statement is ``CREATE ... IF NOT EXISTS``
or ``CREATE OR REPLACE``. Applying it to a database that already holds a
million readings adds the missing objects and touches nothing else, which is
what lets ``helios init-db`` be safe to run on every deploy.
"""

from __future__ import annotations

import datetime as dt
import time

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from helios.config import Settings, get_settings
from helios.db.engine import get_engine
from helios.db.sql_files import list_sql_files, render_sql
from helios.exceptions import DatabaseError
from helios.logging_config import get_logger

logger = get_logger(__name__)


def _execute_sql_file(name: str, sql: str, settings: Settings) -> None:
    """Execute one rendered SQL file in a single transaction."""
    engine = get_engine(settings)
    started = time.perf_counter()
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
    except SQLAlchemyError as exc:
        raise DatabaseError(f"failed to execute {name}: {exc}") from exc
    logger.info(
        "sql file applied",
        extra={"file": name, "duration_seconds": round(time.perf_counter() - started, 3)},
    )


def create_schema(settings: Settings | None = None) -> list[str]:
    """Create schemas, tables, constraints, indexes and partitions."""
    cfg = settings or get_settings()
    applied: list[str] = []
    for path in list_sql_files("schema"):
        _execute_sql_file(path.name, render_sql(path.read_text(encoding="utf-8"), cfg), cfg)
        applied.append(path.name)
    logger.info("schema applied", extra={"files": len(applied)})
    return applied


def create_marts(settings: Settings | None = None) -> list[str]:
    """Create or replace the mart views.

    Idempotent by construction, so this runs after every deploy and after any
    change to a business definition -- without touching data.
    """
    cfg = settings or get_settings()
    applied: list[str] = []
    for path in list_sql_files("marts"):
        _execute_sql_file(path.name, render_sql(path.read_text(encoding="utf-8"), cfg), cfg)
        applied.append(path.name)
    logger.info("marts applied", extra={"files": len(applied)})
    return applied


def initialise_database(settings: Settings | None = None) -> dict[str, list[str]]:
    """Apply the full DDL: schema first, then marts."""
    cfg = settings or get_settings()
    return {"schema": create_schema(cfg), "marts": create_marts(cfg)}


def ensure_reading_partitions(
    start: dt.date, end: dt.date, settings: Settings | None = None
) -> list[str]:
    """Create the monthly reading partitions covering ``[start, end]``.

    Called by the loader before every write. Creating partitions lazily like
    this is what stops the first insert after midnight on the 1st of a month
    from failing -- the classic way a partitioned table breaks in production.

    Returns:
        The partition names that now exist for the range.
    """
    cfg = settings or get_settings()
    engine = get_engine(cfg)

    months: list[dt.date] = []
    cursor = start.replace(day=1)
    last = end.replace(day=1)
    while cursor <= last:
        months.append(cursor)
        cursor = (cursor + dt.timedelta(days=32)).replace(day=1)

    created: list[str] = []
    try:
        with engine.begin() as conn:
            for month in months:
                name = conn.execute(
                    text(f"SELECT {cfg.core_schema}.ensure_reading_partition(CAST(:m AS DATE))"),
                    {"m": month},
                ).scalar_one()
                created.append(str(name))
    except SQLAlchemyError as exc:
        raise DatabaseError(f"failed to create reading partitions: {exc}") from exc

    logger.info(
        "reading partitions ensured",
        extra={"months": len(months), "from": str(start), "to": str(end)},
    )
    return created


def list_reading_partitions(settings: Settings | None = None) -> list[tuple[str, int]]:
    """Return each reading partition with its exact row count.

    Exact rather than ``pg_stat_user_tables.n_live_tup``: the statistics are
    only populated by autovacuum, so a freshly created partition reports zero
    rows however many it holds -- which is exactly the moment you want to look.
    There are a handful of partitions, so counting them is cheap.
    """
    cfg = settings or get_settings()
    engine = get_engine(cfg)
    names_query = text(
        """
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class      c ON c.oid = i.inhrelid
        JOIN pg_class      p ON p.oid = i.inhparent
        JOIN pg_namespace  n ON n.oid = p.relnamespace
        WHERE p.relname = 'meter_reading' AND n.nspname = :schema
        ORDER BY c.relname
        """
    )
    try:
        with engine.connect() as conn:
            names = [row[0] for row in conn.execute(names_query, {"schema": cfg.core_schema})]
            # The partition name comes from the catalogue, not from user input,
            # and is quoted before it reaches the statement.
            return [
                (
                    name,
                    int(
                        conn.execute(
                            text(f'SELECT COUNT(*) FROM {cfg.core_schema}."{name}"')
                        ).scalar_one()
                    ),
                )
                for name in names
            ]
    except SQLAlchemyError as exc:
        raise DatabaseError(f"cannot list partitions: {exc}") from exc


def drop_schemas(settings: Settings | None = None) -> None:
    """Drop all four schemas. Destructive -- used by tests and ``helios reset``."""
    cfg = settings or get_settings()
    engine = get_engine(cfg)
    try:
        with engine.begin() as conn:
            for schema in (cfg.mart_schema, cfg.core_schema, cfg.raw_schema, cfg.meta_schema):
                conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
    except SQLAlchemyError as exc:
        raise DatabaseError(f"failed to drop schemas: {exc}") from exc
    logger.warning("schemas dropped", extra={"schemas": 4})
