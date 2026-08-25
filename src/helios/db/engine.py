"""Engine and connection management.

One engine per connection URL, created lazily and pooled. ``pool_pre_ping`` is
on because the database lives in another container: a connection can be dropped
between two pipeline stages, and a stale one must be detected and replaced
rather than surfacing as a random failure halfway through a load.

``raw_connection`` exposes the underlying psycopg connection, which is what the
COPY loader needs -- SQLAlchemy has no API for ``COPY ... FROM STDIN``.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from helios.config import Settings, get_settings
from helios.exceptions import DatabaseError
from helios.logging_config import get_logger

logger = get_logger(__name__)

#: Settings is a mutable Pydantic model and therefore unhashable, so the cache
#: is keyed on the connection URL rather than on the object.
_ENGINES: dict[str, Engine] = {}


def get_engine(settings: Settings | None = None) -> Engine:
    """Return the engine for these settings, creating it once."""
    cfg = settings or get_settings()
    url = cfg.sqlalchemy_url

    cached = _ENGINES.get(url)
    if cached is not None:
        return cached

    try:
        engine = create_engine(
            url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=5,
            pool_recycle=1800,
            future=True,
        )
    except SQLAlchemyError as exc:  # pragma: no cover - configuration failure
        raise DatabaseError(f"cannot create the database engine: {exc}") from exc

    _ENGINES[url] = engine
    logger.debug("engine created", extra={"dsn": cfg.safe_dsn})
    return engine


def dispose_engines() -> None:
    """Close every pooled connection. Used by test teardown."""
    for engine in _ENGINES.values():
        engine.dispose()
    _ENGINES.clear()


def check_connection(
    settings: Settings | None = None,
    *,
    retries: int = 1,
    delay_seconds: float = 2.0,
) -> bool:
    """Verify the database answers, retrying while it may still be starting."""
    cfg = settings or get_settings()
    engine = get_engine(cfg)

    for attempt in range(retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("database reachable", extra={"dsn": cfg.safe_dsn})
            return True
        except SQLAlchemyError as exc:
            if attempt == retries:
                logger.error(
                    "database unreachable",
                    extra={"dsn": cfg.safe_dsn, "error": str(exc).splitlines()[0]},
                )
                return False
            logger.warning(
                "database not ready, retrying",
                extra={"attempt": attempt + 1, "max_attempts": retries + 1},
            )
            time.sleep(delay_seconds)
    return False


@contextmanager
def raw_connection(settings: Settings | None = None) -> Iterator[Any]:
    """Yield the underlying psycopg connection, committing on success.

    Needed for ``COPY ... FROM STDIN``: it is a protocol-level operation that
    SQLAlchemy does not wrap. The connection is returned to the pool afterwards.
    """
    engine = get_engine(settings)
    sa_connection = engine.raw_connection()
    try:
        yield sa_connection.driver_connection
        sa_connection.commit()
    except Exception:
        sa_connection.rollback()
        raise
    finally:
        sa_connection.close()
