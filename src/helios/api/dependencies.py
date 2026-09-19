"""Shared dependencies for the API.

The reading store is a process-wide singleton: loading 170 000 records on every
request would make the pagination benchmark measure JSON parsing rather than
the pipeline.
"""

from __future__ import annotations

from functools import lru_cache

from helios.api.store import ReadingStore
from helios.config import Settings, get_settings


def get_source_settings() -> Settings:
    """Settings, re-read through the cached singleton."""
    return get_settings()


@lru_cache(maxsize=1)
def get_reading_store() -> ReadingStore:
    """The process-wide reading store."""
    cfg = get_settings()
    return ReadingStore(cfg.upstream_dir / "readings.ndjson")


def reset_dependencies() -> None:
    """Clear the cached singletons. Used by tests that change the data directory."""
    get_reading_store.cache_clear()
    get_settings.cache_clear()
