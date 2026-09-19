"""Shared fixtures.

Unit tests run anywhere with no external dependency. Integration tests need a
live PostgreSQL and are skipped unless ``HELIOS_RUN_INTEGRATION=1``, so a plain
``pytest`` passes on a laptop with no database while CI runs the full suite.

The API is exercised through Starlette's ``TestClient``, which is an
``httpx.Client`` talking to the ASGI app in-process. That is not a mock: the
real routing, real pagination, real rate limiting and real fault injection all
run. It just does so without a socket.
"""

from __future__ import annotations

import datetime as dt
import os
from collections.abc import Iterator

import httpx
import pytest
from starlette.testclient import TestClient

from helios.config import Settings

RUN_INTEGRATION = os.environ.get("HELIOS_RUN_INTEGRATION", "0") == "1"

requires_database = pytest.mark.skipif(
    not RUN_INTEGRATION,
    reason="integration test: set HELIOS_RUN_INTEGRATION=1 with a live PostgreSQL",
)


@pytest.fixture
def settings(tmp_path) -> Settings:
    """Small, fast settings pointing at a temporary data directory."""
    return Settings(
        data_dir=tmp_path,
        random_seed=4242,
        n_sites=6,
        n_meters=8,
        history_days=3,
        interval_minutes=15,
        gap_rate=0.02,
        reset_rate=0.25,
        log_level="WARNING",
        # Deterministic by default: fault injection is enabled explicitly by the
        # tests that are about fault injection.
        source_fault_rate=0.0,
        source_rate_limit_rate=0.0,
    )


@pytest.fixture
def seeded_settings(settings: Settings) -> Settings:
    """Settings whose upstream files have been generated."""
    from helios.generation import generate_upstream, write_upstream

    write_upstream(generate_upstream(settings), settings)
    return settings


@pytest.fixture
def api_client(seeded_settings: Settings, monkeypatch) -> Iterator[httpx.Client]:
    """An httpx client wired to the app, reading the temporary upstream store."""
    from helios.api import dependencies
    from helios.api.app import create_app
    from helios.config import get_settings

    monkeypatch.setenv("HELIOS_DATA_DIR", str(seeded_settings.data_dir))
    monkeypatch.setenv("HELIOS_SOURCE_FAULT_RATE", "0")
    monkeypatch.setenv("HELIOS_SOURCE_RATE_LIMIT_RATE", "0")
    get_settings.cache_clear()
    dependencies.get_reading_store.cache_clear()

    with TestClient(create_app(), base_url="http://upstream") as client:
        yield client

    get_settings.cache_clear()
    dependencies.get_reading_store.cache_clear()


@pytest.fixture
def utc_now() -> dt.datetime:
    return dt.datetime.now(tz=dt.UTC)


@pytest.fixture
def valid_reading(utc_now: dt.datetime) -> dict[str, object]:
    """One record that satisfies the meter_readings contract."""
    return {
        "meter_id": "M00001",
        "reading_ts": (utc_now - dt.timedelta(minutes=15)).isoformat(),
        "register_value": 123456.789,
        "quality_flag": "measured",
        "updated_at": utc_now.isoformat(),
    }
