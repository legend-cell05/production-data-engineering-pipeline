"""Wiring: which connector serves which source.

One function builds them all, so the runner never constructs a connector and
never needs to know what kind it is. Swapping the simulated API for a real one
is a change to the ``httpx.Client`` passed in here -- nothing else moves.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import httpx

from helios.config import Settings, get_settings
from helios.contracts.registry import METER_READINGS, METERS, SITES, TARIFFS, WEATHER
from helios.exceptions import ConfigurationError
from helios.logging_config import get_logger
from helios.sources.base import Source
from helios.sources.file_sources import CsvSource, JsonSource
from helios.sources.http_source import ApiSource
from helios.sources.retry import RetryPolicy

logger = get_logger(__name__)

#: The order sources are ingested in when none is named.
#:
#: Reference before telemetry, because a reading whose meter is unknown cannot
#: be promoted and waits in raw until the next run. Ingesting in this order
#: means a new meter and its first readings land in the same pass.
DEFAULT_SOURCE_ORDER: tuple[str, ...] = ("sites", "meters", "tariffs", "weather", "meter_readings")


def list_source_names() -> tuple[str, ...]:
    """Every source this pipeline knows about, in ingestion order."""
    return DEFAULT_SOURCE_ORDER


@contextmanager
def api_client(settings: Settings | None = None) -> Iterator[httpx.Client]:
    """An HTTP client pointed at the upstream API."""
    cfg = settings or get_settings()
    client = httpx.Client(
        base_url=cfg.api_base_url,
        timeout=httpx.Timeout(cfg.api_timeout_seconds),
        headers={"Accept": "application/json", "User-Agent": "helios-pipeline"},
    )
    try:
        yield client
    finally:
        client.close()


def build_sources(
    settings: Settings | None = None, *, client: httpx.Client | None = None
) -> dict[str, Source]:
    """Construct every connector.

    Args:
        settings: Configuration.
        client: An ``httpx.Client`` for the API source. Tests pass one backed
            by ``ASGITransport`` so the FastAPI app is called in-process --
            same code path, no server, no network.
    """
    cfg = settings or get_settings()
    retry = RetryPolicy(
        max_attempts=cfg.retry_max_attempts,
        base_delay_seconds=cfg.retry_base_delay_seconds,
        max_delay_seconds=cfg.retry_max_delay_seconds,
    )

    sources: dict[str, Source] = {
        "sites": CsvSource("sites", SITES, cfg.landing_dir / "sites.csv"),
        "meters": CsvSource("meters", METERS, cfg.landing_dir / "meters.csv"),
        "weather": CsvSource("weather", WEATHER, cfg.landing_dir / "weather.csv"),
        "tariffs": JsonSource(
            "tariffs", TARIFFS, cfg.landing_dir / "tariffs.json", records_key="tariffs"
        ),
    }

    if client is not None:
        sources["meter_readings"] = ApiSource(
            "meter_readings",
            METER_READINGS,
            client,
            path="/source/readings",
            page_size=cfg.api_page_size,
            retry_policy=retry,
        )

    return sources


def get_source(
    source_name: str, settings: Settings | None = None, *, client: httpx.Client | None = None
) -> Source:
    """Return one connector by name.

    Raises:
        ConfigurationError: Unknown source, or the API source requested without
            a client.
    """
    sources = build_sources(settings, client=client)
    if source_name not in sources:
        if source_name in DEFAULT_SOURCE_ORDER:
            raise ConfigurationError(
                f"source {source_name!r} needs an HTTP client; "
                f"call build_sources(client=...) or use the CLI, which provides one"
            )
        raise ConfigurationError(
            f"unknown source {source_name!r}; expected one of {list(DEFAULT_SOURCE_ORDER)}"
        )
    return sources[source_name]
