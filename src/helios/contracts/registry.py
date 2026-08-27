"""The contract for every source.

This module is the data contract of the whole pipeline. If you want to know
what Helios accepts, read this file -- not the transformation code.

Versioning rule: a change that makes previously valid records invalid (a new
required field, a narrowed enum, a tighter bound) is a **major** bump and needs
a migration plan for the dead letters it will create. Widening -- a new
optional field, a relaxed bound -- is a minor bump and is safe to deploy.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from helios.config import Settings, get_settings
from helios.contracts.models import Contract, FieldSpec
from helios.db.engine import get_engine
from helios.exceptions import ConfigurationError
from helios.logging_config import get_logger

logger = get_logger(__name__)


METER_READINGS = Contract(
    source_name="meter_readings",
    version="1.2.0",
    description="Interval readings from the metering platform's REST API.",
    natural_key_fields=("meter_id", "reading_ts"),
    updated_at_field="updated_at",
    fields=(
        FieldSpec(
            "meter_id",
            "string",
            required=True,
            max_length=32,
            description="Device identifier, unique across the estate.",
        ),
        FieldSpec(
            "reading_ts",
            "timestamp",
            required=True,
            description="Start of the interval the reading closes, UTC.",
        ),
        FieldSpec(
            "register_value",
            "number",
            required=True,
            minimum=0,
            maximum=1e12,
            description=(
                "Cumulative register value BEFORE the meter multiplier. "
                "Negative is impossible on a physical register, so it is a "
                "violation rather than a value to correct."
            ),
        ),
        FieldSpec(
            "quality_flag",
            "string",
            required=False,
            allowed=frozenset({"measured", "estimated", "substituted", "suspect"}),
            description="How the value was obtained. A new value here means upstream changed.",
        ),
        FieldSpec(
            "updated_at",
            "timestamp",
            required=True,
            description="When the record became available upstream. Drives the watermark.",
        ),
    ),
)


SITES = Contract(
    source_name="sites",
    version="1.0.0",
    description="Nightly CSV export of the client site register.",
    natural_key_fields=("site_id",),
    updated_at_field="updated_at",
    fields=(
        FieldSpec("site_id", "string", required=True, max_length=16),
        FieldSpec("site_name", "string", required=True, max_length=120),
        FieldSpec("city", "string", required=True, max_length=80),
        FieldSpec("region_code", "string", required=True, max_length=8),
        FieldSpec(
            "sector",
            "string",
            required=True,
            allowed=frozenset(
                {
                    "Office",
                    "Retail",
                    "Industrial",
                    "Logistics",
                    "Data centre",
                    "Healthcare",
                    "Education",
                }
            ),
        ),
        FieldSpec("floor_area_m2", "number", required=True, minimum=1, maximum=1e7),
        FieldSpec("commissioned_on", "date", required=False),
        FieldSpec("is_active", "boolean", required=False),
        FieldSpec("tariff_id", "string", required=False, max_length=32),
        FieldSpec("valid_from", "date", required=False),
        FieldSpec("valid_to", "date", required=False),
        FieldSpec("updated_at", "timestamp", required=True),
    ),
)


METERS = Contract(
    source_name="meters",
    version="1.1.0",
    description="Nightly CSV export of the meter register.",
    natural_key_fields=("meter_id",),
    updated_at_field="updated_at",
    fields=(
        FieldSpec("meter_id", "string", required=True, max_length=32),
        FieldSpec("site_id", "string", required=True, max_length=16),
        FieldSpec(
            "meter_type",
            "string",
            required=True,
            allowed=frozenset(
                {"main_incomer", "hvac", "lighting", "process", "ev_charging", "submeter"}
            ),
        ),
        FieldSpec("unit", "string", required=False, allowed=frozenset({"kWh", "MWh"})),
        FieldSpec("multiplier", "number", required=False, minimum=0.0001, maximum=10_000),
        FieldSpec(
            "index_digits",
            "integer",
            required=False,
            minimum=4,
            maximum=12,
            description="Register width. Needed to tell a rollover from a reset.",
        ),
        FieldSpec(
            "interval_minutes",
            "integer",
            required=False,
            minimum=1,
            maximum=1440,
            description="Configured reporting interval. Needed to compute completeness.",
        ),
        FieldSpec("installed_on", "date", required=False),
        FieldSpec("is_active", "boolean", required=False),
        FieldSpec("updated_at", "timestamp", required=True),
    ),
)


TARIFFS = Contract(
    source_name="tariffs",
    version="1.0.0",
    description="Supplier tariff catalogue, JSON, with time bands nested per tariff.",
    natural_key_fields=("tariff_id",),
    updated_at_field="updated_at",
    fields=(
        FieldSpec("tariff_id", "string", required=True, max_length=32),
        FieldSpec("tariff_name", "string", required=True, max_length=120),
        FieldSpec("supplier", "string", required=True, max_length=120),
        FieldSpec("currency", "string", required=False, allowed=frozenset({"EUR", "GBP", "CHF"})),
        FieldSpec("standing_charge_per_day", "number", required=False, minimum=0, maximum=1000),
        FieldSpec("valid_from", "date", required=True),
        FieldSpec("valid_to", "date", required=False),
        FieldSpec(
            "bands",
            "array",
            required=True,
            description="Time bands. Flattened into core.tariff_band during promotion.",
        ),
        FieldSpec("updated_at", "timestamp", required=True),
    ),
)


WEATHER = Contract(
    source_name="weather",
    version="1.0.0",
    description="Daily observations per weather station, CSV.",
    natural_key_fields=("station_id", "observed_on"),
    updated_at_field="updated_at",
    fields=(
        FieldSpec("station_id", "string", required=True, max_length=32),
        FieldSpec("station_name", "string", required=True, max_length=120),
        FieldSpec("region_code", "string", required=True, max_length=8),
        FieldSpec("observed_on", "date", required=True),
        FieldSpec("temp_min_c", "number", required=True, minimum=-60, maximum=60),
        FieldSpec("temp_max_c", "number", required=True, minimum=-60, maximum=60),
        FieldSpec("temp_avg_c", "number", required=True, minimum=-60, maximum=60),
        FieldSpec("updated_at", "timestamp", required=True),
    ),
)


CONTRACTS: dict[str, Contract] = {
    c.source_name: c for c in (METER_READINGS, SITES, METERS, TARIFFS, WEATHER)
}


def get_contract(source_name: str) -> Contract:
    """Return a contract by source name.

    Raises:
        ConfigurationError: For an unknown source -- an allow-list, because the
            name ends up selecting SQL and files.
    """
    try:
        return CONTRACTS[source_name]
    except KeyError as exc:
        raise ConfigurationError(
            f"unknown source {source_name!r}; expected one of {sorted(CONTRACTS)}"
        ) from exc


def register_contracts(settings: Settings | None = None) -> int:
    """Record the current contract versions in ``meta.schema_contract``.

    Called on every ``init-db``. Storing the definition, not just the version
    number, means a row ingested months ago can be checked against the rules
    that were actually in force -- rather than against whatever the code says
    today.
    """
    cfg = settings or get_settings()
    engine = get_engine(cfg)
    statement = text(
        f"""
        INSERT INTO {cfg.meta_schema}.schema_contract (source_name, contract_version, definition)
        VALUES (:source_name, :version, CAST(:definition AS JSONB))
        ON CONFLICT (source_name, contract_version) DO UPDATE
        SET definition = EXCLUDED.definition
        """
    )
    import json

    rows = [
        {
            "source_name": c.source_name,
            "version": c.version,
            "definition": json.dumps(c.to_json(), ensure_ascii=False),
        }
        for c in CONTRACTS.values()
    ]
    try:
        with engine.begin() as conn:
            conn.execute(statement, rows)
    except SQLAlchemyError as exc:  # pragma: no cover - persistence failure
        logger.error("could not register contracts", extra={"error": str(exc)})
        return 0

    logger.info("contracts registered", extra={"contracts": len(rows)})
    return len(rows)
