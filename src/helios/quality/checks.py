"""Post-load data-quality checks.

Contracts judge records on the way in. These checks judge the *result*, and
answer a different question: is this warehouse fit to publish?

The checks that matter most here are the ones that catch **silence**. A
telemetry pipeline rarely fails loudly -- it keeps running while a meter stops
reporting, and the monthly total is quietly four per cent low. Freshness,
completeness and meter health are what catch that; a row count never will.

Severities:

* ``BLOCKING`` -- the run is failed and the caller is expected to stop;
* ``WARNING``  -- recorded and logged, the run continues;
* ``INFO``     -- recorded for the record.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from helios.config import Settings, get_settings
from helios.db.engine import get_engine
from helios.exceptions import DataQualityError
from helios.logging_config import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class QualityCheck:
    """One assertion about the loaded warehouse."""

    name: str
    layer: str
    severity: str
    sql: str
    assertion: Callable[[float], bool]
    expected: str
    rationale: str = ""


@dataclass(frozen=True)
class CheckResult:
    check: QualityCheck
    observed: float
    passed: bool


def build_checks(settings: Settings) -> list[QualityCheck]:
    """The check suite, built against the configured schemas."""
    raw, core, mart, meta = (
        settings.raw_schema,
        settings.core_schema,
        settings.mart_schema,
        settings.meta_schema,
    )

    return [
        # --- Structural: did the load actually happen? ---------------------
        QualityCheck(
            name="raw_not_empty",
            layer="raw",
            severity="BLOCKING",
            sql=f"SELECT COUNT(*) FROM {raw}.record",
            assertion=lambda v: v > 0,
            expected="> 0",
        ),
        QualityCheck(
            name="core_readings_not_empty",
            layer="core",
            severity="BLOCKING",
            sql=f"SELECT COUNT(*) FROM {core}.meter_reading",
            assertion=lambda v: v > 0,
            expected="> 0",
        ),
        QualityCheck(
            name="every_meter_has_a_site",
            layer="core",
            severity="BLOCKING",
            sql=(
                f"SELECT COUNT(*) FROM {core}.meter m "
                f"LEFT JOIN {core}.site s ON s.site_id = m.site_id WHERE s.site_id IS NULL"
            ),
            assertion=lambda v: v == 0,
            expected="= 0",
            rationale="Enforced by a foreign key; the check exists so a schema drift is caught too.",
        ),
        QualityCheck(
            name="no_readings_in_default_partition",
            layer="core",
            severity="WARNING",
            sql=f"SELECT COUNT(*) FROM {core}.meter_reading_default",
            assertion=lambda v: v == 0,
            expected="= 0",
            rationale=(
                "Rows here mean a timestamp fell outside every monthly partition -- "
                "usually a clock problem upstream, sometimes a partition that was never created."
            ),
        ),
        # --- The promotion actually promoted -------------------------------
        QualityCheck(
            name="raw_readings_promoted",
            layer="core",
            severity="WARNING",
            # Raw holds one row per (meter, interval, content hash); core holds
            # one per (meter, interval). The gap is corrections, and a few per
            # thousand is normal. A large gap means promotion is dropping rows.
            sql=(
                f"SELECT COALESCE(100.0 * (SELECT COUNT(*) FROM {core}.meter_reading) "
                f"/ NULLIF((SELECT COUNT(DISTINCT (payload ->> 'meter_id') || (payload ->> 'reading_ts')) "
                f"          FROM {raw}.record WHERE source_name = 'meter_readings'), 0), 0)"
            ),
            assertion=lambda v: v >= 99.0,
            expected=">= 99% of distinct raw intervals",
        ),
        # --- Freshness: the failure mode that is silent ---------------------
        QualityCheck(
            name="reading_freshness_hours",
            layer="core",
            severity="WARNING",
            sql=(
                f"SELECT COALESCE(EXTRACT(EPOCH FROM (now() - MAX(reading_ts))) / 3600.0, 9999) "
                f"FROM {core}.meter_reading"
            ),
            assertion=lambda v: v <= 48.0,
            expected="<= 48 hours",
            rationale="A pipeline that runs successfully on stale data is the expensive failure.",
        ),
        QualityCheck(
            name="no_future_readings",
            layer="core",
            severity="WARNING",
            sql=(
                f"SELECT COUNT(*) FROM {core}.meter_reading "
                f"WHERE reading_ts > now() + INTERVAL '15 minutes'"
            ),
            assertion=lambda v: v == 0,
            expected="= 0",
            rationale="A reading from the future is a clock problem upstream, and it poisons freshness.",
        ),
        QualityCheck(
            name="all_sources_have_a_watermark",
            layer="meta",
            severity="WARNING",
            sql=f"SELECT COUNT(*) FROM {meta}.source_watermark WHERE watermark_value IS NOT NULL",
            assertion=lambda v: v >= 5,
            expected=">= 5 sources",
        ),
        # --- Completeness ---------------------------------------------------
        QualityCheck(
            name="interval_completeness_pct",
            layer="mart",
            severity="WARNING",
            # Excludes the first and last day, which are partial by construction.
            sql=(
                f"""SELECT COALESCE(AVG(completeness_pct), 0)
                    FROM {mart}.v_interval_completeness
                    WHERE reading_date > (SELECT MIN(reading_date) FROM {mart}.v_interval_completeness)
                      AND reading_date < (SELECT MAX(reading_date) FROM {mart}.v_interval_completeness)"""
            ),
            assertion=lambda v: v >= 95.0,
            expected=">= 95%",
        ),
        QualityCheck(
            name="meters_reporting_pct",
            layer="mart",
            severity="BLOCKING",
            sql=(
                f"""SELECT COALESCE(100.0 * COUNT(DISTINCT c.meter_id)
                        / NULLIF((SELECT COUNT(*) FROM {core}.meter WHERE is_active), 0), 0)
                    FROM {mart}.consumption_interval c"""
            ),
            assertion=lambda v: v >= 90.0,
            expected=">= 90% of active meters",
        ),
        # --- Derived values are sane ---------------------------------------
        QualityCheck(
            name="usable_interval_pct",
            layer="mart",
            severity="WARNING",
            sql=(
                f"""SELECT COALESCE(100.0 * COUNT(*) FILTER (WHERE {mart}.is_usable(delta_flag))
                        / NULLIF(COUNT(*), 0), 0)
                    FROM {mart}.consumption_interval"""
            ),
            assertion=lambda v: v >= 97.0,
            expected=">= 97% of intervals usable",
        ),
        QualityCheck(
            name="no_negative_consumption",
            layer="mart",
            severity="BLOCKING",
            sql=f"SELECT COUNT(*) FROM {mart}.consumption_interval WHERE consumption_kwh < 0",
            assertion=lambda v: v == 0,
            expected="= 0",
            rationale="A negative delta means the rollover/reset logic let one through.",
        ),
        QualityCheck(
            name="reset_rate_pct",
            layer="mart",
            severity="WARNING",
            sql=(
                f"""SELECT COALESCE(100.0 * COUNT(*) FILTER (WHERE delta_flag = 'reset')
                        / NULLIF(COUNT(*), 0), 0)
                    FROM {mart}.consumption_interval"""
            ),
            assertion=lambda v: v < 1.0,
            expected="< 1% of intervals",
            rationale="A jump in unexplained backward steps usually means an upstream format change.",
        ),
        # --- Tariffs: the silent-corruption case ----------------------------
        QualityCheck(
            name="tariff_bands_tile_the_day",
            layer="core",
            severity="BLOCKING",
            # Bands are joined on `hour >= start AND hour < end`. A gap silently
            # drops consumption from the cost; an overlap silently doubles it.
            # Neither shows up as an error anywhere else.
            sql=(
                f"""SELECT COUNT(*) FROM (
                        SELECT tariff_id, SUM(hour_end - hour_start) AS covered
                        FROM {core}.tariff_band GROUP BY tariff_id
                    ) t WHERE covered <> 24"""
            ),
            assertion=lambda v: v == 0,
            expected="= 0 tariffs with imperfect coverage",
        ),
        QualityCheck(
            name="every_site_has_a_tariff",
            layer="core",
            severity="WARNING",
            sql=(
                f"SELECT COUNT(*) FROM {core}.site s "
                f"LEFT JOIN {core}.site_tariff st ON st.site_id = s.site_id "
                f"WHERE st.site_id IS NULL"
            ),
            assertion=lambda v: v == 0,
            expected="= 0",
        ),
        # --- Dead letters ---------------------------------------------------
        QualityCheck(
            name="dead_letter_rate_pct",
            layer="meta",
            severity="WARNING",
            sql=(
                f"""SELECT COALESCE(100.0 *
                        (SELECT COUNT(*) FROM {meta}.dead_letter WHERE status = 'PENDING')
                        / NULLIF((SELECT COUNT(*) FROM {raw}.record), 0), 0)"""
            ),
            assertion=lambda v: v < 0.5,
            expected="< 0.5% of ingested records",
        ),
        QualityCheck(
            name="dead_letters_abandoned",
            layer="meta",
            severity="INFO",
            sql=f"SELECT COUNT(*) FROM {meta}.dead_letter WHERE status = 'ABANDONED'",
            assertion=lambda v: True,
            expected="informational",
        ),
        QualityCheck(
            name="weather_covers_reading_period",
            layer="core",
            severity="WARNING",
            sql=(
                f"""SELECT COALESCE(100.0 * (
                        SELECT COUNT(DISTINCT observed_on) FROM {core}.weather_daily
                        WHERE observed_on >= (SELECT MIN(reading_ts)::DATE FROM {core}.meter_reading)
                    ) / NULLIF((
                        SELECT MAX(reading_ts)::DATE - MIN(reading_ts)::DATE + 1
                        FROM {core}.meter_reading
                    ), 0), 0)"""
            ),
            assertion=lambda v: v >= 90.0,
            expected=">= 90% of reading days have weather",
            rationale="Degree-day normalisation is silently wrong wherever weather is missing.",
        ),
    ]


def run_quality_checks(
    batch_id: uuid.UUID,
    settings: Settings | None = None,
    *,
    raise_on_blocking: bool = True,
) -> list[CheckResult]:
    """Run every check, persist the results, optionally fail on a blocker."""
    cfg = settings or get_settings()
    engine = get_engine(cfg)
    results: list[CheckResult] = []

    for check in build_checks(cfg):
        try:
            with engine.connect() as conn:
                raw_value = conn.execute(text(check.sql)).scalar_one()
        except SQLAlchemyError as exc:
            raise DataQualityError(
                f"check {check.name!r} could not be evaluated: {exc}", check_name=check.name
            ) from exc

        observed = float(raw_value if raw_value is not None else 0)
        passed = check.assertion(observed)
        results.append(CheckResult(check=check, observed=observed, passed=passed))

        log = (
            logger.info
            if passed
            else (logger.error if check.severity == "BLOCKING" else logger.warning)
        )
        log(
            "quality check",
            extra={
                "check": check.name,
                "layer": check.layer,
                "severity": check.severity,
                "observed": round(observed, 4),
                "expected": check.expected,
                "passed": passed,
            },
        )

    _persist(batch_id, results, cfg)

    blocking = [r for r in results if not r.passed and r.check.severity == "BLOCKING"]
    if blocking and raise_on_blocking:
        first = blocking[0]
        raise DataQualityError(
            f"{len(blocking)} blocking data-quality check(s) failed; first: "
            f"{first.check.name} observed={first.observed:g}, expected {first.check.expected}",
            check_name=first.check.name,
            observed=first.observed,
        )
    return results


def _persist(batch_id: uuid.UUID, results: list[CheckResult], settings: Settings) -> None:
    """Store the results for this batch."""
    rows = [
        {
            "batch_id": str(batch_id),
            "check_name": r.check.name,
            "layer": r.check.layer,
            "severity": r.check.severity,
            "passed": r.passed,
            "observed_value": f"{r.observed:.4f}",
            "expected_value": r.check.expected,
            "details": r.check.rationale or None,
        }
        for r in results
    ]
    statement = text(
        f"""
        INSERT INTO {settings.meta_schema}.quality_result
            (batch_id, check_name, layer, severity, passed,
             observed_value, expected_value, details)
        VALUES (CAST(:batch_id AS UUID), :check_name, :layer, :severity, :passed,
                :observed_value, :expected_value, :details)
        ON CONFLICT (batch_id, check_name) DO UPDATE
        SET passed = EXCLUDED.passed,
            observed_value = EXCLUDED.observed_value,
            checked_at = now()
        """
    )
    try:
        with get_engine(settings).begin() as conn:
            conn.execute(statement, rows)
    except SQLAlchemyError as exc:  # pragma: no cover
        logger.error("could not persist quality results", extra={"error": str(exc)})
