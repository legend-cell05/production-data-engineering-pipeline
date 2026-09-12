"""Integration tests for the consumption mart and the analytical views.

The most important test here is ``TestDeltaClassification``. It builds a meter
whose history contains, by construction, one of every awkward case -- a clean
step, a gap, a register rollover, an unexplained reset, a flat interval and a
tiny backward correction -- and asserts that each one is classified correctly
and contributes (or does not contribute) the right amount.

That fixture is hand-built rather than generated, because a test that asserts
on generated data is only ever asserting that two pieces of code agree with
each other.
"""

from __future__ import annotations

import datetime as dt
import uuid

import pytest
from sqlalchemy import text

from helios.config import Settings
from helios.db.engine import get_engine
from helios.db.schema import (
    drop_schemas,
    ensure_reading_partitions,
    initialise_database,
)
from helios.load.promote import refresh_consumption
from tests.conftest import requires_database

pytestmark = [requires_database, pytest.mark.integration]

BASE = dt.datetime(2026, 3, 2, 0, 0, tzinfo=dt.UTC)


@pytest.fixture(scope="module")
def mart_settings(tmp_path_factory) -> Settings:
    return Settings(
        data_dir=tmp_path_factory.mktemp("helios-marts"),
        raw_schema="test_mart_raw",
        core_schema="test_mart_core",
        mart_schema="test_mart_mart",
        meta_schema="test_mart_meta",
        interval_minutes=15,
        log_level="WARNING",
    )


@pytest.fixture(scope="module")
def controlled_warehouse(mart_settings: Settings):
    """A warehouse holding one meter with a hand-built, fully known history.

    Register: 6 digits, multiplier 1, so it wraps at 1 000 000 kWh.
    Each step below is exactly one 15-minute interval unless stated.
    """
    drop_schemas(mart_settings)
    initialise_database(mart_settings)
    ensure_reading_partitions(BASE.date(), BASE.date(), mart_settings)

    core = mart_settings.core_schema
    engine = get_engine(mart_settings)
    batch = str(uuid.uuid4())

    #  #  offset(min)  index      what it represents
    #  0        0      999_900.0  start, near the top of the register
    #  1       15      999_950.0  clean step of 50
    #  2       30      999_990.0  clean step of 40
    #  3       45           30.0  ROLLOVER: wrapped, real consumption 40
    #  4       60           80.0  clean step of 50
    #  5       90          180.0  GAP: 30 minutes, consumption 100
    #  6      105          180.0  FLAT: nothing consumed
    #  7      120          179.5  CORRECTION: index nudged down by 0.5
    #  8      135       50_000.0  RESET-like jump is forward, so implausible
    #  9      150            5.0  RESET: backwards and unexplained
    readings = [
        (0, 999_900.0),
        (15, 999_950.0),
        (30, 999_990.0),
        (45, 30.0),
        (60, 80.0),
        (90, 180.0),
        (105, 180.0),
        (120, 179.5),
        (135, 50_000.0),
        (150, 5.0),
    ]

    with engine.begin() as conn:
        conn.execute(
            text(
                f"""INSERT INTO {core}.site
                    (site_id, site_name, city, region_code, sector, floor_area_m2,
                     is_active, source_updated_at)
                    VALUES ('S-TEST', 'Test site', 'Paris', 'IDF', 'Office', 1000, TRUE, now())
                    ON CONFLICT (site_id) DO NOTHING"""
            )
        )
        conn.execute(
            text(
                f"""INSERT INTO {core}.meter
                    (meter_id, site_id, meter_type, unit, multiplier, index_digits,
                     interval_minutes, is_active, source_updated_at)
                    VALUES ('M-TEST', 'S-TEST', 'main_incomer', 'kWh', 1, 6, 15, TRUE, now())
                    ON CONFLICT (meter_id) DO NOTHING"""
            )
        )
        for offset, index in readings:
            conn.execute(
                text(
                    f"""INSERT INTO {core}.meter_reading
                        (meter_id, reading_ts, index_kwh, quality_flag,
                         source_updated_at, batch_id)
                        VALUES ('M-TEST', :ts, :idx, 'measured', :ts, CAST(:b AS UUID))
                        ON CONFLICT (meter_id, reading_ts) DO NOTHING"""
                ),
                {"ts": BASE + dt.timedelta(minutes=offset), "idx": index, "b": batch},
            )

    refresh_consumption(BASE.date(), BASE.date(), mart_settings)
    yield mart_settings
    drop_schemas(mart_settings)


def _rows(settings: Settings) -> dict[int, dict[str, object]]:
    """The mart rows, keyed by minutes since BASE."""
    with get_engine(settings).connect() as conn:
        result = (
            conn.execute(
                text(
                    f"""SELECT reading_ts, index_kwh, previous_index, consumption_kwh,
                           span_minutes, delta_flag
                    FROM {settings.mart_schema}.consumption_interval
                    WHERE meter_id = 'M-TEST'
                    ORDER BY reading_ts"""
                )
            )
            .mappings()
            .all()
        )
    return {
        int((dict(row)["reading_ts"] - BASE).total_seconds() // 60): dict(row) for row in result
    }


class TestDeltaClassification:
    def test_first_reading_has_no_delta(self, controlled_warehouse: Settings) -> None:
        row = _rows(controlled_warehouse)[0]
        assert row["delta_flag"] == "first_reading"
        assert row["consumption_kwh"] is None

    def test_clean_step(self, controlled_warehouse: Settings) -> None:
        row = _rows(controlled_warehouse)[15]
        assert row["delta_flag"] == "ok"
        assert float(row["consumption_kwh"]) == pytest.approx(50.0)

    def test_rollover_is_detected_and_computed_correctly(
        self, controlled_warehouse: Settings
    ) -> None:
        """The case that makes or breaks a metering pipeline.

        The index fell from 999 990 to 30 on a six-digit register. No energy
        was lost: (1 000 000 - 999 990) + 30 = 40 kWh.
        """
        row = _rows(controlled_warehouse)[45]
        assert row["delta_flag"] == "rollover"
        assert float(row["consumption_kwh"]) == pytest.approx(40.0)

    def test_rollover_is_not_reported_as_a_negative_delta(
        self, controlled_warehouse: Settings
    ) -> None:
        assert float(_rows(controlled_warehouse)[45]["consumption_kwh"]) > 0

    def test_gap_keeps_the_total_and_records_the_real_span(
        self, controlled_warehouse: Settings
    ) -> None:
        row = _rows(controlled_warehouse)[90]
        assert row["delta_flag"] == "gap"
        assert int(row["span_minutes"]) == 30
        assert float(row["consumption_kwh"]) == pytest.approx(100.0)

    def test_flat_interval_is_zero_not_missing(self, controlled_warehouse: Settings) -> None:
        row = _rows(controlled_warehouse)[105]
        assert row["delta_flag"] == "flat"
        assert float(row["consumption_kwh"]) == pytest.approx(0.0)

    def test_minor_backward_step_is_a_correction_not_a_reset(
        self, controlled_warehouse: Settings
    ) -> None:
        """The distinction that stops an estate of four resets reporting
        hundreds of them."""
        row = _rows(controlled_warehouse)[120]
        assert row["delta_flag"] == "correction"
        assert float(row["consumption_kwh"]) == pytest.approx(0.0)

    def test_large_forward_jump_is_implausible(self, controlled_warehouse: Settings) -> None:
        row = _rows(controlled_warehouse)[135]
        assert row["delta_flag"] == "implausible"

    def test_unexplained_backward_step_is_a_reset_with_null_consumption(
        self, controlled_warehouse: Settings
    ) -> None:
        # 50 000 -> 5 is backwards, and the wrapped reading (950 005) is far
        # beyond anything this meter consumes. Unknowable, so NULL.
        row = _rows(controlled_warehouse)[150]
        assert row["delta_flag"] == "reset"
        assert row["consumption_kwh"] is None

    def test_no_negative_consumption_anywhere(self, controlled_warehouse: Settings) -> None:
        values = [
            r["consumption_kwh"]
            for r in _rows(controlled_warehouse).values()
            if r["consumption_kwh"] is not None
        ]
        assert all(float(v) >= 0 for v in values)

    def test_only_usable_flags_contribute_to_the_total(
        self, controlled_warehouse: Settings
    ) -> None:
        """50 + 40 + 40 + 50 + 100 + 0 + 0 = 280 kWh.

        The implausible jump and the reset are excluded; the rollover is
        included because its arithmetic is exact.
        """
        with get_engine(controlled_warehouse).connect() as conn:
            total = conn.execute(
                text(
                    f"""SELECT SUM(consumption_kwh)
                        FROM {controlled_warehouse.mart_schema}.consumption_interval
                        WHERE meter_id = 'M-TEST'
                          AND {controlled_warehouse.mart_schema}.is_usable(delta_flag)"""
                )
            ).scalar_one()
        assert float(total) == pytest.approx(280.0)


class TestRefreshIdempotency:
    def test_refreshing_twice_changes_nothing(self, controlled_warehouse: Settings) -> None:
        first = _rows(controlled_warehouse)
        refresh_consumption(BASE.date(), BASE.date(), controlled_warehouse)
        assert _rows(controlled_warehouse) == first

    def test_refresh_is_scoped_to_the_window(self, controlled_warehouse: Settings) -> None:
        # Refreshing a different day must not delete this day's rows.
        other = BASE.date() + dt.timedelta(days=10)
        refresh_consumption(other, other, controlled_warehouse)
        assert len(_rows(controlled_warehouse)) == 10


class TestAnalyticalViews:
    """Every view must be queryable and internally consistent."""

    VIEWS = (
        "v_consumption_hourly",
        "v_consumption_daily",
        "v_load_curve",
        "v_peak_demand",
        "v_degree_days",
        "v_consumption_vs_weather",
        "v_cost_by_band",
        "v_site_benchmark",
        "v_interval_completeness",
        "v_meter_health",
        "v_ingestion_overview",
        "v_dlq_overview",
        "v_pipeline_health",
    )

    @pytest.mark.parametrize("view", VIEWS)
    def test_view_is_queryable(self, controlled_warehouse: Settings, view: str) -> None:
        with get_engine(controlled_warehouse).connect() as conn:
            conn.execute(
                text(f"SELECT * FROM {controlled_warehouse.mart_schema}.{view} LIMIT 1")
            ).all()

    def test_hourly_sums_match_the_interval_table(self, controlled_warehouse: Settings) -> None:
        mart = controlled_warehouse.mart_schema
        with get_engine(controlled_warehouse).connect() as conn:
            hourly = conn.execute(
                text(f"SELECT COALESCE(SUM(consumption_kwh), 0) FROM {mart}.v_consumption_hourly")
            ).scalar_one()
            intervals = conn.execute(
                text(
                    f"""SELECT COALESCE(SUM(consumption_kwh), 0)
                        FROM {mart}.consumption_interval
                        WHERE {mart}.is_usable(delta_flag)"""
                )
            ).scalar_one()
        assert float(hourly) == pytest.approx(float(intervals))

    def test_daily_sums_match_hourly(self, controlled_warehouse: Settings) -> None:
        mart = controlled_warehouse.mart_schema
        with get_engine(controlled_warehouse).connect() as conn:
            daily = conn.execute(
                text(f"SELECT COALESCE(SUM(consumption_kwh), 0) FROM {mart}.v_consumption_daily")
            ).scalar_one()
            hourly = conn.execute(
                text(f"SELECT COALESCE(SUM(consumption_kwh), 0) FROM {mart}.v_consumption_hourly")
            ).scalar_one()
        assert float(daily) == pytest.approx(float(hourly))

    def test_completeness_compares_against_the_configured_interval(
        self, controlled_warehouse: Settings
    ) -> None:
        # 10 readings against 96 expected for a 15-minute meter.
        with get_engine(controlled_warehouse).connect() as conn:
            row = conn.execute(
                text(
                    f"""SELECT intervals_received, intervals_expected
                        FROM {controlled_warehouse.mart_schema}.v_interval_completeness
                        WHERE meter_id = 'M-TEST'"""
                )
            ).one()
        assert row.intervals_expected == 96
        assert row.intervals_received == 10

    def test_meter_health_reports_the_pathologies(self, controlled_warehouse: Settings) -> None:
        with get_engine(controlled_warehouse).connect() as conn:
            row = conn.execute(
                text(
                    f"""SELECT rollovers, resets, implausible, gap_intervals, health_status
                        FROM {controlled_warehouse.mart_schema}.v_meter_health
                        WHERE meter_id = 'M-TEST'"""
                )
            ).one()
        assert row.rollovers == 1
        assert row.resets == 1
        assert row.implausible == 1
        assert row.gap_intervals == 1
        # Resets are reported first: they are the most consequential.
        assert row.health_status == "reset_detected"
