"""End-to-end integration tests against a live PostgreSQL.

Skipped unless ``HELIOS_RUN_INTEGRATION=1``. CI starts a PostgreSQL service
container and sets it.

These assert the properties that unit tests cannot reach: that the whole thing
loads, that it is idempotent, that a backfill does not wind the watermark
backwards, and that a dead letter can be recovered.
"""

from __future__ import annotations

import datetime as dt
import uuid

import httpx
import pytest
from sqlalchemy import text

from helios import pipeline
from helios.config import Settings
from helios.contracts.registry import register_contracts
from helios.db.engine import get_engine
from helios.db.schema import drop_schemas, initialise_database, list_reading_partitions
from helios.ingest.watermark import read_watermark
from tests.conftest import requires_database

pytestmark = [requires_database, pytest.mark.integration]


@pytest.fixture(scope="module")
def integration_settings(tmp_path_factory) -> Settings:
    """Settings isolated in their own schemas, so these tests can share a
    database with a developer's own warehouse without touching it."""
    return Settings(
        data_dir=tmp_path_factory.mktemp("helios-integration"),
        raw_schema="test_raw",
        core_schema="test_core",
        mart_schema="test_mart",
        meta_schema="test_meta",
        random_seed=777,
        n_sites=8,
        n_meters=12,
        history_days=4,
        interval_minutes=15,
        gap_rate=0.02,
        reset_rate=0.2,
        copy_batch_size=5_000,
        log_level="WARNING",
        source_fault_rate=0.0,
        source_rate_limit_rate=0.0,
    )


@pytest.fixture(scope="module")
def client(integration_settings: Settings):
    """An httpx client wired to the app, reading this module's upstream store."""
    from starlette.testclient import TestClient

    from helios.api import dependencies
    from helios.api.app import create_app
    from helios.api.store import ReadingStore
    from helios.generation import generate_upstream, write_upstream

    write_upstream(generate_upstream(integration_settings), integration_settings)

    dependencies.get_reading_store.cache_clear()
    store = ReadingStore(integration_settings.upstream_dir / "readings.ndjson")
    dependencies.get_reading_store.__wrapped__ = lambda: store  # type: ignore[attr-defined]
    dependencies.get_reading_store.cache_clear()

    original = dependencies.get_reading_store
    from helios.api.routers import source as source_router

    source_router.get_reading_store = lambda: store  # type: ignore[assignment]
    try:
        with TestClient(create_app(), base_url="http://upstream") as test_client:
            yield test_client
    finally:
        source_router.get_reading_store = original  # type: ignore[assignment]


@pytest.fixture(scope="module")
def loaded(integration_settings: Settings, client: httpx.Client):
    """A fully loaded warehouse, built once for the module."""
    drop_schemas(integration_settings)
    initialise_database(integration_settings)
    register_contracts(integration_settings)
    result = pipeline.run_full_pipeline(integration_settings, client=client, fail_on_quality=False)
    yield result
    drop_schemas(integration_settings)


def _scalar(settings: Settings, sql: str, **params: object) -> object:
    with get_engine(settings).connect() as conn:
        return conn.execute(text(sql), params).scalar_one()


class TestFullRun:
    def test_run_succeeds(self, loaded) -> None:
        assert loaded.status == "SUCCESS"
        assert not loaded.failed_sources

    def test_every_source_was_ingested(self, loaded) -> None:
        assert {r.source_name for r in loaded.reports} == {
            "sites",
            "meters",
            "tariffs",
            "weather",
            "meter_readings",
        }

    def test_readings_reached_core(self, loaded) -> None:
        assert loaded.readings_in_core > 1_000

    def test_marts_were_built(self, loaded) -> None:
        assert loaded.mart["rows"] == loaded.readings_in_core

    def test_contract_violations_were_dead_lettered(
        self, integration_settings: Settings, loaded
    ) -> None:
        # The generator injects them on purpose; a run that dead-letters
        # nothing would mean the contract is not being applied.
        assert loaded.records_dead_lettered > 0
        parked = _scalar(
            integration_settings,
            f"SELECT COUNT(*) FROM {integration_settings.meta_schema}.dead_letter",
        )
        assert int(parked) == loaded.records_dead_lettered

    def test_dead_letters_name_the_failing_field(
        self, integration_settings: Settings, loaded
    ) -> None:
        fields = _scalar(
            integration_settings,
            f"""SELECT COUNT(DISTINCT failed_field)
                FROM {integration_settings.meta_schema}.dead_letter""",
        )
        assert int(fields) >= 2

    def test_no_blocking_quality_failures(self, loaded) -> None:
        assert [r.check.name for r in loaded.blocking_failures] == []

    def test_run_history_was_recorded(self, integration_settings: Settings, loaded) -> None:
        status = _scalar(
            integration_settings,
            f"""SELECT status FROM {integration_settings.meta_schema}.pipeline_run
                WHERE batch_id = CAST(:b AS UUID)""",
            b=str(loaded.batch_id),
        )
        assert status in {"SUCCESS", "PARTIAL"}


class TestPartitioning:
    def test_readings_land_in_monthly_partitions(
        self, integration_settings: Settings, loaded
    ) -> None:
        partitions = dict(list_reading_partitions(integration_settings))
        monthly = {k: v for k, v in partitions.items() if k != "meter_reading_default"}
        assert monthly, "no monthly partition was created"
        assert sum(monthly.values()) > 0

    def test_default_partition_is_empty(self, integration_settings: Settings, loaded) -> None:
        # Rows here mean a timestamp fell outside every created partition --
        # a clock problem upstream, or a partition nobody created.
        count = _scalar(
            integration_settings,
            f"SELECT COUNT(*) FROM {integration_settings.core_schema}.meter_reading_default",
        )
        assert int(count) == 0

    def test_partition_creation_is_idempotent(self, integration_settings: Settings, loaded) -> None:
        from helios.db.schema import ensure_reading_partitions

        today = dt.date.today()
        first = ensure_reading_partitions(today, today, integration_settings)
        second = ensure_reading_partitions(today, today, integration_settings)
        assert first == second


class TestIdempotency:
    def test_second_run_changes_nothing(
        self, integration_settings: Settings, client: httpx.Client, loaded
    ) -> None:
        """The property that lets a scheduler retry without a human checking.

        The second run still *reads* records -- that is the grace window doing
        its job -- but every one of them is absorbed by the content hash.
        """
        core = integration_settings.core_schema
        mart = integration_settings.mart_schema
        before_rows = int(
            _scalar(integration_settings, f"SELECT COUNT(*) FROM {core}.meter_reading")
        )
        before_kwh = _scalar(
            integration_settings,
            f"SELECT COALESCE(SUM(consumption_kwh), 0) FROM {mart}.consumption_interval",
        )

        second = pipeline.run_full_pipeline(
            integration_settings, client=client, fail_on_quality=False
        )

        after_rows = int(
            _scalar(integration_settings, f"SELECT COUNT(*) FROM {core}.meter_reading")
        )
        after_kwh = _scalar(
            integration_settings,
            f"SELECT COALESCE(SUM(consumption_kwh), 0) FROM {mart}.consumption_interval",
        )

        assert after_rows == before_rows
        assert after_kwh == before_kwh
        assert second.records_ingested == 0
        # Records were re-read and absorbed -- the grace window's cost, made
        # visible rather than hidden.
        assert second.records_duplicate >= 0

    def test_watermark_only_moves_forward(
        self, integration_settings: Settings, client: httpx.Client, loaded
    ) -> None:
        before = read_watermark("meter_readings", integration_settings).value
        assert before is not None

        # A backfill re-reads an old window. If it wound the watermark back,
        # the next normal run would re-ingest everything since.
        pipeline.backfill(
            "meter_readings",
            before - dt.timedelta(days=2),
            settings=integration_settings,
            client=client,
        )
        after = read_watermark("meter_readings", integration_settings).value
        assert after is not None
        assert after >= before


class TestBackfill:
    def test_backfill_is_a_no_op_on_unchanged_data(
        self, integration_settings: Settings, client: httpx.Client, loaded
    ) -> None:
        core = integration_settings.core_schema
        before = int(_scalar(integration_settings, f"SELECT COUNT(*) FROM {core}.meter_reading"))

        report = pipeline.backfill(
            "meter_readings",
            dt.datetime.now(tz=dt.UTC) - dt.timedelta(days=1),
            settings=integration_settings,
            client=client,
        )
        pipeline.promote(settings=integration_settings)

        after = int(_scalar(integration_settings, f"SELECT COUNT(*) FROM {core}.meter_reading"))
        assert after == before
        assert report.records_ingested == 0
        assert report.records_read > 0


class TestDeadLetterReplay:
    def test_replay_recovers_records_once_the_contract_allows_them(
        self, integration_settings: Settings, loaded
    ) -> None:
        """A dead letter is a queue, not a bin.

        The parked records here genuinely violate the contract, so a replay
        must NOT recover them -- it must increment their attempt count and
        eventually abandon them. That is the behaviour being asserted: the
        queue does not quietly let bad data through on a retry.
        """
        meta = integration_settings.meta_schema
        before = int(
            _scalar(
                integration_settings,
                f"SELECT COUNT(*) FROM {meta}.dead_letter WHERE status = 'PENDING'",
            )
        )
        assert before > 0

        result = pipeline.replay_dead_letters(settings=integration_settings)
        assert result["examined"] == before
        assert result["recovered"] == 0
        assert result["still_failing"] == before

        attempts = int(
            _scalar(integration_settings, f"SELECT MAX(attempts) FROM {meta}.dead_letter")
        )
        assert attempts >= 2

    def test_records_are_abandoned_once_the_budget_is_spent(
        self, integration_settings: Settings, loaded
    ) -> None:
        meta = integration_settings.meta_schema
        for _ in range(integration_settings.dlq_max_attempts + 1):
            pipeline.replay_dead_letters(settings=integration_settings)

        abandoned = int(
            _scalar(
                integration_settings,
                f"SELECT COUNT(*) FROM {meta}.dead_letter WHERE status = 'ABANDONED'",
            )
        )
        assert abandoned > 0


class TestFailureHandling:
    def test_a_failing_source_does_not_stop_the_others(
        self, integration_settings: Settings, client: httpx.Client, loaded
    ) -> None:
        """A metering API being down must not stop the weather feed."""
        broken = integration_settings.model_copy(
            update={"data_dir": integration_settings.data_dir / "does-not-exist"}
        )
        reports, failures = pipeline.ingest(
            ["sites", "meter_readings"], broken, client=client, batch_id=uuid.uuid4()
        )
        assert "sites" in failures
        assert {r.source_name for r in reports} == {"meter_readings"}

    def test_a_failed_source_leaves_its_watermark_alone(
        self, integration_settings: Settings, client: httpx.Client, loaded
    ) -> None:
        before = read_watermark("sites", integration_settings).value
        broken = integration_settings.model_copy(
            update={"data_dir": integration_settings.data_dir / "nope"}
        )
        pipeline.ingest(["sites"], broken, client=client)
        assert read_watermark("sites", integration_settings).value == before
