"""Unit tests for the connectors and the upstream store.

The API tests run against the real FastAPI app through ``TestClient``, so
pagination, cursors, rate limiting and fault injection are exercised for real --
in-process, but not mocked.
"""

from __future__ import annotations

import datetime as dt
import json

import httpx
import pytest
from starlette.testclient import TestClient

from helios.api.store import ReadingStore, decode_cursor, encode_cursor
from helios.config import Settings
from helios.contracts.registry import METER_READINGS, SITES, TARIFFS
from helios.exceptions import PermanentSourceError, TransientSourceError
from helios.sources.base import FetchResult
from helios.sources.file_sources import CsvSource, JsonSource
from helios.sources.http_source import ApiSource, _parse_retry_after
from helios.sources.registry import DEFAULT_SOURCE_ORDER, build_sources
from helios.sources.retry import RetryPolicy

# ---------------------------------------------------------------------------
# Cursor
# ---------------------------------------------------------------------------


class TestCursor:
    def test_round_trip(self) -> None:
        cursor = encode_cursor("2026-01-01T00:00:00+0000", "R000000001")
        assert decode_cursor(cursor) == ("2026-01-01T00:00:00+0000", "R000000001")

    def test_cursor_is_opaque(self) -> None:
        # A client that can read a cursor will eventually depend on its shape.
        assert "2026" not in encode_cursor("2026-01-01T00:00:00+0000", "R1")

    @pytest.mark.parametrize("bad", ["not-base64!!", "YWJj"])
    def test_malformed_cursor_raises(self, bad: str) -> None:
        with pytest.raises(ValueError):
            decode_cursor(bad)


# ---------------------------------------------------------------------------
# Reading store
# ---------------------------------------------------------------------------


@pytest.fixture
def store(tmp_path) -> ReadingStore:
    """Five records, two of which share an updated_at."""
    path = tmp_path / "readings.ndjson"
    records = [
        {"reading_id": "R1", "meter_id": "M1", "updated_at": "2026-01-01T00:00:00+0000"},
        {"reading_id": "R2", "meter_id": "M2", "updated_at": "2026-01-01T00:00:00+0000"},
        {"reading_id": "R3", "meter_id": "M3", "updated_at": "2026-01-01T00:01:00+0000"},
        {"reading_id": "R4", "meter_id": "M4", "updated_at": "2026-01-01T00:02:00+0000"},
        {"reading_id": "R5", "meter_id": "M5", "updated_at": "2026-01-01T00:03:00+0000"},
    ]
    path.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
    return ReadingStore(path)


class TestReadingStore:
    def test_loads_and_sorts(self, store: ReadingStore) -> None:
        assert [r["reading_id"] for r in store.records] == ["R1", "R2", "R3", "R4", "R5"]

    def test_missing_file_is_not_an_error(self, tmp_path) -> None:
        empty = ReadingStore(tmp_path / "absent.ndjson")
        assert not empty.is_available()
        assert empty.records == []

    def test_pagination_walks_every_record_exactly_once(self, store: ReadingStore) -> None:
        seen: list[str] = []
        cursor = None
        for _ in range(10):
            page = store.page(cursor=cursor, limit=2)
            seen.extend(r["reading_id"] for r in page.records)
            cursor = page.next_cursor
            if not cursor:
                break
        assert seen == ["R1", "R2", "R3", "R4", "R5"]

    def test_tied_timestamps_do_not_break_the_cursor(self, store: ReadingStore) -> None:
        """The case a timestamp-only cursor gets wrong.

        R1 and R2 share an updated_at. A cursor on the timestamp alone would
        either skip R2 or return R1 forever; the (timestamp, id) cursor does
        neither.
        """
        first = store.page(limit=1)
        assert [r["reading_id"] for r in first.records] == ["R1"]
        second = store.page(cursor=first.next_cursor, limit=1)
        assert [r["reading_id"] for r in second.records] == ["R2"]

    def test_updated_since_is_inclusive(self, store: ReadingStore) -> None:
        # Inclusive on purpose: an exclusive bound drops every record sharing
        # the boundary timestamp, and re-reading one costs nothing.
        bound = dt.datetime(2026, 1, 1, 0, 1, tzinfo=dt.UTC)
        page = store.page(updated_since=bound, limit=10)
        assert [r["reading_id"] for r in page.records] == ["R3", "R4", "R5"]

    def test_no_next_cursor_on_the_last_page(self, store: ReadingStore) -> None:
        assert store.page(limit=100).next_cursor is None

    def test_stats(self, store: ReadingStore) -> None:
        stats = store.stats()
        assert stats["records"] == 5
        assert stats["earliest_updated_at"] == "2026-01-01T00:00:00+0000"


# ---------------------------------------------------------------------------
# File connectors
# ---------------------------------------------------------------------------


class TestFileSources:
    def test_csv_yields_every_row(self, seeded_settings: Settings) -> None:
        source = CsvSource("sites", SITES, seeded_settings.landing_dir / "sites.csv")
        result = FetchResult()
        rows = list(source.fetch(None, result))
        assert len(rows) == seeded_settings.n_sites
        assert result.records_read == len(rows)

    def test_csv_keeps_identifiers_as_text(self, seeded_settings: Settings) -> None:
        # dtype=str on purpose: inferred types turn "S0012" into 12.0 and
        # silently break every join that uses it.
        source = CsvSource("sites", SITES, seeded_settings.landing_dir / "sites.csv")
        first = next(iter(source.fetch(None, FetchResult())))
        assert isinstance(first["site_id"], str)
        assert first["site_id"].startswith("S")

    def test_missing_file_is_transient(self, tmp_path) -> None:
        # "The export has not landed yet" resolves itself on the next run.
        source = CsvSource("sites", SITES, tmp_path / "absent.csv")
        with pytest.raises(TransientSourceError):
            list(source.fetch(None, FetchResult()))

    def test_missing_required_column_is_permanent(self, tmp_path) -> None:
        # A column that disappeared is a format change, not a timing problem.
        path = tmp_path / "sites.csv"
        path.write_text("site_id,site_name\nS1,Thing\n", encoding="utf-8")
        source = CsvSource("sites", SITES, path)
        with pytest.raises(PermanentSourceError, match="missing required column"):
            list(source.fetch(None, FetchResult()))

    def test_json_source_yields_nested_records(self, seeded_settings: Settings) -> None:
        source = JsonSource(
            "tariffs", TARIFFS, seeded_settings.landing_dir / "tariffs.json", records_key="tariffs"
        )
        rows = list(source.fetch(None, FetchResult()))
        assert rows
        # Nesting is preserved and flattened in SQL, not in Python.
        assert isinstance(rows[0]["bands"], list)

    def test_malformed_json_is_permanent(self, tmp_path) -> None:
        path = tmp_path / "tariffs.json"
        path.write_text("{not json", encoding="utf-8")
        source = JsonSource("tariffs", TARIFFS, path, records_key="tariffs")
        with pytest.raises(PermanentSourceError):
            list(source.fetch(None, FetchResult()))

    def test_file_sources_are_not_incremental(self, seeded_settings: Settings) -> None:
        source = CsvSource("sites", SITES, seeded_settings.landing_dir / "sites.csv")
        assert source.supports_incremental is False


# ---------------------------------------------------------------------------
# API connector
# ---------------------------------------------------------------------------


class TestApiSource:
    def _source(self, client: httpx.Client, page_size: int = 500) -> ApiSource:
        return ApiSource(
            "meter_readings",
            METER_READINGS,
            client,
            path="/source/readings",
            page_size=page_size,
            retry_policy=RetryPolicy(max_attempts=6, base_delay_seconds=0.001),
        )

    def test_fetches_every_record_across_pages(self, api_client: httpx.Client) -> None:
        total = api_client.get("/source/readings/stats").json()["records"]
        result = FetchResult()
        records = list(self._source(api_client, page_size=200).fetch(None, result))
        assert len(records) == total
        assert result.pages_fetched > 1

    def test_since_narrows_the_read(self, api_client: httpx.Client) -> None:
        everything = list(self._source(api_client).fetch(None, FetchResult()))
        timestamps = sorted(r["updated_at"] for r in everything)
        midpoint = dt.datetime.fromisoformat(
            timestamps[len(timestamps) // 2].replace("Z", "+00:00")
        )
        narrowed = list(self._source(api_client).fetch(midpoint, FetchResult()))
        assert 0 < len(narrowed) < len(everything)

    def test_no_duplicates_across_pages(self, api_client: httpx.Client) -> None:
        records = list(self._source(api_client, page_size=97).fetch(None, FetchResult()))
        ids = [r["reading_id"] for r in records]
        assert len(ids) == len(set(ids))

    def test_transient_failures_are_retried(self, api_client: httpx.Client, monkeypatch) -> None:
        """The whole point of the simulated source failing."""
        from helios.api.routers import source as source_router

        # Force the first call of every page to fail, then succeed.
        state = {"fail_next": True}
        original = source_router._maybe_fail

        def flaky() -> None:
            if state["fail_next"]:
                state["fail_next"] = False
                raise source_router.HTTPException(status_code=503, detail="injected")
            state["fail_next"] = True

        monkeypatch.setattr(source_router, "_maybe_fail", flaky)
        result = FetchResult()
        records = list(self._source(api_client, page_size=50_000).fetch(None, result))
        monkeypatch.setattr(source_router, "_maybe_fail", original)

        assert records
        assert result.retries_performed >= 1

    def test_oversized_page_request_is_permanent(self, api_client: httpx.Client) -> None:
        # The API caps `limit`; asking for more is a 422, and repeating an
        # invalid request unchanged would only burn the retry budget.
        with pytest.raises(PermanentSourceError, match="422"):
            list(self._source(api_client, page_size=999_999).fetch(None, FetchResult()))

    def test_malformed_cursor_is_permanent(self, api_client: httpx.Client) -> None:
        response = api_client.get("/source/readings", params={"cursor": "!!!not base64"})
        # 400, not 500 -- and 4xx is not retried, which is the correct outcome
        # for a client-side mistake.
        assert response.status_code == 400

    def test_api_source_is_incremental(self, api_client: httpx.Client) -> None:
        assert self._source(api_client).supports_incremental is True


class TestRetryAfterParsing:
    @pytest.mark.parametrize(("value", "expected"), [("3", 3.0), ("0", 0.0), ("2.5", 2.5)])
    def test_seconds_form(self, value: str, expected: float) -> None:
        assert _parse_retry_after(value) == expected

    @pytest.mark.parametrize("value", [None, "", "Wed, 21 Oct 2026 07:28:00 GMT"])
    def test_unsupported_forms_fall_back_to_backoff(self, value: str | None) -> None:
        assert _parse_retry_after(value) is None


class TestRegistry:
    def test_file_sources_are_built_without_a_client(self, settings: Settings) -> None:
        sources = build_sources(settings)
        assert set(sources) == {"sites", "meters", "weather", "tariffs"}

    def test_api_source_appears_with_a_client(self, settings: Settings) -> None:
        with httpx.Client(base_url="http://unused") as client:
            sources = build_sources(settings, client=client)
        assert set(sources) == set(DEFAULT_SOURCE_ORDER)

    def test_reference_is_ingested_before_telemetry(self) -> None:
        # A reading whose meter is unknown cannot be promoted, so reference
        # data has to land first.
        order = list(DEFAULT_SOURCE_ORDER)
        assert order.index("meters") < order.index("meter_readings")
        assert order.index("sites") < order.index("meters")


class TestApiEndpoints:
    def test_liveness_does_not_touch_the_database(self, seeded_settings: Settings) -> None:
        """A liveness probe that depends on the database restarts the API
        every time the database hiccups."""
        from helios.api.app import create_app

        with TestClient(create_app()) as client:
            assert client.get("/health").status_code == 200

    def test_index_lists_the_endpoints(self, api_client: httpx.Client) -> None:
        body = api_client.get("/").json()
        assert "upstream_source" in body["endpoints"]
        assert "synthetic" in body["data"]

    def test_openapi_is_served(self, api_client: httpx.Client) -> None:
        schema = api_client.get("/openapi.json").json()
        assert "/source/readings" in schema["paths"]
        assert "/pipeline/metrics" in schema["paths"]
