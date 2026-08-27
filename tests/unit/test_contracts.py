"""Unit tests for the schema contracts."""

from __future__ import annotations

import datetime as dt

import pytest

from helios.contracts.models import Contract, FieldSpec, content_hash
from helios.contracts.registry import CONTRACTS, METER_READINGS, get_contract
from helios.exceptions import ConfigurationError, ContractViolation


class TestValidation:
    def test_valid_record_passes(self, valid_reading: dict[str, object]) -> None:
        result = METER_READINGS.validate(valid_reading)
        assert result.source_name == "meter_readings"
        assert result.payload["meter_id"] == "M00001"
        assert result.contract_version == METER_READINGS.version

    def test_missing_required_field_names_that_field(
        self, valid_reading: dict[str, object]
    ) -> None:
        del valid_reading["register_value"]
        with pytest.raises(ContractViolation) as exc:
            METER_READINGS.validate(valid_reading)
        # The field name is what makes a dead-letter report actionable rather
        # than merely a count.
        assert exc.value.field == "register_value"

    def test_empty_string_counts_as_missing(self, valid_reading: dict[str, object]) -> None:
        valid_reading["meter_id"] = "   "
        with pytest.raises(ContractViolation):
            METER_READINGS.validate(valid_reading)

    def test_negative_register_is_refused(self, valid_reading: dict[str, object]) -> None:
        # A physical register cannot run backwards, so this is a violation
        # rather than a value to clamp.
        valid_reading["register_value"] = -5.0
        with pytest.raises(ContractViolation) as exc:
            METER_READINGS.validate(valid_reading)
        assert exc.value.field == "register_value"

    def test_value_outside_the_enum_is_refused(self, valid_reading: dict[str, object]) -> None:
        valid_reading["quality_flag"] = "teleported"
        with pytest.raises(ContractViolation) as exc:
            METER_READINGS.validate(valid_reading)
        assert exc.value.field == "quality_flag"

    def test_unparseable_timestamp_is_refused(self, valid_reading: dict[str, object]) -> None:
        valid_reading["reading_ts"] = "not-a-timestamp"
        with pytest.raises(ContractViolation) as exc:
            METER_READINGS.validate(valid_reading)
        assert exc.value.field == "reading_ts"

    def test_optional_field_may_be_absent(self, valid_reading: dict[str, object]) -> None:
        del valid_reading["quality_flag"]
        assert METER_READINGS.validate(valid_reading).payload["quality_flag"] is None

    def test_unknown_fields_are_dropped(self, valid_reading: dict[str, object]) -> None:
        # An upstream that starts sending an extra column must not break the
        # pipeline; the contract defines what is *used*, not what may arrive.
        valid_reading["surprise_column"] = "hello"
        assert "surprise_column" not in METER_READINGS.validate(valid_reading).payload


class TestNormalisation:
    def test_naive_timestamps_are_treated_as_utc(self, valid_reading: dict[str, object]) -> None:
        valid_reading["updated_at"] = "2026-03-01T10:00:00"
        result = METER_READINGS.validate(valid_reading)
        assert result.source_updated_at == dt.datetime(2026, 3, 1, 10, 0, tzinfo=dt.UTC)

    def test_offsets_are_converted_to_utc(self, valid_reading: dict[str, object]) -> None:
        valid_reading["updated_at"] = "2026-03-01T12:00:00+02:00"
        result = METER_READINGS.validate(valid_reading)
        assert result.source_updated_at == dt.datetime(2026, 3, 1, 10, 0, tzinfo=dt.UTC)

    def test_z_suffix_is_accepted(self, valid_reading: dict[str, object]) -> None:
        valid_reading["updated_at"] = "2026-03-01T10:00:00Z"
        assert METER_READINGS.validate(valid_reading).source_updated_at.tzinfo is dt.UTC

    def test_comma_decimal_separator(self, valid_reading: dict[str, object]) -> None:
        valid_reading["register_value"] = "1234,50"
        assert METER_READINGS.validate(valid_reading).payload["register_value"] == pytest.approx(
            1234.50
        )

    def test_integer_field_refuses_a_fraction(self) -> None:
        contract = Contract(
            source_name="t",
            version="1.0.0",
            fields=(
                FieldSpec("k", "string"),
                FieldSpec("n", "integer"),
                FieldSpec("updated_at", "timestamp"),
            ),
            natural_key_fields=("k",),
            updated_at_field="updated_at",
        )
        with pytest.raises(ContractViolation):
            contract.validate({"k": "a", "n": "1.5", "updated_at": "2026-01-01T00:00:00Z"})


class TestNaturalKey:
    def test_composite_key_is_stable(self, valid_reading: dict[str, object]) -> None:
        key = METER_READINGS.natural_key(valid_reading)
        assert key.startswith("M00001|")
        assert METER_READINGS.natural_key(valid_reading) == key

    def test_entirely_empty_key_is_refused(self, valid_reading: dict[str, object]) -> None:
        valid_reading["meter_id"] = ""
        valid_reading["reading_ts"] = ""
        with pytest.raises(ContractViolation):
            METER_READINGS.validate(valid_reading)


class TestContentHash:
    def test_key_order_does_not_change_the_hash(self) -> None:
        # The whole basis of idempotency: the same content must hash the same
        # way whatever order the upstream happened to serialise it in.
        assert content_hash({"a": 1, "b": 2}) == content_hash({"b": 2, "a": 1})

    def test_different_content_gives_a_different_hash(self) -> None:
        assert content_hash({"a": 1}) != content_hash({"a": 2})

    def test_hash_is_stable_across_type_representations(
        self, valid_reading: dict[str, object]
    ) -> None:
        # "1234.5" and 1234.5 are the same reading. If they hashed differently,
        # re-reading a record during the grace window would create a duplicate
        # every single run.
        first = METER_READINGS.validate({**valid_reading, "register_value": 1234.5})
        second = METER_READINGS.validate({**valid_reading, "register_value": "1234.50"})
        assert first.content_hash == second.content_hash

    def test_hash_is_sha256_shaped(self, valid_reading: dict[str, object]) -> None:
        digest = METER_READINGS.validate(valid_reading).content_hash
        assert len(digest) == 64
        assert set(digest) <= set("0123456789abcdef")


class TestRegistry:
    def test_every_source_has_a_contract(self) -> None:
        from helios.sources.registry import DEFAULT_SOURCE_ORDER

        assert set(CONTRACTS) == set(DEFAULT_SOURCE_ORDER)

    def test_unknown_source_is_refused(self) -> None:
        with pytest.raises(ConfigurationError, match="unknown source"):
            get_contract("not_a_source")

    @pytest.mark.parametrize("name", sorted(CONTRACTS))
    def test_contract_is_self_consistent(self, name: str) -> None:
        contract = CONTRACTS[name]
        field_names = {f.name for f in contract.fields}
        # A natural key or cursor referring to a field the contract does not
        # declare would fail at runtime on the first record.
        assert set(contract.natural_key_fields) <= field_names
        assert contract.updated_at_field in field_names

    @pytest.mark.parametrize("name", sorted(CONTRACTS))
    def test_contract_serialises(self, name: str) -> None:
        payload = CONTRACTS[name].to_json()
        assert payload["source_name"] == name
        assert payload["fields"]
