"""Schema contracts.

A contract is a declarative statement of what a source is allowed to send. It
does three jobs at once:

1. **It is the documentation.** Reading the contract tells you exactly what the
   pipeline accepts, without reading the transformation code.
2. **It decides accept or dead-letter.** A record that fails is parked with the
   field that failed, and the run continues. One bad record out of a hundred
   thousand must not fail a batch.
3. **It normalises.** The value is coerced to a canonical JSON type, so
   ``"1.0"``, ``1.0`` and ``1`` become the same payload and therefore the same
   content hash. Without that, idempotency would depend on how the upstream
   happened to serialise a number that day.

Contracts are versioned and registered in ``meta.schema_contract``, so a row
ingested six months ago can be traced to the rules that were in force then.

What a contract deliberately does **not** do is repair business content. A
missing identifier is not invented, a negative meter reading is not clamped to
zero. Formatting is normalised; meaning is never guessed.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import dataclass
from typing import Any, Literal

from helios.exceptions import ContractViolation

FieldKind = Literal["string", "integer", "number", "boolean", "timestamp", "date", "array"]


@dataclass(frozen=True)
class FieldSpec:
    """One field of a contract."""

    name: str
    kind: FieldKind
    required: bool = True
    #: Closed vocabulary. A value outside it is a violation, not a new category
    #: to accept silently -- an unexpected enum member usually means the
    #: upstream changed and nobody said so.
    allowed: frozenset[str] | None = None
    minimum: float | None = None
    maximum: float | None = None
    max_length: int | None = None
    description: str = ""

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "required": self.required,
            "allowed": sorted(self.allowed) if self.allowed else None,
            "minimum": self.minimum,
            "maximum": self.maximum,
            "max_length": self.max_length,
            "description": self.description,
        }


@dataclass(frozen=True)
class ValidatedRecord:
    """A record that satisfied its contract, ready for the raw layer."""

    source_name: str
    natural_key: str
    payload: dict[str, Any]
    content_hash: str
    source_updated_at: dt.datetime
    contract_version: str


def _coerce_timestamp(value: Any, field: str, source: str, key: str) -> dt.datetime:
    """Parse a timestamp and force it to UTC.

    Naive timestamps are treated as UTC rather than rejected: sources that omit
    the offset are common, and guessing the local zone would be far worse than
    documenting the assumption.
    """
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            parsed = dt.datetime.fromisoformat(text)
        except ValueError as exc:
            raise ContractViolation(
                f"{field!r} is not an ISO-8601 timestamp: {value!r}",
                source=source,
                field=field,
                natural_key=key,
            ) from exc
    return parsed.astimezone(dt.UTC) if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)


def _coerce_date(value: Any, field: str, source: str, key: str) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(str(value).strip()[:10])
    except ValueError as exc:
        raise ContractViolation(
            f"{field!r} is not an ISO-8601 date: {value!r}",
            source=source,
            field=field,
            natural_key=key,
        ) from exc


def _coerce_boolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "t", "yes", "y", "1"}


@dataclass(frozen=True)
class Contract:
    """The full agreement for one source."""

    source_name: str
    version: str
    fields: tuple[FieldSpec, ...]
    natural_key_fields: tuple[str, ...]
    updated_at_field: str
    description: str = ""

    @property
    def field_map(self) -> dict[str, FieldSpec]:
        return {f.name: f for f in self.fields}

    # -- Validation ---------------------------------------------------------

    def natural_key(self, record: dict[str, Any]) -> str:
        """Build the record's stable identity.

        Pipe-separated because it has to be a single text column in raw and
        has to be readable in a dead-letter report. Any field that is missing
        contributes an empty segment, which is itself a signal.
        """
        return "|".join(str(record.get(name, "")).strip() for name in self.natural_key_fields)

    def validate(self, record: dict[str, Any]) -> ValidatedRecord:
        """Validate and normalise one record.

        Raises:
            ContractViolation: With the offending field named, so the
                dead-letter report says *why* rather than just *that*.
        """
        key = self.natural_key(record)
        payload: dict[str, Any] = {}

        for spec in self.fields:
            raw_value = record.get(spec.name)

            missing = raw_value is None or (isinstance(raw_value, str) and not raw_value.strip())
            if missing:
                if spec.required:
                    raise ContractViolation(
                        f"required field {spec.name!r} is missing or empty",
                        source=self.source_name,
                        field=spec.name,
                        natural_key=key,
                    )
                payload[spec.name] = None
                continue

            payload[spec.name] = self._coerce(spec, raw_value, key)

        if not any(str(record.get(name, "")).strip() for name in self.natural_key_fields):
            raise ContractViolation(
                "natural key is entirely empty",
                source=self.source_name,
                field="|".join(self.natural_key_fields),
                natural_key=key,
            )

        updated_at = _coerce_timestamp(
            record.get(self.updated_at_field), self.updated_at_field, self.source_name, key
        )

        return ValidatedRecord(
            source_name=self.source_name,
            natural_key=key,
            payload=payload,
            content_hash=content_hash(payload),
            source_updated_at=updated_at,
            contract_version=self.version,
        )

    def _coerce(self, spec: FieldSpec, value: Any, key: str) -> Any:
        """Coerce one value to its canonical type and check its constraints."""
        source = self.source_name

        if spec.kind == "timestamp":
            return _coerce_timestamp(value, spec.name, source, key).isoformat()

        if spec.kind == "date":
            return _coerce_date(value, spec.name, source, key).isoformat()

        if spec.kind == "boolean":
            return _coerce_boolean(value)

        if spec.kind in {"integer", "number"}:
            try:
                number = float(str(value).strip().replace(",", "."))
            except (TypeError, ValueError) as exc:
                raise ContractViolation(
                    f"{spec.name!r} is not numeric: {value!r}",
                    source=source,
                    field=spec.name,
                    natural_key=key,
                ) from exc
            if spec.kind == "integer":
                if number != int(number):
                    raise ContractViolation(
                        f"{spec.name!r} must be a whole number, got {value!r}",
                        source=source,
                        field=spec.name,
                        natural_key=key,
                    )
                number = int(number)
            self._check_bounds(spec, float(number), key)
            return number

        if spec.kind == "array":
            if not isinstance(value, list):
                raise ContractViolation(
                    f"{spec.name!r} must be an array, got {type(value).__name__}",
                    source=source,
                    field=spec.name,
                    natural_key=key,
                )
            return value

        text = str(value).strip()
        if spec.max_length is not None and len(text) > spec.max_length:
            raise ContractViolation(
                f"{spec.name!r} exceeds {spec.max_length} characters",
                source=source,
                field=spec.name,
                natural_key=key,
            )
        if spec.allowed is not None and text not in spec.allowed:
            raise ContractViolation(
                f"{spec.name!r} value {text!r} is outside the allowed set {sorted(spec.allowed)}",
                source=source,
                field=spec.name,
                natural_key=key,
            )
        return text

    def _check_bounds(self, spec: FieldSpec, number: float, key: str) -> None:
        if spec.minimum is not None and number < spec.minimum:
            raise ContractViolation(
                f"{spec.name!r} = {number} is below the minimum {spec.minimum}",
                source=self.source_name,
                field=spec.name,
                natural_key=key,
            )
        if spec.maximum is not None and number > spec.maximum:
            raise ContractViolation(
                f"{spec.name!r} = {number} is above the maximum {spec.maximum}",
                source=self.source_name,
                field=spec.name,
                natural_key=key,
            )

    # -- Serialisation ------------------------------------------------------

    def to_json(self) -> dict[str, Any]:
        """The contract as stored in ``meta.schema_contract``."""
        return {
            "source_name": self.source_name,
            "version": self.version,
            "description": self.description,
            "natural_key_fields": list(self.natural_key_fields),
            "updated_at_field": self.updated_at_field,
            "fields": [f.to_json() for f in self.fields],
        }


def content_hash(payload: dict[str, Any]) -> str:
    """SHA-256 of the canonical JSON form of a payload.

    ``sort_keys`` and fixed separators make the hash independent of dictionary
    ordering and of whitespace, so the same content always hashes the same way.
    This is the whole basis of idempotent ingestion: re-reading a record during
    the grace window produces a row that already exists, and the insert is a
    no-op rather than a duplicate.
    """
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
