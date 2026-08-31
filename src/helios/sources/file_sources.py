"""File-based connectors: CSV drops and nested JSON.

These sources deliver full snapshots rather than deltas, which is how most
reference data actually arrives -- a nightly export written to a share. The
pipeline still runs them through the same contract, the same content hash and
the same raw table, so an unchanged snapshot re-ingests as zero new rows.

CSV is read with ``dtype=str`` and parsed by the contract. Letting pandas infer
types on a source file is how ``site_id = "S0012"`` silently becomes ``12.0``
and how a leading zero disappears from an identifier -- a class of bug that is
invisible until a join returns nothing.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas as pd

from helios.contracts.models import Contract
from helios.exceptions import PermanentSourceError, TransientSourceError
from helios.logging_config import get_logger
from helios.sources.base import FetchResult

logger = get_logger(__name__)


class _FileSourceBase:
    """Shared plumbing for the file connectors."""

    def __init__(self, name: str, contract: Contract, path: Path) -> None:
        self._name = name
        self._contract = contract
        self._path = path

    @property
    def name(self) -> str:
        return self._name

    @property
    def contract(self) -> Contract:
        return self._contract

    @property
    def supports_incremental(self) -> bool:
        # A snapshot file has no cursor: the whole file is re-read every run.
        # Correct and cheap for a few hundred rows; the content hash makes the
        # re-read a no-op at the database.
        return False

    def _require_file(self) -> Path:
        if not self._path.exists():
            # A missing drop is usually "the export has not landed yet", which
            # is transient by nature: the next scheduled run will find it.
            raise TransientSourceError(
                f"source file not found: {self._path.name}. Run `helios seed` first.",
                source=self._name,
            )
        if self._path.stat().st_size == 0:
            raise PermanentSourceError(
                f"source file is empty: {self._path.name}", source=self._name
            )
        return self._path


class CsvSource(_FileSourceBase):
    """Reads a CSV snapshot."""

    def fetch(self, since: dt.datetime | None, result: FetchResult) -> Iterator[dict[str, Any]]:
        path = self._require_file()
        try:
            frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        except (pd.errors.ParserError, UnicodeDecodeError) as exc:
            raise PermanentSourceError(
                f"cannot parse {path.name}: {exc}", source=self._name
            ) from exc
        except OSError as exc:
            raise TransientSourceError(
                f"cannot read {path.name}: {exc}", source=self._name
            ) from exc

        missing = [
            spec.name
            for spec in self._contract.fields
            if spec.required and spec.name not in frame.columns
        ]
        if missing:
            # A column that has disappeared is a contract break at file level,
            # not at row level: dead-lettering every row would be noise when
            # the real message is "the export format changed".
            raise PermanentSourceError(
                f"{path.name} is missing required column(s): {', '.join(missing)}",
                source=self._name,
            )

        result.pages_fetched += 1
        for record in frame.to_dict(orient="records"):
            result.records_read += 1
            yield {str(k): (v if v != "" else None) for k, v in record.items()}

        logger.info(
            "csv fetch complete",
            extra={"source": self._name, "file": path.name, "records": result.records_read},
        )


class JsonSource(_FileSourceBase):
    """Reads a JSON document and yields the records under a given key.

    Nested structure is preserved and passed through to raw as-is. Flattening
    happens in SQL during promotion, where ``jsonb_array_elements`` does it in
    one set-based operation instead of a Python loop per record.
    """

    def __init__(self, name: str, contract: Contract, path: Path, *, records_key: str) -> None:
        super().__init__(name, contract, path)
        self._records_key = records_key

    def fetch(self, since: dt.datetime | None, result: FetchResult) -> Iterator[dict[str, Any]]:
        path = self._require_file()
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise PermanentSourceError(
                f"{path.name} is not valid JSON: {exc}", source=self._name
            ) from exc
        except OSError as exc:
            raise TransientSourceError(
                f"cannot read {path.name}: {exc}", source=self._name
            ) from exc

        records = document.get(self._records_key)
        if not isinstance(records, list):
            raise PermanentSourceError(
                f"{path.name} has no array under {self._records_key!r}", source=self._name
            )

        result.pages_fetched += 1
        for record in records:
            if not isinstance(record, dict):
                raise PermanentSourceError(
                    f"{path.name}: {self._records_key} must contain objects", source=self._name
                )
            result.records_read += 1
            yield record

        logger.info(
            "json fetch complete",
            extra={"source": self._name, "file": path.name, "records": result.records_read},
        )
