"""Source connectors.

Adding a source costs one class and one contract -- never a change to the
ingestion runner, the raw schema or the loader. That is the whole point of the
:class:`~helios.sources.base.Source` protocol.
"""

from helios.sources.base import FetchResult, Source
from helios.sources.file_sources import CsvSource, JsonSource
from helios.sources.http_source import ApiSource
from helios.sources.registry import build_sources, list_source_names
from helios.sources.retry import RetryPolicy, with_retry

__all__ = [
    "ApiSource",
    "CsvSource",
    "FetchResult",
    "JsonSource",
    "RetryPolicy",
    "Source",
    "build_sources",
    "list_source_names",
    "with_retry",
]
