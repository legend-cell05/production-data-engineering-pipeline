"""Domain exceptions.

The hierarchy exists so a caller can tell a *transient* failure (retry it) from
a *permanent* one (dead-letter it) from a *configuration* mistake (stop and tell
someone). That distinction is the whole basis of the retry and DLQ logic --
without it, a pipeline either retries forever on bad data or gives up on a
network blip.
"""

from __future__ import annotations


class HeliosError(Exception):
    """Base class for every error raised by this pipeline."""


class ConfigurationError(HeliosError):
    """Invalid or missing configuration. Never retried."""


class DatabaseError(HeliosError):
    """The database is unreachable, or a statement failed."""


# ---------------------------------------------------------------------------
# Source errors
# ---------------------------------------------------------------------------


class SourceError(HeliosError):
    """Base class for anything that went wrong reading a source."""

    def __init__(self, message: str, *, source: str = "") -> None:
        super().__init__(message)
        self.source = source


class TransientSourceError(SourceError):
    """A failure that is expected to succeed on retry.

    Connection resets, timeouts, 5xx responses, and rate limiting. Carries the
    server's ``Retry-After`` when there was one, because honouring it is the
    difference between backing off politely and being blocked.
    """

    def __init__(
        self,
        message: str,
        *,
        source: str = "",
        status_code: int | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message, source=source)
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds


class PermanentSourceError(SourceError):
    """A failure that retrying cannot fix: 4xx, a missing file, bad auth."""

    def __init__(self, message: str, *, source: str = "", status_code: int | None = None) -> None:
        super().__init__(message, source=source)
        self.status_code = status_code


class RetryBudgetExhausted(SourceError):
    """Every retry attempt was used and the source still failed."""

    def __init__(self, message: str, *, source: str = "", attempts: int = 0) -> None:
        super().__init__(message, source=source)
        self.attempts = attempts


# ---------------------------------------------------------------------------
# Data errors
# ---------------------------------------------------------------------------


class ContractViolation(HeliosError):
    """A record does not satisfy its source's schema contract.

    Always permanent for that record: the payload will not become valid by
    being read again. The record is dead-lettered with the failing field so it
    can be investigated and, once upstream is fixed, replayed.
    """

    def __init__(
        self,
        message: str,
        *,
        source: str = "",
        field: str = "",
        natural_key: str = "",
    ) -> None:
        super().__init__(message)
        self.source = source
        self.field = field
        self.natural_key = natural_key


class TransformationError(HeliosError):
    """A raw record could not be promoted to the core layer."""


class LoadError(HeliosError):
    """Writing to the warehouse failed."""


class DataQualityError(HeliosError):
    """A blocking data-quality check failed after the load."""

    def __init__(self, message: str, *, check_name: str = "", observed: object = None) -> None:
        super().__init__(message)
        self.check_name = check_name
        self.observed = observed
