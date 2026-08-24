"""Typed, environment-driven configuration.

Every tunable is declared once, validated by Pydantic and read from ``HELIOS_*``
environment variables (optionally via a local ``.env``). The same code then runs
unchanged on a laptop, inside Docker Compose and in CI -- only the environment
changes.
"""

from __future__ import annotations

import datetime as dt
import os
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _detect_project_root() -> Path:
    """Locate the directory holding ``sql/`` and ``data/``.

    Works both from a repository checkout (``pip install -e .``, where this file
    sits at ``<root>/src/helios/config.py``) and from an installed package in a
    container, where the code lives in ``site-packages`` and the SQL sits next
    to the working directory. ``HELIOS_PROJECT_ROOT`` overrides both.
    """
    override = os.environ.get("HELIOS_PROJECT_ROOT")
    if override:
        return Path(override).resolve()

    from_source = Path(__file__).resolve().parents[2]
    if (from_source / "sql").is_dir():
        return from_source

    return Path.cwd().resolve()


PROJECT_ROOT: Path = _detect_project_root()

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
LogFormat = Literal["text", "json"]

_SAFE_IDENTIFIER_CHARS = frozenset("abcdefghijklmnopqrstuvwxyz0123456789_")


class Settings(BaseSettings):
    """Runtime configuration. See ``.env.example`` for the annotated list."""

    model_config = SettingsConfigDict(
        env_prefix="HELIOS_",
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Database ----------------------------------------------------------
    db_host: str = "localhost"
    db_port: int = Field(default=5432, ge=1, le=65535)
    db_name: str = "helios"
    db_user: str = "helios_app"
    db_password: SecretStr = SecretStr("change_me_local_only")

    raw_schema: str = "raw"
    core_schema: str = "core"
    mart_schema: str = "mart"
    meta_schema: str = "meta"

    # --- Upstream API ------------------------------------------------------
    api_base_url: str = "http://localhost:8000"
    #: Capped at the same value the API enforces, so a misconfiguration
    #: fails at startup rather than as a 422 on the first page.
    api_page_size: int = Field(default=5_000, ge=1, le=50_000)
    api_timeout_seconds: float = Field(default=30.0, gt=0, le=600)

    retry_max_attempts: int = Field(default=5, ge=1, le=20)
    retry_base_delay_seconds: float = Field(default=0.5, gt=0, le=60)
    retry_max_delay_seconds: float = Field(default=30.0, gt=0, le=600)

    source_fault_rate: float = Field(default=0.15, ge=0.0, le=1.0)
    source_rate_limit_rate: float = Field(default=0.05, ge=0.0, le=1.0)

    # --- Incremental ingestion ---------------------------------------------
    late_arrival_grace_minutes: int = Field(default=90, ge=0, le=60 * 24 * 30)
    copy_batch_size: int = Field(default=50_000, ge=1_000, le=1_000_000)
    dlq_max_attempts: int = Field(default=3, ge=1, le=20)

    # --- Upstream data generation ------------------------------------------
    random_seed: int = 20260215
    n_sites: int = Field(default=40, ge=1, le=10_000)
    n_meters: int = Field(default=60, ge=1, le=50_000)
    history_days: int = Field(default=30, ge=1, le=1_825)
    interval_minutes: int = Field(default=15, ge=1, le=1_440)
    gap_rate: float = Field(default=0.012, ge=0.0, le=0.5)
    reset_rate: float = Field(default=0.05, ge=0.0, le=1.0)

    # --- Paths -------------------------------------------------------------
    data_dir: Path = Path("data")

    # --- Runtime -----------------------------------------------------------
    log_level: LogLevel = "INFO"
    log_format: LogFormat = "text"

    # -- Validation ---------------------------------------------------------

    @field_validator("data_dir")
    @classmethod
    def _resolve_data_dir(cls, value: Path) -> Path:
        return value if value.is_absolute() else (PROJECT_ROOT / value)

    @field_validator("raw_schema", "core_schema", "mart_schema", "meta_schema")
    @classmethod
    def _validate_schema_name(cls, value: str) -> str:
        """Reject anything that is not a plain identifier.

        Schema names are interpolated into DDL, so they can never be trusted
        input. Restricting them to ``[a-z_][a-z0-9_]*`` removes the injection
        surface entirely.
        """
        lowered = value.lower()
        if not lowered or (not lowered[0].isalpha() and lowered[0] != "_"):
            raise ValueError(f"invalid schema name: {value!r}")
        if not set(lowered) <= _SAFE_IDENTIFIER_CHARS:
            raise ValueError(f"invalid schema name: {value!r}")
        return lowered

    @field_validator("api_base_url")
    @classmethod
    def _normalise_base_url(cls, value: str) -> str:
        return value.rstrip("/")

    @model_validator(mode="after")
    def _validate_retry_window(self) -> Settings:
        if self.retry_max_delay_seconds < self.retry_base_delay_seconds:
            raise ValueError(
                "HELIOS_RETRY_MAX_DELAY_SECONDS must be >= HELIOS_RETRY_BASE_DELAY_SECONDS"
            )
        if 1_440 % self.interval_minutes != 0:
            raise ValueError(
                "HELIOS_INTERVAL_MINUTES must divide 1440 so a day holds whole intervals"
            )
        return self

    # -- Derived values -----------------------------------------------------

    @property
    def sqlalchemy_url(self) -> str:
        """SQLAlchemy URL including the password -- never log this."""
        pwd = self.db_password.get_secret_value()
        return f"postgresql+psycopg://{self.db_user}:{pwd}@{self.db_host}:{self.db_port}/{self.db_name}"

    @property
    def safe_dsn(self) -> str:
        """Connection string with the password masked -- safe to log."""
        return f"postgresql://{self.db_user}:***@{self.db_host}:{self.db_port}/{self.db_name}"

    @property
    def upstream_dir(self) -> Path:
        """Where the simulated source system keeps its data."""
        return self.data_dir / "upstream"

    @property
    def landing_dir(self) -> Path:
        """Where file-based sources drop their exports."""
        return self.data_dir / "landing"

    @property
    def intervals_per_day(self) -> int:
        return 1_440 // self.interval_minutes

    @property
    def grace_window(self) -> dt.timedelta:
        return dt.timedelta(minutes=self.late_arrival_grace_minutes)

    def ensure_directories(self) -> None:
        for directory in (self.upstream_dir, self.landing_dir):
            directory.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings singleton.

    Cached so the ``.env`` file is parsed once. Tests that need a different
    environment call ``get_settings.cache_clear()``.
    """
    return Settings()
