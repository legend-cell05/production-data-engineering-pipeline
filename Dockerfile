# =============================================================================
# Multi-stage build.
#
# Stage 1 installs the dependencies into a virtual environment; stage 2 copies
# only that environment and the application, so compilers and build caches
# never reach the published image.
#
# The container runs as a non-root user. A data pipeline has no reason to hold
# root inside its own container, and an image that does is the first thing a
# security review flags.
# =============================================================================

# ---------- Stage 1: builder -------------------------------------------------
FROM python:3.12-slim-bookworm AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /build
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Dependency metadata first: this layer is cached as long as pyproject.toml is
# unchanged, so editing application code does not reinstall pandas.
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --upgrade pip setuptools wheel && pip install .

# ---------- Stage 2: runtime -------------------------------------------------
FROM python:3.12-slim-bookworm AS runtime

LABEL org.opencontainers.image.title="production-data-engineering-pipeline" \
      org.opencontainers.image.description="Incremental multi-source ingestion into a partitioned PostgreSQL warehouse." \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.source="https://github.com/legend-cell05/production-data-engineering-pipeline"

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HELIOS_PROJECT_ROOT=/app \
    HELIOS_LOG_FORMAT=json

# psql is used by the Makefile targets and is invaluable when debugging from
# inside the container.
RUN apt-get update \
    && apt-get install --no-install-recommends -y postgresql-client curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/bash --uid 10001 helios

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY --chown=helios:helios sql ./sql
COPY --chown=helios:helios README.md LICENSE pyproject.toml ./
RUN mkdir -p /app/data/upstream /app/data/landing && chown -R helios:helios /app/data

USER helios

# `helios doctor` already exits non-zero when the database is unreachable,
# which is exactly the semantics a healthcheck needs.
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD helios doctor > /dev/null 2>&1 || exit 1

ENTRYPOINT ["helios"]
CMD ["--help"]
