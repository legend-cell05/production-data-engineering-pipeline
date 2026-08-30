"""The FastAPI application.

One process serves two unrelated things, which is unusual and deliberate:

* ``/source/*`` **is** the simulated upstream metering platform. The pipeline
  is its client and has no special access to it -- it pages through HTTP like
  any other consumer, and gets rate-limited and 503'd like any other consumer.
* ``/pipeline/*`` is the pipeline's own observability surface: runs,
  watermarks, dead letters, quality, Prometheus metrics.

In a real deployment these are two systems owned by two teams. Keeping them in
one app here means the repository can be run with a single ``docker compose
up``; the routers are separate modules precisely so splitting them later is a
move, not a rewrite.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from helios import __version__
from helios.api.routers import pipeline, source
from helios.config import get_settings
from helios.logging_config import configure_logging, get_logger

logger = get_logger(__name__)

DESCRIPTION = """
Two APIs in one process.

**`/source`** simulates the upstream metering platform: cursor-paginated
readings, rate limiting and intermittent 503s. It exists so the pipeline's
retry and pagination logic is exercised against something that actually fails.

**`/pipeline`** exposes the pipeline's own state: run history, per-source
watermarks, the dead-letter queue, data-quality results and Prometheus metrics.

All data is synthetic. Helios Energy does not exist.
"""


def create_app() -> FastAPI:
    """Build the application.

    A factory rather than a module-level instance so tests can construct an app
    after pointing the settings at a temporary directory.
    """
    cfg = get_settings()
    configure_logging(cfg.log_level, cfg.log_format)

    app = FastAPI(
        title="Helios data platform",
        version=__version__,
        description=DESCRIPTION,
        openapi_tags=[
            {
                "name": "upstream source",
                "description": "The simulated third-party metering API the pipeline reads from.",
            },
            {
                "name": "pipeline",
                "description": "Observability for the pipeline itself.",
            },
        ],
        contact={"name": "Ayman Bara", "url": "https://github.com/legend-cell05"},
        license_info={"name": "MIT"},
    )

    app.include_router(source.router)
    app.include_router(pipeline.router)

    @app.get("/", tags=["pipeline"], summary="Service index")
    def index() -> dict[str, Any]:
        return {
            "service": "helios",
            "version": __version__,
            "data": "synthetic -- Helios Energy does not exist",
            "endpoints": {
                "upstream_source": "/source/readings",
                "pipeline_health": "/pipeline/health",
                "watermarks": "/pipeline/watermarks",
                "dead_letter_queue": "/pipeline/dlq",
                "metrics": "/pipeline/metrics",
                "openapi": "/docs",
            },
        }

    @app.get("/health", tags=["pipeline"], summary="Liveness")
    def health() -> JSONResponse:
        """Liveness only -- deliberately does not touch the database.

        A liveness probe that depends on the database restarts the API every
        time the database hiccups, which turns a recoverable blip into an
        outage. Readiness lives at `/pipeline/health`.
        """
        return JSONResponse({"status": "ok", "version": __version__})

    logger.info("api ready", extra={"version": __version__, "dsn": cfg.safe_dsn})
    return app


#: Module-level instance for `uvicorn helios.api.app:app`.
app = create_app()
