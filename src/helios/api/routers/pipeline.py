"""Pipeline observability endpoints.

Everything here is read-only and answers a question someone asks during an
incident: is the pipeline running, how far behind is it, what is stuck in the
dead-letter queue, and did the last load pass its checks.

Every endpoint degrades rather than crashes when the database is unreachable --
a status endpoint that returns 500 when things are broken is a status endpoint
that is useless exactly when it is needed.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Query, Response, status
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from helios.config import get_settings
from helios.db.engine import get_engine
from helios.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


def _query(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Run a read-only query and return plain dictionaries."""
    cfg = get_settings()
    with get_engine(cfg).connect() as conn:
        rows = conn.execute(text(sql), params or {}).mappings().all()
    return [dict(row) for row in rows]


@router.get("/health", summary="Pipeline health")
def pipeline_health(response: Response) -> dict[str, Any]:
    """One-line answer to 'is the pipeline healthy?'."""
    cfg = get_settings()
    try:
        rows = _query(f"SELECT * FROM {cfg.mart_schema}.v_pipeline_health")
    except SQLAlchemyError as exc:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "database_unreachable", "detail": str(exc).splitlines()[0]}

    if not rows:
        return {"status": "no_runs_yet"}

    health = rows[0]
    blocking = int(health.get("blocking_failures") or 0)
    last_status = health.get("last_run_status")

    if last_status == "FAILED" or blocking > 0:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        health["status"] = "unhealthy"
    elif int(health.get("dlq_pending") or 0) > 0:
        health["status"] = "degraded"
    else:
        health["status"] = "ok"
    return health


@router.get("/runs", summary="Recent pipeline runs")
def list_runs(
    limit: Annotated[int, Query(ge=1, le=200)] = 20,
    source: Annotated[str | None, Query(description="Filter by source name.")] = None,
) -> dict[str, Any]:
    """The run history, newest first."""
    cfg = get_settings()
    rows = _query(
        f"""
        SELECT batch_id, command, source_name, started_at, finished_at, status,
               records_read, records_ingested, records_duplicate,
               records_dead_lettered, rows_promoted, retries_performed,
               watermark_before, watermark_after, duration_seconds, error_message
        FROM {cfg.meta_schema}.pipeline_run
        WHERE (CAST(:source AS TEXT) IS NULL OR source_name = :source)
        ORDER BY started_at DESC
        LIMIT :limit
        """,
        {"limit": limit, "source": source},
    )
    return {"runs": rows, "count": len(rows)}


@router.get("/watermarks", summary="Per-source ingestion cursors")
def list_watermarks() -> dict[str, Any]:
    """Where each source has got to, and how far behind that is."""
    cfg = get_settings()
    return {"watermarks": _query(f"SELECT * FROM {cfg.mart_schema}.v_ingestion_overview")}


@router.get("/dlq", summary="Dead-letter queue")
def list_dead_letters(
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    source: Annotated[str | None, Query()] = None,
    pending_only: Annotated[bool, Query()] = True,
) -> dict[str, Any]:
    """Records the pipeline refused, with the field that failed."""
    cfg = get_settings()
    summary = _query(f"SELECT * FROM {cfg.mart_schema}.v_dlq_overview")
    records = _query(
        f"""
        SELECT dlq_id, source_name, natural_key, error_type, error_message,
               failed_field, attempts, status, first_failed_at, last_failed_at
        FROM {cfg.meta_schema}.dead_letter
        WHERE (CAST(:source AS TEXT) IS NULL OR source_name = :source)
          AND (NOT :pending_only OR status = 'PENDING')
        ORDER BY last_failed_at DESC
        LIMIT :limit
        """,
        {"limit": limit, "source": source, "pending_only": pending_only},
    )
    return {"summary": summary, "records": records, "count": len(records)}


@router.get("/quality", summary="Data-quality results of the latest batch")
def latest_quality() -> dict[str, Any]:
    """Every check of the most recent run, with what it observed."""
    cfg = get_settings()
    rows = _query(
        f"""
        SELECT check_name, layer, severity, passed, observed_value, expected_value, checked_at
        FROM {cfg.meta_schema}.quality_result
        WHERE batch_id = (
            SELECT batch_id FROM {cfg.meta_schema}.pipeline_run
            ORDER BY started_at DESC LIMIT 1
        )
        ORDER BY severity, check_name
        """
    )
    return {
        "checks": rows,
        "passed": sum(1 for r in rows if r["passed"]),
        "failed": sum(1 for r in rows if not r["passed"]),
    }


@router.get(
    "/metrics",
    summary="Prometheus metrics",
    response_class=Response,
    responses={200: {"content": {"text/plain": {}}}},
)
def metrics() -> Response:
    """Pipeline state in Prometheus text exposition format.

    Deliberately a handful of metrics that answer operational questions, not
    every number the database holds. `watermark_lag_seconds` is the one worth
    alerting on: it rises whether the pipeline crashed, the source went quiet,
    or the scheduler stopped firing -- three different failures with the same
    consequence.
    """
    cfg = get_settings()
    lines: list[str] = []

    def emit(name: str, kind: str, help_text: str, samples: list[tuple[str, float]]) -> None:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {kind}")
        lines.extend(f"{name}{labels} {value}" for labels, value in samples)

    try:
        watermarks = _query(f"SELECT * FROM {cfg.mart_schema}.v_ingestion_overview")
        health = _query(f"SELECT * FROM {cfg.mart_schema}.v_pipeline_health")
        dlq = _query(
            f"""SELECT source_name, COUNT(*) AS pending
                FROM {cfg.meta_schema}.dead_letter
                WHERE status = 'PENDING' GROUP BY source_name"""
        )
    except SQLAlchemyError as exc:
        logger.error("metrics unavailable", extra={"error": str(exc).splitlines()[0]})
        return Response(
            content="# database unreachable\nhelios_up 0\n",
            media_type="text/plain; version=0.0.4",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    emit("helios_up", "gauge", "1 when the pipeline database is reachable.", [("", 1)])

    emit(
        "helios_watermark_lag_seconds",
        "gauge",
        "Seconds between now and the source watermark.",
        [
            (f'{{source="{w["source_name"]}"}}', float(w["watermark_lag_minutes"] or 0) * 60)
            for w in watermarks
        ],
    )
    emit(
        "helios_records_ingested_last_run",
        "gauge",
        "Records ingested by the most recent run of each source.",
        [
            (f'{{source="{w["source_name"]}"}}', float(w["last_run_ingested"] or 0))
            for w in watermarks
        ],
    )
    emit(
        "helios_retries_last_run",
        "gauge",
        "Transient failures retried during the most recent run of each source.",
        [
            (f'{{source="{w["source_name"]}"}}', float(w["last_run_retries"] or 0))
            for w in watermarks
        ],
    )
    emit(
        "helios_dead_letters_pending",
        "gauge",
        "Records parked in the dead-letter queue.",
        [(f'{{source="{d["source_name"]}"}}', float(d["pending"])) for d in dlq] or [("", 0.0)],
    )

    if health:
        h = health[0]
        emit(
            "helios_last_run_succeeded",
            "gauge",
            "1 when the most recent run finished with status SUCCESS.",
            [("", 1.0 if h.get("last_run_status") == "SUCCESS" else 0.0)],
        )
        emit(
            "helios_quality_checks_failed",
            "gauge",
            "Failing data-quality checks in the most recent batch.",
            [("", float(h.get("checks_failed") or 0))],
        )
        emit(
            "helios_readings_total",
            "gauge",
            "Rows currently held in core.meter_reading.",
            [("", float(h.get("readings_in_core") or 0))],
        )

    return Response(content="\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")
