"""Airflow DAG for the Helios pipeline.

Every task is a ``BashOperator`` calling one ``helios`` command, because the CLI
already guarantees idempotency and retryability. Re-implementing any of that in
Python operators would create a second version of the pipeline's logic that
drifts from the first one.

NOT DEPLOYED. Airflow is not a dependency of this project and this DAG has not
been executed against a live scheduler. It is checked in because the shape of
the CLI only makes sense once you see what it was shaped for. The rationale --
``max_active_runs``, the two layers of retry, the sensor in ``reschedule``
mode -- is in ``docs/orchestration.md``.
"""

from __future__ import annotations

import datetime as dt

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.task_group import TaskGroup

REFERENCE_SOURCES = ("sites", "meters", "tariffs", "weather")

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    # The orchestrator's retry is coarse: it re-runs a whole task, and exists
    # for a dying worker or a restarted database. The fine-grained retry lives
    # in ``sources/retry.py``, where it knows a 503 from a 400 and can resume a
    # paginated fetch without re-reading the pages before it.
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": dt.timedelta(minutes=30),
    "email_on_failure": False,
}

with DAG(
    dag_id="helios_pipeline",
    description="Incremental ingestion of meter telemetry into the Helios warehouse.",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    # The watermark is the cursor, not the execution date. Two concurrent runs
    # would both read it and both advance it -- harmless, because everything
    # downstream is idempotent, but wasteful and hard to read in a run report.
    max_active_runs=1,
    dagrun_timeout=dt.timedelta(minutes=20),
    default_args=default_args,
    tags=["helios", "ingestion", "postgres"],
) as dag:
    upstream_available = HttpSensor(
        task_id="upstream_available",
        http_conn_id="helios_upstream",
        endpoint="health",
        # Fail rather than hold a worker slot: the next run is fifteen minutes
        # away, and skipping one is lossless because the watermark did not move.
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )

    with TaskGroup(group_id="reference") as reference:
        for source in REFERENCE_SOURCES:
            BashOperator(
                task_id=f"ingest_{source}",
                bash_command=f"helios ingest --source {source}",
            )

    ingest_readings = BashOperator(
        task_id="ingest_meter_readings",
        bash_command="helios ingest --source meter_readings",
        execution_timeout=dt.timedelta(minutes=10),
    )

    promote = BashOperator(
        task_id="promote",
        bash_command="helios promote",
    )

    refresh_marts = BashOperator(
        task_id="refresh_marts",
        # Rebuild today and yesterday: a late arrival inside the grace window
        # can change a day that has already been built. Bounded, and cheap
        # because the refresh is scoped per day.
        bash_command="helios refresh-marts --from-date {{ macros.ds_add(ds, -1) }}",
    )

    quality = BashOperator(
        task_id="quality",
        # Exits non-zero on a BLOCKING check, which is what fails the task.
        bash_command="helios quality",
    )

    dead_letter_report = BashOperator(
        task_id="dead_letter_report",
        bash_command="helios dlq show",
        # Reporting, not gating: a dead letter is not a reason to fail the run.
        trigger_rule="all_done",
    )

    upstream_available >> reference >> ingest_readings
    ingest_readings >> promote >> refresh_marts >> quality >> dead_letter_report
