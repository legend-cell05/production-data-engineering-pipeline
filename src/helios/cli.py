"""Command-line interface.

One entry point for every operation, so the same invocations are used locally,
inside Docker and in CI -- and so an orchestrator has something concrete to
call. Each command maps to exactly one task in ``helios.pipeline``, which is
what makes the Airflow DAG in ``docs/orchestration.md`` a list of
``BashOperator`` calls rather than a rewrite.

Exit codes: 0 success, 1 handled failure, 2 misuse.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from helios import __version__, pipeline
from helios.config import get_settings
from helios.contracts.registry import CONTRACTS, register_contracts
from helios.db.engine import check_connection
from helios.db.schema import drop_schemas, initialise_database, list_reading_partitions
from helios.exceptions import HeliosError
from helios.generation import generate_upstream, write_upstream
from helios.ingest.deadletter import summary as dlq_summary
from helios.ingest.runner import IngestReport
from helios.ingest.watermark import all_watermarks
from helios.logging_config import configure_logging
from helios.quality.checks import CheckResult
from helios.sources.registry import DEFAULT_SOURCE_ORDER, api_client

app = typer.Typer(
    add_completion=False,
    help="Helios -- incremental data-engineering pipeline for meter telemetry (synthetic data).",
)
console = Console()


def _bootstrap() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)


def _fail(message: str) -> None:
    console.print(f"[bold red]FAILED[/bold red] {message}")
    raise typer.Exit(code=1)


def _require_database() -> None:
    if not check_connection(get_settings(), retries=5):
        _fail("database unreachable -- check .env and that PostgreSQL is running")


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    version: Annotated[bool, typer.Option("--version", help="Print the version and exit.")] = False,
) -> None:
    """Global options."""
    if version:
        console.print(f"production-data-engineering-pipeline {__version__}")
        raise typer.Exit()
    if ctx.invoked_subcommand is None:
        console.print(ctx.get_help())
        raise typer.Exit()
    _bootstrap()


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


@app.command()
def doctor() -> None:
    """Check configuration and database connectivity."""
    settings = get_settings()
    table = Table(title="Configuration", header_style="bold")
    table.add_column("Setting")
    table.add_column("Value")
    table.add_row("Database", settings.safe_dsn)
    table.add_row(
        "Schemas",
        f"{settings.raw_schema} / {settings.core_schema} / "
        f"{settings.mart_schema} / {settings.meta_schema}",
    )
    table.add_row("Upstream API", settings.api_base_url)
    table.add_row("Page size", f"{settings.api_page_size:,}")
    table.add_row(
        "Retry budget",
        f"{settings.retry_max_attempts} attempts, {settings.retry_base_delay_seconds}s base",
    )
    table.add_row("Grace window", f"{settings.late_arrival_grace_minutes} minutes")
    table.add_row("COPY batch", f"{settings.copy_batch_size:,} rows")
    table.add_row("Data directory", str(settings.data_dir))
    console.print(table)

    if check_connection(settings, retries=2):
        console.print("[bold green]OK[/bold green] database reachable")
    else:
        _fail("database unreachable")


@app.command()
def seed(
    seed_value: Annotated[
        int | None, typer.Option("--seed", help="Override the random seed.")
    ] = None,
) -> None:
    """Generate the simulated upstream systems (files + API store)."""
    settings = get_settings()
    if seed_value is not None:
        settings = settings.model_copy(update={"random_seed": seed_value})

    try:
        dataset = generate_upstream(settings)
        write_upstream(dataset, settings)
    except HeliosError as exc:
        _fail(str(exc))

    table = Table(title="Simulated upstream (synthetic data)", header_style="bold")
    table.add_column("Dataset")
    table.add_column("Records", justify="right")
    for name, count in dataset.summary().items():
        table.add_row(name, f"{count:,}")
    console.print(table)
    console.print(f"[bold green]OK[/bold green] written to {settings.data_dir}")


@app.command(name="init-db")
def init_db() -> None:
    """Create schemas, tables, partitions, views and register the contracts.

    Additive and safe to re-run: every statement is CREATE ... IF NOT EXISTS or
    CREATE OR REPLACE, so this runs on every deploy without touching data.
    """
    _require_database()
    settings = get_settings()
    try:
        applied = initialise_database(settings)
        contracts = register_contracts(settings)
    except HeliosError as exc:
        _fail(str(exc))
    console.print(
        f"[bold green]OK[/bold green] {len(applied['schema'])} schema file(s), "
        f"{len(applied['marts'])} mart file(s), {contracts} contract(s) registered"
    )


# ---------------------------------------------------------------------------
# Pipeline tasks
# ---------------------------------------------------------------------------


@app.command(name="ingest")
def ingest_command(
    source: Annotated[
        str | None, typer.Option(help="Ingest a single source instead of all of them.")
    ] = None,
    full: Annotated[
        bool, typer.Option(help="Ignore the watermark and re-read everything.")
    ] = False,
) -> None:
    """Ingest sources into the raw layer."""
    _require_database()
    settings = get_settings()
    names = [source] if source else None

    if source and source not in DEFAULT_SOURCE_ORDER:
        console.print(f"[red]Unknown source: {source}[/red]")
        console.print(f"Known sources: {', '.join(DEFAULT_SOURCE_ORDER)}")
        raise typer.Exit(code=2)

    try:
        with api_client(settings) as client:
            reports, failures = pipeline.ingest(
                names, settings, client=client, ignore_watermark=full
            )
    except HeliosError as exc:
        _fail(str(exc))

    _print_ingest_table(reports)
    if failures:
        for name, error in failures.items():
            console.print(f"[bold red]FAILED[/bold red] {name}: {error}")
        raise typer.Exit(code=1)


def _print_ingest_table(reports: Sequence[IngestReport]) -> None:
    table = Table(title="Ingestion", header_style="bold")
    for column in (
        "source",
        "read",
        "ingested",
        "duplicate",
        "dead-lettered",
        "retries",
        "watermark",
    ):
        table.add_column(column, justify="right" if column != "source" else "left")
    for r in reports:
        table.add_row(
            r.source_name,
            f"{r.records_read:,}",
            f"{r.records_ingested:,}",
            f"{r.records_duplicate:,}",
            f"{r.records_dead_lettered:,}",
            str(r.retries_performed),
            r.watermark_after.strftime("%Y-%m-%d %H:%M") if r.watermark_after else "-",
        )
    console.print(table)


@app.command(name="promote")
def promote_command() -> None:
    """Promote raw into the core layer."""
    _require_database()
    try:
        counts, readings = pipeline.promote(settings=get_settings())
    except HeliosError as exc:
        _fail(str(exc))

    table = Table(title="Core layer", header_style="bold")
    table.add_column("Table")
    table.add_column("Rows", justify="right")
    for name, count in counts.items():
        table.add_row(name, f"{count:,}")
    table.add_row("meter_reading", f"{readings:,}")
    console.print(table)


@app.command(name="refresh-marts")
def refresh_marts_command(
    from_date: Annotated[str | None, typer.Option(help="Start date, YYYY-MM-DD.")] = None,
    to_date: Annotated[str | None, typer.Option(help="End date, YYYY-MM-DD.")] = None,
) -> None:
    """Rebuild the consumption mart for a date window."""
    _require_database()
    try:
        start = dt.date.fromisoformat(from_date) if from_date else None
        end = dt.date.fromisoformat(to_date) if to_date else None
    except ValueError as exc:
        console.print(f"[red]Invalid date: {exc}[/red]")
        raise typer.Exit(code=2) from exc

    try:
        result = pipeline.refresh_marts(start, end, get_settings())
    except HeliosError as exc:
        _fail(str(exc))
    console.print(
        f"[bold green]OK[/bold green] {result['rows']:,} interval(s), {result['flagged']:,} flagged"
    )


@app.command(name="quality")
def quality_command(
    allow_failure: Annotated[
        bool, typer.Option(help="Report blocking failures without exiting non-zero.")
    ] = False,
) -> None:
    """Run the post-load data-quality checks."""
    _require_database()
    try:
        results = pipeline.check_quality(settings=get_settings(), raise_on_blocking=False)
    except HeliosError as exc:
        _fail(str(exc))

    _print_quality_table(results)
    blocking = [r for r in results if not r.passed and r.check.severity == "BLOCKING"]
    if blocking and not allow_failure:
        _fail(f"{len(blocking)} blocking check(s) failed")


def _print_quality_table(results: Sequence[CheckResult]) -> None:
    table = Table(title="Data-quality checks", header_style="bold")
    for column in ("check", "layer", "severity", "observed", "expected", "result"):
        table.add_column(column)
    for r in results:
        status = (
            "[green]PASS[/green]"
            if r.passed
            else ("[red]FAIL[/red]" if r.check.severity == "BLOCKING" else "[yellow]WARN[/yellow]")
        )
        table.add_row(
            r.check.name,
            r.check.layer,
            r.check.severity,
            f"{r.observed:,.3f}",
            r.check.expected,
            status,
        )
    console.print(table)


@app.command(name="run")
def run_command(
    allow_quality_failure: Annotated[
        bool, typer.Option(help="Continue even if a blocking quality check fails.")
    ] = False,
) -> None:
    """Run the whole pipeline: ingest, promote, refresh, check."""
    _require_database()
    settings = get_settings()
    try:
        with api_client(settings) as client:
            result = pipeline.run_full_pipeline(
                settings, client=client, fail_on_quality=not allow_quality_failure
            )
    except HeliosError as exc:
        _fail(str(exc))

    _print_ingest_table(result.reports)

    summary = Table(title=f"Pipeline run {result.batch_id}", header_style="bold")
    summary.add_column("Metric")
    summary.add_column("Value", justify="right")
    summary.add_row("Status", result.status)
    summary.add_row("Records read", f"{result.records_read:,}")
    summary.add_row("Ingested", f"{result.records_ingested:,}")
    summary.add_row("Duplicates absorbed", f"{result.records_duplicate:,}")
    summary.add_row("Dead-lettered", f"{result.records_dead_lettered:,}")
    summary.add_row("Transient retries", f"{result.retries:,}")
    summary.add_row("Readings in core", f"{result.readings_in_core:,}")
    summary.add_row("Mart intervals", f"{result.mart.get('rows', 0):,}")
    summary.add_row("Duration", f"{result.duration_seconds:.2f} s")
    console.print(summary)

    _print_quality_table(result.quality)

    if result.failed_sources:
        for name, error in result.failed_sources.items():
            console.print(f"[bold red]SOURCE FAILED[/bold red] {name}: {error}")
        raise typer.Exit(code=1)
    console.print("[bold green]OK[/bold green] pipeline finished")


@app.command(name="all")
def all_command() -> None:
    """Seed, initialise and run, in order."""
    seed()
    init_db()
    run_command()


# ---------------------------------------------------------------------------
# Recovery operations
# ---------------------------------------------------------------------------


@app.command(name="backfill")
def backfill_command(
    source: Annotated[str, typer.Argument(help="Source to re-read.")],
    since: Annotated[str, typer.Option(help="Re-read from this instant, ISO-8601.")],
    refresh: Annotated[bool, typer.Option(help="Promote and rebuild the marts afterwards.")] = True,
) -> None:
    """Re-read a bounded window of a source.

    Idempotent: unchanged records are absorbed by the content hash, changed
    ones replace their predecessor, and the watermark is never wound backwards.
    """
    _require_database()
    settings = get_settings()
    try:
        from_ts = dt.datetime.fromisoformat(since.replace("Z", "+00:00"))
    except ValueError as exc:
        console.print(f"[red]Invalid timestamp: {since}[/red]")
        raise typer.Exit(code=2) from exc
    if from_ts.tzinfo is None:
        from_ts = from_ts.replace(tzinfo=dt.UTC)

    try:
        with api_client(settings) as client:
            report = pipeline.backfill(source, from_ts, settings=settings, client=client)
        if refresh:
            pipeline.promote(settings=settings)
            pipeline.refresh_marts(from_ts.date(), None, settings)
    except HeliosError as exc:
        _fail(str(exc))

    _print_ingest_table([report])
    console.print(f"[bold green]OK[/bold green] backfilled {source} from {from_ts.isoformat()}")


@app.command(name="dlq")
def dlq_command(
    action: Annotated[str, typer.Argument(help="'show' or 'replay'.")] = "show",
    source: Annotated[str | None, typer.Option(help="Restrict to one source.")] = None,
    limit: Annotated[int, typer.Option(help="Maximum records to replay.")] = 1000,
) -> None:
    """Inspect or replay the dead-letter queue."""
    _require_database()
    settings = get_settings()

    if action == "show":
        try:
            rows = dlq_summary(settings)
        except HeliosError as exc:
            _fail(str(exc))
        if not rows:
            console.print("[green]Dead-letter queue is empty.[/green]")
            return
        table = Table(title="Dead-letter queue", header_style="bold")
        for column in ("source", "error", "field", "status", "records", "max attempts"):
            table.add_column(column)
        for row in rows:
            table.add_row(
                str(row["source_name"]),
                str(row["error_type"]),
                str(row["failed_field"] or "-"),
                str(row["status"]),
                f"{row['records']:,}",
                str(row["max_attempts"]),
            )
        console.print(table)
        return

    if action == "replay":
        try:
            result = pipeline.replay_dead_letters(source, settings, limit=limit)
        except HeliosError as exc:
            _fail(str(exc))
        console.print(
            f"[bold green]OK[/bold green] examined {result['examined']}, "
            f"recovered {result['recovered']}, still failing {result['still_failing']}"
        )
        return

    console.print("[red]Action must be 'show' or 'replay'.[/red]")
    raise typer.Exit(code=2)


@app.command(name="watermarks")
def watermarks_command() -> None:
    """Show where each source has got to."""
    _require_database()
    try:
        rows = all_watermarks(get_settings())
    except HeliosError as exc:
        _fail(str(exc))

    if not rows:
        console.print("[yellow]No source has run yet.[/yellow]")
        return

    table = Table(title="Source watermarks", header_style="bold")
    for column in ("source", "watermark", "grace (min)", "records seen", "last success"):
        table.add_column(column)
    for w in rows:
        table.add_row(
            w.source_name,
            w.value.strftime("%Y-%m-%d %H:%M:%S") if w.value else "never read",
            str(w.grace_minutes),
            f"{w.records_seen:,}",
            w.last_success_at.strftime("%Y-%m-%d %H:%M:%S") if w.last_success_at else "-",
        )
    console.print(table)


@app.command(name="reset-source")
def reset_source_command(
    source: Annotated[str, typer.Argument(help="Source whose watermark to clear.")],
    yes: Annotated[bool, typer.Option("--yes", help="Skip the confirmation.")] = False,
) -> None:
    """Clear a source's watermark so the next run re-reads it in full."""
    _require_database()
    if not yes and not typer.confirm(
        f"Clear the watermark for {source!r}? The next run will re-read everything."
    ):
        console.print("Cancelled.")
        raise typer.Exit()
    try:
        pipeline.reset_source(source, get_settings())
    except HeliosError as exc:
        _fail(str(exc))
    console.print(f"[bold green]OK[/bold green] watermark cleared for {source}")


# ---------------------------------------------------------------------------
# Inspection
# ---------------------------------------------------------------------------


@app.command(name="partitions")
def partitions_command() -> None:
    """List the reading partitions and their approximate row counts."""
    _require_database()
    try:
        rows = list_reading_partitions(get_settings())
    except HeliosError as exc:
        _fail(str(exc))

    table = Table(title="core.meter_reading partitions", header_style="bold")
    table.add_column("partition")
    table.add_column("approx. rows", justify="right")
    for name, count in rows:
        table.add_row(name, f"{count:,}")
    console.print(table)


@app.command(name="contracts")
def contracts_command() -> None:
    """Show the schema contract of every source."""
    table = Table(title="Schema contracts", header_style="bold")
    for column in ("source", "version", "fields", "natural key", "cursor field"):
        table.add_column(column)
    for contract in CONTRACTS.values():
        table.add_row(
            contract.source_name,
            contract.version,
            str(len(contract.fields)),
            " + ".join(contract.natural_key_fields),
            contract.updated_at_field,
        )
    console.print(table)


@app.command(name="serve")
def serve_command(
    host: Annotated[str, typer.Option(help="Bind address.")] = "0.0.0.0",
    port: Annotated[int, typer.Option(help="Bind port.")] = 8000,
    reload: Annotated[bool, typer.Option(help="Reload on code changes.")] = False,
) -> None:
    """Start the API (simulated source + pipeline observability)."""
    import uvicorn

    console.print(f"[bold]Helios API[/bold] on http://{host}:{port} -- docs at /docs")
    uvicorn.run("helios.api.app:app", host=host, port=port, reload=reload, log_level="info")


@app.command(name="reset")
def reset_command(
    yes: Annotated[bool, typer.Option("--yes", help="Skip the confirmation.")] = False,
) -> None:
    """Drop every schema. Destructive."""
    settings = get_settings()
    if not yes and not typer.confirm(f"Drop all four schemas on {settings.safe_dsn}?"):
        console.print("Cancelled.")
        raise typer.Exit()
    try:
        drop_schemas(settings)
    except HeliosError as exc:
        _fail(str(exc))
    console.print("[bold green]OK[/bold green] schemas dropped")


if __name__ == "__main__":  # pragma: no cover
    app()
