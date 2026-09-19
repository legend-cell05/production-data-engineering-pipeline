# Operations runbook

## Routine commands

| Task | Command |
| --- | --- |
| Check configuration and connectivity | `helios doctor` |
| Generate the simulated upstream | `helios seed` |
| Create/refresh schemas and contracts | `helios init-db` |
| Start the API | `helios serve` |
| Ingest every source | `helios ingest` |
| Ingest one source | `helios ingest --source weather` |
| Promote raw → core | `helios promote` |
| Rebuild the marts | `helios refresh-marts --from-date 2026-09-01` |
| Data-quality gate | `helios quality` |
| Everything, in order | `helios run` |
| From scratch | `helios all` |
| Where is each source? | `helios watermarks` |
| What is parked? | `helios dlq show` |
| Recover parked records | `helios dlq replay` |
| Re-read a window | `helios backfill meter_readings --since 2026-09-12T00:00:00Z` |
| Partitions and row counts | `helios partitions` |
| Contracts in force | `helios contracts` |
| Drop everything | `helios reset --yes` |

Inside Docker: `docker compose run --rm pipeline <command>`.

## First run

```bash
cp .env.example .env
docker compose up --build
```

Expect roughly: 0.4 s to generate, ~16 s for the pipeline, 18 quality checks,
26 records dead-lettered (injected on purpose), a handful of retries.

---

## Diagnostics

### `helios doctor` says the database is unreachable

1. `docker compose ps` — `postgres` must be **healthy**, not merely running.
2. Does `.env` exist? Without it the defaults apply, including
   `HELIOS_DB_HOST=localhost`, which is wrong **inside** a container (it must
   be `postgres`).
3. Port conflict with another PostgreSQL — change `HELIOS_DB_PORT`.
4. The compose database is created with the `.env` values on **first start
   only**. If you changed the password afterwards the volume still holds the
   old one: `make clean-volumes` and start again.

### A source keeps failing with `RetryBudgetExhausted`

The upstream is down for longer than the retry budget. The watermark has not
moved, so nothing is lost — the next run picks up from the same place.

```sql
SELECT source_name, status, retries_performed, error_message
FROM meta.pipeline_run
WHERE status = 'FAILED'
ORDER BY started_at DESC LIMIT 10;
```

If it is genuinely down for hours, raise `HELIOS_RETRY_MAX_ATTEMPTS` or let the
scheduler handle it — retrying for an hour inside one process just hides the
outage from whatever is watching run duration.

### The dead-letter queue is filling up

```sql
SELECT source_name, failed_field, error_message, COUNT(*)
FROM meta.dead_letter
WHERE status = 'PENDING'
GROUP BY 1, 2, 3
ORDER BY 4 DESC;
```

Then look at a payload:

```sql
SELECT payload FROM meta.dead_letter
WHERE failed_field = 'register_value' LIMIT 3;
```

A sudden jump in one field usually means the upstream changed its format. Fix
the source or widen the contract (a **minor** version bump if it only relaxes
a rule), then:

```bash
helios dlq replay
```

If the records are genuinely invalid, leave them: they are marked `ABANDONED`
after `HELIOS_DLQ_MAX_ATTEMPTS` and stop being retried.

### Completeness has dropped

```sql
SELECT meter_id, site_name, reading_date, intervals_received,
       intervals_expected, completeness_pct
FROM mart.v_interval_completeness
WHERE completeness_pct < 90
ORDER BY reading_date DESC, completeness_pct;
```

A single meter → a device problem. Every meter on one day → the pipeline missed
a window, and a backfill fixes it:

```bash
helios backfill meter_readings --since 2026-09-12T00:00:00Z
```

Every meter from one date onwards → the pipeline stopped running. Check
`helios watermarks` and the scheduler.

### `reset_rate_pct` warns

Backward index steps have jumped. Usually an upstream format change — a
multiplier applied twice, or a register width that changed.

```sql
SELECT meter_id, COUNT(*) AS resets,
       MIN(reading_ts) AS first_seen, MAX(reading_ts) AS last_seen
FROM mart.consumption_interval
WHERE delta_flag = 'reset'
GROUP BY meter_id ORDER BY resets DESC LIMIT 20;
```

Concentrated on a few meters → hardware. Spread across all of them from one
date → the source changed something.

### Rows in the default partition

```sql
SELECT MIN(reading_ts), MAX(reading_ts), COUNT(*)
FROM core.meter_reading_default;
```

A timestamp fell outside every created partition — almost always a clock
problem upstream (a reading from 1970 or from 2099). Fix the source, delete the
rows, and re-run. The partition existing at all is deliberate: without it the
insert would fail and take the whole batch with it.

### The pipeline is slow

`helios run` should take ~16 s at the default size.

1. The logs carry a duration per stage — find which one grew.
2. The raw load usually dominates. Raise `HELIOS_COPY_BATCH_SIZE` on a fast
   network; lower it on a slow one.
3. After a large load, refresh the planner's statistics:
   `ANALYZE core.meter_reading;`
4. `EXPLAIN (ANALYZE, BUFFERS)` on whatever query is slow. If a BRIN index is
   being ignored, the data is probably no longer arriving in time order.

### A run is stuck in `RUNNING`

The process died without closing its record. Informational only; nothing is
locked.

```sql
UPDATE meta.pipeline_run
SET status = 'FAILED', finished_at = now(),
    error_message = 'process terminated before completion'
WHERE status = 'RUNNING' AND started_at < now() - INTERVAL '2 hours';
```

---

## Recovery

### Rebuild core and mart from raw

Raw is the only non-regenerable layer, so everything below it can be rebuilt
without going near the source:

```bash
helios promote          # raw -> core, for the whole of raw
helios refresh-marts    # core -> mart
```

### Re-read a window from the source

```bash
helios backfill meter_readings --since 2026-09-12T00:00:00Z
```

### Re-read a source completely

```bash
helios reset-source meter_readings --yes   # clears the watermark
helios ingest --source meter_readings
```

Only after a contract change. As a reflex when a run fails, it re-reads
everything for no reason.

### Start over

```bash
helios reset --yes && helios all
```

---

## Monitoring

`GET /pipeline/metrics` exposes Prometheus text.

| Metric | Alert when | Why |
| --- | --- | --- |
| `helios_watermark_lag_seconds` | `> 3 × schedule interval` | **The one to alert on.** It rises whether the pipeline crashed, the source went quiet, or the scheduler stopped firing — three different failures with the same consequence |
| `helios_last_run_succeeded` | `== 0` | The last run failed |
| `helios_dead_letters_pending` | growing over hours | The upstream is sending something the contract refuses |
| `helios_quality_checks_failed` | `> 0` | Something downstream is wrong |
| `helios_retries_last_run` | trending up | The source is degrading before it fails |
| `helios_up` | `== 0` | The database is unreachable |

A suggested alert:

```yaml
- alert: HeliosIngestionStalled
  expr: helios_watermark_lag_seconds{source="meter_readings"} > 3600
  for: 15m
  annotations:
    summary: "Helios has not ingested meter readings for over an hour"
```

Set `HELIOS_LOG_FORMAT=json` to feed the structured logs into a collector; every
stage emits a record with source, counts, retries and duration.
