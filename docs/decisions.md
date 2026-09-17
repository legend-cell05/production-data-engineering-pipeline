# Decisions

Architecture decision records. Each one states the choice, the reason, what it
costs, and **the condition under which it becomes the wrong choice** -- the last
column being the part that makes an ADR worth writing down.

Status: all `Accepted` unless noted.

---

## ADR-001 -- PostgreSQL as the single store

**Context.** The pipeline needs an append-only landing area for semi-structured
payloads, a typed relational core, an analytical mart, and its own operational
metadata (watermarks, runs, dead letters).

**Decision.** One PostgreSQL 16 instance, four schemas: `raw`, `core`, `mart`,
`meta`.

**Why.** The features the model actually depends on are all in one engine:
declarative LIST and RANGE partitioning, JSONB with expression indexes, `COPY`,
`ON CONFLICT`, BRIN, window functions, `PERCENTILE_CONT`, and real foreign keys.
Splitting raw into a document store would buy schema flexibility that JSONB
already provides, and would cost the ability to promote with a single SQL
statement inside one transaction.

**Cost.** One machine to scale. Analytical queries and ingestion contend for the
same buffers.

**Wrong when.** Raw volume passes what a single node can hold cheaply, or the
analytical workload starts starving ingestion. The move then is `raw` to object
storage in Parquet, `core`/`mart` to a columnar warehouse -- and because
promotion is already set-based SQL against a defined contract, that is a
rewrite of `load/`, not of the pipeline's logic.

---

## ADR-002 -- Raw as one generic JSONB table, LIST-partitioned by source

**Decision.** `raw.record(source_name, natural_key, content_hash, payload JSONB,
source_updated_at, …)`, partitioned `BY LIST (source_name)`.

**Why.** Adding a source is a connector class plus a contract entry -- no
migration, no new table, no change to the loader. Partitioning keeps a scan of
`sites` (40 rows) away from `meter_readings` (167 000 rows); the two live in the
same logical table but never in the same physical one.

**Cost.** Raw is 103 MB against core's 23 MB for the same data: JSONB stores its
keys on every row. Queries against raw need `->>` and a cast, so they are slower
and uglier than against core -- which is fine, because raw is for replay and
forensics, not for reporting.

**Wrong when.** A source's payload becomes wide enough that key repetition
dominates storage, or raw is queried often enough that the casts matter. Then
that source gets its own typed landing table.

---

## ADR-003 -- RANGE-partition `core.meter_reading` by month, created lazily

**Decision.** Monthly RANGE partitions on `reading_ts`, plus a `DEFAULT`
partition. `ensure_reading_partition()` creates the partition for a month the
first time a row needs it.

**Why.** Retention becomes `DROP TABLE` rather than a `DELETE` that bloats the
heap and holds locks. Pruning keeps "last 7 days" off eleven months of history.
Lazy creation means a backfill into an old month works without an operator
having pre-created anything.

**Cost.** A `DEFAULT` partition is a trap: rows that land there are invisible to
pruning and can block the later creation of the partition that should have held
them. Mitigated by the `no_readings_in_default_partition` quality check and by
a CI assertion that the partition is empty after every run.

**Wrong when.** Readings arrive for hundreds of distinct months (partition count
becomes a planning cost), or a single month outgrows a node. Weekly partitions,
or a time-series extension, at that point.

---

## ADR-004 -- `COPY` into a TEMP table, then `INSERT … ON CONFLICT DO NOTHING`

**Decision.** Never `INSERT` row-by-row and never `COPY` straight into `raw`.

**Why.** `COPY` is the only fast path into PostgreSQL -- measured at ~17 000
rows/second here -- but it has no conflict handling, so copying straight into a
table with a unique key fails the whole chunk on one duplicate. The two-step
pattern keeps `COPY`'s speed and gets idempotency from the subsequent `INSERT`.
The difference between rows staged and rows inserted is then a free measurement
of how many duplicates the grace window re-read.

**Cost.** Each chunk writes its data twice.

**Note (learned the hard way).** The staging column for `payload` is **`text`**,
not `jsonb`, and the `INSERT` does `CAST(payload AS JSONB)`. Declaring the COPY
type as `jsonb` while passing an already-serialised string stores the payload as
a JSON *string*, so every `payload ->> 'site_id'` silently returns `NULL`. That
cost an afternoon; the shape of the fix is in `load/copy_loader.py`.

**Wrong when.** Chunks get large enough that the double write matters. Then it
is `COPY` into a permanent staging table partitioned by batch, swapped in.

---

## ADR-005 -- Watermark = highest `updated_at` observed, minus a grace window

**Decision.** Per-source watermark in `meta.source_watermark`; the next read
starts at `watermark - HELIOS_LATE_ARRIVAL_GRACE_MINUTES`; the watermark is
advanced with `GREATEST(existing, new)`.

**Why.** Three separate failure modes, one design:

- Storing the wall clock loses records the source had not yet produced.
- Reading strictly `>` the watermark loses records that commit out of stamp
  order -- silently, permanently.
- Without `GREATEST`, a backfill winds the cursor backwards and the next
  scheduled run re-ingests everything since.

**Cost.** Measured: 347 records re-read and discarded on a second run, 4.8 s.
Every run pays it.

**Wrong when.** Lateness regularly exceeds the window. Widening it is a
treadmill; the real answer is a bitemporal model (`valid_time` /
`transaction_time`) or a change stream. See `incremental.md`.

**Rejected alternative.** Deleting and re-reading a fixed trailing window every
run. Simpler, and it would have made the pipeline non-idempotent against
corrections -- the delete would remove a fix that the re-read no longer returns.

---

## ADR-006 -- Content hash over canonical JSON as the raw idempotency key

**Decision.** `sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")))`,
computed **after** contract normalisation. Key is
`(source_name, natural_key, content_hash)`.

**Why.** `sort_keys` removes serialisation order from the hash: without it a
source that emits its keys differently one day duplicates every record. Hashing
after normalisation removes formatting from it: `"1234.50"` and `1234.5` are the
same reading and must hash identically (a unit test asserts exactly this).
Keying on content rather than on the natural key alone makes raw append-only, so
a correction adds a row instead of overwriting the value it corrects.

**Cost.** A hash per record, and corrections accumulate rows in raw.

**Wrong when.** Payloads carry a field that changes on every read without
meaning anything (a server-side `retrieved_at`, a trace id). Then the hash must
be computed over a projection, and the projection needs to be versioned with the
contract.

---

## ADR-007 -- `ON CONFLICT DO UPDATE … WHERE EXCLUDED.source_updated_at >= existing`

**Decision.** Core upserts are guarded by a recency predicate.

**Why.** This is the clause that is usually missing. Without it, replaying an
older batch overwrites a newer correction with the value it superseded. The bug
only appears after someone runs a backfill, which is to say after the pipeline
is in production and trusted.

**Cost.** One more comparison per row, and a genuine out-of-order correction
from the source is ignored rather than applied -- correct, given that the
source's own `updated_at` is the only ordering signal available.

**Wrong when.** The source's `updated_at` is unreliable. Then ordering has to
come from a sequence or a log offset, not from a timestamp.

---

## ADR-008 -- Contract violations are dead-lettered, not fatal

**Decision.** A record failing its contract goes to `meta.dead_letter` with the
payload and the failing field; the run continues. Replay re-validates against
current contracts. Repeat failures are counted and marked `ABANDONED` after
`HELIOS_DLQ_MAX_ATTEMPTS`.

**Why.** Failing the batch sounds rigorous and is not: one malformed record in a
hundred thousand blocks a hundred thousand good ones, at 3 a.m., and whoever is
on call switches the pipeline off within a week. A quarantine that names the
failing field turns "26 records failed" into "8 records arrived with an empty
`meter_id`", which is a conversation with the upstream team.

**Cost.** The pipeline can succeed while quietly dropping data. Mitigated by
making `dead_letter_rate_pct` a quality check with a threshold, and by
`helios dlq show` being part of the routine.

**Wrong when.** The domain cannot tolerate partial loads at all -- billing
settlement, financial close. There, fail the batch and page someone.

---

## ADR-009 -- Only transient errors are retried, with full jitter

**Decision.** Retry `TransientSourceError` only: timeouts, connection resets,
408, 425, 429, 5xx. Delay is `Retry-After` when the server sends one, otherwise
`uniform(0, min(base · 2ⁿ, cap))`.

**Why.** A 4xx, a missing column or a contract violation does not become correct
by being asked again; retrying them burns the budget and delays the real
failure. Full jitter -- a uniform draw over the whole interval rather than the
interval's endpoint -- is what stops every client that failed at the same moment
from retrying at the same moment; it is the variant AWS measured as best in
*Exponential Backoff and Jitter* (2015). Obeying `Retry-After` is the difference
between being throttled and being blocked.

**Cost.** Worst-case latency is unbounded up to the retry budget, so a run can
be slow rather than failing fast.

**Testability.** `sleeper` and `rng` are injected, so the unit tests assert the
delay sequence against the formula without waiting for it.

**Wrong when.** The caller has a hard deadline. Then the budget becomes a
wall-clock deadline rather than an attempt count.

---

## ADR-010 -- The upstream is an HTTP service, and its faults stay on in CI

**Decision.** `helios serve` runs a FastAPI service with cursor pagination,
injected 503s (15%) and rate limits (5%). The pipeline is an ordinary client of
it. Fault injection is **not** disabled in CI.

**Why.** A pipeline demonstrated against a local file never exercises the parts
that break in production: pagination that skips a row on a tie, a cursor that
loops, a 503 halfway through page 30, a rate limiter that expects to be obeyed.
A retry policy that has never retried anything in CI is a retry policy nobody
has tested.

**Cost.** CI is non-deterministic in its timing and occasionally slower. The
*outcome* stays deterministic, because the retries succeed -- which is the
property being tested.

**Wrong when.** Flakiness ever becomes indistinguishable from a real failure.
The seed (`HELIOS_RANDOM_SEED`) exists so the fault pattern can be pinned.

---

## ADR-011 -- Cursor pagination keyed `(updated_at, reading_id)`

**Decision.** Opaque base64 cursor over a composite key; `updated_since` is
**inclusive**.

**Why.** Offset pagination over a table that is being written to skips or
repeats rows. A cursor on `updated_at` alone is no better here: at one-second
resolution with sixty meters, ties are the normal case, so a cursor on the
timestamp either drops the rest of a tied group or returns it forever. The
composite key totally orders the set. The bound is inclusive because re-sending
a record the client already has costs nothing (the hash absorbs it) while losing
one costs a hole nobody notices for weeks.

**Cost.** Boundary records are transferred twice.

**Opaqueness.** Base64 because a cursor is the server's business; a client that
parses one will depend on its shape, and the shape will change.

---

## ADR-012 -- Consumption is a physical mart table, refreshed per day

**Decision.** `mart.consumption_interval` is a real table. `refresh_consumption.sql`
deletes and rebuilds whole days.

**Why.** The delta calculation is a window function over each meter's whole
history; running it on every dashboard query wastes work on a result that only
changes when readings land. Delete-then-insert scoped per day keeps the refresh
idempotent and makes a backfill cheap -- rebuilding yesterday does not touch
three months of history.

**Cost.** The mart is stale between refreshes, and the staleness is visible in
`mart.v_ingestion_overview` rather than hidden.

**Rejected alternative.** A materialised view. `REFRESH MATERIALIZED VIEW` is
all-or-nothing: no per-day scope, so a one-day backfill would rebuild
everything.

---

## ADR-013 -- Meter deltas are classified per meter, against that meter's own scale

**Decision.** Each interval is flagged `ok`, `gap`, `flat`, `first_reading`,
`correction`, `reset`, `rollover` or `implausible`. Thresholds are derived per
meter from `PERCENTILE_CONT(0.95)` and the median of its own forward deltas.

**Why.** This is the decision the project was most wrong about first, and the
correction is the interesting part. The original rollover heuristic was
global -- "the previous index is above 85% of the register width, so this
backward step is a wrap". It misclassified meter resets as rollovers and
invented roughly **10⁹ kWh** of consumption, which surfaced as a
`NumericValueOutOfRange` rather than as a wrong number, purely by luck.

A register that wraps produces a backward step whose wrapped delta is plausible
*for that meter*. A meter replaced on site produces one that is not. Nothing
global distinguishes them; the meter's own recent history does. A second split
was needed after that: one corrupt spike inflated its own p99 threshold and hid
itself, so rollover and correction are judged against p95 while `implausible`
is judged against the median.

**Cost.** A meter with fewer than a handful of clean readings has no reliable
scale, so its first intervals are conservative. Flags are a heuristic, and the
document says so.

**Consequence.** `reset` and `implausible` yield `NULL` consumption, never
zero: zero is a measurement, `NULL` is the absence of one, and summing zeros
understates a total silently.

**A third bug, found while writing this document.** `implausible` was being
decided in the final `SELECT`, *after* the value had already been computed --
so the row was flagged and kept its 5 · 10⁸ kWh delta. Every view that filtered
on the flag was correct; a plain `SUM(consumption_kwh)` over the table was
wrong by a factor of fifty. The classification now happens one CTE earlier, so
the value is nulled at the same moment the flag is set. The general lesson is
narrow and worth stating: **a flag that does not change the number is a flag
nobody downstream honours.**

---

## ADR-014 -- Quality checks are graded, and only BLOCKING fails the run

**Decision.** 18 checks at three severities. BLOCKING raises `DataQualityError`;
WARNING and INFO are recorded in `meta.quality_check` and printed.

**Why.** A check that cannot fail anything is decoration; a suite where
everything fails the run gets disabled the first time a weekend produces a low
completeness figure. The split is the whole point: `no_negative_consumption`
means the delta logic is broken and must stop; `reset_rate_pct` above threshold
means someone should look at the estate.

**Cost.** Warnings are ignorable, and will be ignored unless someone watches
`mart.v_quality_dashboard`.

**Wrong when.** A warning has been firing for a month. Either it becomes
blocking or it is deleted -- a permanently amber check is worse than no check.

---

## ADR-015 -- Timestamps are `timestamptz` in UTC; Europe/Paris is a presentation concern

**Decision.** Everything stored is `timestamptz`, normalised to UTC at the
contract boundary. Business-day and tariff-band logic converts to
`Europe/Paris` at query time.

**Why.** Storing local time loses an hour every October and invents one every
March. Storing UTC and converting late makes the DST transition a property of
the query rather than a corruption of the data.

**Cost.** Two days a year have 23 and 25 hours, so `interval_completeness_pct`
is expected to deviate on exactly those dates. Documented rather than silently
smoothed.

---

## ADR-016 -- One CLI command per pipeline task

**Decision.** `helios <task>` maps one-to-one onto a function in
`helios.pipeline`. No orchestration logic in the CLI.

**Why.** It makes local runs, Docker, CI and an orchestrator invoke exactly the
same code path, and it makes the Airflow DAG in `orchestration.md` a list of
`BashOperator` calls rather than a second implementation of the pipeline that
drifts from the first one.

**Cost.** Cross-task state has to go through the database (`meta.pipeline_run`,
`meta.source_watermark`) rather than through process memory. That is also why
each task is independently retryable.
