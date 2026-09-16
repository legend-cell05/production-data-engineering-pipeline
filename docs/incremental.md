# Incremental ingestion

This is the document that explains what the project is actually about.

Anyone can write a loop that reads an API and inserts rows. What makes a
pipeline survive contact with a scheduler is the answer to four questions:

1. Where did I get to? — **watermarks**
2. What if I read the same thing twice? — **idempotency**
3. What if the source is down? — **retries**
4. What if one record is broken? — **the dead-letter queue**

---

## 1. Watermarks

A watermark is the highest source-side `updated_at` that has been ingested for
a source. It lives in `meta.source_watermark`, one row per source.

```
next read starts at:  watermark − grace_window
```

### Why not "now"

The obvious implementation stores the wall clock at the end of a run. It is
wrong, and wrong in the direction that loses data permanently: any record the
source had not yet produced when the run finished now sits *before* the
cursor and will never be read.

The watermark here is set to **the highest `updated_at` actually observed in
the response**, never to the clock:

```python
if fetch_result.max_source_updated_at is not None:
    advance_watermark(source.name, fetch_result.max_source_updated_at, ...)
```

If the source returns nothing, the watermark does not move. A quiet source and
an up-to-date source look different, which is what `watermark_lag_minutes` in
`mart.v_ingestion_overview` is for.

### Why the grace window

Records do not become visible in the order they are stamped. A transaction
that starts first can commit second, so a row with an earlier `updated_at`
appears *after* one with a later `updated_at` has already been read. Reading
strictly greater than the watermark loses exactly those rows — silently,
permanently, and in a way nobody notices until a monthly total is short.

So every run re-reads a window:

```
HELIOS_LATE_ARRIVAL_GRACE_MINUTES=90
```

### What it costs, measured

On the default configuration, the second run of the day:

| | |
| --- | --- |
| Records read | 347 |
| Records ingested | **0** |
| Duplicates absorbed | **347** |
| Duration | 4.9 s |

Three hundred and forty-seven records re-read and thrown away by the primary
key. That is the entire price of the guarantee, and it appears in every run
report rather than being hidden.

### What it does not fix

A record that arrives **later than the grace window** is still missed. The
generator produces some on purpose (`very_late_arrival`, ~5 hours against a
90-minute window) so the limitation is demonstrable rather than theoretical.

Widening the window trades cost for coverage and never reaches certainty. The
honest mitigations are:

- **detect it** — `mart.v_interval_completeness` compares received against
  expected per meter per day, so a hole shows up as a completeness dip;
- **repair it** — `helios backfill --since` re-reads a bounded window, and is
  safe because everything below is idempotent.

Claiming the grace window makes loss impossible would be the real mistake.

---

## 2. Idempotency

Three mechanisms, each at a different layer.

### Content hash, in raw

```python
canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
content_hash = hashlib.sha256(canonical.encode()).hexdigest()
```

`raw.record` is keyed on `(source_name, natural_key, content_hash)`. Re-reading
an unchanged record produces a row that already exists, and the insert is a
no-op. A record whose *content* changed produces a new hash and a new row —
raw is append-only, so corrections accumulate rather than overwrite.

`sort_keys` matters more than it looks: without it, a source that serialises
its JSON keys in a different order on Tuesday would produce a different hash
for identical data, and every record would duplicate.

So does normalisation. `"1234.50"` and `1234.5` are the same reading; the
contract coerces both to the same canonical number before hashing, and a unit
test asserts they hash identically.

### Natural-key upsert, in core

```sql
ON CONFLICT (meter_id, reading_ts) DO UPDATE
SET index_kwh = EXCLUDED.index_kwh, …
WHERE EXCLUDED.source_updated_at >= core.meter_reading.source_updated_at
```

The `WHERE` on the `DO UPDATE` is the part that is usually missing. Without it,
replaying an old batch overwrites a newer correction with the value it
superseded — a data-loss bug that only appears after someone runs a backfill.

### Delete-then-insert, in the mart

`refresh_consumption.sql` deletes the window it is about to rebuild. Scoped per
day, so rebuilding yesterday does not touch three months of history.

### Proven, not asserted

An integration test runs the whole pipeline twice and compares **both** the row
count and the total kWh, and CI does the same. Comparing only the row count
would miss an upsert that silently changed a value.

---

## 3. Retries

Only `TransientSourceError` is retried: timeouts, connection resets, 408, 425,
429, 5xx. A 4xx, a missing column or a contract violation will not become
correct by being asked again, and retrying them burns the budget while delaying
the real failure.

```
delay = Retry-After                      when the server sent one
      = uniform(0, min(base·2ⁿ, cap))    otherwise
```

**Exponential**, so a source that is genuinely down is not hammered.
**Full jitter** — a uniform draw over `[0, ceiling]` rather than the ceiling
itself — because every client that failed at the same moment would otherwise
retry at the same moment and keep colliding. That variant is the one AWS
measured as best in *Exponential Backoff and Jitter* (2015).
**`Retry-After` wins**, because ignoring a rate limiter is how a client gets
blocked rather than throttled.

The simulated source fails 15% of requests and rate-limits 5% by default, and
those rates stay on in CI. A typical run performs 4 retries across 34 pages and
loses nothing.

`sleeper` and `rng` are injectable, so the unit tests assert the delay sequence
against the formula without waiting.

---

## 4. The dead-letter queue

A record that violates its contract is parked in `meta.dead_letter` with its
payload and the failing field, and the run continues.

Failing the batch instead sounds rigorous and is not: one malformed record out
of a hundred thousand would block a hundred thousand good ones, at 3 a.m., and
the pipeline would be switched off within a week by whoever is on call.

```
$ helios dlq show
┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┓
┃ source         ┃ error             ┃ field          ┃ status  ┃ records ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━┩
│ meter_readings │ ContractViolation │ register_value │ PENDING │      12 │
│ meter_readings │ ContractViolation │ meter_id       │ PENDING │       8 │
│ meter_readings │ ContractViolation │ quality_flag   │ PENDING │       6 │
└────────────────┴───────────────────┴────────────────┴─────────┴─────────┘
```

Naming the field is what makes that report actionable. "26 records failed" is a
number; "8 records arrived with an empty `meter_id`" is a conversation with the
upstream team.

### It is a queue, not a bin

Each entry keeps the original payload. Once the upstream is fixed — or the
contract is widened — `helios dlq replay` re-validates them against the
*current* contracts and ingests the ones that now pass, exactly as a fresh read
would.

Records that keep failing have their attempt count incremented and are marked
`ABANDONED` after `HELIOS_DLQ_MAX_ATTEMPTS`. Visible, counted, and no longer
pretending they will fix themselves.

One entry per record, not one per failure: `ON CONFLICT (source_name,
natural_key) DO UPDATE SET attempts = attempts + 1`. Otherwise a permanently
broken upstream fills the table with copies of the same problem.

### What the DLQ does *not* catch

A record can satisfy its contract and still be wrong. Five readings per run
reference a meter that does not exist: every field is well-formed, so the
contract accepts them, and they land in raw. Promotion joins them against
`core.meter`, finds nothing, and skips them — deliberately, because the meter
may simply not have been exported yet and the next run may resolve it.

That is a real trade-off: a genuinely unknown meter stays in raw indefinitely
and nobody is paged. It is caught in aggregate by `raw_readings_promoted`,
which compares distinct raw intervals against core rows and warns below 99%.

---

## 5. Backfill

```bash
helios backfill meter_readings --since 2026-09-12T00:00:00Z
```

Re-reads a bounded window, then promotes and refreshes the affected days.

Safe by construction:

- unchanged records are absorbed by the content hash;
- changed ones replace their predecessor, but only if genuinely newer;
- the watermark is updated with `GREATEST`, so re-reading last week cannot wind
  the cursor backwards and cause a re-read of everything since.

That last point is the one that bites. Without `GREATEST`, a backfill would set
the cursor to a week ago, and the next scheduled run would quietly re-ingest
seven days of telemetry. An integration test asserts the watermark only ever
moves forward.

---

## 6. When this design stops being right

| Signal | What to change |
| --- | --- |
| Fact table past ~100 M rows | Drop old partitions on a retention policy; consider monthly → weekly partitions |
| Source emits a change stream | Replace the watermark with a consumer offset; the rest of the pipeline is unchanged |
| More than one writer | Add advisory locks per source; watermark updates are already atomic |
| Records regularly later than the grace window | Move to a bitemporal model (`valid_time` and `transaction_time`) rather than widening the window indefinitely |
| Sub-minute latency required | The batch model is the wrong shape; this becomes a streaming consumer |

The current design is a **batch pipeline with a bounded lateness window**. It
is the right answer for telemetry arriving on a 15-minute cadence with
occasional buffered flushes. It is the wrong answer for anything that needs
exactly-once semantics across a distributed log, and saying so is cheaper than
discovering it later.
