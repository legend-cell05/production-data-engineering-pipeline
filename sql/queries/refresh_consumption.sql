-- ===========================================================================
-- Refresh mart.consumption_interval for a date window.
--
-- This is where a cumulative meter index becomes usable consumption, and where
-- the things that make metering data awkward are dealt with once, explicitly,
-- instead of being rediscovered by every analyst.
--
-- THE HARD CASE: telling a rollover from a reset.
--
-- Both look identical in the data -- the index went backwards. But they mean
-- opposite things:
--
--   ROLLOVER  The register wrapped at 10^digits. No energy was lost; the real
--             consumption is (register_max - previous) + current.
--   RESET     The index went backwards by more than this meter plausibly
--             consumes, and the wrapped reading is not credible either: a
--             replacement, a cleared register, or a corrupted value. In
--             practice this flag means "backwards and unexplained", which is
--             the operationally useful definition -- the consumption over that
--             step is unknowable whatever the cause.
--
-- The obvious test -- "was the previous index near the top of the register?" --
-- is wrong, and wrong in the expensive direction. A meter sitting at 86% of its
-- register that is then reset to zero passes that test, and the wrapped
-- arithmetic invents 14% of a register's worth of consumption out of nothing.
-- On an eight-digit register with a CT multiplier that is a billion phantom
-- kilowatt-hours in a single row.
--
-- So the test used here is physical rather than positional: compute the wrapped
-- delta and ask whether it is plausible for the time elapsed. "Plausible" is
-- calibrated per meter from its own interval consumption, so a data centre and
-- a village school are each judged against themselves. A wrapped delta within
-- an order of magnitude of that is a rollover; anything larger is a reset, and
-- its consumption is NULL.
--
-- Writing 0 instead would understate the total; writing the negative delta
-- would corrupt every aggregate above it. NULL is the only honest answer, and
-- `delta_flag` records why.
--
-- GAPS: a meter that was offline missed intervals. The delta is still correct
-- in total but covers more than one interval, so `span_minutes` records the
-- real span and the row is flagged. Attributing it all to the closing timestamp
-- is approximate, and saying so in a column beats hiding it.
--
-- TIMEZONE: dates and hours are computed in Europe/Paris, because tariff bands
-- and "which day did this belong to" are local-time questions. Storage stays in
-- UTC. Two hours therefore exist twice a year and one does not exist at all --
-- see docs/data-model.md.
--
-- Parameters: :from_date, :to_date (inclusive), :interval_minutes
-- ===========================================================================

DELETE FROM ${MART}.consumption_interval
WHERE reading_date >= CAST(:from_date AS DATE)
  AND reading_date <= CAST(:to_date AS DATE);

INSERT INTO ${MART}.consumption_interval (
    meter_id, site_id, reading_ts, reading_date, hour_of_day,
    index_kwh, previous_index, consumption_kwh, span_minutes,
    average_power_kw, delta_flag
)
WITH scoped AS (
    -- One extra day of lookback so the first interval of :from_date has a
    -- predecessor. A meter offline for more than a day is reported as
    -- `first_reading` rather than silently bridging a long gap.
    SELECT
        r.meter_id,
        r.reading_ts,
        r.index_kwh,
        m.site_id,
        m.interval_minutes,
        POWER(10, m.index_digits)::NUMERIC * m.multiplier AS register_max_kwh
    FROM ${CORE}.meter_reading r
    JOIN ${CORE}.meter m ON m.meter_id = r.meter_id
    WHERE r.reading_ts >= (CAST(:from_date AS DATE) - INTERVAL '1 day')
      AND r.reading_ts <  (CAST(:to_date AS DATE) + INTERVAL '1 day')
),
with_previous AS (
    SELECT
        s.*,
        LAG(s.index_kwh)  OVER w AS previous_index,
        LAG(s.reading_ts) OVER w AS previous_ts
    FROM scoped s
    WINDOW w AS (PARTITION BY s.meter_id ORDER BY s.reading_ts)
),
-- What a normal interval looks like for THIS meter, described by two
-- statistics because the two questions need different ones:
--
--   p95    generous. Used to decide whether a backward step is small enough
--          to be a correction, and whether a wrapped delta is credible. Being
--          strict here would report corrections as resets and throw away
--          usable intervals.
--   median robust. Used to decide whether a FORWARD step is absurd. A
--          percentile is the wrong tool for that: one corrupt value of a
--          million inflates the p95 enough to make itself look normal, which
--          is precisely the value the check exists to catch.
meter_scale AS (
    SELECT
        meter_id,
        PERCENTILE_CONT(0.95) WITHIN GROUP (
            ORDER BY (index_kwh - previous_index)
        ) AS p95_delta_kwh,
        PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY (index_kwh - previous_index)
        ) AS median_delta_kwh
    FROM with_previous
    WHERE previous_index IS NOT NULL
      AND index_kwh >= previous_index
    GROUP BY meter_id
),
measured AS (
    SELECT
        p.*,
        ROUND(EXTRACT(EPOCH FROM (p.reading_ts - p.previous_ts)) / 60.0)::INT AS span_minutes,
        (p.register_max_kwh - p.previous_index) + p.index_kwh                 AS wrapped_delta,
        COALESCE(ms.p95_delta_kwh, 0)                                         AS p95_delta_kwh,
        COALESCE(ms.median_delta_kwh, 0)                                      AS median_delta_kwh
    FROM with_previous p
    LEFT JOIN meter_scale ms ON ms.meter_id = p.meter_id
),
classified AS (
    SELECT
        m.*,
        -- Intervals the step actually spans, so a delta across a gap is
        -- compared against the consumption of that many intervals.
        GREATEST(1.0, m.span_minutes::NUMERIC / NULLIF(m.interval_minutes, 0)) AS interval_factor,
        CASE
            WHEN m.previous_index IS NULL THEN 'first_reading'
            -- A tiny backward step is neither a wrap nor a reset. It is a
            -- substituted reading that came back slightly below the one before
            -- it -- routine in metering, and the reason a naive
            -- "index went down => reset" rule reports hundreds of resets on an
            -- estate that had four.
            WHEN m.index_kwh < m.previous_index
             AND m.p95_delta_kwh > 0
             AND (m.previous_index - m.index_kwh) <= m.p95_delta_kwh
                  * GREATEST(1.0, m.span_minutes::NUMERIC / NULLIF(m.interval_minutes, 0))
                THEN 'correction'
            WHEN m.index_kwh < m.previous_index
             AND m.p95_delta_kwh > 0
             AND m.wrapped_delta <= 10 * m.p95_delta_kwh
                  * GREATEST(1.0, m.span_minutes::NUMERIC / NULLIF(m.interval_minutes, 0))
                THEN 'rollover'
            WHEN m.index_kwh < m.previous_index THEN 'reset'
            ELSE 'forward'
        END AS step_kind
    FROM measured m
),
valued AS (
    SELECT
        c.*,
        CASE c.step_kind
            WHEN 'first_reading' THEN NULL
            WHEN 'reset'         THEN NULL
            WHEN 'rollover'      THEN c.wrapped_delta
            -- The index did not really move. Zero understates by less than one
            -- interval's consumption, which is the smallest honest answer
            -- available; NULL would discard a usable interval.
            WHEN 'correction'    THEN 0
            ELSE c.index_kwh - c.previous_index
        END AS consumption_kwh
    FROM classified c
)
SELECT
    v.meter_id,
    v.site_id,
    v.reading_ts,
    (v.reading_ts AT TIME ZONE 'Europe/Paris')::DATE                        AS reading_date,
    EXTRACT(HOUR FROM (v.reading_ts AT TIME ZONE 'Europe/Paris'))::SMALLINT  AS hour_of_day,
    v.index_kwh,
    v.previous_index,
    v.consumption_kwh,
    v.span_minutes,
    CASE
        WHEN v.consumption_kwh IS NULL OR COALESCE(v.span_minutes, 0) = 0 THEN NULL
        ELSE ROUND(v.consumption_kwh / (v.span_minutes / 60.0), 3)
    END                                                                     AS average_power_kw,
    CASE
        WHEN v.step_kind = 'first_reading' THEN 'first_reading'
        WHEN v.step_kind = 'rollover'      THEN 'rollover'
        WHEN v.step_kind = 'reset'         THEN 'reset'
        WHEN v.step_kind = 'correction'    THEN 'correction'
        -- A forward step far beyond what this meter normally does in the time
        -- elapsed. Kept, flagged, and excluded from totals: a value that large
        -- is a data problem, not a consumption event.
        WHEN v.median_delta_kwh > 0
         AND v.consumption_kwh > 25 * v.median_delta_kwh * v.interval_factor
                                           THEN 'implausible'
        WHEN v.span_minutes > CAST(:interval_minutes AS INT) * 1.5 THEN 'gap'
        WHEN v.consumption_kwh = 0         THEN 'flat'
        ELSE 'ok'
    END                                                                     AS delta_flag
FROM valued v
WHERE (v.reading_ts AT TIME ZONE 'Europe/Paris')::DATE >= CAST(:from_date AS DATE)
  AND (v.reading_ts AT TIME ZONE 'Europe/Paris')::DATE <= CAST(:to_date AS DATE)
ON CONFLICT (meter_id, reading_ts) DO NOTHING;
