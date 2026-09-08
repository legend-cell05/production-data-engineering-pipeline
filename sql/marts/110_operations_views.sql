-- ===========================================================================
-- 110 -- Pipeline and data-health views
--
-- These are not "nice to have". In telemetry the common failure is not a
-- crash, it is silence: a meter stops reporting, nobody notices for three
-- weeks, and the monthly total is quietly 4% low. Completeness and meter
-- health are the metrics that catch that.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- v_interval_completeness -- received versus expected, per meter per day.
--
-- Expected count comes from the meter's configured interval, which is why
-- core.meter stores it rather than assuming 15 minutes everywhere.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${MART}.v_interval_completeness AS
WITH received AS (
    SELECT
        r.meter_id,
        (r.reading_ts AT TIME ZONE 'Europe/Paris')::DATE AS reading_date,
        COUNT(*)                                          AS intervals_received
    FROM ${CORE}.meter_reading r
    GROUP BY r.meter_id, (r.reading_ts AT TIME ZONE 'Europe/Paris')::DATE
)
SELECT
    rec.meter_id,
    m.site_id,
    s.site_name,
    m.meter_type,
    rec.reading_date,
    rec.intervals_received,
    (1440 / m.interval_minutes)                          AS intervals_expected,
    ROUND(
        100.0 * rec.intervals_received / NULLIF(1440 / m.interval_minutes, 0), 2
    )                                                    AS completeness_pct,
    (1440 / m.interval_minutes) - rec.intervals_received AS intervals_missing
FROM received rec
JOIN ${CORE}.meter m ON m.meter_id = rec.meter_id
JOIN ${CORE}.site  s ON s.site_id  = m.site_id;

COMMENT ON VIEW ${MART}.v_interval_completeness IS
    'Received vs expected readings per meter per day. The metric that catches a meter going silent.';


-- ---------------------------------------------------------------------------
-- v_meter_health -- one row per meter, summarising whether it can be trusted.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${MART}.v_meter_health AS
WITH flags AS (
    SELECT
        meter_id,
        COUNT(*)                                                  AS intervals,
        COUNT(*) FILTER (WHERE delta_flag = 'ok')                 AS ok_intervals,
        COUNT(*) FILTER (WHERE delta_flag = 'gap')                AS gap_intervals,
        COUNT(*) FILTER (WHERE delta_flag = 'flat')               AS flat_intervals,
        COUNT(*) FILTER (WHERE delta_flag = 'rollover')           AS rollovers,
        COUNT(*) FILTER (WHERE delta_flag = 'reset')              AS resets,
        COUNT(*) FILTER (WHERE delta_flag = 'implausible')        AS implausible,
        SUM(consumption_kwh)                                      AS total_kwh,
        MAX(reading_ts)                                           AS last_reading_at
    FROM ${MART}.consumption_interval
    GROUP BY meter_id
),
completeness AS (
    SELECT meter_id, ROUND(AVG(completeness_pct), 2) AS avg_completeness_pct
    FROM ${MART}.v_interval_completeness
    GROUP BY meter_id
)
SELECT
    f.meter_id,
    m.site_id,
    s.site_name,
    m.meter_type,
    f.intervals,
    f.ok_intervals,
    f.gap_intervals,
    f.flat_intervals,
    f.rollovers,
    f.resets,
    f.implausible,
    ROUND(f.total_kwh, 1)                                  AS total_kwh,
    f.last_reading_at,
    c.avg_completeness_pct,
    ROUND(100.0 * f.ok_intervals / NULLIF(f.intervals, 0), 2) AS clean_interval_pct,
    -- A meter reporting nothing but zeros is not a quiet building; it is a
    -- meter that has stopped counting. Ninety per cent flat is the threshold
    -- where that stops being plausible.
    (f.flat_intervals > 0.90 * f.intervals)                AS looks_stuck,
    CASE
        WHEN f.resets > 0                                   THEN 'reset_detected'
        WHEN f.implausible > 0                              THEN 'implausible_values'
        WHEN f.flat_intervals > 0.90 * f.intervals          THEN 'stuck'
        WHEN COALESCE(c.avg_completeness_pct, 0) < 90       THEN 'incomplete'
        WHEN f.gap_intervals > 0.05 * f.intervals           THEN 'gappy'
        ELSE 'healthy'
    END                                                    AS health_status
FROM flags f
JOIN ${CORE}.meter m ON m.meter_id = f.meter_id
JOIN ${CORE}.site  s ON s.site_id  = m.site_id
LEFT JOIN completeness c ON c.meter_id = f.meter_id;


-- ---------------------------------------------------------------------------
-- v_ingestion_overview -- the last run per source, and how fresh it is.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${MART}.v_ingestion_overview AS
SELECT
    w.source_name,
    w.watermark_value,
    w.grace_minutes,
    w.records_seen,
    w.last_success_at,
    ROUND(EXTRACT(EPOCH FROM (now() - w.last_success_at)) / 60.0, 1) AS minutes_since_success,
    ROUND(EXTRACT(EPOCH FROM (now() - w.watermark_value)) / 60.0, 1) AS watermark_lag_minutes,
    r.status                          AS last_run_status,
    r.records_read                    AS last_run_read,
    r.records_ingested                AS last_run_ingested,
    r.records_duplicate               AS last_run_duplicate,
    r.records_dead_lettered           AS last_run_dead_lettered,
    r.retries_performed               AS last_run_retries,
    r.duration_seconds                AS last_run_seconds
FROM ${META}.source_watermark w
LEFT JOIN LATERAL (
    SELECT *
    FROM ${META}.pipeline_run pr
    WHERE pr.source_name = w.source_name
    ORDER BY pr.started_at DESC
    LIMIT 1
) r ON TRUE;


-- ---------------------------------------------------------------------------
-- v_dlq_overview -- what is parked, per source and per cause.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${MART}.v_dlq_overview AS
SELECT
    source_name,
    error_type,
    failed_field,
    status,
    COUNT(*)              AS records,
    MAX(attempts)         AS max_attempts,
    MIN(first_failed_at)  AS oldest_failure,
    MAX(last_failed_at)   AS latest_failure
FROM ${META}.dead_letter
GROUP BY source_name, error_type, failed_field, status;


-- ---------------------------------------------------------------------------
-- v_pipeline_health -- one row, for a status endpoint or a dashboard banner.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${MART}.v_pipeline_health AS
WITH last_run AS (
    SELECT batch_id, status, started_at, finished_at, duration_seconds
    FROM ${META}.pipeline_run
    ORDER BY started_at DESC
    LIMIT 1
),
quality AS (
    SELECT
        COUNT(*)                                             AS checks_run,
        COUNT(*) FILTER (WHERE passed)                       AS checks_passed,
        COUNT(*) FILTER (WHERE NOT passed)                   AS checks_failed,
        COUNT(*) FILTER (WHERE NOT passed AND severity = 'BLOCKING') AS blocking_failures
    FROM ${META}.quality_result
    WHERE batch_id = (SELECT batch_id FROM last_run)
)
SELECT
    lr.batch_id,
    lr.status                                          AS last_run_status,
    lr.started_at                                      AS last_run_at,
    lr.duration_seconds,
    q.checks_run,
    q.checks_passed,
    q.checks_failed,
    q.blocking_failures,
    (SELECT COUNT(*) FROM ${META}.dead_letter WHERE status = 'PENDING') AS dlq_pending,
    (SELECT COUNT(*) FROM ${CORE}.meter_reading)       AS readings_in_core,
    (SELECT MAX(reading_ts) FROM ${CORE}.meter_reading) AS latest_reading_at,
    ROUND(
        EXTRACT(EPOCH FROM (now() - (SELECT MAX(reading_ts) FROM ${CORE}.meter_reading))) / 3600.0,
        2
    )                                                  AS hours_since_latest_reading
FROM last_run lr CROSS JOIN quality q;
