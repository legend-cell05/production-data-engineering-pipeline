-- ===========================================================================
-- Promote raw -> core for meter readings.
--
-- This is the high-volume path: the readings partition of raw holds two orders
-- of magnitude more rows than every reference source combined.
--
-- Three things are happening here that are worth reading closely:
--
--   1. The multiplier is applied. Raw holds the register value exactly as the
--      meter reported it; core holds kWh. Keeping both means a reading can
--      always be reconciled against the physical device.
--
--   2. `DISTINCT ON (meter_id, reading_ts)` collapses corrections. Raw is
--      append-only, so a re-sent interval with a different value is a second
--      row with a different content hash. The freshest source_updated_at wins.
--
--   3. Readings whose meter is not in core are skipped by the join rather than
--      failing the batch. They stay in raw and are picked up by a later
--      promotion once the reference data catches up -- reference and telemetry
--      arrive on different schedules, and telemetry is usually first.
--
-- Parameter: :batch_id -- one batch, or NULL to rebuild from the whole of raw.
-- ===========================================================================

INSERT INTO ${CORE}.meter_reading (
    meter_id, reading_ts, index_kwh, quality_flag, source_updated_at, batch_id
)
SELECT DISTINCT ON (r.payload ->> 'meter_id', (r.payload ->> 'reading_ts')::TIMESTAMPTZ)
    r.payload ->> 'meter_id',
    (r.payload ->> 'reading_ts')::TIMESTAMPTZ,
    ROUND((r.payload ->> 'register_value')::NUMERIC * m.multiplier, 3),
    COALESCE(r.payload ->> 'quality_flag', 'measured'),
    r.source_updated_at,
    r.batch_id
FROM ${RAW}.record r
JOIN ${CORE}.meter m ON m.meter_id = r.payload ->> 'meter_id'
WHERE r.source_name = 'meter_readings'
  AND (CAST(:batch_id AS UUID) IS NULL OR r.batch_id = CAST(:batch_id AS UUID))
ORDER BY
    r.payload ->> 'meter_id',
    (r.payload ->> 'reading_ts')::TIMESTAMPTZ,
    r.source_updated_at DESC,
    r.ingested_at DESC
ON CONFLICT (meter_id, reading_ts) DO UPDATE
SET index_kwh         = EXCLUDED.index_kwh,
    quality_flag      = EXCLUDED.quality_flag,
    source_updated_at = EXCLUDED.source_updated_at,
    batch_id          = EXCLUDED.batch_id,
    ingested_at       = now()
-- A correction only wins if it is genuinely newer. Replaying an old batch
-- must not resurrect a superseded value.
WHERE EXCLUDED.source_updated_at >= ${CORE}.meter_reading.source_updated_at;
