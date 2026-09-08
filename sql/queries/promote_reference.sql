-- ===========================================================================
-- Promote raw -> core for the reference sources.
--
-- Every statement is an UPSERT on the natural key, so promotion is idempotent:
-- running it twice on the same raw rows leaves core identical.
--
-- `DISTINCT ON` picks the freshest payload when raw holds several versions of
-- the same record -- which it will, because raw is append-only and a corrected
-- export produces a new content hash rather than replacing the old row.
--
-- Parameter: :batch_id -- promote one batch, or NULL to rebuild core from the
-- whole of raw. Both paths give the same result; the batch-scoped one is just
-- cheaper.
-- ===========================================================================

-- --- Sites -----------------------------------------------------------------
INSERT INTO ${CORE}.site (
    site_id, site_name, city, region_code, sector,
    floor_area_m2, commissioned_on, is_active, source_updated_at
)
SELECT DISTINCT ON (natural_key)
    payload ->> 'site_id',
    payload ->> 'site_name',
    payload ->> 'city',
    payload ->> 'region_code',
    payload ->> 'sector',
    (payload ->> 'floor_area_m2')::NUMERIC,
    NULLIF(payload ->> 'commissioned_on', '')::DATE,
    COALESCE((payload ->> 'is_active')::BOOLEAN, TRUE),
    source_updated_at
FROM ${RAW}.record
WHERE source_name = 'sites'
  AND (CAST(:batch_id AS UUID) IS NULL OR batch_id = CAST(:batch_id AS UUID))
ORDER BY natural_key, source_updated_at DESC, ingested_at DESC
ON CONFLICT (site_id) DO UPDATE
SET site_name         = EXCLUDED.site_name,
    city              = EXCLUDED.city,
    region_code       = EXCLUDED.region_code,
    sector            = EXCLUDED.sector,
    floor_area_m2     = EXCLUDED.floor_area_m2,
    commissioned_on   = EXCLUDED.commissioned_on,
    is_active         = EXCLUDED.is_active,
    source_updated_at = EXCLUDED.source_updated_at,
    updated_at        = now()
-- Never let an older snapshot overwrite a newer one. Without this guard, a
-- replayed old batch would silently roll the reference data backwards.
WHERE EXCLUDED.source_updated_at >= ${CORE}.site.source_updated_at;


-- --- Meters ----------------------------------------------------------------
INSERT INTO ${CORE}.meter (
    meter_id, site_id, meter_type, unit, multiplier,
    index_digits, interval_minutes, installed_on, is_active, source_updated_at
)
SELECT DISTINCT ON (natural_key)
    payload ->> 'meter_id',
    payload ->> 'site_id',
    payload ->> 'meter_type',
    COALESCE(payload ->> 'unit', 'kWh'),
    COALESCE((payload ->> 'multiplier')::NUMERIC, 1),
    COALESCE((payload ->> 'index_digits')::SMALLINT, 8),
    COALESCE((payload ->> 'interval_minutes')::SMALLINT, 15),
    NULLIF(payload ->> 'installed_on', '')::DATE,
    COALESCE((payload ->> 'is_active')::BOOLEAN, TRUE),
    source_updated_at
FROM ${RAW}.record
WHERE source_name = 'meters'
  AND (CAST(:batch_id AS UUID) IS NULL OR batch_id = CAST(:batch_id AS UUID))
ORDER BY natural_key, source_updated_at DESC, ingested_at DESC
ON CONFLICT (meter_id) DO UPDATE
SET site_id           = EXCLUDED.site_id,
    meter_type        = EXCLUDED.meter_type,
    unit              = EXCLUDED.unit,
    multiplier        = EXCLUDED.multiplier,
    index_digits      = EXCLUDED.index_digits,
    interval_minutes  = EXCLUDED.interval_minutes,
    installed_on      = EXCLUDED.installed_on,
    is_active         = EXCLUDED.is_active,
    source_updated_at = EXCLUDED.source_updated_at,
    updated_at        = now()
WHERE EXCLUDED.source_updated_at >= ${CORE}.meter.source_updated_at;


-- --- Tariffs ---------------------------------------------------------------
INSERT INTO ${CORE}.tariff (
    tariff_id, tariff_name, supplier, currency,
    standing_charge_per_day, valid_from, valid_to, source_updated_at
)
SELECT DISTINCT ON (natural_key)
    payload ->> 'tariff_id',
    payload ->> 'tariff_name',
    payload ->> 'supplier',
    COALESCE(payload ->> 'currency', 'EUR'),
    COALESCE((payload ->> 'standing_charge_per_day')::NUMERIC, 0),
    (payload ->> 'valid_from')::DATE,
    NULLIF(payload ->> 'valid_to', '')::DATE,
    source_updated_at
FROM ${RAW}.record
WHERE source_name = 'tariffs'
  AND (CAST(:batch_id AS UUID) IS NULL OR batch_id = CAST(:batch_id AS UUID))
ORDER BY natural_key, source_updated_at DESC, ingested_at DESC
ON CONFLICT (tariff_id) DO UPDATE
SET tariff_name             = EXCLUDED.tariff_name,
    supplier                = EXCLUDED.supplier,
    currency                = EXCLUDED.currency,
    standing_charge_per_day = EXCLUDED.standing_charge_per_day,
    valid_from              = EXCLUDED.valid_from,
    valid_to                = EXCLUDED.valid_to,
    source_updated_at       = EXCLUDED.source_updated_at,
    updated_at              = now()
WHERE EXCLUDED.source_updated_at >= ${CORE}.tariff.source_updated_at;


-- --- Tariff bands ----------------------------------------------------------
-- The source JSON nests the bands inside the tariff. `jsonb_array_elements`
-- flattens them into rows: keeping them as JSONB would make "cost by time
-- band" an unindexable mess of operators.
INSERT INTO ${CORE}.tariff_band (tariff_id, band_code, hour_start, hour_end, price_per_kwh)
SELECT DISTINCT ON (r.natural_key, band ->> 'band_code')
    r.payload ->> 'tariff_id',
    band ->> 'band_code',
    (band ->> 'hour_start')::SMALLINT,
    (band ->> 'hour_end')::SMALLINT,
    (band ->> 'price_per_kwh')::NUMERIC
FROM ${RAW}.record r
CROSS JOIN LATERAL jsonb_array_elements(r.payload -> 'bands') AS band
WHERE r.source_name = 'tariffs'
  AND (CAST(:batch_id AS UUID) IS NULL OR r.batch_id = CAST(:batch_id AS UUID))
  AND jsonb_typeof(r.payload -> 'bands') = 'array'
ORDER BY r.natural_key, band ->> 'band_code', r.source_updated_at DESC
ON CONFLICT (tariff_id, band_code) DO UPDATE
SET hour_start    = EXCLUDED.hour_start,
    hour_end      = EXCLUDED.hour_end,
    price_per_kwh = EXCLUDED.price_per_kwh;


-- --- Site-to-tariff assignment --------------------------------------------
INSERT INTO ${CORE}.site_tariff (site_id, tariff_id, valid_from, valid_to)
SELECT DISTINCT ON (natural_key)
    payload ->> 'site_id',
    payload ->> 'tariff_id',
    (payload ->> 'valid_from')::DATE,
    NULLIF(payload ->> 'valid_to', '')::DATE
FROM ${RAW}.record
WHERE source_name = 'sites'
  AND payload ? 'tariff_id'
  AND (CAST(:batch_id AS UUID) IS NULL OR batch_id = CAST(:batch_id AS UUID))
ORDER BY natural_key, source_updated_at DESC
ON CONFLICT (site_id, valid_from) DO UPDATE
SET tariff_id = EXCLUDED.tariff_id,
    valid_to  = EXCLUDED.valid_to;


-- --- Weather stations ------------------------------------------------------
INSERT INTO ${CORE}.weather_station (station_id, station_name, region_code)
SELECT DISTINCT ON (payload ->> 'station_id')
    payload ->> 'station_id',
    payload ->> 'station_name',
    payload ->> 'region_code'
FROM ${RAW}.record
WHERE source_name = 'weather'
  AND (CAST(:batch_id AS UUID) IS NULL OR batch_id = CAST(:batch_id AS UUID))
ORDER BY payload ->> 'station_id', source_updated_at DESC
ON CONFLICT (station_id) DO UPDATE
SET station_name = EXCLUDED.station_name,
    region_code  = EXCLUDED.region_code;


-- --- Daily weather ---------------------------------------------------------
INSERT INTO ${CORE}.weather_daily (
    station_id, observed_on, temp_min_c, temp_max_c, temp_avg_c, source_updated_at
)
SELECT DISTINCT ON (natural_key)
    payload ->> 'station_id',
    (payload ->> 'observed_on')::DATE,
    (payload ->> 'temp_min_c')::NUMERIC,
    (payload ->> 'temp_max_c')::NUMERIC,
    (payload ->> 'temp_avg_c')::NUMERIC,
    source_updated_at
FROM ${RAW}.record
WHERE source_name = 'weather'
  AND (CAST(:batch_id AS UUID) IS NULL OR batch_id = CAST(:batch_id AS UUID))
ORDER BY natural_key, source_updated_at DESC, ingested_at DESC
ON CONFLICT (station_id, observed_on) DO UPDATE
SET temp_min_c        = EXCLUDED.temp_min_c,
    temp_max_c        = EXCLUDED.temp_max_c,
    temp_avg_c        = EXCLUDED.temp_avg_c,
    source_updated_at = EXCLUDED.source_updated_at,
    ingested_at       = now()
WHERE EXCLUDED.source_updated_at >= ${CORE}.weather_daily.source_updated_at;
