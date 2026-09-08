-- ===========================================================================
-- 100 -- Energy analytics
--
-- Everything here reads mart.consumption_interval, never core.meter_reading:
-- the awkward cases (rollover, reset, gap) are resolved once in the refresh,
-- and no analyst should have to rediscover them.
--
-- Consistent rule: only rows whose delta is trustworthy contribute to a total.
--
--   ok          a clean step
--   rollover    the register wrapped; the wrapped arithmetic is exact
--   gap         correct in total, approximate in its attribution to one instant
--   flat        the meter really did record nothing
--   correction  the index effectively did not move; counted as zero
--
-- `first_reading` (no predecessor), `reset` (unknowable) and `implausible`
-- (corrupt) are excluded. v_meter_health reports how much that costs, per
-- meter, so the exclusion is visible rather than quietly shrinking a total.
-- ===========================================================================

CREATE OR REPLACE FUNCTION ${MART}.is_usable(p_flag TEXT)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $fn$
    SELECT p_flag IN ('ok', 'rollover', 'gap', 'flat', 'correction');
$fn$;

COMMENT ON FUNCTION ${MART}.is_usable(TEXT) IS
    'Single definition of a delta that may contribute to a total. Used by every view.';


-- ---------------------------------------------------------------------------
-- v_consumption_hourly -- the base for the load curve and for costing.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${MART}.v_consumption_hourly AS
SELECT
    c.site_id,
    s.site_name,
    s.sector,
    s.region_code,
    c.reading_date,
    c.hour_of_day,
    COUNT(*)                                        AS intervals,
    COUNT(*) FILTER (WHERE c.delta_flag <> 'ok')    AS intervals_flagged,
    SUM(c.consumption_kwh)                          AS consumption_kwh,
    ROUND(AVG(c.average_power_kw), 3)               AS average_power_kw,
    ROUND(MAX(c.average_power_kw), 3)               AS peak_power_kw
FROM ${MART}.consumption_interval c
JOIN ${CORE}.site s ON s.site_id = c.site_id
WHERE ${MART}.is_usable(c.delta_flag)
GROUP BY c.site_id, s.site_name, s.sector, s.region_code, c.reading_date, c.hour_of_day;


-- ---------------------------------------------------------------------------
-- v_consumption_daily -- one row per site per day.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${MART}.v_consumption_daily AS
SELECT
    c.site_id,
    s.site_name,
    s.sector,
    s.region_code,
    s.floor_area_m2,
    c.reading_date,
    COUNT(DISTINCT c.meter_id)                      AS meters_reporting,
    COUNT(*)                                        AS intervals,
    SUM(c.consumption_kwh)                          AS consumption_kwh,
    ROUND(SUM(c.consumption_kwh) / NULLIF(s.floor_area_m2, 0), 4) AS kwh_per_m2,
    ROUND(MAX(c.average_power_kw), 3)               AS peak_power_kw,
    ROUND(AVG(c.average_power_kw), 3)               AS average_power_kw,
    -- Base load: the quietest hour of the day is what the site draws when
    -- nothing is happening. A base load close to the daily average means
    -- something is running around the clock that probably should not be.
    ROUND(MIN(c.average_power_kw), 3)               AS min_power_kw
FROM ${MART}.consumption_interval c
JOIN ${CORE}.site s ON s.site_id = c.site_id
WHERE ${MART}.is_usable(c.delta_flag)
GROUP BY c.site_id, s.site_name, s.sector, s.region_code, s.floor_area_m2, c.reading_date;


-- ---------------------------------------------------------------------------
-- v_load_curve -- average power by hour of day, per site.
-- The single most useful chart in energy management: it shows when a site
-- actually uses power, and how much it draws when it should be empty.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${MART}.v_load_curve AS
WITH hourly AS (
    SELECT
        site_id, site_name, sector,
        hour_of_day,
        AVG(average_power_kw)  AS avg_power_kw,
        MAX(peak_power_kw)     AS peak_power_kw,
        COUNT(DISTINCT reading_date) AS days_observed
    FROM ${MART}.v_consumption_hourly
    GROUP BY site_id, site_name, sector, hour_of_day
)
SELECT
    h.*,
    ROUND(h.avg_power_kw / NULLIF(MAX(h.avg_power_kw) OVER (PARTITION BY h.site_id), 0), 4)
        AS load_factor,
    ROUND(MIN(h.avg_power_kw) OVER (PARTITION BY h.site_id), 3) AS base_load_kw,
    ROUND(
        MIN(h.avg_power_kw) OVER (PARTITION BY h.site_id)
        / NULLIF(AVG(h.avg_power_kw) OVER (PARTITION BY h.site_id), 0), 4
    ) AS base_load_ratio
FROM hourly h;

COMMENT ON VIEW ${MART}.v_load_curve IS
    'Average power by hour of day per site. base_load_ratio near 1 means the site never really switches off.';


-- ---------------------------------------------------------------------------
-- v_peak_demand -- the monthly peak and when it happened.
-- Capacity charges are billed on the peak, so a single 15-minute spike can
-- cost more than a week of ordinary consumption.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${MART}.v_peak_demand AS
SELECT DISTINCT ON (c.site_id, DATE_TRUNC('month', c.reading_date))
    c.site_id,
    s.site_name,
    s.sector,
    DATE_TRUNC('month', c.reading_date)::DATE       AS month,
    c.reading_ts                                    AS peak_at,
    c.meter_id                                      AS peak_meter_id,
    c.average_power_kw                              AS peak_power_kw,
    c.hour_of_day                                   AS peak_hour
FROM ${MART}.consumption_interval c
JOIN ${CORE}.site s ON s.site_id = c.site_id
WHERE ${MART}.is_usable(c.delta_flag)
  AND c.average_power_kw IS NOT NULL
ORDER BY c.site_id, DATE_TRUNC('month', c.reading_date), c.average_power_kw DESC;


-- ---------------------------------------------------------------------------
-- v_degree_days -- heating and cooling degree days, base 18 degrees.
--
-- The standard way to ask "was consumption high because the building is
-- wasteful, or because it was cold?". HDD = max(0, 18 - mean temperature);
-- CDD = max(0, mean - 18).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${MART}.v_degree_days AS
SELECT
    w.station_id,
    ws.region_code,
    w.observed_on,
    w.temp_avg_c,
    GREATEST(0, 18.0 - w.temp_avg_c)                AS heating_degree_days,
    GREATEST(0, w.temp_avg_c - 18.0)                AS cooling_degree_days
FROM ${CORE}.weather_daily w
JOIN ${CORE}.weather_station ws ON ws.station_id = w.station_id;


-- ---------------------------------------------------------------------------
-- v_consumption_vs_weather -- daily consumption normalised by degree days.
--
-- kWh per HDD is roughly constant for a well-behaved heated building. A site
-- whose ratio drifts upward over a season is losing efficiency; one whose
-- ratio is high compared with its peers is a candidate for investigation.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${MART}.v_consumption_vs_weather AS
SELECT
    d.site_id,
    d.site_name,
    d.sector,
    d.reading_date,
    d.consumption_kwh,
    d.kwh_per_m2,
    dd.temp_avg_c,
    dd.heating_degree_days,
    dd.cooling_degree_days,
    CASE WHEN dd.heating_degree_days > 0.5
         THEN ROUND(d.consumption_kwh / dd.heating_degree_days, 3)
    END                                             AS kwh_per_hdd,
    CASE WHEN dd.cooling_degree_days > 0.5
         THEN ROUND(d.consumption_kwh / dd.cooling_degree_days, 3)
    END                                             AS kwh_per_cdd
FROM ${MART}.v_consumption_daily d
JOIN ${MART}.v_degree_days dd
  ON dd.region_code = d.region_code
 AND dd.observed_on = d.reading_date;


-- ---------------------------------------------------------------------------
-- v_cost_by_band -- consumption priced through the site's tariff bands.
--
-- The join on the hour is why tariff bands were flattened out of the source
-- JSON: pricing an hour against a nested array would be a per-row function
-- call instead of a hash join.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${MART}.v_cost_by_band AS
SELECT
    h.site_id,
    h.site_name,
    h.reading_date,
    t.tariff_id,
    t.tariff_name,
    b.band_code,
    t.currency,
    SUM(h.consumption_kwh)                                        AS consumption_kwh,
    ROUND(SUM(h.consumption_kwh * b.price_per_kwh), 2)            AS energy_cost,
    ROUND(AVG(b.price_per_kwh), 5)                                AS price_per_kwh,
    ROUND(
        100.0 * SUM(h.consumption_kwh)
        / NULLIF(SUM(SUM(h.consumption_kwh)) OVER (PARTITION BY h.site_id, h.reading_date), 0),
        2
    )                                                             AS share_of_day_pct
FROM ${MART}.v_consumption_hourly h
JOIN ${CORE}.site_tariff st
  ON st.site_id = h.site_id
 AND h.reading_date >= st.valid_from
 AND (st.valid_to IS NULL OR h.reading_date < st.valid_to)
JOIN ${CORE}.tariff t      ON t.tariff_id = st.tariff_id
JOIN ${CORE}.tariff_band b
  ON b.tariff_id = t.tariff_id
 AND h.hour_of_day >= b.hour_start
 AND h.hour_of_day <  b.hour_end
GROUP BY h.site_id, h.site_name, h.reading_date,
         t.tariff_id, t.tariff_name, b.band_code, t.currency;


-- ---------------------------------------------------------------------------
-- v_site_benchmark -- kWh per square metre, ranked within sector.
-- Comparing a data centre with an office in absolute kWh says nothing;
-- comparing them within their sector, per square metre, says something.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${MART}.v_site_benchmark AS
WITH per_site AS (
    SELECT
        site_id,
        site_name,
        sector,
        region_code,
        floor_area_m2,
        COUNT(DISTINCT reading_date)          AS days_observed,
        SUM(consumption_kwh)                  AS total_kwh,
        AVG(kwh_per_m2)                       AS avg_daily_kwh_per_m2,
        MAX(peak_power_kw)                    AS peak_power_kw
    FROM ${MART}.v_consumption_daily
    GROUP BY site_id, site_name, sector, region_code, floor_area_m2
)
SELECT
    p.*,
    ROUND(p.avg_daily_kwh_per_m2, 4)                                        AS intensity,
    RANK() OVER (PARTITION BY p.sector ORDER BY p.avg_daily_kwh_per_m2 DESC) AS rank_in_sector,
    ROUND(AVG(p.avg_daily_kwh_per_m2) OVER (PARTITION BY p.sector), 4)       AS sector_average,
    ROUND(
        100.0 * (p.avg_daily_kwh_per_m2 - AVG(p.avg_daily_kwh_per_m2) OVER (PARTITION BY p.sector))
        / NULLIF(AVG(p.avg_daily_kwh_per_m2) OVER (PARTITION BY p.sector), 0), 1
    )                                                                        AS vs_sector_pct
FROM per_site p;
