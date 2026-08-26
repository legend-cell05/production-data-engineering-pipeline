-- ===========================================================================
-- 004 -- Core reference entities
--
-- Typed and conformed. Every table is upserted on its natural key, so
-- promotion from raw is idempotent and a re-run changes nothing.
--
-- Reference data arrives as full snapshots (a nightly CSV export), so there is
-- no incremental logic here -- the interesting incremental work is on the
-- readings, in 005.
-- ===========================================================================

-- --- Sites -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CORE}.site (
    site_id          TEXT         PRIMARY KEY,
    site_name        TEXT         NOT NULL,
    city             TEXT         NOT NULL,
    region_code      TEXT         NOT NULL,
    sector           TEXT         NOT NULL,
    floor_area_m2    NUMERIC(10, 1) NOT NULL,
    commissioned_on  DATE,
    is_active        BOOLEAN      NOT NULL DEFAULT TRUE,
    source_updated_at TIMESTAMPTZ NOT NULL,
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT ck_site_area   CHECK (floor_area_m2 > 0),
    CONSTRAINT ck_site_sector CHECK (sector IN
        ('Office', 'Retail', 'Industrial', 'Logistics', 'Data centre', 'Healthcare', 'Education'))
);

CREATE INDEX IF NOT EXISTS ix_site_region ON ${CORE}.site (region_code);

COMMENT ON COLUMN ${CORE}.site.floor_area_m2 IS
    'Used to normalise consumption (kWh/m2), which is the only way to compare sites of different sizes.';


-- --- Meters ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CORE}.meter (
    meter_id          TEXT         PRIMARY KEY,
    site_id           TEXT         NOT NULL REFERENCES ${CORE}.site (site_id),
    meter_type        TEXT         NOT NULL,
    unit              TEXT         NOT NULL DEFAULT 'kWh',
    -- A physical meter often counts in units that must be scaled (a CT ratio,
    -- a pulse weight). Storing the multiplier rather than pre-multiplying the
    -- index keeps the raw reading auditable against the physical device.
    multiplier        NUMERIC(10, 4) NOT NULL DEFAULT 1,
    index_digits      SMALLINT     NOT NULL DEFAULT 8,
    -- The interval the device is configured to report at. Stored rather than
    -- assumed, because completeness ("did we get all the readings we should
    -- have?") is meaningless without knowing how many were expected.
    interval_minutes  SMALLINT     NOT NULL DEFAULT 15,
    installed_on      DATE,
    is_active         BOOLEAN      NOT NULL DEFAULT TRUE,
    source_updated_at TIMESTAMPTZ  NOT NULL,
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT ck_meter_multiplier CHECK (multiplier > 0),
    CONSTRAINT ck_meter_digits     CHECK (index_digits BETWEEN 4 AND 12),
    CONSTRAINT ck_meter_interval   CHECK (interval_minutes > 0 AND 1440 % interval_minutes = 0),
    CONSTRAINT ck_meter_type CHECK (meter_type IN
        ('main_incomer', 'hvac', 'lighting', 'process', 'ev_charging', 'submeter'))
);

CREATE INDEX IF NOT EXISTS ix_meter_site ON ${CORE}.meter (site_id);

COMMENT ON COLUMN ${CORE}.meter.index_digits IS
    'Register width. A meter with 6 digits rolls over at 999999, which is what tells a rollover apart from a reset.';


-- --- Tariffs ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CORE}.tariff (
    tariff_id         TEXT         PRIMARY KEY,
    tariff_name       TEXT         NOT NULL,
    supplier          TEXT         NOT NULL,
    currency          CHAR(3)      NOT NULL DEFAULT 'EUR',
    standing_charge_per_day NUMERIC(10, 4) NOT NULL DEFAULT 0,
    valid_from        DATE         NOT NULL,
    valid_to          DATE,
    source_updated_at TIMESTAMPTZ  NOT NULL,
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT ck_tariff_validity CHECK (valid_to IS NULL OR valid_to > valid_from)
);

-- The nested `bands` array of the source JSON, flattened into rows. Keeping it
-- as JSONB would make "cost by time band" an unindexable mess.
CREATE TABLE IF NOT EXISTS ${CORE}.tariff_band (
    tariff_id      TEXT     NOT NULL REFERENCES ${CORE}.tariff (tariff_id) ON DELETE CASCADE,
    band_code      TEXT     NOT NULL,
    hour_start     SMALLINT NOT NULL,
    hour_end       SMALLINT NOT NULL,
    price_per_kwh  NUMERIC(10, 5) NOT NULL,

    PRIMARY KEY (tariff_id, band_code),
    CONSTRAINT ck_band_hours CHECK (hour_start BETWEEN 0 AND 23 AND hour_end BETWEEN 1 AND 24),
    CONSTRAINT ck_band_order CHECK (hour_end > hour_start),
    CONSTRAINT ck_band_price CHECK (price_per_kwh >= 0)
);

CREATE TABLE IF NOT EXISTS ${CORE}.site_tariff (
    site_id    TEXT NOT NULL REFERENCES ${CORE}.site (site_id),
    tariff_id  TEXT NOT NULL REFERENCES ${CORE}.tariff (tariff_id),
    valid_from DATE NOT NULL,
    valid_to   DATE,

    PRIMARY KEY (site_id, valid_from),
    CONSTRAINT ck_site_tariff_validity CHECK (valid_to IS NULL OR valid_to > valid_from)
);


-- --- Weather ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CORE}.weather_station (
    station_id  TEXT PRIMARY KEY,
    station_name TEXT NOT NULL,
    region_code TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS ${CORE}.weather_daily (
    station_id        TEXT         NOT NULL REFERENCES ${CORE}.weather_station (station_id),
    observed_on       DATE         NOT NULL,
    temp_min_c        NUMERIC(5, 2) NOT NULL,
    temp_max_c        NUMERIC(5, 2) NOT NULL,
    temp_avg_c        NUMERIC(5, 2) NOT NULL,
    source_updated_at TIMESTAMPTZ  NOT NULL,
    ingested_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),

    PRIMARY KEY (station_id, observed_on),
    CONSTRAINT ck_weather_range CHECK (temp_max_c >= temp_min_c),
    CONSTRAINT ck_weather_plausible CHECK (temp_min_c > -60 AND temp_max_c < 60)
);

CREATE INDEX IF NOT EXISTS ix_weather_date ON ${CORE}.weather_daily (observed_on);
