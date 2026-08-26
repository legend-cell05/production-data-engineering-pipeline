-- ===========================================================================
-- 006 -- Mart layer
--
-- `consumption_interval` is a physical table, not a view. The delta
-- calculation is a window function over the whole reading history; running it
-- on every dashboard query would be wasteful, and the result only changes when
-- new readings land.
--
-- It is refreshed per day (delete + insert for the window), which makes the
-- refresh idempotent and lets a backfill rebuild three days without touching
-- the other three hundred.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS ${MART}.consumption_interval (
    meter_id         TEXT          NOT NULL,
    site_id          TEXT          NOT NULL,
    reading_ts       TIMESTAMPTZ   NOT NULL,
    reading_date     DATE          NOT NULL,
    hour_of_day      SMALLINT      NOT NULL,

    index_kwh        NUMERIC(14, 3) NOT NULL,
    previous_index   NUMERIC(14, 3),
    -- NULL when the delta cannot be trusted: the first reading of a meter, a
    -- register rollover, or a reset. Reporting NULL is honest; reporting 0
    -- would quietly understate consumption, and reporting a negative number
    -- would corrupt every aggregate above it.
    consumption_kwh  NUMERIC(16, 3),
    span_minutes     INTEGER,
    -- Wide on purpose. A mis-detected rollover can produce an absurd figure,
    -- and overflowing the column would abort the whole refresh instead of
    -- letting the row through to be flagged `implausible` and excluded.
    average_power_kw NUMERIC(16, 3),
    delta_flag       TEXT          NOT NULL,

    refreshed_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),

    PRIMARY KEY (meter_id, reading_ts),
    CONSTRAINT ck_consumption_nonneg CHECK (consumption_kwh IS NULL OR consumption_kwh >= 0),
    CONSTRAINT ck_delta_flag CHECK (delta_flag IN
        ('ok', 'first_reading', 'rollover', 'reset', 'correction', 'gap', 'flat', 'implausible'))
);

CREATE INDEX IF NOT EXISTS ix_consumption_date
    ON ${MART}.consumption_interval (reading_date);
CREATE INDEX IF NOT EXISTS ix_consumption_site_date
    ON ${MART}.consumption_interval (site_id, reading_date);
CREATE INDEX IF NOT EXISTS ix_consumption_suspect
    ON ${MART}.consumption_interval (reading_date)
    WHERE delta_flag <> 'ok';

COMMENT ON TABLE ${MART}.consumption_interval IS
    'Interval consumption derived from consecutive meter indexes. Refreshed per day, idempotently.';
COMMENT ON COLUMN ${MART}.consumption_interval.delta_flag IS
    'Why a delta is or is not usable: ok | first_reading | rollover | reset | correction | gap | flat | implausible.';
COMMENT ON COLUMN ${MART}.consumption_interval.span_minutes IS
    'Minutes covered by this delta. Greater than the nominal interval means readings were missed.';
