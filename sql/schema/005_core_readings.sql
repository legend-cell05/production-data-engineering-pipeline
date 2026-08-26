-- ===========================================================================
-- 005 -- Meter readings: the high-volume table
--
-- Grain: ONE ROW PER METER PER INTERVAL.
--
-- What is stored is the **cumulative index** the meter displays, not the
-- consumption. That is what a real meter reports, and keeping it means the
-- warehouse can always be reconciled against the physical device. Consumption
-- is a difference between consecutive readings and is derived in the mart
-- layer, where the awkward cases (rollover, reset, gaps) are handled once.
--
-- RANGE partitioning by month because every access pattern is time-bounded:
-- ingest the last few hours, rebuild yesterday's marts, backfill a week, drop
-- data older than the retention period. Dropping a partition is instant;
-- a DELETE over a million rows is not.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS ${CORE}.meter_reading (
    meter_id          TEXT         NOT NULL,
    reading_ts        TIMESTAMPTZ  NOT NULL,
    index_kwh         NUMERIC(14, 3) NOT NULL,
    quality_flag      TEXT         NOT NULL DEFAULT 'measured',
    source_updated_at TIMESTAMPTZ  NOT NULL,
    batch_id          UUID         NOT NULL,
    ingested_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),

    -- The natural key IS the primary key: re-ingesting the same interval
    -- updates it instead of duplicating it. The partition key must be part of
    -- the primary key, which reading_ts is.
    CONSTRAINT pk_meter_reading PRIMARY KEY (meter_id, reading_ts),
    CONSTRAINT ck_reading_index CHECK (index_kwh >= 0),
    CONSTRAINT ck_reading_quality CHECK (quality_flag IN
        ('measured', 'estimated', 'substituted', 'suspect'))
) PARTITION BY RANGE (reading_ts);

COMMENT ON TABLE ${CORE}.meter_reading IS
    'Cumulative meter index at interval grain. Synthetic data. Partitioned by month.';
COMMENT ON COLUMN ${CORE}.meter_reading.index_kwh IS
    'Cumulative register value, NOT consumption. Consumption is the delta -- see mart.consumption_interval.';

-- The foreign key is declared on the partitioned parent and inherited by every
-- partition (PostgreSQL 12+). It costs a lookup per insert and is worth it:
-- a reading for a meter that does not exist is a bug, not data.
ALTER TABLE ${CORE}.meter_reading
    DROP CONSTRAINT IF EXISTS fk_meter_reading_meter;
ALTER TABLE ${CORE}.meter_reading
    ADD CONSTRAINT fk_meter_reading_meter
    FOREIGN KEY (meter_id) REFERENCES ${CORE}.meter (meter_id);

-- BRIN rather than B-tree on the timestamp: rows arrive in roughly
-- chronological order, so each block range covers a narrow time span. A BRIN
-- index on a million rows is a few dozen kilobytes against several megabytes
-- for a B-tree, and range scans are what every query here does.
CREATE INDEX IF NOT EXISTS ix_meter_reading_ts_brin
    ON ${CORE}.meter_reading USING BRIN (reading_ts) WITH (pages_per_range = 32);

CREATE INDEX IF NOT EXISTS ix_meter_reading_batch
    ON ${CORE}.meter_reading (batch_id);


-- ---------------------------------------------------------------------------
-- Partition management
--
-- Creating partitions by hand does not survive contact with a scheduler: the
-- first insert after midnight on the 1st of the month fails. This function is
-- called by the loader for every month it is about to write, and is safe to
-- call repeatedly.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ${CORE}.ensure_reading_partition(p_month DATE)
RETURNS TEXT
LANGUAGE plpgsql
AS $fn$
DECLARE
    v_start        DATE := date_trunc('month', p_month)::DATE;
    v_end          DATE := (date_trunc('month', p_month) + INTERVAL '1 month')::DATE;
    v_partition    TEXT := format('meter_reading_%s', to_char(v_start, 'YYYYMM'));
    v_exists       BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = v_partition AND n.nspname = '${CORE}'
    ) INTO v_exists;

    IF NOT v_exists THEN
        EXECUTE format(
            'CREATE TABLE %I.%I PARTITION OF %I.meter_reading FOR VALUES FROM (%L) TO (%L)',
            '${CORE}', v_partition, '${CORE}', v_start, v_end
        );
    END IF;

    RETURN v_partition;
END;
$fn$;

COMMENT ON FUNCTION ${CORE}.ensure_reading_partition(DATE) IS
    'Create the monthly partition covering p_month if it does not exist. Idempotent.';


-- A DEFAULT partition catches anything outside the created ranges. Without it
-- an out-of-range timestamp fails the insert and takes the batch with it;
-- with it, the row lands somewhere visible and a quality check reports it.
CREATE TABLE IF NOT EXISTS ${CORE}.meter_reading_default
    PARTITION OF ${CORE}.meter_reading DEFAULT;
