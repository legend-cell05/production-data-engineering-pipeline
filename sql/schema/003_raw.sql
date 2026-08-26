-- ===========================================================================
-- 003 -- Raw landing zone
--
-- ONE generic table for every source, partitioned by source name.
--
-- Why generic rather than one typed table per source: adding a source then
-- costs a connector class and a contract, not a migration. The payload stays
-- exactly as the upstream sent it, so a transformation bug can be fixed and
-- replayed from raw without asking the source for the data again -- which is
-- usually impossible once an API cursor has moved on.
--
-- Why LIST partitioning by source_name: the reading partition holds two orders
-- of magnitude more rows than the reference ones. Partitioning keeps a scan of
-- `sites` from touching a million reading rows, and lets a single source be
-- truncated and re-ingested without a delete over the whole table.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS ${RAW}.record (
    source_name       TEXT        NOT NULL,
    natural_key       TEXT        NOT NULL,
    -- SHA-256 of the canonical payload. Two identical payloads produce the
    -- same hash, so re-reading a record during the grace window is a no-op
    -- rather than a duplicate. This is what makes the ingestion idempotent.
    content_hash      CHAR(64)    NOT NULL,
    payload           JSONB       NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL,
    contract_version  TEXT        NOT NULL,
    batch_id          UUID        NOT NULL,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- The partition key must be part of every unique constraint, which it
    -- already is here.
    CONSTRAINT pk_raw_record PRIMARY KEY (source_name, natural_key, content_hash)
) PARTITION BY LIST (source_name);

COMMENT ON TABLE ${RAW}.record IS
    'Append-only landing zone. Payloads are stored exactly as received; the primary key makes re-ingestion idempotent.';
COMMENT ON COLUMN ${RAW}.record.content_hash IS
    'SHA-256 of the canonical JSON payload. Same content => same row => no duplicate.';
COMMENT ON COLUMN ${RAW}.record.source_updated_at IS
    'Source-side update timestamp. This is what the watermark tracks -- never the ingestion time.';

-- One partition per source, plus a default so an unexpected source name lands
-- somewhere visible instead of failing the insert.
CREATE TABLE IF NOT EXISTS ${RAW}.record_meter_readings
    PARTITION OF ${RAW}.record FOR VALUES IN ('meter_readings');
CREATE TABLE IF NOT EXISTS ${RAW}.record_sites
    PARTITION OF ${RAW}.record FOR VALUES IN ('sites');
CREATE TABLE IF NOT EXISTS ${RAW}.record_meters
    PARTITION OF ${RAW}.record FOR VALUES IN ('meters');
CREATE TABLE IF NOT EXISTS ${RAW}.record_tariffs
    PARTITION OF ${RAW}.record FOR VALUES IN ('tariffs');
CREATE TABLE IF NOT EXISTS ${RAW}.record_weather
    PARTITION OF ${RAW}.record FOR VALUES IN ('weather');
CREATE TABLE IF NOT EXISTS ${RAW}.record_unknown
    PARTITION OF ${RAW}.record DEFAULT;

-- Promotion reads "everything from this batch", and backfills read
-- "everything in this time window".
CREATE INDEX IF NOT EXISTS ix_raw_record_batch
    ON ${RAW}.record (batch_id);
CREATE INDEX IF NOT EXISTS ix_raw_record_updated
    ON ${RAW}.record (source_name, source_updated_at);

-- The watermark query is `MAX(source_updated_at) WHERE source_name = ?`, run
-- once per source per run. A BRIN index on the readings partition costs almost
-- nothing and suits a column that is naturally correlated with insertion order.
CREATE INDEX IF NOT EXISTS ix_raw_readings_updated_brin
    ON ${RAW}.record_meter_readings USING BRIN (source_updated_at);
