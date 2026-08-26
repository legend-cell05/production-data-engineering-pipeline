-- ===========================================================================
-- 002 -- Pipeline state
--
-- Four tables that together answer, at any moment:
--   where did each source get to?          source_watermark
--   what happened on the last runs?        pipeline_run
--   what did we refuse, and why?           dead_letter
--   is the result fit to publish?          quality_result
--
-- A pipeline that cannot answer those four from SQL alone is a pipeline that
-- has to be debugged by reading logs.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Watermarks -- the heart of incremental ingestion.
--
-- One row per source. `watermark_value` is the highest source-side update
-- timestamp that has been ingested. The next run asks the source for records
-- strictly after (watermark - grace window); see docs/incremental.md for why
-- the grace window exists and what it costs.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${META}.source_watermark (
    source_name       TEXT        PRIMARY KEY,
    watermark_value   TIMESTAMPTZ,
    grace_minutes     INTEGER     NOT NULL DEFAULT 0,
    records_seen      BIGINT      NOT NULL DEFAULT 0,
    last_batch_id     UUID,
    last_success_at   TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ck_watermark_grace CHECK (grace_minutes >= 0),
    CONSTRAINT ck_watermark_seen  CHECK (records_seen >= 0)
);

COMMENT ON TABLE ${META}.source_watermark IS
    'Per-source ingestion cursor. Losing this table means a full re-read, not data loss.';
COMMENT ON COLUMN ${META}.source_watermark.watermark_value IS
    'Highest source-side update timestamp ingested. NULL means the source has never been read.';


-- ---------------------------------------------------------------------------
-- Run history
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${META}.pipeline_run (
    run_id                BIGSERIAL   PRIMARY KEY,
    batch_id              UUID        NOT NULL UNIQUE,
    command               TEXT        NOT NULL,
    source_name           TEXT,
    started_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at           TIMESTAMPTZ,
    status                TEXT        NOT NULL DEFAULT 'RUNNING',

    records_read          BIGINT      NOT NULL DEFAULT 0,
    records_ingested      BIGINT      NOT NULL DEFAULT 0,
    records_duplicate     BIGINT      NOT NULL DEFAULT 0,
    records_dead_lettered BIGINT      NOT NULL DEFAULT 0,
    rows_promoted         BIGINT      NOT NULL DEFAULT 0,

    watermark_before      TIMESTAMPTZ,
    watermark_after       TIMESTAMPTZ,
    retries_performed     INTEGER     NOT NULL DEFAULT 0,
    duration_seconds      NUMERIC(12, 3),
    error_message         TEXT,
    pipeline_version      TEXT        NOT NULL DEFAULT 'unknown',

    CONSTRAINT ck_run_status CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL')),
    CONSTRAINT ck_run_counts CHECK (
        records_read >= 0 AND records_ingested >= 0
        AND records_duplicate >= 0 AND records_dead_lettered >= 0
        AND rows_promoted >= 0
    ),
    CONSTRAINT ck_run_chronology CHECK (finished_at IS NULL OR finished_at >= started_at)
);

CREATE INDEX IF NOT EXISTS ix_pipeline_run_started
    ON ${META}.pipeline_run (started_at DESC);
CREATE INDEX IF NOT EXISTS ix_pipeline_run_source
    ON ${META}.pipeline_run (source_name, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_pipeline_run_failed
    ON ${META}.pipeline_run (started_at DESC) WHERE status = 'FAILED';


-- ---------------------------------------------------------------------------
-- Dead-letter queue
--
-- A record that violates its contract is not dropped and is not allowed to
-- fail the whole run: it is parked here with the payload and the failing
-- field, and the run continues. `helios dlq replay` re-processes them once the
-- upstream problem is fixed.
--
-- A pipeline that aborts on one bad row out of a hundred thousand is a
-- pipeline that gets disabled by whoever is on call.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${META}.dead_letter (
    dlq_id          BIGSERIAL   PRIMARY KEY,
    source_name     TEXT        NOT NULL,
    natural_key     TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    error_type      TEXT        NOT NULL,
    error_message   TEXT        NOT NULL,
    failed_field    TEXT,
    attempts        INTEGER     NOT NULL DEFAULT 1,
    status          TEXT        NOT NULL DEFAULT 'PENDING',
    first_batch_id  UUID        NOT NULL,
    last_batch_id   UUID        NOT NULL,
    first_failed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_failed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at     TIMESTAMPTZ,

    CONSTRAINT ck_dlq_status   CHECK (status IN ('PENDING', 'RESOLVED', 'ABANDONED')),
    CONSTRAINT ck_dlq_attempts CHECK (attempts >= 1),
    -- One row per failing record, so a replay that fails again increments the
    -- attempt count instead of creating a second entry.
    CONSTRAINT uq_dlq_record   UNIQUE (source_name, natural_key)
);

CREATE INDEX IF NOT EXISTS ix_dlq_pending
    ON ${META}.dead_letter (source_name, last_failed_at DESC)
    WHERE status = 'PENDING';

COMMENT ON TABLE ${META}.dead_letter IS
    'Records refused by their schema contract, kept with payload and cause for replay.';


-- ---------------------------------------------------------------------------
-- Quality results
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${META}.quality_result (
    result_id       BIGSERIAL   PRIMARY KEY,
    batch_id        UUID        NOT NULL,
    check_name      TEXT        NOT NULL,
    layer           TEXT        NOT NULL,
    severity        TEXT        NOT NULL,
    passed          BOOLEAN     NOT NULL,
    observed_value  TEXT,
    expected_value  TEXT,
    details         TEXT,
    checked_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ck_quality_severity CHECK (severity IN ('BLOCKING', 'WARNING', 'INFO')),
    CONSTRAINT uq_quality_result   UNIQUE (batch_id, check_name)
);

CREATE INDEX IF NOT EXISTS ix_quality_failed
    ON ${META}.quality_result (checked_at DESC) WHERE passed = FALSE;


-- ---------------------------------------------------------------------------
-- Schema contract registry
--
-- Records which contract version was in force when a batch was ingested. When
-- a contract changes, this is what lets you answer "were these rows validated
-- under the old rules or the new ones?" without guessing from dates.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${META}.schema_contract (
    source_name      TEXT        NOT NULL,
    contract_version TEXT        NOT NULL,
    definition       JSONB       NOT NULL,
    registered_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (source_name, contract_version)
);
