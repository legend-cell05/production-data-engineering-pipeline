-- ===========================================================================
-- 001 -- Schemas
--
-- Four layers. The separation is not decoration: each one has a different
-- retention policy, a different owner in a real deployment, and a different
-- answer to "can I drop this and rebuild it?".
--
--   ${RAW}    Landing. Append-only, untyped payloads kept as JSONB exactly as
--             the source sent them. This is the ONLY layer that cannot be
--             regenerated -- once the upstream API has moved its cursor past a
--             record, the raw row is the only copy.
--   ${CORE}   Typed, conformed entities. Rebuildable from raw.
--   ${MART}   Aggregates and business logic. Rebuildable from core.
--   ${META}   Operational state: runs, watermarks, dead letters, quality.
--             Rebuildable from nothing -- losing it loses the pipeline's memory
--             of where it got to.
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS ${RAW};
CREATE SCHEMA IF NOT EXISTS ${CORE};
CREATE SCHEMA IF NOT EXISTS ${MART};
CREATE SCHEMA IF NOT EXISTS ${META};

COMMENT ON SCHEMA ${RAW} IS
    'Landing zone: append-only JSONB payloads as received. The only non-regenerable layer.';
COMMENT ON SCHEMA ${CORE} IS
    'Typed, conformed entities. Rebuildable from raw.';
COMMENT ON SCHEMA ${MART} IS
    'Aggregates and business logic. Rebuildable from core.';
COMMENT ON SCHEMA ${META} IS
    'Pipeline state: runs, watermarks, dead-letter queue, quality results.';
