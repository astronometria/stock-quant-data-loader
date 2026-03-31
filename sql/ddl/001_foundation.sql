-- ===================================================================
-- Foundation schema v1
-- ===================================================================
-- This schema defines the scientific core needed for:
-- - stable instrument identity
-- - symbol mapping through time
-- - listing lifecycle history
-- - universe definitions and membership history
--
-- Design goals:
-- - point-in-time aware
-- - survivor-bias aware
-- - explicit validity intervals
-- - future API-friendly structure
-- ===================================================================

-- -------------------------------------------------------------------
-- Metadata table describing schema initialization events.
-- This helps track whether init-db has been run and with which version.
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR PRIMARY KEY,
    applied_at TIMESTAMP NOT NULL,
    description VARCHAR NOT NULL
);

-- -------------------------------------------------------------------
-- Stable instrument identity.
--
-- IMPORTANT:
-- - instrument_id is the durable internal identity.
-- - ticker symbols may change over time, but instrument_id should remain.
-- - this is essential to avoid identity drift across symbol changes.
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS instrument (
    instrument_id BIGINT PRIMARY KEY,
    security_type VARCHAR NOT NULL,
    company_id VARCHAR,
    primary_ticker VARCHAR,
    primary_exchange VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- -------------------------------------------------------------------
-- Symbol mapping history through time.
--
-- IMPORTANT:
-- - one instrument may have multiple symbols over time
-- - one symbol may map differently across history in edge cases
-- - effective_from / effective_to defines the validity window
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS symbol_reference_history (
    symbol_reference_history_id BIGINT PRIMARY KEY,
    instrument_id BIGINT NOT NULL,
    symbol VARCHAR NOT NULL,
    exchange VARCHAR,
    is_primary BOOLEAN NOT NULL DEFAULT TRUE,
    effective_from DATE NOT NULL,
    effective_to DATE,
    observed_at TIMESTAMP,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- -------------------------------------------------------------------
-- Listing status lifecycle history.
--
-- Examples:
-- - ACTIVE
-- - INACTIVE
-- - DELISTED
-- - RENAMED
-- - TICKER_CHANGED
-- - MERGED
-- - ACQUIRED
-- - BANKRUPT
--
-- This table is one of the main defenses against survivor bias.
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS listing_status_history (
    listing_status_history_id BIGINT PRIMARY KEY,
    instrument_id BIGINT NOT NULL,
    symbol VARCHAR NOT NULL,
    listing_status VARCHAR NOT NULL,
    event_type VARCHAR,
    effective_from DATE NOT NULL,
    effective_to DATE,
    source_name VARCHAR,
    observed_at TIMESTAMP,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- -------------------------------------------------------------------
-- Logical universe definitions.
--
-- Examples:
-- - US_LISTED_COMMON_STOCKS
-- - NASDAQ_LISTED
-- - NYSE_LISTED
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS universe_definition (
    universe_id BIGINT PRIMARY KEY,
    universe_name VARCHAR NOT NULL UNIQUE,
    description VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- -------------------------------------------------------------------
-- Universe membership history.
--
-- IMPORTANT:
-- - membership is historized, never inferred from "latest only"
-- - supports scientific universe reconstruction as-of a date
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS universe_membership_history (
    universe_membership_history_id BIGINT PRIMARY KEY,
    universe_id BIGINT NOT NULL,
    instrument_id BIGINT NOT NULL,
    membership_status VARCHAR NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    source_name VARCHAR,
    observed_at TIMESTAMP,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- -------------------------------------------------------------------
-- Release metadata to document published serving artifacts.
-- This table belongs in build DB first, then can be projected to serving.
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS release_metadata (
    release_id VARCHAR PRIMARY KEY,
    build_started_at TIMESTAMP,
    build_finished_at TIMESTAMP,
    published_at TIMESTAMP,
    schema_version VARCHAR NOT NULL,
    checks_passed BOOLEAN NOT NULL,
    build_git_commit VARCHAR,
    manifest_json VARCHAR
);

-- -------------------------------------------------------------------
-- Validation views for easier debugging and future checks.
-- -------------------------------------------------------------------

-- Active symbol mappings by date interval logic.
CREATE OR REPLACE VIEW v_symbol_reference_history_open_intervals AS
SELECT
    symbol_reference_history_id,
    instrument_id,
    symbol,
    exchange,
    is_primary,
    effective_from,
    effective_to,
    observed_at,
    ingested_at
FROM symbol_reference_history;

-- Active universe membership intervals.
CREATE OR REPLACE VIEW v_universe_membership_history_open_intervals AS
SELECT
    universe_membership_history_id,
    universe_id,
    instrument_id,
    membership_status,
    effective_from,
    effective_to,
    source_name,
    observed_at,
    ingested_at
FROM universe_membership_history;
