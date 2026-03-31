-- ===================================================================
-- Core scientific foundation schema
-- ===================================================================
-- This file defines the minimal historical foundation required for:
-- - stable instrument identity
-- - historical symbol mapping
-- - historical listing lifecycle
-- - historical universe membership
--
-- Design goals:
-- - point-in-time friendly
-- - survivor-bias aware
-- - explicit time windows
-- - explicit provenance fields
--
-- Important naming rule:
-- - We intentionally use schema name "api" instead of "serving".
-- - The published database file is named "serving.duckdb".
-- - Using schema name "serving" inside serving.duckdb creates an
--   ambiguity in DuckDB because "serving" can refer both to the
--   catalog and to the schema.
-- ===================================================================

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS api;
CREATE SCHEMA IF NOT EXISTS meta;

-- -------------------------------------------------------------------
-- Stable instrument identity.
--
-- This is the anchor entity that should survive ticker changes,
-- exchange migrations, delistings, mergers, etc.
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.instrument (
    instrument_id VARCHAR PRIMARY KEY,
    company_id VARCHAR,
    security_type VARCHAR NOT NULL,
    primary_ticker VARCHAR,
    primary_exchange VARCHAR,
    instrument_status VARCHAR NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- -------------------------------------------------------------------
-- Historical symbol mapping.
--
-- A symbol can map to an instrument only for a defined historical window.
-- This is essential for PIT resolution and for tracking ticker changes.
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.symbol_reference_history (
    symbol_reference_history_id BIGINT PRIMARY KEY,
    instrument_id VARCHAR NOT NULL,
    symbol VARCHAR NOT NULL,
    exchange VARCHAR,
    mic VARCHAR,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    effective_from DATE NOT NULL,
    effective_to DATE,
    observed_at TIMESTAMP,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_name VARCHAR NOT NULL,
    record_status VARCHAR NOT NULL DEFAULT 'ACTIVE'
);

-- -------------------------------------------------------------------
-- Historical listing lifecycle.
--
-- This table preserves the lifecycle of listings through time and is
-- one of the main protections against survivor bias.
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.listing_status_history (
    listing_status_history_id BIGINT PRIMARY KEY,
    instrument_id VARCHAR NOT NULL,
    symbol VARCHAR NOT NULL,
    exchange VARCHAR,
    listing_status VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    observed_at TIMESTAMP,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_name VARCHAR NOT NULL
);

-- -------------------------------------------------------------------
-- Universe definitions.
--
-- A universe is a named logical collection, such as:
-- - US_LISTED_COMMON_STOCKS
-- - NASDAQ_LISTED
-- - NYSE_LISTED
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.universe_definition (
    universe_id VARCHAR PRIMARY KEY,
    universe_name VARCHAR NOT NULL UNIQUE,
    description VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- -------------------------------------------------------------------
-- Historical universe membership.
--
-- This table records when an instrument belonged to an explicit universe.
-- It must support past reconstruction without leaking today's membership.
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.universe_membership_history (
    universe_membership_history_id BIGINT PRIMARY KEY,
    universe_id VARCHAR NOT NULL,
    instrument_id VARCHAR NOT NULL,
    membership_status VARCHAR NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    observed_at TIMESTAMP,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_name VARCHAR NOT NULL
);

-- -------------------------------------------------------------------
-- Release metadata.
--
-- Each published serving release should expose what was published and when.
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.release_registry (
    release_id VARCHAR PRIMARY KEY,
    build_db_path VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP,
    checks_passed BOOLEAN NOT NULL DEFAULT FALSE,
    notes VARCHAR
);

-- -------------------------------------------------------------------
-- Helpful indexes.
-- -------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_symbol_reference_history_symbol
ON core.symbol_reference_history(symbol);

CREATE INDEX IF NOT EXISTS idx_symbol_reference_history_instrument
ON core.symbol_reference_history(instrument_id);

CREATE INDEX IF NOT EXISTS idx_listing_status_history_symbol
ON core.listing_status_history(symbol);

CREATE INDEX IF NOT EXISTS idx_listing_status_history_instrument
ON core.listing_status_history(instrument_id);

CREATE INDEX IF NOT EXISTS idx_universe_definition_name
ON core.universe_definition(universe_name);

CREATE INDEX IF NOT EXISTS idx_universe_membership_history_universe
ON core.universe_membership_history(universe_id);

CREATE INDEX IF NOT EXISTS idx_universe_membership_history_instrument
ON core.universe_membership_history(instrument_id);
