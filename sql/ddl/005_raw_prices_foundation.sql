-- ===================================================================
-- Raw prices foundation
-- ===================================================================
-- This raw table is the landing zone for local CSV ingestion.
--
-- Design goals:
-- - append-friendly raw landing
-- - preserve source symbol as received
-- - preserve file provenance
-- - keep raw ingestion simple and debuggable
-- ===================================================================

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.price_source_daily (
    raw_price_source_daily_id BIGINT,
    source_symbol VARCHAR NOT NULL,
    source_exchange VARCHAR,
    price_date DATE NOT NULL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    adj_close DOUBLE,
    volume BIGINT,
    currency VARCHAR,
    source_name VARCHAR NOT NULL,
    source_file_path VARCHAR NOT NULL,
    observed_at TIMESTAMP,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_price_source_daily_symbol_date
ON raw.price_source_daily(source_symbol, price_date);
