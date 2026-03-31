-- ===================================================================
-- Core prices foundation
-- ===================================================================
-- This table is the canonical daily EOD price history foundation for
-- the first PIT-aware price slice.
--
-- Design goals:
-- - simple daily history contract
-- - instrument-linked, not symbol-native
-- - deterministic seed-friendly schema
-- ===================================================================

CREATE TABLE IF NOT EXISTS core.price_history (
    price_history_id BIGINT PRIMARY KEY,
    instrument_id VARCHAR NOT NULL,
    price_date DATE NOT NULL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    adj_close DOUBLE,
    volume BIGINT,
    source_name VARCHAR NOT NULL,
    observed_at TIMESTAMP,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_price_history_instrument_date
ON core.price_history(instrument_id, price_date);
