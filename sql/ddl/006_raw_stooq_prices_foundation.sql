-- ===================================================================
-- Raw Stooq daily prices foundation
-- ===================================================================
-- IMPORTANT DESIGN RULE:
-- - This table stores Stooq daily files exactly as landed from disk.
-- - No business transformation is applied here.
-- - No ticker normalization is applied here.
-- - No exchange inference is applied here.
-- - No security type inference is applied here.
--
-- The purpose of this table is forensic-grade raw preservation.
-- Any later transformation must happen in a separate normalization step.
--
-- Note:
-- - source_line_number is nullable because the SQL-first bulk loader
--   does not preserve the exact physical source line number cheaply.
-- ===================================================================

CREATE TABLE IF NOT EXISTS raw.price_source_daily_stooq (
    raw_price_source_daily_stooq_id BIGINT PRIMARY KEY,

    source_name VARCHAR NOT NULL,
    source_root_dir VARCHAR NOT NULL,
    source_file_path VARCHAR NOT NULL,
    source_file_name VARCHAR NOT NULL,
    source_line_number BIGINT,

    ticker_raw VARCHAR,
    per_raw VARCHAR,
    date_raw VARCHAR,
    time_raw VARCHAR,
    open_raw VARCHAR,
    high_raw VARCHAR,
    low_raw VARCHAR,
    close_raw VARCHAR,
    vol_raw VARCHAR,
    openint_raw VARCHAR,

    row_raw VARCHAR,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_price_source_daily_stooq_file
ON raw.price_source_daily_stooq(source_file_path);

CREATE INDEX IF NOT EXISTS idx_raw_price_source_daily_stooq_ticker_raw
ON raw.price_source_daily_stooq(ticker_raw);

CREATE INDEX IF NOT EXISTS idx_raw_price_source_daily_stooq_date_raw
ON raw.price_source_daily_stooq(date_raw);
