-- ===================================================================
-- Raw yfinance daily prices foundation
-- ===================================================================
-- IMPORTANT DESIGN RULE:
-- - This table stores yfinance CSV files exactly as landed from disk.
-- - No business transformation is applied here.
-- - No canonical ticker mapping is applied here.
-- - No instrument resolution is applied here.
--
-- Notes:
-- - source_line_number is nullable in SQL-first bulk ingestion mode.
-- - file path is preserved fully for provenance.
-- - raw columns are preserved as VARCHAR to keep the landing forensic-grade.
-- - ONLY columns actually observed in the source CSV are stored here.
-- ===================================================================

CREATE TABLE IF NOT EXISTS raw.price_source_daily_yfinance (
    raw_price_source_daily_yfinance_id BIGINT PRIMARY KEY,

    source_name VARCHAR NOT NULL,
    source_root_dir VARCHAR NOT NULL,
    source_file_path VARCHAR NOT NULL,
    source_file_name VARCHAR NOT NULL,
    source_line_number BIGINT,

    date_raw VARCHAR,
    open_raw VARCHAR,
    high_raw VARCHAR,
    low_raw VARCHAR,
    close_raw VARCHAR,
    adj_close_raw VARCHAR,
    volume_raw VARCHAR,
    dividends_raw VARCHAR,
    stock_splits_raw VARCHAR,

    row_raw VARCHAR,

    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_price_source_daily_yfinance_file
ON raw.price_source_daily_yfinance(source_file_path);

CREATE INDEX IF NOT EXISTS idx_raw_price_source_daily_yfinance_date_raw
ON raw.price_source_daily_yfinance(date_raw);
