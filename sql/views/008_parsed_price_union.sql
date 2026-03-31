-- ===================================================================
-- Parsed typed union of Stooq + yfinance daily prices
-- ===================================================================
-- IMPORTANT DESIGN RULE:
-- - This is a typed source union only.
-- - No canonical instrument resolution yet.
-- - No source preference logic yet.
-- - No deduplication across sources yet.
-- - Provenance is preserved.
-- ===================================================================

CREATE SCHEMA IF NOT EXISTS parsed;

CREATE OR REPLACE VIEW parsed.price_source_daily_union AS
SELECT
    'stooq' AS source_family,
    source_name,
    source_root_dir,
    source_file_path,
    source_file_name,
    source_line_number,
    symbol,
    as_of_timestamp_tz_raw_text,
    as_of_date,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    dividends,
    stock_splits,
    ingested_at
FROM parsed.price_source_daily_stooq

UNION ALL

SELECT
    'yfinance' AS source_family,
    source_name,
    source_root_dir,
    source_file_path,
    source_file_name,
    source_line_number,
    symbol,
    as_of_timestamp_tz_raw_text,
    as_of_date,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    dividends,
    stock_splits,
    ingested_at
FROM parsed.price_source_daily_yfinance;
