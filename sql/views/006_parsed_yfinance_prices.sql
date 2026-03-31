-- ===================================================================
-- Parsed / typed yfinance daily prices
-- ===================================================================
-- IMPORTANT DESIGN RULE:
-- - This layer parses raw yfinance landing data into typed fields.
-- - It does NOT resolve canonical instruments yet.
-- - It does NOT merge with Stooq yet.
-- - It remains provenance-preserving.
--
-- Notes:
-- - Symbol is derived from the directory name in source_file_path.
-- - date_raw currently looks like: 2026-03-11 00:00:00-04:00
-- - We derive:
--     * as_of_timestamp_tz_raw_text  (original string retained)
--     * as_of_date                   (typed DATE from first 10 chars)
-- ===================================================================

CREATE SCHEMA IF NOT EXISTS parsed;

CREATE OR REPLACE VIEW parsed.price_source_daily_yfinance AS
WITH base AS (
    SELECT
        raw_price_source_daily_yfinance_id,
        source_name,
        source_root_dir,
        source_file_path,
        source_file_name,
        source_line_number,
        date_raw,
        open_raw,
        high_raw,
        low_raw,
        close_raw,
        adj_close_raw,
        volume_raw,
        dividends_raw,
        stock_splits_raw,
        row_raw,
        ingested_at,

        regexp_extract(source_file_path, '.*/([^/]+)/[^/]+$', 1) AS symbol
    FROM raw.price_source_daily_yfinance
)
SELECT
    raw_price_source_daily_yfinance_id,
    source_name,
    source_root_dir,
    source_file_path,
    source_file_name,
    source_line_number,

    symbol,

    date_raw AS as_of_timestamp_tz_raw_text,
    TRY_CAST(substr(date_raw, 1, 10) AS DATE) AS as_of_date,

    TRY_CAST(open_raw AS DOUBLE) AS open,
    TRY_CAST(high_raw AS DOUBLE) AS high,
    TRY_CAST(low_raw AS DOUBLE) AS low,
    TRY_CAST(close_raw AS DOUBLE) AS close,
    TRY_CAST(adj_close_raw AS DOUBLE) AS adj_close,
    TRY_CAST(volume_raw AS BIGINT) AS volume,
    TRY_CAST(dividends_raw AS DOUBLE) AS dividends,
    TRY_CAST(stock_splits_raw AS DOUBLE) AS stock_splits,

    date_raw,
    open_raw,
    high_raw,
    low_raw,
    close_raw,
    adj_close_raw,
    volume_raw,
    dividends_raw,
    stock_splits_raw,

    row_raw,
    ingested_at
FROM base;
