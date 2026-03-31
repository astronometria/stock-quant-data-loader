-- ===================================================================
-- Parsed / typed stooq daily prices
-- ===================================================================
-- IMPORTANT DESIGN RULE:
-- - This layer parses raw Stooq landing data into typed fields.
-- - It does NOT resolve canonical instruments yet.
-- - It does NOT merge to core.price_history yet.
-- - It remains provenance-preserving.
--
-- Observed raw schema:
-- - ticker_raw
-- - per_raw
-- - date_raw
-- - time_raw
-- - open_raw
-- - high_raw
-- - low_raw
-- - close_raw
-- - vol_raw
-- - openint_raw
--
-- Notes:
-- - Stooq date_raw is observed as YYYYMMDD.
-- - We keep adj_close/dividends/stock_splits as NULL because current
--   Stooq raw landing does not provide them.
-- - We preserve per_raw and time_raw in the parsed layer for provenance.
-- ===================================================================

CREATE SCHEMA IF NOT EXISTS parsed;

CREATE OR REPLACE VIEW parsed.price_source_daily_stooq AS
SELECT
    raw_price_source_daily_stooq_id,
    source_name,
    source_root_dir,
    source_file_path,
    source_file_name,
    source_line_number,

    UPPER(ticker_raw) AS symbol,
    CAST(TRY_STRPTIME(date_raw, '%Y%m%d') AS DATE) AS as_of_date,
    date_raw AS as_of_timestamp_tz_raw_text,

    TRY_CAST(open_raw AS DOUBLE) AS open,
    TRY_CAST(high_raw AS DOUBLE) AS high,
    TRY_CAST(low_raw AS DOUBLE) AS low,
    TRY_CAST(close_raw AS DOUBLE) AS close,
    NULL::DOUBLE AS adj_close,
    TRY_CAST(vol_raw AS BIGINT) AS volume,
    NULL::DOUBLE AS dividends,
    NULL::DOUBLE AS stock_splits,

    ticker_raw,
    per_raw,
    date_raw,
    time_raw,
    open_raw,
    high_raw,
    low_raw,
    close_raw,
    vol_raw,
    openint_raw,

    row_raw,
    ingested_at
FROM raw.price_source_daily_stooq;
