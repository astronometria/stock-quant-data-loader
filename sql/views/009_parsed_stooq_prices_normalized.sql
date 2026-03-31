-- ===================================================================
-- Parsed normalized Stooq daily prices
-- ===================================================================
-- IMPORTANT DESIGN RULE:
-- - This layer keeps the source-native symbol AND a normalized symbol.
-- - It does NOT resolve canonical instruments yet.
-- - It removes the observed ".US" suffix from Stooq native symbols.
-- - It keeps provenance columns for audit/debug.
-- ===================================================================

CREATE SCHEMA IF NOT EXISTS parsed;

CREATE OR REPLACE VIEW parsed.price_source_daily_stooq_normalized AS
SELECT
    raw_price_source_daily_stooq_id,
    source_name,
    source_root_dir,
    source_file_path,
    source_file_name,
    source_line_number,

    -- Source symbol exactly as observed in raw Stooq files.
    symbol AS source_symbol_native,

    -- Normalized symbol for downstream source alignment.
    regexp_replace(symbol, '\.US$', '') AS symbol_normalized,

    -- Keep parsed temporal fields.
    as_of_timestamp_tz_raw_text,
    as_of_date,

    -- Keep typed price fields.
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    dividends,
    stock_splits,

    -- Keep raw/parsed provenance fields.
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
FROM parsed.price_source_daily_stooq;
