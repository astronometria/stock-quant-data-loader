-- ===================================================================
-- Current Nasdaq symbol directory symbols
-- ===================================================================
-- Design rules:
-- - select the latest observed row per symbol/file family
-- - still observational, not PIT-complete history
-- - useful for enrichment / manual review / downloader targeting
-- ===================================================================

CREATE SCHEMA IF NOT EXISTS normalized;

CREATE OR REPLACE VIEW normalized.nasdaq_current_symbols AS
WITH ranked AS (
    SELECT
        file_family,
        source_name,
        source_root_dir,
        source_file_path,
        source_file_name,
        symbol_raw,
        security_name_raw,
        market_category_raw,
        test_issue_raw,
        financial_status_raw,
        round_lot_size_raw,
        etf_flag_raw,
        nextshares_flag_raw,
        exchange_raw,
        cqs_symbol_raw,
        nasdaq_symbol_raw,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY file_family, symbol_raw
            ORDER BY ingested_at DESC, source_file_name DESC, source_line_number DESC
        ) AS rn
    FROM parsed.nasdaq_symbol_directory_snapshot
)
SELECT
    file_family,
    symbol_raw,
    security_name_raw,
    market_category_raw,
    test_issue_raw,
    financial_status_raw,
    round_lot_size_raw,
    etf_flag_raw,
    nextshares_flag_raw,
    exchange_raw,
    cqs_symbol_raw,
    nasdaq_symbol_raw,
    source_name,
    source_root_dir,
    source_file_path,
    source_file_name,
    ingested_at
FROM ranked
WHERE rn = 1;
