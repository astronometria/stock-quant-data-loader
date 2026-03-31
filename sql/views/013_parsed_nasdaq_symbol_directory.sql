-- ===================================================================
-- Parsed Nasdaq symbol directory snapshots
-- ===================================================================
-- Design rules:
-- - typed, still source-faithful
-- - separate shape handling for nasdaqlisted vs otherlisted
-- - no heavy business normalization here
-- ===================================================================

CREATE SCHEMA IF NOT EXISTS parsed;

CREATE OR REPLACE VIEW parsed.nasdaq_symbol_directory_snapshot AS
WITH base AS (
    SELECT
        source_name,
        source_root_dir,
        source_file_path,
        source_file_name,
        snapshot_file_type,
        source_line_number,
        row_raw,
        col_01_raw,
        col_02_raw,
        col_03_raw,
        col_04_raw,
        col_05_raw,
        col_06_raw,
        col_07_raw,
        col_08_raw,
        ingested_at,
        regexp_extract(source_file_name, '^([^_]+_[^_]+|[^_]+)', 1) AS batch_prefix
    FROM raw.nasdaq_symbol_directory_snapshot
),
nasdaqlisted_rows AS (
    SELECT
        'nasdaqlisted' AS file_family,
        source_name,
        source_root_dir,
        source_file_path,
        source_file_name,
        snapshot_file_type,
        source_line_number,
        row_raw,
        col_01_raw AS symbol_raw,
        col_02_raw AS security_name_raw,
        col_03_raw AS market_category_raw,
        col_04_raw AS test_issue_raw,
        col_05_raw AS financial_status_raw,
        col_06_raw AS round_lot_size_raw,
        col_07_raw AS etf_flag_raw,
        col_08_raw AS nextshares_flag_raw,
        NULL::VARCHAR AS exchange_raw,
        NULL::VARCHAR AS cqs_symbol_raw,
        NULL::VARCHAR AS nasdaq_symbol_raw,
        ingested_at
    FROM base
    WHERE snapshot_file_type = 'nasdaqlisted'
      AND col_01_raw IS NOT NULL
      AND col_01_raw <> 'Symbol'
      AND col_01_raw <> 'File Creation Time'
),
otherlisted_rows AS (
    SELECT
        'otherlisted' AS file_family,
        source_name,
        source_root_dir,
        source_file_path,
        source_file_name,
        snapshot_file_type,
        source_line_number,
        row_raw,
        col_01_raw AS symbol_raw,
        col_02_raw AS security_name_raw,
        NULL::VARCHAR AS market_category_raw,
        col_07_raw AS test_issue_raw,
        NULL::VARCHAR AS financial_status_raw,
        col_06_raw AS round_lot_size_raw,
        col_05_raw AS etf_flag_raw,
        NULL::VARCHAR AS nextshares_flag_raw,
        col_03_raw AS exchange_raw,
        col_04_raw AS cqs_symbol_raw,
        col_08_raw AS nasdaq_symbol_raw,
        ingested_at
    FROM base
    WHERE snapshot_file_type = 'otherlisted'
      AND col_01_raw IS NOT NULL
      AND col_01_raw <> 'ACT Symbol'
      AND col_01_raw <> 'File Creation Time'
)
SELECT * FROM nasdaqlisted_rows
UNION ALL
SELECT * FROM otherlisted_rows;
