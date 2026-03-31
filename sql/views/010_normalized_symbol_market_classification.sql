-- ===================================================================
-- Normalized symbol market classification
-- ===================================================================
-- IMPORTANT DESIGN RULE:
-- - This is a first-pass source-aware classification view.
-- - It stays transparent and rule-based.
-- - It does NOT yet represent final canonical truth.
-- - Rules are intentionally simple and auditable.
-- ===================================================================

CREATE SCHEMA IF NOT EXISTS normalized;

CREATE OR REPLACE VIEW normalized.symbol_market_classification AS
WITH stooq_base AS (
    SELECT DISTINCT
        'stooq' AS source_family,
        source_name,
        source_symbol_native,
        symbol_normalized,

        -- Extract physical Stooq bucket from the file path.
        regexp_extract(source_file_path, '.*/daily/us/([^/]+)/.*', 1) AS listing_venue_bucket,

        source_file_path,
        source_file_name,
        ingested_at
    FROM parsed.price_source_daily_stooq_normalized
),
stooq_classified AS (
    SELECT
        source_family,
        source_name,
        source_symbol_native,
        symbol_normalized,

        -- First observed market classification for this source.
        'US' AS market_code,

        CASE
            WHEN listing_venue_bucket LIKE 'nasdaq %' THEN 'NASDAQ'
            WHEN listing_venue_bucket LIKE 'nyse %' THEN 'NYSE'
            WHEN listing_venue_bucket LIKE 'nysemkt %' THEN 'NYSEMKT'
            ELSE 'UNKNOWN'
        END AS exchange_code,

        listing_venue_bucket,

        'stooq_path_bucket_rule' AS classification_source,
        'bucket_to_market_exchange' AS classification_rule,

        source_file_path,
        source_file_name,
        ingested_at
    FROM stooq_base
),
yfinance_base AS (
    SELECT DISTINCT
        'yfinance' AS source_family,
        source_name,
        symbol AS source_symbol_native,
        symbol AS symbol_normalized,
        source_file_path,
        source_file_name,
        ingested_at
    FROM parsed.price_source_daily_yfinance
),
yfinance_classified AS (
    SELECT
        source_family,
        source_name,
        source_symbol_native,
        symbol_normalized,

        -- For now this downloader flow is US-only.
        'US' AS market_code,

        -- Exchange is not asserted yet from yfinance raw files alone.
        'UNKNOWN' AS exchange_code,

        NULL AS listing_venue_bucket,

        'yfinance_default_us_rule' AS classification_source,
        'current_downloader_scope_us_only' AS classification_rule,

        source_file_path,
        source_file_name,
        ingested_at
    FROM yfinance_base
)
SELECT * FROM stooq_classified
UNION ALL
SELECT * FROM yfinance_classified;
