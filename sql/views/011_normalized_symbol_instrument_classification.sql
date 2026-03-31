-- ===================================================================
-- Normalized symbol instrument classification (source-aware)
-- ===================================================================
-- IMPORTANT DESIGN RULE:
-- - Manual JSON overrides are first-class inputs.
-- - JSON schema is declared explicitly to avoid fragile type inference.
-- - Overrides may target:
--   * symbol_normalized only
--   * symbol_normalized + source_family
--   * symbol_normalized + source_family + source_symbol_native
-- - Higher priority wins.
-- - Date fields are preserved for future PIT-aware logic.
-- - Automatic rules remain intentionally conservative:
--   * Stooq "... etfs" bucket -> ETF
--   * Stooq "... stocks" bucket -> COMMON_STOCK
--   * yfinance default -> UNKNOWN
-- ===================================================================

CREATE SCHEMA IF NOT EXISTS normalized;

CREATE OR REPLACE VIEW normalized.symbol_instrument_classification AS
WITH manual_overrides_raw AS (
    SELECT
        UPPER(TRIM(symbol_normalized)) AS symbol_normalized,
        CASE
            WHEN source_family IS NULL OR TRIM(source_family) = '' THEN NULL
            ELSE LOWER(TRIM(source_family))
        END AS source_family,
        CASE
            WHEN source_symbol_native IS NULL OR TRIM(source_symbol_native) = '' THEN NULL
            ELSE UPPER(TRIM(source_symbol_native))
        END AS source_symbol_native,
        UPPER(TRIM(instrument_type)) AS instrument_type,
        UPPER(TRIM(security_family)) AS security_family,
        CASE
            WHEN market_code IS NULL OR TRIM(market_code) = '' THEN NULL
            ELSE UPPER(TRIM(market_code))
        END AS market_code,
        CASE
            WHEN exchange_code IS NULL OR TRIM(exchange_code) = '' THEN NULL
            ELSE UPPER(TRIM(exchange_code))
        END AS exchange_code,
        CASE
            WHEN listing_venue_bucket IS NULL OR TRIM(listing_venue_bucket) = '' THEN NULL
            ELSE TRIM(listing_venue_bucket)
        END AS listing_venue_bucket,
        CAST(NULLIF(TRIM(effective_from), '') AS DATE) AS effective_from,
        CAST(NULLIF(TRIM(effective_to), '') AS DATE) AS effective_to,
        COALESCE(priority, 100) AS priority,
        notes
    FROM read_json_auto(
        'data/manual/symbol_instrument_overrides.json',
        columns = {
            'symbol_normalized': 'VARCHAR',
            'source_family': 'VARCHAR',
            'source_symbol_native': 'VARCHAR',
            'instrument_type': 'VARCHAR',
            'security_family': 'VARCHAR',
            'market_code': 'VARCHAR',
            'exchange_code': 'VARCHAR',
            'listing_venue_bucket': 'VARCHAR',
            'effective_from': 'VARCHAR',
            'effective_to': 'VARCHAR',
            'priority': 'INTEGER',
            'notes': 'VARCHAR'
        },
        maximum_object_size = 10485760
    )
),
stooq_base AS (
    SELECT DISTINCT
        'stooq' AS source_family,
        source_name,
        source_symbol_native,
        symbol_normalized,
        'US' AS market_code,
        CASE
            WHEN regexp_extract(source_file_path, '.*/daily/us/([^/]+)/.*', 1) LIKE 'nasdaq %' THEN 'NASDAQ'
            WHEN regexp_extract(source_file_path, '.*/daily/us/([^/]+)/.*', 1) LIKE 'nyse %' THEN 'NYSE'
            WHEN regexp_extract(source_file_path, '.*/daily/us/([^/]+)/.*', 1) LIKE 'nysemkt %' THEN 'NYSEMKT'
            ELSE 'UNKNOWN'
        END AS exchange_code,
        regexp_extract(source_file_path, '.*/daily/us/([^/]+)/.*', 1) AS listing_venue_bucket,
        source_file_path,
        source_file_name,
        ingested_at
    FROM parsed.price_source_daily_stooq_normalized
),
stooq_manual_candidates AS (
    SELECT
        base.*,
        manual.instrument_type AS manual_instrument_type,
        manual.security_family AS manual_security_family,
        manual.market_code AS manual_market_code,
        manual.exchange_code AS manual_exchange_code,
        manual.listing_venue_bucket AS manual_listing_venue_bucket,
        manual.effective_from AS manual_effective_from,
        manual.effective_to AS manual_effective_to,
        manual.priority AS manual_priority,
        manual.notes AS manual_notes,
        ROW_NUMBER() OVER (
            PARTITION BY base.source_family, base.source_symbol_native, base.symbol_normalized
            ORDER BY
                CASE
                    WHEN manual.source_symbol_native IS NOT NULL THEN 3
                    WHEN manual.source_family IS NOT NULL THEN 2
                    ELSE 1
                END DESC,
                manual.priority DESC,
                manual.instrument_type,
                manual.security_family
        ) AS rn
    FROM stooq_base base
    LEFT JOIN manual_overrides_raw manual
        ON manual.symbol_normalized = UPPER(TRIM(base.symbol_normalized))
       AND (manual.source_family IS NULL OR manual.source_family = base.source_family)
       AND (manual.source_symbol_native IS NULL OR manual.source_symbol_native = UPPER(TRIM(base.source_symbol_native)))
),
stooq_best_manual AS (
    SELECT *
    FROM stooq_manual_candidates
    WHERE rn = 1
),
stooq_classified AS (
    SELECT
        source_family,
        source_name,
        source_symbol_native,
        symbol_normalized,

        COALESCE(manual_market_code, market_code) AS market_code,
        COALESCE(manual_exchange_code, exchange_code) AS exchange_code,
        COALESCE(manual_listing_venue_bucket, listing_venue_bucket) AS listing_venue_bucket,

        COALESCE(
            manual_instrument_type,
            CASE
                WHEN listing_venue_bucket LIKE '% etfs' THEN 'ETF'
                WHEN listing_venue_bucket LIKE '% stocks' THEN 'COMMON_STOCK'
                ELSE 'UNKNOWN'
            END
        ) AS instrument_type,

        COALESCE(
            manual_security_family,
            CASE
                WHEN listing_venue_bucket LIKE '% etfs' THEN 'FUND'
                WHEN listing_venue_bucket LIKE '% stocks' THEN 'EQUITY'
                ELSE 'UNKNOWN'
            END
        ) AS security_family,

        CASE
            WHEN manual_instrument_type IS NOT NULL THEN 'high'
            WHEN listing_venue_bucket LIKE '% etfs' THEN 'high'
            WHEN listing_venue_bucket LIKE '% stocks' THEN 'medium'
            ELSE 'low'
        END AS classification_confidence,

        CASE
            WHEN manual_instrument_type IS NOT NULL THEN 'manual_json_override'
            WHEN listing_venue_bucket LIKE '% etfs' THEN 'stooq_bucket_rule'
            WHEN listing_venue_bucket LIKE '% stocks' THEN 'stooq_stock_bucket_rule'
            ELSE 'stooq_unclassified_default'
        END AS classification_source,

        CASE
            WHEN manual_instrument_type IS NOT NULL THEN 'manual_override'
            WHEN listing_venue_bucket LIKE '% etfs' THEN 'bucket_contains_etfs'
            WHEN listing_venue_bucket LIKE '% stocks' THEN 'bucket_contains_stocks'
            ELSE 'no_reliable_source_specific_rule'
        END AS classification_rule,

        manual_effective_from AS override_effective_from,
        manual_effective_to AS override_effective_to,
        manual_priority AS override_priority,
        manual_notes AS override_notes,

        source_file_path,
        source_file_name,
        ingested_at
    FROM stooq_best_manual
),
yfinance_base AS (
    SELECT DISTINCT
        'yfinance' AS source_family,
        source_name,
        symbol AS source_symbol_native,
        symbol AS symbol_normalized,
        'US' AS market_code,
        'UNKNOWN' AS exchange_code,
        NULL AS listing_venue_bucket,
        source_file_path,
        source_file_name,
        ingested_at
    FROM parsed.price_source_daily_yfinance
),
yfinance_manual_candidates AS (
    SELECT
        base.*,
        manual.instrument_type AS manual_instrument_type,
        manual.security_family AS manual_security_family,
        manual.market_code AS manual_market_code,
        manual.exchange_code AS manual_exchange_code,
        manual.listing_venue_bucket AS manual_listing_venue_bucket,
        manual.effective_from AS manual_effective_from,
        manual.effective_to AS manual_effective_to,
        manual.priority AS manual_priority,
        manual.notes AS manual_notes,
        ROW_NUMBER() OVER (
            PARTITION BY base.source_family, base.source_symbol_native, base.symbol_normalized
            ORDER BY
                CASE
                    WHEN manual.source_symbol_native IS NOT NULL THEN 3
                    WHEN manual.source_family IS NOT NULL THEN 2
                    ELSE 1
                END DESC,
                manual.priority DESC,
                manual.instrument_type,
                manual.security_family
        ) AS rn
    FROM yfinance_base base
    LEFT JOIN manual_overrides_raw manual
        ON manual.symbol_normalized = UPPER(TRIM(base.symbol_normalized))
       AND (manual.source_family IS NULL OR manual.source_family = base.source_family)
       AND (manual.source_symbol_native IS NULL OR manual.source_symbol_native = UPPER(TRIM(base.source_symbol_native)))
),
yfinance_best_manual AS (
    SELECT *
    FROM yfinance_manual_candidates
    WHERE rn = 1
),
yfinance_classified AS (
    SELECT
        source_family,
        source_name,
        source_symbol_native,
        symbol_normalized,

        COALESCE(manual_market_code, market_code) AS market_code,
        COALESCE(manual_exchange_code, exchange_code) AS exchange_code,
        COALESCE(manual_listing_venue_bucket, listing_venue_bucket) AS listing_venue_bucket,

        COALESCE(manual_instrument_type, 'UNKNOWN') AS instrument_type,
        COALESCE(manual_security_family, 'UNKNOWN') AS security_family,

        CASE
            WHEN manual_instrument_type IS NOT NULL THEN 'high'
            ELSE 'low'
        END AS classification_confidence,

        CASE
            WHEN manual_instrument_type IS NOT NULL THEN 'manual_json_override'
            ELSE 'yfinance_unclassified_default'
        END AS classification_source,

        CASE
            WHEN manual_instrument_type IS NOT NULL THEN 'manual_override'
            ELSE 'no_reliable_source_specific_rule'
        END AS classification_rule,

        manual_effective_from AS override_effective_from,
        manual_effective_to AS override_effective_to,
        manual_priority AS override_priority,
        manual_notes AS override_notes,

        source_file_path,
        source_file_name,
        ingested_at
    FROM yfinance_best_manual
)
SELECT * FROM stooq_classified
UNION ALL
SELECT * FROM yfinance_classified;
