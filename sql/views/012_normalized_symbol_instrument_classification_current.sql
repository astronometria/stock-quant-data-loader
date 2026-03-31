-- ===================================================================
-- Normalized symbol instrument classification (resolved current view)
-- ===================================================================
-- IMPORTANT DESIGN RULE:
-- - Resolve to one current row per symbol_normalized.
-- - Prefer manual overrides over automatic rules.
-- - Prefer higher confidence, then richer source.
-- - This is a convenience current-state view, not a PIT engine.
-- ===================================================================

CREATE SCHEMA IF NOT EXISTS normalized;

CREATE OR REPLACE VIEW normalized.symbol_instrument_classification_current AS
WITH ranked AS (
    SELECT
        source_family,
        source_name,
        source_symbol_native,
        symbol_normalized,
        market_code,
        exchange_code,
        listing_venue_bucket,
        instrument_type,
        security_family,
        classification_confidence,
        classification_source,
        classification_rule,
        override_effective_from,
        override_effective_to,
        override_priority,
        override_notes,
        source_file_path,
        source_file_name,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY symbol_normalized
            ORDER BY
                CASE WHEN classification_source = 'manual_json_override' THEN 1 ELSE 0 END DESC,
                CASE
                    WHEN classification_confidence = 'high' THEN 3
                    WHEN classification_confidence = 'medium' THEN 2
                    ELSE 1
                END DESC,
                COALESCE(override_priority, 0) DESC,
                CASE
                    WHEN source_family = 'stooq' THEN 2
                    WHEN source_family = 'yfinance' THEN 1
                    ELSE 0
                END DESC,
                source_symbol_native
        ) AS rn
    FROM normalized.symbol_instrument_classification
)
SELECT
    source_family,
    source_name,
    source_symbol_native,
    symbol_normalized,
    market_code,
    exchange_code,
    listing_venue_bucket,
    instrument_type,
    security_family,
    classification_confidence,
    classification_source,
    classification_rule,
    override_effective_from,
    override_effective_to,
    override_priority,
    override_notes,
    source_file_path,
    source_file_name,
    ingested_at
FROM ranked
WHERE rn = 1;
