-- ===================================================================
-- Price scientific validation checks
-- ===================================================================
-- These checks are publication blockers for core.price_history quality.
-- ===================================================================

SELECT
    'price_history_duplicate_instrument_date' AS check_name,
    COUNT(*) AS failed_rows
FROM (
    SELECT
        instrument_id,
        price_date,
        COUNT(*) AS row_count
    FROM core.price_history
    GROUP BY instrument_id, price_date
    HAVING COUNT(*) > 1
) AS duplicate_rows

UNION ALL

SELECT
    'price_history_negative_volume' AS check_name,
    COUNT(*) AS failed_rows
FROM core.price_history
WHERE volume IS NOT NULL
  AND volume < 0

UNION ALL

SELECT
    'price_history_ohlc_inconsistent' AS check_name,
    COUNT(*) AS failed_rows
FROM core.price_history
WHERE
    (high IS NOT NULL AND low IS NOT NULL AND high < low)
    OR (open IS NOT NULL AND high IS NOT NULL AND open > high)
    OR (open IS NOT NULL AND low IS NOT NULL AND open < low)
    OR (close IS NOT NULL AND high IS NOT NULL AND close > high)
    OR (close IS NOT NULL AND low IS NOT NULL AND close < low)

UNION ALL

SELECT
    'raw_price_source_daily_null_source_symbol' AS check_name,
    COUNT(*) AS failed_rows
FROM raw.price_source_daily
WHERE source_symbol IS NULL
   OR TRIM(source_symbol) = '';
