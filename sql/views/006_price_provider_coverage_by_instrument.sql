-- Provider coverage summary by instrument.
CREATE OR REPLACE VIEW v_price_provider_coverage_by_instrument AS
WITH normalized AS (
    SELECT
        instrument_id,
        UPPER(source_name) AS source_name,
        price_date
    FROM price_source_daily_normalized
    WHERE instrument_id IS NOT NULL
),
listing_open AS (
    SELECT
        instrument_id,
        listing_status,
        status_reason
    FROM listing_status_history
    WHERE effective_to IS NULL
)
SELECT
    i.instrument_id,
    i.primary_ticker,
    i.security_type,
    i.primary_exchange,
    lo.listing_status,
    lo.status_reason AS listing_status_reason,
    MIN(CASE WHEN n.source_name = 'STOOQ' THEN n.price_date END) AS stooq_min_price_date,
    MAX(CASE WHEN n.source_name = 'STOOQ' THEN n.price_date END) AS stooq_max_price_date,
    MIN(CASE WHEN n.source_name = 'YAHOO' THEN n.price_date END) AS yahoo_min_price_date,
    MAX(CASE WHEN n.source_name = 'YAHOO' THEN n.price_date END) AS yahoo_max_price_date,
    MAX(n.price_date) AS canonical_max_price_date
FROM instrument i
LEFT JOIN listing_open lo
  ON lo.instrument_id = i.instrument_id
LEFT JOIN normalized n
  ON n.instrument_id = i.instrument_id
GROUP BY
    i.instrument_id,
    i.primary_ticker,
    i.security_type,
    i.primary_exchange,
    lo.listing_status,
    lo.status_reason;
