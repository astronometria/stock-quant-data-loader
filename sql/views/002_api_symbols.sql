-- ===================================================================
-- API symbol views
-- ===================================================================
-- These views define the first read contract for historical symbol data.
-- ===================================================================

CREATE OR REPLACE VIEW api.symbol_history AS
SELECT
    srh.symbol_reference_history_id,
    srh.instrument_id,
    i.company_id,
    i.security_type,
    i.primary_ticker,
    i.primary_exchange,
    srh.symbol,
    srh.exchange,
    srh.mic,
    srh.is_primary,
    srh.effective_from,
    srh.effective_to,
    srh.observed_at,
    srh.ingested_at,
    srh.source_name,
    srh.record_status
FROM core.symbol_reference_history AS srh
JOIN core.instrument AS i
  ON i.instrument_id = srh.instrument_id;

CREATE OR REPLACE VIEW api.symbol_latest AS
SELECT
    instrument_id,
    company_id,
    security_type,
    primary_ticker,
    primary_exchange,
    symbol,
    exchange,
    mic,
    is_primary,
    effective_from,
    effective_to,
    observed_at,
    ingested_at,
    source_name,
    record_status
FROM (
    SELECT
        ash.*,
        ROW_NUMBER() OVER (
            PARTITION BY ash.symbol
            ORDER BY
                COALESCE(ash.effective_to, DATE '2999-12-31') DESC,
                ash.effective_from DESC,
                ash.symbol_reference_history_id DESC
        ) AS rn
    FROM api.symbol_history AS ash
) ranked
WHERE rn = 1;
