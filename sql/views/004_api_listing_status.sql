-- ===================================================================
-- API listing status views
-- ===================================================================
-- These views expose listing lifecycle history for PIT-aware consumers.
-- ===================================================================

CREATE OR REPLACE VIEW api.listing_status_history AS
SELECT
    lsh.listing_status_history_id,
    lsh.instrument_id,
    i.company_id,
    i.security_type,
    i.primary_ticker,
    i.primary_exchange,
    lsh.symbol,
    lsh.exchange,
    lsh.listing_status,
    lsh.event_type,
    lsh.effective_from,
    lsh.effective_to,
    lsh.observed_at,
    lsh.ingested_at,
    lsh.source_name
FROM core.listing_status_history AS lsh
JOIN core.instrument AS i
  ON i.instrument_id = lsh.instrument_id;
