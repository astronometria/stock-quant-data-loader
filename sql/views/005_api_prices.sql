-- ===================================================================
-- API prices views
-- ===================================================================
-- These views expose a simple daily price history contract.
-- ===================================================================

CREATE OR REPLACE VIEW api.price_eod_history AS
SELECT
    ph.price_history_id,
    ph.instrument_id,
    i.company_id,
    i.security_type,
    i.primary_ticker,
    i.primary_exchange,
    srh.symbol,
    srh.exchange,
    ph.price_date,
    ph.open,
    ph.high,
    ph.low,
    ph.close,
    ph.adj_close,
    ph.volume,
    ph.source_name,
    ph.observed_at,
    ph.ingested_at
FROM core.price_history AS ph
JOIN core.instrument AS i
  ON i.instrument_id = ph.instrument_id
JOIN core.symbol_reference_history AS srh
  ON srh.instrument_id = ph.instrument_id
 AND srh.effective_from <= ph.price_date
 AND (
        srh.effective_to IS NULL
        OR srh.effective_to > ph.price_date
     );
