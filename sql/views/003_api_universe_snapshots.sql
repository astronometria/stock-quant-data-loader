-- ===================================================================
-- API universe snapshot views
-- ===================================================================
-- These views expose PIT-ready read contracts for universe membership.
-- ===================================================================

CREATE OR REPLACE VIEW api.universe_membership_history AS
SELECT
    umh.universe_membership_history_id,
    umh.universe_id,
    ud.universe_name,
    ud.description AS universe_description,
    umh.instrument_id,
    i.company_id,
    i.security_type,
    i.primary_ticker,
    i.primary_exchange,
    umh.membership_status,
    umh.effective_from,
    umh.effective_to,
    umh.observed_at,
    umh.ingested_at,
    umh.source_name
FROM core.universe_membership_history AS umh
JOIN core.universe_definition AS ud
  ON ud.universe_id = umh.universe_id
JOIN core.instrument AS i
  ON i.instrument_id = umh.instrument_id;
