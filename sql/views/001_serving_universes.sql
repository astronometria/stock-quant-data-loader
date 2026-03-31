-- ===================================================================
-- API views for universe exposure
-- ===================================================================
-- These views are intentionally simple in v1.
-- They project stable read-only API objects from the core schema.
--
-- Important naming rule:
-- - schema name is "api", not "serving"
-- - this avoids DuckDB catalog/schema ambiguity inside serving.duckdb
-- ===================================================================

CREATE OR REPLACE VIEW api.universes AS
SELECT
    ud.universe_id,
    ud.universe_name,
    ud.description,
    ud.created_at,
    CAST(COUNT(DISTINCT umh.instrument_id) AS BIGINT) AS historical_instrument_count
FROM core.universe_definition AS ud
LEFT JOIN core.universe_membership_history AS umh
    ON ud.universe_id = umh.universe_id
GROUP BY
    ud.universe_id,
    ud.universe_name,
    ud.description,
    ud.created_at
ORDER BY
    ud.universe_name;
