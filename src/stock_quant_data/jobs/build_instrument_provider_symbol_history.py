"""Build instrument_provider_symbol_history for current fetch providers."""
from __future__ import annotations
import logging
from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)

def run() -> None:
    configure_logging()
    LOGGER.info("build-instrument-provider-symbol-history started")
    conn = connect_build_db()
    try:
        required_counts = {
            "instrument": conn.execute("SELECT COUNT(*) FROM instrument").fetchone()[0],
            "symbol_reference_history": conn.execute("SELECT COUNT(*) FROM symbol_reference_history").fetchone()[0],
            "listing_status_history": conn.execute("SELECT COUNT(*) FROM listing_status_history").fetchone()[0],
        }
        conn.execute("DELETE FROM instrument_provider_symbol_history WHERE provider_name = 'YAHOO'")
        conn.execute("""
            INSERT INTO instrument_provider_symbol_history (
                instrument_provider_symbol_history_id,
                instrument_id,
                provider_name,
                provider_symbol,
                effective_from,
                effective_to,
                symbol_role,
                mapping_status,
                confidence_score,
                source_name,
                source_priority,
                source_detail,
                discovered_at
            )
            WITH listing_open AS (
                SELECT instrument_id, effective_from
                FROM listing_status_history
                WHERE listing_status = 'ACTIVE'
            ),
            symbol_bounds AS (
                SELECT instrument_id, MIN(effective_from) AS min_symbol_effective_from
                FROM symbol_reference_history
                GROUP BY instrument_id
            ),
            base AS (
                SELECT
                    i.instrument_id,
                    UPPER(TRIM(i.primary_ticker)) AS provider_symbol,
                    COALESCE(sb.min_symbol_effective_from, MIN(lo.effective_from), DATE '1970-01-01') AS effective_from
                FROM instrument i
                LEFT JOIN symbol_bounds sb ON sb.instrument_id = i.instrument_id
                LEFT JOIN listing_open lo ON lo.instrument_id = i.instrument_id
                WHERE i.primary_ticker IS NOT NULL
                  AND TRIM(i.primary_ticker) <> ''
                GROUP BY i.instrument_id, UPPER(TRIM(i.primary_ticker)), sb.min_symbol_effective_from
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY instrument_id, provider_symbol) AS instrument_provider_symbol_history_id,
                instrument_id,
                'YAHOO' AS provider_name,
                provider_symbol,
                effective_from,
                NULL AS effective_to,
                'PRIMARY_FETCH_SYMBOL' AS symbol_role,
                'ACTIVE' AS mapping_status,
                1.0 AS confidence_score,
                'build-instrument-provider-symbol-history' AS source_name,
                100 AS source_priority,
                'derived from instrument.primary_ticker + loader identity layer' AS source_detail,
                CURRENT_TIMESTAMP AS discovered_at
            FROM base
        """)
        written_count = conn.execute("SELECT COUNT(*) FROM instrument_provider_symbol_history WHERE provider_name = 'YAHOO'").fetchone()[0]
        print({"status": "ok", "job": "build-instrument-provider-symbol-history", "required_counts": required_counts, "written_count": written_count})
    finally:
        conn.close()
        LOGGER.info("build-instrument-provider-symbol-history finished")

if __name__ == "__main__":
    run()
