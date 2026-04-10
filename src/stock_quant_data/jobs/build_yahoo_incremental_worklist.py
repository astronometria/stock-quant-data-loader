"""Build yahoo_incremental_worklist from PIT-aware loader state."""
from __future__ import annotations
import logging
from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)

def run() -> None:
    configure_logging()
    LOGGER.info("build-yahoo-incremental-worklist started")
    conn = connect_build_db()
    try:
        required_counts = {
            "provider_symbols_active": conn.execute("""
                SELECT COUNT(*)
                FROM v_instrument_provider_symbol_active
                WHERE provider_name = 'YAHOO'
            """).fetchone()[0],
            "coverage_rows": conn.execute("SELECT COUNT(*) FROM v_price_provider_coverage_by_instrument").fetchone()[0],
        }
        conn.execute("DELETE FROM yahoo_incremental_worklist WHERE run_date = CURRENT_DATE")
        conn.execute("""
            INSERT INTO yahoo_incremental_worklist (
                yahoo_incremental_worklist_id,
                run_date,
                instrument_id,
                provider_name,
                provider_symbol,
                fetch_mode,
                fetch_reason,
                fetch_start_date,
                fetch_end_date,
                stooq_max_price_date,
                yahoo_max_price_date,
                canonical_max_price_date,
                expected_latest_trade_date,
                listing_status,
                listing_status_reason,
                security_type,
                primary_exchange,
                priority_rank,
                is_active_listing,
                allow_overlap_redownload,
                source_name
            )
            WITH params AS (
                SELECT CURRENT_DATE AS run_date, CURRENT_DATE AS expected_latest_trade_date
            ),
            base AS (
                SELECT
                    c.instrument_id,
                    s.provider_name,
                    s.provider_symbol,
                    c.stooq_max_price_date,
                    c.yahoo_max_price_date,
                    c.canonical_max_price_date,
                    c.listing_status,
                    c.listing_status_reason,
                    c.security_type,
                    c.primary_exchange,
                    p.run_date,
                    p.expected_latest_trade_date
                FROM v_price_provider_coverage_by_instrument c
                JOIN v_instrument_provider_symbol_active s
                  ON s.instrument_id = c.instrument_id
                 AND s.provider_name = 'YAHOO'
                CROSS JOIN params p
                WHERE c.security_type IN ('COMMON_STOCK', 'ETF')
                  AND c.stooq_max_price_date IS NOT NULL
                  AND (
                        c.listing_status = 'ACTIVE'
                        OR (
                            c.listing_status = 'INACTIVE'
                            AND c.canonical_max_price_date >= p.expected_latest_trade_date - INTERVAL 30 DAY
                        )
                      )
            ),
            staged AS (
                SELECT
                    *,
                    CASE
                        WHEN yahoo_max_price_date IS NULL THEN 'REPAIR'
                        WHEN yahoo_max_price_date >= expected_latest_trade_date THEN 'SKIP'
                        ELSE 'REPAIR'
                    END AS fetch_mode_raw,
                    CAST(stooq_max_price_date + INTERVAL 1 DAY AS DATE) AS repair_start_date
                FROM base
            ),
            finalized AS (
                SELECT
                    *,
                    CASE
                        WHEN fetch_mode_raw = 'SKIP' THEN NULL
                        WHEN yahoo_max_price_date IS NULL THEN 'STOOQ_ENDED_NEEDS_INITIAL_YAHOO_CONTINUATION'
                        ELSE 'REPAIR_FROM_STOOQ_FRONTIER'
                    END AS fetch_reason,
                    CASE
                        WHEN fetch_mode_raw = 'SKIP' THEN NULL
                        ELSE repair_start_date
                    END AS fetch_start_date,
                    expected_latest_trade_date AS fetch_end_date,
                    CASE
                        WHEN fetch_mode_raw = 'SKIP' THEN FALSE
                        WHEN yahoo_max_price_date IS NULL THEN FALSE
                        ELSE TRUE
                    END AS allow_overlap_redownload
                FROM staged
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY instrument_id, provider_symbol) AS yahoo_incremental_worklist_id,
                run_date,
                instrument_id,
                provider_name,
                provider_symbol,
                'REPAIR' AS fetch_mode,
                fetch_reason,
                fetch_start_date,
                fetch_end_date,
                stooq_max_price_date,
                yahoo_max_price_date,
                canonical_max_price_date,
                expected_latest_trade_date,
                listing_status,
                listing_status_reason,
                security_type,
                primary_exchange,
                100 AS priority_rank,
                CASE WHEN listing_status = 'ACTIVE' THEN TRUE ELSE FALSE END AS is_active_listing,
                allow_overlap_redownload,
                'build-yahoo-incremental-worklist' AS source_name
            FROM finalized
            WHERE fetch_start_date IS NOT NULL
              AND fetch_start_date <= fetch_end_date
        """)
        written_count = conn.execute("SELECT COUNT(*) FROM yahoo_incremental_worklist WHERE run_date = CURRENT_DATE").fetchone()[0]
        print({"status": "ok", "job": "build-yahoo-incremental-worklist", "required_counts": required_counts, "written_count": written_count})
    finally:
        conn.close()
        LOGGER.info("build-yahoo-incremental-worklist finished")

if __name__ == "__main__":
    run()
