"""
Build PIT-ready universe_membership_history from listing_status_history.

Design goals:
- use listing_status_history as the canonical status layer
- keep SQL-first logic for the actual history construction
- produce deterministic rebuilds
- separate common stocks and ETFs into different universes
- exclude obvious non-core instruments from the common stock universe
- keep the code very explicit and heavily commented for maintainability
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild universe_membership_history from listing_status_history.

    Universes created:
    - US_LISTED_COMMON_STOCKS
    - US_LISTED_ETFS

    Important notes:
    - We only include listing_status='ACTIVE'.
    - We exclude OTC from research-grade listed universes.
    - We keep common stocks and ETFs separate.
    - For common stocks, we exclude obvious non-core suffix patterns
      like warrants, rights, units, and test symbols.
    """
    configure_logging()
    LOGGER.info("build-universe-membership-history-from-listing-status started")

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Basic guardrails: fail early if required inputs are missing.
        # ------------------------------------------------------------------
        required_counts = {
            "listing_status_history": conn.execute(
                "SELECT COUNT(*) FROM listing_status_history"
            ).fetchone()[0],
            "instrument": conn.execute(
                "SELECT COUNT(*) FROM instrument"
            ).fetchone()[0],
        }

        if required_counts["listing_status_history"] == 0:
            raise RuntimeError("listing_status_history is empty; build it first")

        # ------------------------------------------------------------------
        # Rebuild target table deterministically.
        # We delete only the universes owned by this job so other universes
        # are not accidentally removed.
        # ------------------------------------------------------------------
        conn.execute(
            """
            DELETE FROM universe_membership_history
            WHERE source_name = 'build-universe-membership-history-from-listing-status'
            """
        )

        # ------------------------------------------------------------------
        # Stage ACTIVE listing rows joined to instrument metadata.
        # This staging table makes the business rules easier to read and debug.
        # ------------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_active_listing_membership_stage")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_active_listing_membership_stage AS
            SELECT
                l.listing_status_history_id,
                l.instrument_id,
                l.symbol,
                l.listing_status,
                l.status_reason,
                l.effective_from,
                l.effective_to,
                i.security_type,
                i.primary_exchange
            FROM listing_status_history l
            LEFT JOIN instrument i
              ON i.instrument_id = l.instrument_id
            WHERE l.listing_status = 'ACTIVE'
            """
        )

        # ------------------------------------------------------------------
        # Insert COMMON STOCK universe membership.
        #
        # Filters:
        # - ACTIVE only (already staged)
        # - COMMON_STOCK only
        # - non-OTC only
        # - exclude test symbols
        # - exclude obvious warrant/right/unit patterns
        # - exclude dash-class series such as BRK-A for this first strict cut
        #
        # This is intentionally conservative for research-grade membership.
        # ------------------------------------------------------------------
        conn.execute(
            """
            INSERT INTO universe_membership_history (
                universe_membership_history_id,
                universe_name,
                instrument_id,
                effective_from,
                effective_to,
                source_name
            )
            SELECT
                listing_status_history_id,
                'US_LISTED_COMMON_STOCKS' AS universe_name,
                instrument_id,
                effective_from,
                effective_to,
                'build-universe-membership-history-from-listing-status' AS source_name
            FROM tmp_active_listing_membership_stage
            WHERE upper(COALESCE(security_type, '')) = 'COMMON_STOCK'
              AND upper(COALESCE(primary_exchange, '')) <> 'OTC'
              AND upper(symbol) NOT LIKE '%TEST%'
              AND upper(symbol) NOT LIKE '%-WS'
              AND upper(symbol) NOT LIKE '%-W'
              AND upper(symbol) NOT LIKE '%-R'
              AND upper(symbol) NOT LIKE '%RT'
              AND upper(symbol) NOT LIKE '%-U'
              AND symbol NOT LIKE '%-%'
            """
        )

        # ------------------------------------------------------------------
        # Insert ETF universe membership.
        #
        # Filters:
        # - ACTIVE only (already staged)
        # - ETF only
        # - non-OTC only
        #
        # ETFs are kept separate from common stocks.
        # ------------------------------------------------------------------
        conn.execute(
            """
            INSERT INTO universe_membership_history (
                universe_membership_history_id,
                universe_name,
                instrument_id,
                effective_from,
                effective_to,
                source_name
            )
            SELECT
                1000000000 + listing_status_history_id AS universe_membership_history_id,
                'US_LISTED_ETFS' AS universe_name,
                instrument_id,
                effective_from,
                effective_to,
                'build-universe-membership-history-from-listing-status' AS source_name
            FROM tmp_active_listing_membership_stage
            WHERE upper(COALESCE(security_type, '')) = 'ETF'
              AND upper(COALESCE(primary_exchange, '')) <> 'OTC'
              AND upper(symbol) NOT LIKE '%TEST%'
            """
        )

        # ------------------------------------------------------------------
        # Collect summary metrics for easy audit in logs.
        # ------------------------------------------------------------------
        written_total = conn.execute(
            """
            SELECT COUNT(*)
            FROM universe_membership_history
            WHERE source_name = 'build-universe-membership-history-from-listing-status'
            """
        ).fetchone()[0]

        rows_by_universe = conn.execute(
            """
            SELECT universe_name, COUNT(*)
            FROM universe_membership_history
            WHERE source_name = 'build-universe-membership-history-from-listing-status'
            GROUP BY universe_name
            ORDER BY universe_name
            """
        ).fetchall()

        common_stock_stage_exclusions = conn.execute(
            """
            WITH base AS (
                SELECT *
                FROM tmp_active_listing_membership_stage
                WHERE upper(COALESCE(security_type, '')) = 'COMMON_STOCK'
            )
            SELECT
                exclusion_reason,
                COUNT(*)
            FROM (
                SELECT
                    CASE
                        WHEN upper(COALESCE(primary_exchange, '')) = 'OTC' THEN 'exclude_otc'
                        WHEN upper(symbol) LIKE '%TEST%' THEN 'exclude_test_symbol'
                        WHEN upper(symbol) LIKE '%-WS'
                          OR upper(symbol) LIKE '%-W'
                          OR upper(symbol) LIKE '%-R'
                          OR upper(symbol) LIKE '%RT' THEN 'exclude_warrant_or_right_like'
                        WHEN upper(symbol) LIKE '%-U' THEN 'exclude_unit_like'
                        WHEN symbol LIKE '%-%' THEN 'exclude_dash_series'
                        ELSE 'included_or_other'
                    END AS exclusion_reason
                FROM base
            )
            GROUP BY exclusion_reason
            ORDER BY exclusion_reason
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "build-universe-membership-history-from-listing-status",
                "required_counts": required_counts,
                "written_total": written_total,
                "rows_by_universe": rows_by_universe,
                "common_stock_stage_exclusions": common_stock_stage_exclusions,
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-universe-membership-history-from-listing-status finished")


if __name__ == "__main__":
    run()
