"""
Enrich symbol_reference_history from the targeted SEC symbol table.

This is a targeted identity-enrichment step fed by the unresolved worklist.
It uses the canonical targeted SEC table names only.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Add missing open symbol references using sec_symbol_company_map_targeted.

    Rules:
    - only create rows for symbols currently missing from open refs
    - reuse existing instrument rows if primary_ticker already exists
    - create missing instrument rows when required
    - keep the current canonical schema only
    """
    configure_logging()
    LOGGER.info("enrich-symbol-reference-from-sec-targeted started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS tmp_sec_targeted_missing")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_sec_targeted_missing AS
            SELECT DISTINCT
                sec.symbol,
                COALESCE(NULLIF(sec.exchange, ''), 'UNKNOWN') AS exchange_name,
                MIN(w.min_price_date) AS effective_from
            FROM sec_symbol_company_map_targeted sec
            JOIN unresolved_symbol_worklist w
              ON w.raw_symbol = sec.symbol
            LEFT JOIN symbol_reference_history srh
              ON srh.symbol = sec.symbol
             AND srh.effective_to IS NULL
            WHERE srh.symbol IS NULL
            GROUP BY sec.symbol, COALESCE(NULLIF(sec.exchange, ''), 'UNKNOWN')
            """
        )

        # Create missing instrument rows first.
        conn.execute(
            """
            INSERT INTO instrument (
                instrument_id,
                security_type,
                company_id,
                primary_ticker,
                primary_exchange
            )
            WITH current_max AS (
                SELECT COALESCE(MAX(instrument_id), 0) AS max_id
                FROM instrument
            ),
            missing AS (
                SELECT
                    m.symbol,
                    m.exchange_name,
                    ROW_NUMBER() OVER (ORDER BY m.symbol) AS rn
                FROM tmp_sec_targeted_missing m
                LEFT JOIN instrument i
                  ON i.primary_ticker = m.symbol
                WHERE i.instrument_id IS NULL
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS instrument_id,
                'COMMON_STOCK' AS security_type,
                'SEC_TARGETED_' || symbol AS company_id,
                symbol AS primary_ticker,
                exchange_name AS primary_exchange
            FROM missing
            """
        )

        conn.execute(
            """
            INSERT INTO symbol_reference_history (
                symbol_reference_history_id,
                instrument_id,
                symbol,
                exchange,
                is_primary,
                effective_from,
                effective_to
            )
            WITH current_max AS (
                SELECT COALESCE(MAX(symbol_reference_history_id), 0) AS max_id
                FROM symbol_reference_history
            ),
            staged AS (
                SELECT
                    i.instrument_id,
                    m.symbol,
                    m.exchange_name,
                    m.effective_from,
                    ROW_NUMBER() OVER (ORDER BY m.symbol) AS rn
                FROM tmp_sec_targeted_missing m
                JOIN instrument i
                  ON i.primary_ticker = m.symbol
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS symbol_reference_history_id,
                instrument_id,
                symbol,
                exchange_name,
                TRUE,
                effective_from,
                NULL
            FROM staged
            """
        )

        added_symbol_count = conn.execute(
            "SELECT COUNT(*) FROM tmp_sec_targeted_missing"
        ).fetchone()[0]

        instrument_count = conn.execute(
            "SELECT COUNT(*) FROM instrument"
        ).fetchone()[0]

        symbol_reference_history_count = conn.execute(
            "SELECT COUNT(*) FROM symbol_reference_history"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "enrich-symbol-reference-from-sec-targeted",
                "added_symbol_count": added_symbol_count,
                "instrument_count": instrument_count,
                "symbol_reference_history_count": symbol_reference_history_count,
            }
        )
    finally:
        conn.close()
        LOGGER.info("enrich-symbol-reference-from-sec-targeted finished")


if __name__ == "__main__":
    run()
