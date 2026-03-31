"""
Enrich instrument and symbol_reference_history from targeted SEC symbol hits.

Design:
- SQL-first
- only add missing symbols from sec_symbol_company_map_targeted
- do not overwrite existing Nasdaq-derived mappings
- keep this as a simple additive enrichment pass
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    configure_logging()
    LOGGER.info("enrich-symbol-reference-from-sec-targeted started")

    conn = connect_build_db()
    try:
        # --------------------------------------------------------------
        # Stage only SEC symbols that are not already present in the
        # current symbol_reference_history.
        # --------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_sec_missing_symbols")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_sec_missing_symbols AS
            SELECT
                s.symbol,
                s.cik,
                s.company_name,
                COALESCE(s.exchange, 'UNKNOWN') AS exchange_name
            FROM sec_symbol_company_map_targeted AS s
            LEFT JOIN symbol_reference_history AS srh
              ON srh.symbol = s.symbol
            WHERE srh.symbol IS NULL
            """
        )

        # --------------------------------------------------------------
        # Insert missing instruments.
        # --------------------------------------------------------------
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
            staged AS (
                SELECT
                    symbol,
                    cik,
                    company_name,
                    exchange_name,
                    ROW_NUMBER() OVER (ORDER BY symbol) AS rn
                FROM tmp_sec_missing_symbols
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS instrument_id,
                'COMMON_STOCK' AS security_type,
                'SEC_' || COALESCE(cik, symbol) AS company_id,
                symbol AS primary_ticker,
                exchange_name AS primary_exchange
            FROM staged
            """
        )

        # --------------------------------------------------------------
        # Insert missing symbol references.
        # --------------------------------------------------------------
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
                    ROW_NUMBER() OVER (ORDER BY m.symbol) AS rn
                FROM tmp_sec_missing_symbols AS m
                JOIN instrument AS i
                  ON i.primary_ticker = m.symbol
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS symbol_reference_history_id,
                instrument_id,
                symbol,
                exchange_name AS exchange,
                TRUE AS is_primary,
                DATE '2026-03-29' AS effective_from,
                NULL AS effective_to
            FROM staged
            """
        )

        added_symbol_count = conn.execute(
            "SELECT COUNT(*) FROM tmp_sec_missing_symbols"
        ).fetchone()[0]

        instrument_count = conn.execute(
            "SELECT COUNT(*) FROM instrument"
        ).fetchone()[0]

        symbol_reference_count = conn.execute(
            "SELECT COUNT(*) FROM symbol_reference_history"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "enrich-symbol-reference-from-sec-targeted",
                "added_symbol_count": added_symbol_count,
                "instrument_count": instrument_count,
                "symbol_reference_history_count": symbol_reference_count,
            }
        )
    finally:
        conn.close()

    LOGGER.info("enrich-symbol-reference-from-sec-targeted finished")


if __name__ == "__main__":
    run()
