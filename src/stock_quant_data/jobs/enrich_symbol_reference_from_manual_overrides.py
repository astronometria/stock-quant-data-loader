"""
Enrich instrument and symbol_reference_history from explicit manual symbol overrides.

Design:
- SQL-first
- only add missing mapped symbols not already present
- keep this layer additive and explicit
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    configure_logging()
    LOGGER.info("enrich-symbol-reference-from-manual-overrides started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS tmp_manual_missing_symbols")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_manual_missing_symbols AS
            SELECT
                mapped_symbol,
                raw_symbol,
                'UNKNOWN' AS exchange_name
            FROM symbol_manual_override_map m
            LEFT JOIN symbol_reference_history srh
              ON srh.symbol = m.mapped_symbol
            WHERE srh.symbol IS NULL
            """
        )

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
                    mapped_symbol,
                    exchange_name,
                    ROW_NUMBER() OVER (ORDER BY mapped_symbol) AS rn
                FROM tmp_manual_missing_symbols
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS instrument_id,
                'COMMON_STOCK' AS security_type,
                'MANUAL_' || mapped_symbol AS company_id,
                mapped_symbol AS primary_ticker,
                exchange_name AS primary_exchange
            FROM staged
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
                    m.mapped_symbol,
                    'UNKNOWN' AS exchange_name,
                    ROW_NUMBER() OVER (ORDER BY m.mapped_symbol) AS rn
                FROM tmp_manual_missing_symbols m
                JOIN instrument i
                  ON i.primary_ticker = m.mapped_symbol
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS symbol_reference_history_id,
                instrument_id,
                mapped_symbol AS symbol,
                exchange_name AS exchange,
                TRUE AS is_primary,
                DATE '2026-03-30' AS effective_from,
                NULL AS effective_to
            FROM staged
            """
        )

        added_symbol_count = conn.execute(
            "SELECT COUNT(*) FROM tmp_manual_missing_symbols"
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
                "job": "enrich-symbol-reference-from-manual-overrides",
                "added_symbol_count": added_symbol_count,
                "instrument_count": instrument_count,
                "symbol_reference_history_count": symbol_reference_count,
            }
        )
    finally:
        conn.close()

    LOGGER.info("enrich-symbol-reference-from-manual-overrides finished")


if __name__ == "__main__":
    run()
