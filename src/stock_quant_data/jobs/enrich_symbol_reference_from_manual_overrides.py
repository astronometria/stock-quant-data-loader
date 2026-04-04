"""
Enrich instrument and symbol_reference_history from explicit manual overrides.

Current canonical inputs:
- symbol_manual_override_map
- instrument
- symbol_reference_history

Current canonical outputs modified:
- instrument
- symbol_reference_history

Important:
- this job only uses current real column names
- it must never insert duplicate instrument.primary_ticker rows
- it must never insert duplicate open-ended symbol_reference_history rows
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Create missing current identities from the explicit manual override map.
    """
    configure_logging()
    LOGGER.info("enrich-symbol-reference-from-manual-overrides started")

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Stage only mapped symbols that are still absent from current open
        # symbol_reference_history rows.
        # ------------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_manual_missing_symbols")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_manual_missing_symbols AS
            SELECT DISTINCT
                m.mapped_symbol,
                'UNKNOWN' AS exchange_name
            FROM symbol_manual_override_map AS m
            LEFT JOIN symbol_reference_history AS srh
                ON srh.symbol = m.mapped_symbol
               AND srh.effective_to IS NULL
            WHERE srh.symbol IS NULL
            """
        )

        # ------------------------------------------------------------------
        # Insert missing instruments first.
        # Reuse existing instrument rows when primary_ticker already exists.
        # ------------------------------------------------------------------
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
                    t.mapped_symbol,
                    t.exchange_name,
                    ROW_NUMBER() OVER (ORDER BY t.mapped_symbol) AS rn
                FROM tmp_manual_missing_symbols AS t
                LEFT JOIN instrument AS i
                    ON i.primary_ticker = t.mapped_symbol
                WHERE i.instrument_id IS NULL
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

        # ------------------------------------------------------------------
        # Insert missing open-ended symbol_reference_history rows.
        # ------------------------------------------------------------------
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
                    t.mapped_symbol,
                    t.exchange_name,
                    ROW_NUMBER() OVER (ORDER BY t.mapped_symbol) AS rn
                FROM tmp_manual_missing_symbols AS t
                JOIN instrument AS i
                    ON i.primary_ticker = t.mapped_symbol
                LEFT JOIN symbol_reference_history AS srh
                    ON srh.instrument_id = i.instrument_id
                   AND srh.symbol = t.mapped_symbol
                   AND srh.effective_to IS NULL
                WHERE srh.symbol IS NULL
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

        symbol_reference_history_count = conn.execute(
            "SELECT COUNT(*) FROM symbol_reference_history"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "enrich-symbol-reference-from-manual-overrides",
                "added_symbol_count": added_symbol_count,
                "instrument_count": instrument_count,
                "symbol_reference_history_count": symbol_reference_history_count,
            }
        )
    finally:
        conn.close()
        LOGGER.info("enrich-symbol-reference-from-manual-overrides finished")


if __name__ == "__main__":
    run()
