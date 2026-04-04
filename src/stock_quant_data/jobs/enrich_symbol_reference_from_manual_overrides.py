"""
Enrich instrument and symbol_reference_history from explicit manual symbol overrides.

Design:
- SQL-first
- conservative
- deduplicate manual targets before inserting anything
- never create a second open-ended reference for the same symbol
- keep Python very thin so the logic stays auditable in SQL

Important production rule:
- manual overrides are an additive fallback layer
- they must never duplicate an existing primary_ticker in instrument
- they must never duplicate an already-open symbol in symbol_reference_history
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild the manual additive identity layer safely.

    SQL-first flow:
    1. collect distinct mapped symbols from symbol_manual_override_map
    2. keep only symbols that are still missing from the current open-ended reference layer
    3. create at most one instrument per missing mapped symbol
    4. create at most one open-ended symbol reference per missing mapped symbol
    """
    configure_logging()
    LOGGER.info("enrich-symbol-reference-from-manual-overrides started")

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Stage ONE row per mapped_symbol.
        #
        # The old version staged (mapped_symbol, raw_symbol), which let
        # several raw aliases of the same mapped symbol produce duplicate
        # inserts for the same target identity. That is exactly what created
        # the AHH duplication.
        # ------------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_manual_missing_symbols")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_manual_missing_symbols AS
            WITH distinct_manual_targets AS (
                SELECT
                    m.mapped_symbol,
                    MIN(m.raw_symbol) AS sample_raw_symbol
                FROM symbol_manual_override_map AS m
                WHERE m.mapped_symbol IS NOT NULL
                  AND m.mapped_symbol <> ''
                GROUP BY m.mapped_symbol
            )
            SELECT
                d.mapped_symbol,
                d.sample_raw_symbol,
                'UNKNOWN' AS exchange_name
            FROM distinct_manual_targets AS d
            WHERE NOT EXISTS (
                SELECT 1
                FROM symbol_reference_history AS srh
                WHERE srh.symbol = d.mapped_symbol
                  AND srh.effective_to IS NULL
            )
            """
        )

        # ------------------------------------------------------------------
        # Insert only truly missing instruments.
        #
        # We still guard against an existing primary_ticker match even though
        # the rebuild path should already be clean, because this makes the
        # job safer to re-run on a partially-built database.
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
        # Insert only truly missing open-ended symbol references.
        #
        # This second NOT EXISTS guard is intentional. Even if the instrument
        # already exists, we still do not want a duplicate open-ended symbol.
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
                    ROW_NUMBER() OVER (ORDER BY t.mapped_symbol, i.instrument_id) AS rn
                FROM tmp_manual_missing_symbols AS t
                JOIN instrument AS i
                  ON i.primary_ticker = t.mapped_symbol
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM symbol_reference_history AS srh
                    WHERE srh.symbol = t.mapped_symbol
                      AND srh.effective_to IS NULL
                )
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

        duplicate_open_ended_symbol_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT symbol
                FROM symbol_reference_history
                WHERE effective_to IS NULL
                GROUP BY symbol
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "enrich-symbol-reference-from-manual-overrides",
                "added_symbol_count": added_symbol_count,
                "instrument_count": instrument_count,
                "symbol_reference_history_count": symbol_reference_count,
                "duplicate_open_ended_symbol_count": duplicate_open_ended_symbol_count,
            }
        )
    finally:
        conn.close()

    LOGGER.info("enrich-symbol-reference-from-manual-overrides finished")


if __name__ == "__main__":
    run()
