"""
Enrich instrument + symbol_reference_history from explicit manual overrides.

Important:
- only creates missing canonical mapped symbols
- never creates duplicate instruments for the same primary_ticker
- never creates duplicate open-ended symbol_reference_history rows
- may backfill effective_from based on earliest unresolved raw price date
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Apply manual override mappings into canonical identity tables.
    """
    configure_logging()
    LOGGER.info("enrich-symbol-reference-from-manual-overrides started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS tmp_manual_missing_symbols")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_manual_missing_symbols AS
            WITH manual_targets AS (
                SELECT DISTINCT
                    mapped_symbol
                FROM symbol_manual_override_map
            ),
            existing_open AS (
                SELECT DISTINCT
                    symbol
                FROM symbol_reference_history
                WHERE effective_to IS NULL
            )
            SELECT
                mt.mapped_symbol
            FROM manual_targets mt
            LEFT JOIN existing_open eo
                ON eo.symbol = mt.mapped_symbol
            WHERE eo.symbol IS NULL
            """
        )

        # --------------------------------------------------------------
        # Create missing instruments only when no instrument already exists
        # for the target primary_ticker.
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
                    m.mapped_symbol,
                    ROW_NUMBER() OVER (ORDER BY m.mapped_symbol) AS rn
                FROM tmp_manual_missing_symbols m
                LEFT JOIN instrument i
                    ON i.primary_ticker = m.mapped_symbol
                WHERE i.instrument_id IS NULL
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS instrument_id,
                'COMMON_STOCK' AS security_type,
                'MANUAL_' || mapped_symbol AS company_id,
                mapped_symbol AS primary_ticker,
                'UNKNOWN' AS primary_exchange
            FROM staged
            """
        )

        # --------------------------------------------------------------
        # Insert open-ended symbol references for mapped symbols that do not
        # already have an open interval.
        # effective_from is the earliest raw price date that uses this raw
        # symbol family if we can infer it, else current date fallback.
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
            price_min AS (
                SELECT
                    m.mapped_symbol,
                    MIN(r.price_date) AS min_price_date
                FROM symbol_manual_override_map m
                JOIN price_source_daily_raw_stooq r
                    ON r.raw_symbol = m.raw_symbol
                GROUP BY m.mapped_symbol
            ),
            staged AS (
                SELECT
                    i.instrument_id,
                    m.mapped_symbol,
                    COALESCE(pm.min_price_date, DATE '2026-03-30') AS inferred_effective_from,
                    ROW_NUMBER() OVER (ORDER BY m.mapped_symbol) AS rn
                FROM tmp_manual_missing_symbols m
                JOIN instrument i
                    ON i.primary_ticker = m.mapped_symbol
                LEFT JOIN price_min pm
                    ON pm.mapped_symbol = m.mapped_symbol
                LEFT JOIN symbol_reference_history srh
                    ON srh.symbol = m.mapped_symbol
                   AND srh.effective_to IS NULL
                WHERE srh.symbol IS NULL
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS symbol_reference_history_id,
                instrument_id,
                mapped_symbol AS symbol,
                'UNKNOWN' AS exchange,
                TRUE AS is_primary,
                inferred_effective_from AS effective_from,
                NULL AS effective_to
            FROM staged
            """
        )

        added_symbol_count = conn.execute(
            "SELECT COUNT(*) FROM tmp_manual_missing_symbols"
        ).fetchone()[0]
        instrument_count = conn.execute("SELECT COUNT(*) FROM instrument").fetchone()[0]
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
