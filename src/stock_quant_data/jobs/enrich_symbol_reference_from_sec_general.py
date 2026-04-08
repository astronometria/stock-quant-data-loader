"""
Enrich symbol_reference_history from the general SEC symbol map.

Purpose:
- add missing open symbol references for symbols already present in sec_symbol_company_map
- create missing instrument rows when required
- keep the implementation SQL-first
- remain idempotent for repeated runs

Important:
- this is the bulk/general SEC enrichment step
- it complements the existing Nasdaq-based identity layer
- it should run before more manual/probe-driven repair steps
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """Add missing open symbol references using sec_symbol_company_map."""
    configure_logging()
    LOGGER.info("enrich-symbol-reference-from-sec-general started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS tmp_sec_general_missing")

        conn.execute(
            """
            CREATE TEMP TABLE tmp_sec_general_missing AS
            WITH unresolved_symbols AS (
                SELECT DISTINCT
                    upper(replace(replace(trim(raw_symbol), '.US', ''), '_', '-')) AS normalized_probe_symbol,
                    MIN(price_date) AS effective_from
                FROM price_source_daily_normalized
                WHERE symbol_resolution_status <> 'RESOLVED'
                GROUP BY 1
            ),
            sec_base AS (
                SELECT DISTINCT
                    upper(trim(symbol)) AS symbol,
                    COALESCE(NULLIF(trim(exchange), ''), 'UNKNOWN') AS exchange_name
                FROM sec_symbol_company_map
                WHERE symbol IS NOT NULL
                  AND trim(symbol) <> ''
            )
            SELECT
                s.symbol,
                s.exchange_name,
                u.effective_from
            FROM unresolved_symbols u
            JOIN sec_base s
              ON s.symbol = u.normalized_probe_symbol
            LEFT JOIN symbol_reference_history srh
              ON upper(trim(srh.symbol)) = s.symbol
             AND srh.effective_to IS NULL
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
            missing AS (
                SELECT
                    m.symbol,
                    m.exchange_name,
                    ROW_NUMBER() OVER (ORDER BY m.symbol) AS rn
                FROM tmp_sec_general_missing m
                LEFT JOIN instrument i
                  ON upper(trim(i.primary_ticker)) = m.symbol
                WHERE i.instrument_id IS NULL
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS instrument_id,
                'COMMON_STOCK' AS security_type,
                'SEC_GENERAL_' || symbol AS company_id,
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
                FROM tmp_sec_general_missing m
                JOIN instrument i
                  ON upper(trim(i.primary_ticker)) = m.symbol
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
            "SELECT COUNT(*) FROM tmp_sec_general_missing"
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
                "job": "enrich-symbol-reference-from-sec-general",
                "added_symbol_count": added_symbol_count,
                "instrument_count": instrument_count,
                "symbol_reference_history_count": symbol_reference_history_count,
            }
        )
    finally:
        conn.close()
        LOGGER.info("enrich-symbol-reference-from-sec-general finished")


if __name__ == "__main__":
    run()
