"""
Enrich symbol_reference_history from the targeted SEC symbol map.

Design:
- SQL-first
- additive only
- current repo canonical schema only
- no duplicate primary_ticker instruments
- no duplicate open-ended symbol rows
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Add missing instruments / symbol references for targeted SEC symbols.

    Important:
    - this creates identities only for symbols absent from the current open
      reference layer and absent from the instrument primary_ticker layer
    - it does not overwrite existing identities
    """
    configure_logging()
    LOGGER.info("enrich-symbol-reference-from-sec-targeted started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS tmp_sec_targeted_missing_symbols")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_sec_targeted_missing_symbols AS
            WITH sec_scope AS (
                SELECT DISTINCT
                    symbol,
                    company_name,
                    exchange,
                    cik
                FROM sec_symbol_company_map_targeted
                WHERE symbol IS NOT NULL
                  AND symbol <> ''
            )
            SELECT
                s.symbol,
                s.company_name,
                s.exchange,
                s.cik
            FROM sec_scope AS s
            LEFT JOIN instrument AS i
              ON i.primary_ticker = s.symbol
            LEFT JOIN symbol_reference_history AS srh
              ON srh.symbol = s.symbol
             AND srh.effective_to IS NULL
            WHERE i.instrument_id IS NULL
              AND srh.symbol IS NULL
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
                    symbol,
                    company_name,
                    exchange,
                    cik,
                    ROW_NUMBER() OVER (ORDER BY symbol) AS rn
                FROM tmp_sec_targeted_missing_symbols
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS instrument_id,
                'COMMON_STOCK' AS security_type,
                'SEC_' || COALESCE(cik, symbol) AS company_id,
                symbol AS primary_ticker,
                COALESCE(exchange, 'UNKNOWN') AS primary_exchange
            FROM staged
            """
        )

        added_rows = conn.execute(
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
                    COALESCE(m.exchange, 'UNKNOWN') AS exchange_name,
                    ROW_NUMBER() OVER (ORDER BY m.symbol) AS rn
                FROM tmp_sec_targeted_missing_symbols AS m
                JOIN instrument AS i
                  ON i.primary_ticker = m.symbol
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS symbol_reference_history_id,
                instrument_id,
                symbol,
                exchange_name,
                TRUE AS is_primary,
                CURRENT_DATE AS effective_from,
                NULL AS effective_to
            FROM staged
            RETURNING symbol
            """
        ).fetchall()

        added_symbol_count = len(added_rows)
        instrument_count = conn.execute("SELECT COUNT(*) FROM instrument").fetchone()[0]
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
