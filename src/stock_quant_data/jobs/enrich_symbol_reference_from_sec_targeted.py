"""
Enrich symbol_reference_history from the targeted SEC symbol map.

Current canonical inputs:
- sec_symbol_company_map_targeted
- instrument
- symbol_reference_history

Current canonical outputs modified:
- instrument
- symbol_reference_history

Important:
- this job adds or repairs only current open-ended symbol identities
- it does not create duplicate instrument.primary_ticker rows
- it does not create duplicate open-ended symbol_reference_history rows
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Enrich current symbol reference layer from targeted SEC symbol rows.
    """
    configure_logging()
    LOGGER.info("enrich-symbol-reference-from-sec-targeted started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS tmp_sec_targeted_symbols")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_sec_targeted_symbols AS
            SELECT DISTINCT
                symbol,
                CASE
                    WHEN upper(COALESCE(exchange, '')) = 'NASDAQ' THEN 'NASDAQ'
                    WHEN upper(COALESCE(exchange, '')) = 'NYSE' THEN 'NYSE'
                    WHEN upper(COALESCE(exchange, '')) = 'NYSEARCA' THEN 'NYSEARCA'
                    ELSE COALESCE(exchange, 'UNKNOWN')
                END AS exchange_name
            FROM sec_symbol_company_map_targeted
            WHERE symbol IS NOT NULL
              AND symbol <> ''
            """
        )

        # Insert missing instruments.
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
                    t.symbol,
                    t.exchange_name,
                    ROW_NUMBER() OVER (ORDER BY t.symbol) AS rn
                FROM tmp_sec_targeted_symbols AS t
                LEFT JOIN instrument AS i
                    ON i.primary_ticker = t.symbol
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

        # Insert missing open-ended symbol refs.
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
            missing AS (
                SELECT
                    i.instrument_id,
                    t.symbol,
                    t.exchange_name,
                    ROW_NUMBER() OVER (ORDER BY t.symbol) AS rn
                FROM tmp_sec_targeted_symbols AS t
                JOIN instrument AS i
                    ON i.primary_ticker = t.symbol
                LEFT JOIN symbol_reference_history AS srh
                    ON srh.instrument_id = i.instrument_id
                   AND srh.symbol = t.symbol
                   AND srh.effective_to IS NULL
                WHERE srh.symbol IS NULL
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS symbol_reference_history_id,
                instrument_id,
                symbol,
                exchange_name AS exchange,
                TRUE AS is_primary,
                DATE '2026-03-31' AS effective_from,
                NULL AS effective_to
            FROM missing
            """
        )

        added_symbol_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM tmp_sec_targeted_symbols AS t
            LEFT JOIN symbol_reference_history AS srh
                ON srh.symbol = t.symbol
               AND srh.effective_to IS NULL
            WHERE srh.symbol IS NOT NULL
            """
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
