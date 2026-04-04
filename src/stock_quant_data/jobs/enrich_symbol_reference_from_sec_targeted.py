"""
Enrich symbol reference state from targeted SEC symbol/company matches.

Intent:
- convert targeted SEC symbol evidence into canonical instruments/open refs
- repair existing open refs by extending effective_from backward when needed
- do not create duplicates
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Apply targeted SEC symbol mappings into canonical master-data tables.
    """
    configure_logging()
    LOGGER.info("enrich-symbol-reference-from-sec-targeted started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS tmp_targeted_sec_symbols")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_targeted_sec_symbols AS
            SELECT DISTINCT
                upper(trim(symbol)) AS symbol,
                cik,
                company_name,
                exchange
            FROM sec_symbol_company_map_targeted
            WHERE symbol IS NOT NULL
              AND trim(symbol) <> ''
            """
        )

        # --------------------------------------------------------------
        # Create any missing instruments for targeted SEC symbols.
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
            missing AS (
                SELECT
                    t.symbol,
                    t.cik,
                    t.exchange,
                    ROW_NUMBER() OVER (ORDER BY t.symbol) AS rn
                FROM tmp_targeted_sec_symbols t
                LEFT JOIN instrument i
                    ON i.primary_ticker = t.symbol
                WHERE i.instrument_id IS NULL
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS instrument_id,
                'COMMON_STOCK' AS security_type,
                'SEC_' || COALESCE(cik, symbol) AS company_id,
                symbol AS primary_ticker,
                COALESCE(exchange, 'UNKNOWN') AS primary_exchange
            FROM missing
            """
        )

        # --------------------------------------------------------------
        # Back-extend existing open symbol refs when SEC-targeted symbols are
        # already present as current refs but started too recently.
        # --------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_targeted_price_min")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_targeted_price_min AS
            SELECT
                raw_symbol AS symbol,
                MIN(price_date) AS min_price_date
            FROM price_source_daily_raw_stooq
            WHERE raw_symbol IN (SELECT symbol FROM tmp_targeted_sec_symbols)
            GROUP BY raw_symbol
            """
        )

        conn.execute(
            """
            UPDATE symbol_reference_history AS srh
            SET effective_from = LEAST(srh.effective_from, tpm.min_price_date)
            FROM tmp_targeted_price_min tpm
            WHERE srh.symbol = tpm.symbol
              AND srh.effective_to IS NULL
              AND tpm.min_price_date IS NOT NULL
            """
        )

        # --------------------------------------------------------------
        # Insert new open refs for targeted symbols still missing after the
        # repair step above.
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
                    t.symbol,
                    COALESCE(t.exchange, i.primary_exchange, 'UNKNOWN') AS exchange_name,
                    COALESCE(p.min_price_date, DATE '2026-03-30') AS effective_from,
                    ROW_NUMBER() OVER (ORDER BY t.symbol) AS rn
                FROM tmp_targeted_sec_symbols t
                JOIN instrument i
                    ON i.primary_ticker = t.symbol
                LEFT JOIN tmp_targeted_price_min p
                    ON p.symbol = t.symbol
                LEFT JOIN symbol_reference_history srh
                    ON srh.symbol = t.symbol
                   AND srh.effective_to IS NULL
                WHERE srh.symbol IS NULL
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS symbol_reference_history_id,
                instrument_id,
                symbol,
                exchange_name,
                TRUE AS is_primary,
                effective_from,
                NULL AS effective_to
            FROM staged
            """
        )

        added_symbol_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM tmp_targeted_sec_symbols t
            LEFT JOIN v_symbol_reference_history_open_intervals v
                ON v.symbol = t.symbol
            WHERE v.symbol IS NULL
            """
        ).fetchone()[0]

        repaired_rows = conn.execute(
            """
            SELECT
                srh.symbol,
                srh.instrument_id,
                srh.exchange,
                srh.effective_from,
                srh.effective_to
            FROM symbol_reference_history srh
            WHERE srh.symbol IN (SELECT symbol FROM tmp_targeted_sec_symbols)
              AND srh.effective_to IS NULL
            ORDER BY srh.symbol
            """
        ).fetchall()

        instrument_count = conn.execute("SELECT COUNT(*) FROM instrument").fetchone()[0]
        symbol_reference_count = conn.execute(
            "SELECT COUNT(*) FROM symbol_reference_history"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "enrich-symbol-reference-from-sec-targeted",
                "added_symbol_count": added_symbol_count,
                "repaired_symbol_count": len(repaired_rows),
                "instrument_count": instrument_count,
                "symbol_reference_history_count": symbol_reference_count,
                "repaired_rows": repaired_rows,
            }
        )
    finally:
        conn.close()
        LOGGER.info("enrich-symbol-reference-from-sec-targeted finished")


if __name__ == "__main__":
    run()
