"""
Repair / enrich symbol_reference_history from the high-priority SEC probe.

Current canonical input:
- high_priority_unresolved_symbol_probe
- instrument
- symbol_reference_history

Current behavior:
- only processes rows where probe_recommendation = 'LIKELY_CREATE_REFERENCE_FROM_SEC'
- if the instrument already exists in instrument.primary_ticker, repair the
  open-ended symbol_reference_history row to the earliest observed unresolved
  date from the probe
- if the symbol is fully absent from instrument, create the instrument and add
  an open-ended symbol_reference_history row

This job intentionally does NOT touch format-mapping rows.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Apply high-confidence SEC-based reference repairs/creations.
    """
    configure_logging()
    LOGGER.info("enrich-symbol-reference-from-high-priority-sec-probe started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS tmp_sec_probe_rows")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_sec_probe_rows AS
            SELECT
                raw_symbol,
                min_price_date,
                COALESCE(NULLIF(exact_current_exchange, ''), 'UNKNOWN') AS exchange_name
            FROM high_priority_unresolved_symbol_probe
            WHERE probe_recommendation = 'LIKELY_CREATE_REFERENCE_FROM_SEC'
            """
        )

        staged_probe_row_count = conn.execute(
            "SELECT COUNT(*) FROM tmp_sec_probe_rows"
        ).fetchone()[0]

        # --------------------------------------------------------------
        # Create missing instruments first.
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
                    p.raw_symbol,
                    p.exchange_name,
                    ROW_NUMBER() OVER (ORDER BY p.raw_symbol) AS rn
                FROM tmp_sec_probe_rows AS p
                LEFT JOIN instrument AS i
                    ON i.primary_ticker = p.raw_symbol
                WHERE i.instrument_id IS NULL
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS instrument_id,
                'COMMON_STOCK' AS security_type,
                'SEC_PROBE_' || raw_symbol AS company_id,
                raw_symbol AS primary_ticker,
                exchange_name AS primary_exchange
            FROM missing
            """
        )

        # --------------------------------------------------------------
        # Repair existing open-ended rows when the symbol exists but the
        # effective_from is too late for observed price history.
        # --------------------------------------------------------------
        conn.execute(
            """
            UPDATE symbol_reference_history AS srh
            SET
                effective_from = p.min_price_date,
                exchange = p.exchange_name
            FROM tmp_sec_probe_rows AS p
            JOIN instrument AS i
                ON i.primary_ticker = p.raw_symbol
            WHERE srh.instrument_id = i.instrument_id
              AND srh.symbol = p.raw_symbol
              AND srh.effective_to IS NULL
              AND (
                    srh.effective_from > p.min_price_date
                    OR COALESCE(srh.exchange, '') <> COALESCE(p.exchange_name, '')
              )
            """
        )

        # --------------------------------------------------------------
        # Create missing open-ended symbol_reference_history rows.
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
            missing AS (
                SELECT
                    i.instrument_id,
                    p.raw_symbol,
                    p.exchange_name,
                    p.min_price_date,
                    ROW_NUMBER() OVER (ORDER BY p.raw_symbol) AS rn
                FROM tmp_sec_probe_rows AS p
                JOIN instrument AS i
                    ON i.primary_ticker = p.raw_symbol
                LEFT JOIN symbol_reference_history AS srh
                    ON srh.instrument_id = i.instrument_id
                   AND srh.symbol = p.raw_symbol
                   AND srh.effective_to IS NULL
                WHERE srh.symbol IS NULL
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS symbol_reference_history_id,
                instrument_id,
                raw_symbol AS symbol,
                exchange_name AS exchange,
                TRUE AS is_primary,
                min_price_date AS effective_from,
                NULL AS effective_to
            FROM missing
            """
        )

        added_symbol_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM tmp_sec_probe_rows AS p
            JOIN instrument AS i
                ON i.primary_ticker = p.raw_symbol
            LEFT JOIN symbol_reference_history AS srh
                ON srh.instrument_id = i.instrument_id
               AND srh.symbol = p.raw_symbol
               AND srh.effective_to IS NULL
            WHERE srh.symbol IS NOT NULL
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
            FROM symbol_reference_history AS srh
            WHERE srh.symbol IN (
                SELECT raw_symbol
                FROM tmp_sec_probe_rows
            )
              AND srh.effective_to IS NULL
            ORDER BY srh.symbol
            """
        ).fetchall()

        instrument_count = conn.execute(
            "SELECT COUNT(*) FROM instrument"
        ).fetchone()[0]

        symbol_reference_history_count = conn.execute(
            "SELECT COUNT(*) FROM symbol_reference_history"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "enrich-symbol-reference-from-high-priority-sec-probe",
                "staged_probe_row_count": staged_probe_row_count,
                "added_symbol_count": added_symbol_count,
                "repaired_symbol_count": len(repaired_rows),
                "instrument_count": instrument_count,
                "symbol_reference_history_count": symbol_reference_history_count,
                "repaired_rows": repaired_rows,
            }
        )
    finally:
        conn.close()
        LOGGER.info("enrich-symbol-reference-from-high-priority-sec-probe finished")


if __name__ == "__main__":
    run()
