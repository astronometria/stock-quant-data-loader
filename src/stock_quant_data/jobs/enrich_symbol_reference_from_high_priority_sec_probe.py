"""
Repair / enrich open-ended symbol references from SEC-backed high-priority probe
rows.

Canonical source:
- high_priority_unresolved_symbol_probe

Canonical target tables:
- instrument
- symbol_reference_history

Important:
- only rows with probe_recommendation = 'LIKELY_CREATE_REFERENCE_FROM_SEC'
- repair existing closed rows when possible
- otherwise create missing open-ended row
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Promote SEC-likely probe rows into current open-ended reference rows.
    """
    configure_logging()
    LOGGER.info("enrich-symbol-reference-from-high-priority-sec-probe started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS tmp_hp_sec_probe")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_hp_sec_probe AS
            SELECT
                raw_symbol AS symbol,
                min_price_date AS effective_from,
                CASE
                    WHEN sec_exact_matches LIKE '% / Nasdaq%' THEN 'Nasdaq'
                    WHEN sec_exact_matches LIKE '% / NASDAQ%' THEN 'NASDAQ'
                    WHEN sec_exact_matches LIKE '% / NYSE%' THEN 'NYSE'
                    ELSE 'UNKNOWN'
                END AS exchange_name
            FROM high_priority_unresolved_symbol_probe
            WHERE probe_recommendation = 'LIKELY_CREATE_REFERENCE_FROM_SEC'
            """
        )

        staged_probe_row_count = conn.execute(
            "SELECT COUNT(*) FROM tmp_hp_sec_probe"
        ).fetchone()[0]

        # First, reopen matching existing same-symbol rows when present.
        conn.execute(
            """
            UPDATE symbol_reference_history AS srh
            SET
                effective_from = LEAST(srh.effective_from, t.effective_from),
                effective_to = NULL,
                exchange = CASE
                    WHEN srh.exchange IS NULL OR srh.exchange = '' OR srh.exchange = 'UNKNOWN'
                        THEN t.exchange_name
                    ELSE srh.exchange
                END
            FROM tmp_hp_sec_probe AS t
            WHERE srh.symbol = t.symbol
              AND srh.effective_to IS NOT NULL
              AND NOT EXISTS (
                    SELECT 1
                    FROM symbol_reference_history AS open_srh
                    WHERE open_srh.symbol = t.symbol
                      AND open_srh.effective_to IS NULL
              )
            """
        )

        # Then create missing instruments only if still fully absent.
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
                FROM tmp_hp_sec_probe AS t
                LEFT JOIN instrument AS i
                    ON i.primary_ticker = t.symbol
                WHERE i.instrument_id IS NULL
            )
            SELECT
                (SELECT max_id FROM current_max) + rn,
                'COMMON_STOCK',
                'SEC_PROBE_' || symbol,
                symbol,
                exchange_name
            FROM missing
            """
        )

        # Finally insert any remaining missing open-ended symbol refs.
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
                    t.effective_from,
                    ROW_NUMBER() OVER (ORDER BY t.symbol) AS rn
                FROM tmp_hp_sec_probe AS t
                JOIN instrument AS i
                    ON i.primary_ticker = t.symbol
                LEFT JOIN symbol_reference_history AS srh
                    ON srh.symbol = t.symbol
                   AND srh.effective_to IS NULL
                WHERE srh.symbol IS NULL
            )
            SELECT
                (SELECT max_id FROM current_max) + rn,
                instrument_id,
                symbol,
                exchange_name,
                TRUE,
                effective_from,
                NULL
            FROM missing
            """
        )

        repaired_rows = conn.execute(
            """
            SELECT
                srh.symbol,
                srh.instrument_id,
                srh.exchange,
                srh.effective_from,
                srh.effective_to
            FROM symbol_reference_history AS srh
            JOIN tmp_hp_sec_probe AS t
                ON t.symbol = srh.symbol
            WHERE srh.effective_to IS NULL
            ORDER BY srh.symbol
            """
        ).fetchall()

        repaired_symbol_count = len(repaired_rows)

        added_symbol_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM tmp_hp_sec_probe AS t
            LEFT JOIN symbol_reference_history AS srh
                ON srh.symbol = t.symbol
               AND srh.effective_to IS NULL
            WHERE srh.symbol IS NULL
            """
        ).fetchone()[0]

        instrument_count = conn.execute("SELECT COUNT(*) FROM instrument").fetchone()[0]
        symbol_reference_history_count = conn.execute(
            "SELECT COUNT(*) FROM symbol_reference_history"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "enrich-symbol-reference-from-high-priority-sec-probe",
                "staged_probe_row_count": staged_probe_row_count,
                "added_symbol_count": added_symbol_count,
                "repaired_symbol_count": repaired_symbol_count,
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
