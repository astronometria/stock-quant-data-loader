"""
Repair symbol_reference_history directly from the
high_priority_unresolved_symbol_probe table when the probe has already
identified exact SEC-backed symbols that should exist as open references.

Important design choice:
- this job repairs / back-extends symbol_reference_history
- it does not create alternate legacy tables
- it does not create backup rows
- it is idempotent for the same probe result set
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Upsert/repair symbol_reference_history rows using the SEC-backed probe.

    For each probe row with probe_recommendation = 'LIKELY_CREATE_REFERENCE_FROM_SEC':
    - if an open row already exists for the symbol, back-extend effective_from
      to the earliest observed unresolved price date
    - otherwise create a new open row using the already-existing instrument row
      when present
    """
    configure_logging()
    LOGGER.info("enrich-symbol-reference-from-high-priority-sec-probe started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS tmp_sec_probe_repairs")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_sec_probe_repairs AS
            SELECT
                raw_symbol AS symbol,
                min_price_date AS effective_from,
                COALESCE(exact_current_instrument_id, i.instrument_id) AS instrument_id,
                COALESCE(NULLIF(exact_current_exchange, ''), i.primary_exchange, 'UNKNOWN') AS exchange_name
            FROM high_priority_unresolved_symbol_probe p
            LEFT JOIN instrument i
                ON i.primary_ticker = p.raw_symbol
            WHERE p.probe_recommendation = 'LIKELY_CREATE_REFERENCE_FROM_SEC'
            """
        )

        staged_probe_row_count = conn.execute(
            "SELECT COUNT(*) FROM tmp_sec_probe_repairs"
        ).fetchone()[0]

        # --------------------------------------------------------------
        # Repair existing open rows by extending the effective_from date
        # backward when the probe proves the symbol should resolve earlier.
        # --------------------------------------------------------------
        conn.execute(
            """
            UPDATE symbol_reference_history AS srh
            SET effective_from = LEAST(srh.effective_from, repair.effective_from),
                exchange = COALESCE(NULLIF(srh.exchange, ''), repair.exchange_name)
            FROM tmp_sec_probe_repairs AS repair
            WHERE srh.symbol = repair.symbol
              AND srh.effective_to IS NULL
            """
        )

        repaired_symbol_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM symbol_reference_history srh
            JOIN tmp_sec_probe_repairs repair
              ON srh.symbol = repair.symbol
             AND srh.effective_to IS NULL
            """
        ).fetchone()[0]

        # --------------------------------------------------------------
        # Insert only genuinely missing open rows.
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
                    repair.instrument_id,
                    repair.symbol,
                    repair.exchange_name,
                    repair.effective_from,
                    ROW_NUMBER() OVER (ORDER BY repair.symbol) AS rn
                FROM tmp_sec_probe_repairs repair
                LEFT JOIN symbol_reference_history srh
                    ON srh.symbol = repair.symbol
                   AND srh.effective_to IS NULL
                WHERE srh.symbol IS NULL
                  AND repair.instrument_id IS NOT NULL
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS symbol_reference_history_id,
                instrument_id,
                symbol,
                exchange_name,
                TRUE,
                effective_from,
                NULL
            FROM missing
            """
        )

        added_symbol_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM tmp_sec_probe_repairs repair
            LEFT JOIN symbol_reference_history srh
              ON srh.symbol = repair.symbol
             AND srh.effective_to IS NULL
            WHERE srh.symbol IS NULL
            """
        ).fetchone()[0]

        instrument_count = conn.execute(
            "SELECT COUNT(*) FROM instrument"
        ).fetchone()[0]

        symbol_reference_history_count = conn.execute(
            "SELECT COUNT(*) FROM symbol_reference_history"
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
            JOIN tmp_sec_probe_repairs repair
              ON srh.symbol = repair.symbol
             AND srh.effective_to IS NULL
            ORDER BY srh.symbol
            """
        ).fetchall()

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
