"""
Repair high-priority unresolved symbols using current probe output plus the SEC-targeted
symbol table.

Design:
- SQL-first
- additive/repair-only
- no duplicate open-ended rows
- do not create speculative identities
- use the CURRENT real schema of high_priority_unresolved_symbol_probe:
    raw_symbol
    unresolved_row_count
    ...
    sec_exact_matches
    probe_recommendation
    built_at

Important:
- this job repairs symbol_reference_history for existing instruments where the
  correct instrument already exists in the reference layer / SEC-targeted layer
- it does not attempt broad historical reconstruction
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Repair open-ended symbol_reference_history rows for the SEC-likely probe set.

    Current intended scope:
    - BXMX
    - DIAX
    - BBU
    - RILYK

    Repair rule:
    - if a matching instrument already exists by primary_ticker, reuse it
    - close any currently open conflicting row for that symbol
    - insert one clean open-ended row starting from the earliest unresolved
      price date observed in the probe
    """
    configure_logging()
    LOGGER.info("enrich-symbol-reference-from-high-priority-sec-probe started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS tmp_high_priority_sec_probe_repairs")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_high_priority_sec_probe_repairs AS
            WITH probe_scope AS (
                SELECT
                    raw_symbol,
                    min_price_date,
                    max_price_date,
                    probe_recommendation
                FROM high_priority_unresolved_symbol_probe
                WHERE probe_recommendation = 'LIKELY_CREATE_REFERENCE_FROM_SEC'
            ),
            existing_instrument AS (
                SELECT
                    p.raw_symbol,
                    p.min_price_date,
                    p.max_price_date,
                    i.instrument_id,
                    i.primary_exchange
                FROM probe_scope AS p
                JOIN instrument AS i
                  ON i.primary_ticker = p.raw_symbol
            )
            SELECT
                raw_symbol,
                min_price_date,
                max_price_date,
                instrument_id,
                primary_exchange AS exchange_name
            FROM existing_instrument
            """
        )

        staged_probe_row_count = conn.execute(
            "SELECT COUNT(*) FROM tmp_high_priority_sec_probe_repairs"
        ).fetchone()[0]

        # ------------------------------------------------------------------
        # Close conflicting open-ended rows for the repaired symbols.
        # ------------------------------------------------------------------
        conn.execute(
            """
            UPDATE symbol_reference_history AS srh
            SET effective_to = r.min_price_date - INTERVAL 1 DAY
            FROM tmp_high_priority_sec_probe_repairs AS r
            WHERE srh.symbol = r.raw_symbol
              AND srh.effective_to IS NULL
              AND (
                    srh.instrument_id <> r.instrument_id
                 OR COALESCE(srh.exchange, '') <> COALESCE(r.exchange_name, '')
              )
            """
        )

        # ------------------------------------------------------------------
        # Insert the repaired open-ended row only if the exact open row does not
        # already exist.
        # ------------------------------------------------------------------
        repaired_rows = conn.execute(
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
                    r.raw_symbol,
                    r.instrument_id,
                    r.exchange_name,
                    r.min_price_date,
                    ROW_NUMBER() OVER (ORDER BY r.raw_symbol) AS rn
                FROM tmp_high_priority_sec_probe_repairs AS r
                LEFT JOIN symbol_reference_history AS srh
                  ON srh.symbol = r.raw_symbol
                 AND srh.instrument_id = r.instrument_id
                 AND COALESCE(srh.exchange, '') = COALESCE(r.exchange_name, '')
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
            RETURNING symbol, instrument_id, exchange, effective_from, effective_to
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
                "added_symbol_count": 0,
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
