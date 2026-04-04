"""
Repair symbol_reference_history from the high priority unresolved symbol probe.

Why this job exists
-------------------
The earlier version only inserted brand new symbols that did not already exist in the
reference layer.

That is not enough for the current loader state, because some symbols already exist in
symbol_reference_history but with a broken interval such as:

    effective_from = 2026-03-29
    effective_to   = 2026-03-29

Those rows technically exist, so the old "insert only if missing" logic adds nothing,
but price normalization still cannot resolve the historical Stooq rows because the
interval does not cover the historical price dates.

This version repairs the identity layer in a SQL-first way:

1. Take rows from high_priority_unresolved_symbol_probe that are marked
   LIKELY_CREATE_REFERENCE_FROM_SEC.
2. Reuse an existing instrument when primary_ticker already matches the raw symbol.
3. Create a missing instrument only when absolutely necessary.
4. Replace the symbol_reference_history row(s) for these symbols with a repaired
   interval:
      effective_from = min_price_date from the probe
      effective_to   = NULL
5. Preserve a single clean open-ended mapping per symbol.

Important note
--------------
This job intentionally focuses on making the loader DB internally consistent and able to
resolve historical prices. It does not attempt a full PIT-perfect corporate action
reconstruction for these names.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Repair or create symbol reference rows for SEC-backed high-priority unresolved symbols.
    """
    configure_logging()
    LOGGER.info("enrich-symbol-reference-from-high-priority-sec-probe started")

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Stage the exact symbols we want to repair.
        #
        # We keep the staging explicit and small:
        # - only SEC-backed recommendations
        # - one row per raw_symbol
        # - attach SEC exchange/company context when available
        # - attach existing instrument if one already exists
        # ------------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_sec_probe_stage")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_sec_probe_stage AS
            WITH probe AS (
                SELECT
                    raw_symbol,
                    min_price_date,
                    max_price_date,
                    sec_exact_matches,
                    probe_recommendation
                FROM high_priority_unresolved_symbol_probe
                WHERE probe_recommendation = 'LIKELY_CREATE_REFERENCE_FROM_SEC'
            ),
            sec_map AS (
                SELECT
                    symbol,
                    MAX(cik) AS cik,
                    MAX(company_name) AS company_name,
                    MAX(exchange) AS exchange
                FROM sec_symbol_company_map_targeted
                GROUP BY symbol
            ),
            existing_instrument AS (
                SELECT
                    primary_ticker,
                    MIN(instrument_id) AS instrument_id
                FROM instrument
                GROUP BY primary_ticker
            )
            SELECT
                p.raw_symbol,
                p.min_price_date,
                p.max_price_date,
                s.cik,
                s.company_name,
                COALESCE(NULLIF(s.exchange, ''), 'UNKNOWN') AS exchange_name,
                ei.instrument_id AS existing_instrument_id
            FROM probe p
            LEFT JOIN sec_map s
                ON s.symbol = p.raw_symbol
            LEFT JOIN existing_instrument ei
                ON ei.primary_ticker = p.raw_symbol
            """
        )

        staged_probe_row_count = conn.execute(
            "SELECT COUNT(*) FROM tmp_sec_probe_stage"
        ).fetchone()[0]

        # ------------------------------------------------------------------
        # Create missing instruments only when no existing primary_ticker match
        # exists already.
        #
        # This keeps the job conservative and avoids creating duplicate primary
        # tickers when we already have an instrument row.
        # ------------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_sec_probe_missing_instruments")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_sec_probe_missing_instruments AS
            SELECT
                raw_symbol,
                min_price_date,
                max_price_date,
                cik,
                company_name,
                exchange_name
            FROM tmp_sec_probe_stage
            WHERE existing_instrument_id IS NULL
            """
        )

        missing_instrument_count = conn.execute(
            "SELECT COUNT(*) FROM tmp_sec_probe_missing_instruments"
        ).fetchone()[0]

        if missing_instrument_count > 0:
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
                        raw_symbol,
                        exchange_name,
                        cik,
                        ROW_NUMBER() OVER (ORDER BY raw_symbol) AS rn
                    FROM tmp_sec_probe_missing_instruments
                )
                SELECT
                    (SELECT max_id FROM current_max) + rn AS instrument_id,
                    'COMMON_STOCK' AS security_type,
                    CASE
                        WHEN cik IS NOT NULL AND cik <> '' THEN 'SEC_' || cik
                        ELSE 'SEC_' || raw_symbol
                    END AS company_id,
                    raw_symbol AS primary_ticker,
                    exchange_name AS primary_exchange
                FROM staged
                """
            )

        # ------------------------------------------------------------------
        # Refresh the stage with a guaranteed instrument_id for every symbol.
        # ------------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_sec_probe_resolved_stage")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_sec_probe_resolved_stage AS
            SELECT
                s.raw_symbol,
                s.min_price_date,
                s.max_price_date,
                s.cik,
                s.company_name,
                s.exchange_name,
                i.instrument_id
            FROM tmp_sec_probe_stage s
            JOIN instrument i
                ON i.primary_ticker = s.raw_symbol
            """
        )

        # ------------------------------------------------------------------
        # Delete existing symbol_reference_history rows for those exact symbols,
        # then insert one repaired open-ended interval per symbol.
        #
        # This is the key behavioral change versus the broken earlier version:
        # existing-but-bad rows are actively repaired.
        # ------------------------------------------------------------------
        conn.execute(
            """
            DELETE FROM symbol_reference_history
            WHERE symbol IN (
                SELECT raw_symbol
                FROM tmp_sec_probe_resolved_stage
            )
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
                    instrument_id,
                    raw_symbol,
                    exchange_name,
                    min_price_date,
                    ROW_NUMBER() OVER (ORDER BY raw_symbol) AS rn
                FROM tmp_sec_probe_resolved_stage
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS symbol_reference_history_id,
                instrument_id,
                raw_symbol AS symbol,
                exchange_name AS exchange,
                TRUE AS is_primary,
                min_price_date AS effective_from,
                NULL AS effective_to
            FROM staged
            """
        )

        added_symbol_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM tmp_sec_probe_missing_instruments
            """
        ).fetchone()[0]

        repaired_symbol_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM tmp_sec_probe_resolved_stage
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
                symbol,
                instrument_id,
                exchange,
                effective_from,
                effective_to
            FROM symbol_reference_history
            WHERE symbol IN (
                SELECT raw_symbol
                FROM tmp_sec_probe_resolved_stage
            )
            ORDER BY symbol
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
