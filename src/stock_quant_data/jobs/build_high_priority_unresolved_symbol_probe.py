"""
Build a richer probe table for the high-priority unresolved symbol worklist.

Design:
- SQL-first
- use only real current table/column names
- no speculative writes to master tables
- produce an analyst-facing triage table
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild high_priority_unresolved_symbol_probe.
    """
    configure_logging()
    LOGGER.info("build-high-priority-unresolved-symbol-probe started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS high_priority_unresolved_symbol_probe")
        conn.execute(
            """
            CREATE TABLE high_priority_unresolved_symbol_probe AS
            WITH hp AS (
                SELECT
                    raw_symbol,
                    unresolved_row_count,
                    min_price_date,
                    max_price_date,
                    candidate_family,
                    suggested_action,
                    recency_bucket
                FROM unresolved_symbol_worklist
                WHERE suggested_action = 'REVIEW_FOR_REFERENCE_IDENTITY_CREATION_HIGH_PRIORITY'
            ),
            open_ref AS (
                SELECT
                    symbol,
                    instrument_id,
                    exchange
                FROM symbol_reference_history
                WHERE effective_to IS NULL
            ),
            sec_targeted AS (
                SELECT
                    symbol,
                    cik,
                    company_name,
                    exchange
                FROM sec_symbol_company_map_targeted
            ),
            nasdaq_latest AS (
                SELECT DISTINCT
                    symbol
                FROM nasdaq_symbol_directory_raw
            )
            SELECT
                hp.raw_symbol,
                hp.unresolved_row_count,
                hp.min_price_date,
                hp.max_price_date,
                hp.candidate_family,
                hp.suggested_action,
                hp.recency_bucket,
                ref_exact.instrument_id AS exact_current_instrument_id,
                ref_exact.symbol AS exact_current_symbol,
                ref_exact.exchange AS exact_current_exchange,
                COALESCE(
                    (
                        SELECT string_agg(
                            ref2.symbol || ' @ ' || COALESCE(ref2.exchange, '?') || ' #' || CAST(ref2.instrument_id AS VARCHAR),
                            ' | '
                            ORDER BY ref2.symbol, ref2.instrument_id
                        )
                        FROM open_ref AS ref2
                        WHERE ref2.symbol LIKE hp.raw_symbol || '%'
                          AND ref2.symbol <> hp.raw_symbol
                    ),
                    ''
                ) AS nearby_reference_matches,
                CASE WHEN nasdaq.symbol IS NOT NULL THEN 1 ELSE 0 END AS in_latest_nasdaq_raw,
                '' AS nasdaq_exact_matches,
                '' AS nasdaq_nearby_matches,
                CASE WHEN sec.symbol IS NOT NULL THEN 1 ELSE 0 END AS in_targeted_sec_symbols,
                COALESCE(
                    sec.symbol || ' @ CIK ' || sec.cik || ' / ' || sec.company_name || ' / ' || sec.exchange,
                    ''
                ) AS sec_exact_matches,
                CASE
                    WHEN sec.symbol IS NOT NULL THEN 'LIKELY_CREATE_REFERENCE_FROM_SEC'
                    ELSE 'MANUAL_RESEARCH_NEEDED'
                END AS probe_recommendation,
                CURRENT_TIMESTAMP AS built_at
            FROM hp
            LEFT JOIN open_ref AS ref_exact
              ON ref_exact.symbol = hp.raw_symbol
            LEFT JOIN sec_targeted AS sec
              ON sec.symbol = hp.raw_symbol
            LEFT JOIN nasdaq_latest AS nasdaq
              ON nasdaq.symbol = hp.raw_symbol
            ORDER BY hp.unresolved_row_count DESC, hp.raw_symbol
            """
        )

        probe_row_count = conn.execute(
            "SELECT COUNT(*) FROM high_priority_unresolved_symbol_probe"
        ).fetchone()[0]

        rows_by_recommendation = conn.execute(
            """
            SELECT
                probe_recommendation,
                COUNT(*)
            FROM high_priority_unresolved_symbol_probe
            GROUP BY probe_recommendation
            ORDER BY COUNT(*) DESC, probe_recommendation
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "build-high-priority-unresolved-symbol-probe",
                "probe_row_count": probe_row_count,
                "rows_by_recommendation": rows_by_recommendation,
                "used_targeted_sec_table": True,
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-high-priority-unresolved-symbol-probe finished")


if __name__ == "__main__":
    run()
