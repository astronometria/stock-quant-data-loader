"""
Build the high-priority unresolved symbol probe.

Current canonical inputs:
- unresolved_symbol_worklist
- symbol_reference_history
- nasdaq_symbol_directory_raw
- sec_symbol_company_map_targeted

Current canonical output:
- high_priority_unresolved_symbol_probe

Important:
- all referenced column names are aligned with the current schema actually
  present in the rebuilt DB
- output column names must remain exactly the canonical current ones
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild high_priority_unresolved_symbol_probe using only current-schema
    inputs and current canonical output columns.
    """
    configure_logging()
    LOGGER.info("build-high-priority-unresolved-symbol-probe started")

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM high_priority_unresolved_symbol_probe")

        # ------------------------------------------------------------------
        # Build a small helper table for the most recent Nasdaq snapshot.
        # We do not assume an external persisted helper table exists.
        # ------------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_latest_nasdaq_snapshot_id")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_latest_nasdaq_snapshot_id AS
            SELECT MAX(snapshot_id) AS latest_snapshot_id
            FROM nasdaq_symbol_directory_raw
            """
        )

        conn.execute(
            """
            INSERT INTO high_priority_unresolved_symbol_probe (
                raw_symbol,
                unresolved_row_count,
                min_price_date,
                max_price_date,
                candidate_family,
                suggested_action,
                recency_bucket,
                exact_current_instrument_id,
                exact_current_symbol,
                exact_current_exchange,
                nearby_reference_matches,
                in_latest_nasdaq_raw,
                nasdaq_exact_matches,
                nasdaq_nearby_matches,
                in_targeted_sec_symbols,
                sec_exact_matches,
                probe_recommendation,
                built_at
            )
            WITH work AS (
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
            exact_ref AS (
                SELECT
                    w.raw_symbol,
                    MAX(srh.instrument_id) AS exact_current_instrument_id,
                    MAX(srh.symbol) AS exact_current_symbol,
                    MAX(srh.exchange) AS exact_current_exchange
                FROM work AS w
                LEFT JOIN symbol_reference_history AS srh
                    ON srh.symbol = w.raw_symbol
                   AND srh.effective_to IS NULL
                GROUP BY w.raw_symbol
            ),
            nearby_ref AS (
                SELECT
                    w.raw_symbol,
                    STRING_AGG(
                        srh.symbol || ' @ ' || COALESCE(srh.exchange, '?') || ' #' || CAST(srh.instrument_id AS VARCHAR),
                        ' | '
                        ORDER BY srh.symbol, srh.instrument_id
                    ) AS nearby_reference_matches
                FROM work AS w
                LEFT JOIN symbol_reference_history AS srh
                    ON srh.effective_to IS NULL
                   AND srh.symbol <> w.raw_symbol
                   AND (
                        srh.symbol LIKE w.raw_symbol || '.%'
                        OR srh.symbol LIKE replace(w.raw_symbol, '-WS', '') || '.W%'
                        OR srh.symbol LIKE replace(w.raw_symbol, '-U', '') || '.U%'
                        OR srh.symbol LIKE replace(w.raw_symbol, '_', '$') || '%'
                   )
                GROUP BY w.raw_symbol
            ),
            latest_nasdaq AS (
                SELECT
                    n.symbol,
                    n.exchange_code,
                    n.security_name
                FROM nasdaq_symbol_directory_raw AS n
                JOIN tmp_latest_nasdaq_snapshot_id AS s
                    ON n.snapshot_id = s.latest_snapshot_id
            ),
            nasdaq_exact AS (
                SELECT
                    w.raw_symbol,
                    MAX(CASE WHEN n.symbol IS NOT NULL THEN 1 ELSE 0 END) AS in_latest_nasdaq_raw,
                    STRING_AGG(
                        n.symbol || ' @ ' || COALESCE(n.exchange_code, '?') || ' / ' || COALESCE(n.security_name, ''),
                        ' | '
                        ORDER BY n.symbol
                    ) FILTER (WHERE n.symbol IS NOT NULL) AS nasdaq_exact_matches
                FROM work AS w
                LEFT JOIN latest_nasdaq AS n
                    ON n.symbol = w.raw_symbol
                GROUP BY w.raw_symbol
            ),
            nasdaq_nearby AS (
                SELECT
                    w.raw_symbol,
                    STRING_AGG(
                        n.symbol || ' @ ' || COALESCE(n.exchange_code, '?') || ' / ' || COALESCE(n.security_name, ''),
                        ' | '
                        ORDER BY n.symbol
                    ) AS nasdaq_nearby_matches
                FROM work AS w
                LEFT JOIN latest_nasdaq AS n
                    ON n.symbol <> w.raw_symbol
                   AND (
                        n.symbol LIKE w.raw_symbol || '.%'
                        OR n.symbol LIKE replace(w.raw_symbol, '-WS', '') || '.W%'
                        OR n.symbol LIKE replace(w.raw_symbol, '-U', '') || '.U%'
                        OR n.symbol LIKE replace(w.raw_symbol, '_', '$') || '%'
                   )
                GROUP BY w.raw_symbol
            ),
            sec_exact AS (
                SELECT
                    w.raw_symbol,
                    MAX(CASE WHEN s.symbol IS NOT NULL THEN 1 ELSE 0 END) AS in_targeted_sec_symbols,
                    STRING_AGG(
                        s.symbol || ' @ CIK ' || COALESCE(s.cik, '') || ' / ' || COALESCE(s.company_name, '') || ' / ' || COALESCE(s.exchange, ''),
                        ' | '
                        ORDER BY s.symbol, s.cik
                    ) FILTER (WHERE s.symbol IS NOT NULL) AS sec_exact_matches
                FROM work AS w
                LEFT JOIN sec_symbol_company_map_targeted AS s
                    ON s.symbol = w.raw_symbol
                GROUP BY w.raw_symbol
            )
            SELECT
                w.raw_symbol,
                w.unresolved_row_count,
                w.min_price_date,
                w.max_price_date,
                w.candidate_family,
                w.suggested_action,
                w.recency_bucket,
                e.exact_current_instrument_id,
                e.exact_current_symbol,
                e.exact_current_exchange,
                COALESCE(r.nearby_reference_matches, '') AS nearby_reference_matches,
                COALESCE(n.in_latest_nasdaq_raw, 0) AS in_latest_nasdaq_raw,
                COALESCE(n.nasdaq_exact_matches, '') AS nasdaq_exact_matches,
                COALESCE(nn.nasdaq_nearby_matches, '') AS nasdaq_nearby_matches,
                COALESCE(s.in_targeted_sec_symbols, 0) AS in_targeted_sec_symbols,
                COALESCE(s.sec_exact_matches, '') AS sec_exact_matches,
                CASE
                    WHEN COALESCE(s.in_targeted_sec_symbols, 0) = 1
                         AND e.exact_current_instrument_id IS NULL
                        THEN 'LIKELY_CREATE_REFERENCE_FROM_SEC'
                    ELSE 'MANUAL_RESEARCH_NEEDED'
                END AS probe_recommendation,
                NOW() AS built_at
            FROM work AS w
            LEFT JOIN exact_ref AS e
                ON e.raw_symbol = w.raw_symbol
            LEFT JOIN nearby_ref AS r
                ON r.raw_symbol = w.raw_symbol
            LEFT JOIN nasdaq_exact AS n
                ON n.raw_symbol = w.raw_symbol
            LEFT JOIN nasdaq_nearby AS nn
                ON nn.raw_symbol = w.raw_symbol
            LEFT JOIN sec_exact AS s
                ON s.raw_symbol = w.raw_symbol
            ORDER BY w.unresolved_row_count DESC, w.raw_symbol
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

        used_targeted_sec_table = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = 'sec_symbol_company_map_targeted'
            """
        ).fetchone()[0] > 0

        print(
            {
                "status": "ok",
                "job": "build-high-priority-unresolved-symbol-probe",
                "probe_row_count": probe_row_count,
                "rows_by_recommendation": rows_by_recommendation,
                "used_targeted_sec_table": used_targeted_sec_table,
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-high-priority-unresolved-symbol-probe finished")


if __name__ == "__main__":
    run()
