"""
Build the high-priority unresolved symbol probe.

Canonical target:
- high_priority_unresolved_symbol_probe

This is a diagnostic table, not a source-of-truth table.
Its purpose is to summarize evidence for the hardest current unresolved names.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild the high-priority probe table from current worklist state.
    """
    configure_logging()
    LOGGER.info("build-high-priority-unresolved-symbol-probe started")

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM high_priority_unresolved_symbol_probe")

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
            WITH hp AS (
                SELECT *
                FROM unresolved_symbol_worklist
                WHERE suggested_action = 'REVIEW_FOR_REFERENCE_IDENTITY_CREATION_HIGH_PRIORITY'
            ),
            exact_ref AS (
                SELECT
                    srh.symbol,
                    srh.instrument_id,
                    srh.exchange
                FROM symbol_reference_history AS srh
                WHERE srh.effective_to IS NULL
            ),
            sec_exact AS (
                SELECT
                    symbol,
                    string_agg(
                        symbol || ' @ CIK ' || cik || ' / ' || company_name || ' / ' || COALESCE(exchange, ''),
                        ' | ' ORDER BY cik
                    ) AS sec_exact_matches
                FROM sec_symbol_company_map_targeted
                GROUP BY symbol
            ),
            nasdaq_exact AS (
                SELECT
                    symbol,
                    string_agg(
                        symbol || ' @ ' || COALESCE(exchange_code, '') || ' / ' || COALESCE(security_name, ''),
                        ' | ' ORDER BY symbol
                    ) AS nasdaq_exact_matches,
                    1 AS in_latest_nasdaq_raw
                FROM nasdaq_symbol_directory_raw
                GROUP BY symbol
            ),
            nearby_ref AS (
                SELECT
                    hp.raw_symbol,
                    string_agg(
                        srh.symbol || ' @ ' || COALESCE(srh.exchange, '') || ' #' || CAST(srh.instrument_id AS VARCHAR),
                        ' | ' ORDER BY srh.symbol
                    ) AS nearby_reference_matches
                FROM hp
                LEFT JOIN symbol_reference_history AS srh
                    ON srh.effective_to IS NULL
                   AND (
                        srh.symbol LIKE hp.raw_symbol || '%'
                        OR hp.raw_symbol LIKE srh.symbol || '%'
                   )
                GROUP BY hp.raw_symbol
            ),
            nasdaq_nearby AS (
                SELECT
                    hp.raw_symbol,
                    string_agg(
                        n.symbol || ' @ ' || COALESCE(n.exchange_code, '') || ' / ' || COALESCE(n.security_name, ''),
                        ' | ' ORDER BY n.symbol
                    ) AS nasdaq_nearby_matches
                FROM hp
                LEFT JOIN nasdaq_symbol_directory_raw AS n
                    ON (
                        n.symbol LIKE hp.raw_symbol || '%'
                        OR hp.raw_symbol LIKE n.symbol || '%'
                    )
                GROUP BY hp.raw_symbol
            )
            SELECT
                hp.raw_symbol,
                hp.unresolved_row_count,
                hp.min_price_date,
                hp.max_price_date,
                hp.candidate_family,
                hp.suggested_action,
                hp.recency_bucket,
                er.instrument_id AS exact_current_instrument_id,
                er.symbol AS exact_current_symbol,
                er.exchange AS exact_current_exchange,
                nr.nearby_reference_matches,
                COALESCE(ne.in_latest_nasdaq_raw, 0) AS in_latest_nasdaq_raw,
                COALESCE(ne.nasdaq_exact_matches, '') AS nasdaq_exact_matches,
                COALESCE(nn.nasdaq_nearby_matches, '') AS nasdaq_nearby_matches,
                CASE WHEN se.symbol IS NOT NULL THEN 1 ELSE 0 END AS in_targeted_sec_symbols,
                COALESCE(se.sec_exact_matches, '') AS sec_exact_matches,
                CASE
                    WHEN se.symbol IS NOT NULL THEN 'LIKELY_CREATE_REFERENCE_FROM_SEC'
                    ELSE 'MANUAL_RESEARCH_NEEDED'
                END AS probe_recommendation,
                CURRENT_TIMESTAMP
            FROM hp
            LEFT JOIN exact_ref AS er
                ON er.symbol = hp.raw_symbol
            LEFT JOIN nearby_ref AS nr
                ON nr.raw_symbol = hp.raw_symbol
            LEFT JOIN nasdaq_exact AS ne
                ON ne.symbol = hp.raw_symbol
            LEFT JOIN nasdaq_nearby AS nn
                ON nn.raw_symbol = hp.raw_symbol
            LEFT JOIN sec_exact AS se
                ON se.symbol = hp.raw_symbol
            ORDER BY hp.unresolved_row_count DESC, hp.raw_symbol
            """
        )

        probe_row_count = conn.execute(
            "SELECT COUNT(*) FROM high_priority_unresolved_symbol_probe"
        ).fetchone()[0]

        rows_by_recommendation = conn.execute(
            """
            SELECT probe_recommendation, COUNT(*)
            FROM high_priority_unresolved_symbol_probe
            GROUP BY probe_recommendation
            ORDER BY COUNT(*) DESC, probe_recommendation
            """
        ).fetchall()

        used_targeted_sec_table = conn.execute(
            """
            SELECT COUNT(*) > 0
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = 'sec_symbol_company_map_targeted'
            """
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "build-high-priority-unresolved-symbol-probe",
                "probe_row_count": probe_row_count,
                "rows_by_recommendation": rows_by_recommendation,
                "used_targeted_sec_table": bool(used_targeted_sec_table),
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-high-priority-unresolved-symbol-probe finished")


if __name__ == "__main__":
    run()
