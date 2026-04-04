"""
Build a high-priority unresolved symbol probe.

This probe enriches the worklist with nearby evidence from:
- current symbol_reference_history open intervals
- latest Nasdaq raw snapshot
- targeted SEC symbol map

Output contract matches the current DB schema exactly.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Materialize a focused probe table for high-priority unresolved symbols.
    """
    configure_logging()
    LOGGER.info("build-high-priority-unresolved-symbol-probe started")

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM high_priority_unresolved_symbol_probe")

        conn.execute("DROP TABLE IF EXISTS tmp_latest_snapshot")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_latest_snapshot AS
            SELECT MAX(snapshot_id) AS snapshot_id
            FROM nasdaq_symbol_directory_raw
            """
        )

        conn.execute("DROP TABLE IF EXISTS tmp_high_priority")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_high_priority AS
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
            WITH exact_current_ref AS (
                SELECT
                    symbol,
                    MIN(instrument_id) AS instrument_id,
                    MIN(exchange) AS exchange_name
                FROM symbol_reference_history
                WHERE effective_to IS NULL
                GROUP BY symbol
            ),
            nearby_ref AS (
                SELECT
                    hp.raw_symbol,
                    string_agg(
                        srh.symbol || ' @ ' || COALESCE(srh.exchange, 'UNKNOWN') || ' #' || CAST(srh.instrument_id AS VARCHAR),
                        ' | '
                        ORDER BY srh.symbol, srh.instrument_id
                    ) AS nearby_reference_matches
                FROM tmp_high_priority hp
                LEFT JOIN symbol_reference_history srh
                    ON srh.effective_to IS NULL
                   AND (
                        srh.symbol = hp.raw_symbol
                        OR srh.symbol LIKE hp.raw_symbol || '.%'
                        OR srh.symbol LIKE regexp_replace(hp.raw_symbol, '[-_]', '.') || '%'
                   )
                GROUP BY hp.raw_symbol
            ),
            latest_nasdaq AS (
                SELECT
                    n.symbol,
                    n.exchange_code,
                    n.security_name
                FROM nasdaq_symbol_directory_raw n
                JOIN tmp_latest_snapshot ls
                    ON n.snapshot_id = ls.snapshot_id
            ),
            nasdaq_exact AS (
                SELECT
                    hp.raw_symbol,
                    COUNT(n.symbol) AS exact_count,
                    string_agg(
                        n.symbol || ' @ ' || COALESCE(n.exchange_code, '?') || ' / ' || COALESCE(n.security_name, ''),
                        ' | '
                        ORDER BY n.symbol
                    ) AS exact_matches
                FROM tmp_high_priority hp
                LEFT JOIN latest_nasdaq n
                    ON n.symbol = hp.raw_symbol
                GROUP BY hp.raw_symbol
            ),
            nasdaq_nearby AS (
                SELECT
                    hp.raw_symbol,
                    string_agg(
                        n.symbol || ' @ ' || COALESCE(n.exchange_code, '?') || ' / ' || COALESCE(n.security_name, ''),
                        ' | '
                        ORDER BY n.symbol
                    ) AS nearby_matches
                FROM tmp_high_priority hp
                LEFT JOIN latest_nasdaq n
                    ON n.symbol LIKE hp.raw_symbol || '%'
                   AND n.symbol <> hp.raw_symbol
                GROUP BY hp.raw_symbol
            ),
            sec_exact AS (
                SELECT
                    hp.raw_symbol,
                    COUNT(sec.symbol) AS exact_count,
                    string_agg(
                        sec.symbol || ' @ CIK ' || COALESCE(sec.cik, '') || ' / ' || COALESCE(sec.company_name, '') || ' / ' || COALESCE(sec.exchange, ''),
                        ' | '
                        ORDER BY sec.symbol
                    ) AS exact_matches
                FROM tmp_high_priority hp
                LEFT JOIN sec_symbol_company_map_targeted sec
                    ON sec.symbol = hp.raw_symbol
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
                ecr.instrument_id AS exact_current_instrument_id,
                ecr.symbol AS exact_current_symbol,
                ecr.exchange_name AS exact_current_exchange,
                COALESCE(nr.nearby_reference_matches, '') AS nearby_reference_matches,
                CASE WHEN COALESCE(ne.exact_count, 0) > 0 THEN 1 ELSE 0 END AS in_latest_nasdaq_raw,
                COALESCE(ne.exact_matches, '') AS nasdaq_exact_matches,
                COALESCE(nn.nearby_matches, '') AS nasdaq_nearby_matches,
                CASE WHEN COALESCE(se.exact_count, 0) > 0 THEN 1 ELSE 0 END AS in_targeted_sec_symbols,
                COALESCE(se.exact_matches, '') AS sec_exact_matches,
                CASE
                    WHEN COALESCE(se.exact_count, 0) > 0 THEN 'LIKELY_CREATE_REFERENCE_FROM_SEC'
                    ELSE 'MANUAL_RESEARCH_NEEDED'
                END AS probe_recommendation,
                CURRENT_TIMESTAMP
            FROM tmp_high_priority hp
            LEFT JOIN exact_current_ref ecr
                ON ecr.symbol = hp.raw_symbol
            LEFT JOIN nearby_ref nr
                ON nr.raw_symbol = hp.raw_symbol
            LEFT JOIN nasdaq_exact ne
                ON ne.raw_symbol = hp.raw_symbol
            LEFT JOIN nasdaq_nearby nn
                ON nn.raw_symbol = hp.raw_symbol
            LEFT JOIN sec_exact se
                ON se.raw_symbol = hp.raw_symbol
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
