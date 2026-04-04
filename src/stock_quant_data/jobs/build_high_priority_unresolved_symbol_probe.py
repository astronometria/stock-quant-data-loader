"""
Build the high-priority unresolved symbol probe.

This probe enriches the current high-priority worklist with nearby matches
from:
- symbol_reference_history
- nasdaq_symbol_directory_raw
- sec_symbol_company_map_targeted

The output table is canonical and its current columns are:
- raw_symbol
- unresolved_row_count
- min_price_date
- max_price_date
- candidate_family
- suggested_action
- recency_bucket
- exact_current_instrument_id
- exact_current_symbol
- exact_current_exchange
- nearby_reference_matches
- in_latest_nasdaq_raw
- nasdaq_exact_matches
- nasdaq_nearby_matches
- in_targeted_sec_symbols
- sec_exact_matches
- probe_recommendation
- built_at
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Build the current high-priority probe table.

    Recommendation rules:
    - SEC exact hit for a high-priority plain symbol -> LIKELY_CREATE_REFERENCE_FROM_SEC
    - otherwise -> MANUAL_RESEARCH_NEEDED
    """
    configure_logging()
    LOGGER.info("build-high-priority-unresolved-symbol-probe started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS high_priority_unresolved_symbol_probe")
        conn.execute(
            """
            CREATE TABLE high_priority_unresolved_symbol_probe AS
            WITH latest_snapshot_id AS (
                SELECT MAX(snapshot_id) AS snapshot_id
                FROM nasdaq_symbol_directory_raw
            ),
            high_priority AS (
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
            exact_open_ref AS (
                SELECT
                    h.raw_symbol,
                    srh.instrument_id AS exact_current_instrument_id,
                    srh.symbol AS exact_current_symbol,
                    srh.exchange AS exact_current_exchange
                FROM high_priority h
                LEFT JOIN symbol_reference_history srh
                  ON srh.symbol = h.raw_symbol
                 AND srh.effective_to IS NULL
            ),
            nearby_ref AS (
                SELECT
                    h.raw_symbol,
                    COALESCE(
                        string_agg(
                            DISTINCT (
                                srh.symbol || ' @ ' || COALESCE(srh.exchange, 'UNKNOWN') || ' #' || CAST(srh.instrument_id AS VARCHAR)
                            ),
                            ' | '
                            ORDER BY (
                                srh.symbol || ' @ ' || COALESCE(srh.exchange, 'UNKNOWN') || ' #' || CAST(srh.instrument_id AS VARCHAR)
                            )
                        ),
                        ''
                    ) AS nearby_reference_matches
                FROM high_priority h
                LEFT JOIN symbol_reference_history srh
                  ON srh.effective_to IS NULL
                 AND (
                        srh.symbol LIKE h.raw_symbol || '%'
                     OR h.raw_symbol LIKE srh.symbol || '%'
                 )
                GROUP BY h.raw_symbol
            ),
            latest_nasdaq AS (
                SELECT
                    r.symbol,
                    r.exchange_code,
                    r.security_name
                FROM nasdaq_symbol_directory_raw r
                JOIN latest_snapshot_id s
                  ON r.snapshot_id = s.snapshot_id
            ),
            nasdaq_probe AS (
                SELECT
                    h.raw_symbol,
                    MAX(CASE WHEN n.symbol = h.raw_symbol THEN 1 ELSE 0 END) AS in_latest_nasdaq_raw,
                    COALESCE(
                        string_agg(
                            DISTINCT CASE
                                WHEN n.symbol = h.raw_symbol
                                THEN n.symbol || ' @ ' || COALESCE(n.exchange_code, '') || ' / ' || COALESCE(n.security_name, '')
                                ELSE NULL
                            END,
                            ' | '
                            ORDER BY CASE
                                WHEN n.symbol = h.raw_symbol
                                THEN n.symbol || ' @ ' || COALESCE(n.exchange_code, '') || ' / ' || COALESCE(n.security_name, '')
                                ELSE NULL
                            END
                        ),
                        ''
                    ) AS nasdaq_exact_matches,
                    COALESCE(
                        string_agg(
                            DISTINCT CASE
                                WHEN n.symbol <> h.raw_symbol
                                 AND (
                                        n.symbol LIKE h.raw_symbol || '%'
                                     OR h.raw_symbol LIKE n.symbol || '%'
                                 )
                                THEN n.symbol || ' @ ' || COALESCE(n.exchange_code, '') || ' / ' || COALESCE(n.security_name, '')
                                ELSE NULL
                            END,
                            ' | '
                            ORDER BY CASE
                                WHEN n.symbol <> h.raw_symbol
                                 AND (
                                        n.symbol LIKE h.raw_symbol || '%'
                                     OR h.raw_symbol LIKE n.symbol || '%'
                                 )
                                THEN n.symbol || ' @ ' || COALESCE(n.exchange_code, '') || ' / ' || COALESCE(n.security_name, '')
                                ELSE NULL
                            END
                        ),
                        ''
                    ) AS nasdaq_nearby_matches
                FROM high_priority h
                LEFT JOIN latest_nasdaq n
                  ON (
                        n.symbol = h.raw_symbol
                     OR n.symbol LIKE h.raw_symbol || '%'
                     OR h.raw_symbol LIKE n.symbol || '%'
                  )
                GROUP BY h.raw_symbol
            ),
            sec_probe AS (
                SELECT
                    h.raw_symbol,
                    MAX(CASE WHEN s.symbol = h.raw_symbol THEN 1 ELSE 0 END) AS in_targeted_sec_symbols,
                    COALESCE(
                        string_agg(
                            DISTINCT CASE
                                WHEN s.symbol = h.raw_symbol
                                THEN s.symbol
                                     || ' @ CIK '
                                     || COALESCE(s.cik, '')
                                     || ' / '
                                     || COALESCE(s.company_name, '')
                                     || ' / '
                                     || COALESCE(s.exchange, '')
                                ELSE NULL
                            END,
                            ' | '
                            ORDER BY CASE
                                WHEN s.symbol = h.raw_symbol
                                THEN s.symbol
                                     || ' @ CIK '
                                     || COALESCE(s.cik, '')
                                     || ' / '
                                     || COALESCE(s.company_name, '')
                                     || ' / '
                                     || COALESCE(s.exchange, '')
                                ELSE NULL
                            END
                        ),
                        ''
                    ) AS sec_exact_matches
                FROM high_priority h
                LEFT JOIN sec_symbol_company_map_targeted s
                  ON s.symbol = h.raw_symbol
                GROUP BY h.raw_symbol
            )
            SELECT
                h.raw_symbol,
                h.unresolved_row_count,
                h.min_price_date,
                h.max_price_date,
                h.candidate_family,
                h.suggested_action,
                h.recency_bucket,
                e.exact_current_instrument_id,
                e.exact_current_symbol,
                e.exact_current_exchange,
                COALESCE(r.nearby_reference_matches, '') AS nearby_reference_matches,
                COALESCE(n.in_latest_nasdaq_raw, 0) AS in_latest_nasdaq_raw,
                COALESCE(n.nasdaq_exact_matches, '') AS nasdaq_exact_matches,
                COALESCE(n.nasdaq_nearby_matches, '') AS nasdaq_nearby_matches,
                COALESCE(s.in_targeted_sec_symbols, 0) AS in_targeted_sec_symbols,
                COALESCE(s.sec_exact_matches, '') AS sec_exact_matches,
                CASE
                    WHEN COALESCE(s.in_targeted_sec_symbols, 0) = 1
                        THEN 'LIKELY_CREATE_REFERENCE_FROM_SEC'
                    ELSE 'MANUAL_RESEARCH_NEEDED'
                END AS probe_recommendation,
                CURRENT_TIMESTAMP AS built_at
            FROM high_priority h
            LEFT JOIN exact_open_ref e
              ON e.raw_symbol = h.raw_symbol
            LEFT JOIN nearby_ref r
              ON r.raw_symbol = h.raw_symbol
            LEFT JOIN nasdaq_probe n
              ON n.raw_symbol = h.raw_symbol
            LEFT JOIN sec_probe s
              ON s.raw_symbol = h.raw_symbol
            ORDER BY h.unresolved_row_count DESC, h.raw_symbol
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
            SELECT CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'main'
                      AND table_name = 'sec_symbol_company_map_targeted'
                )
                THEN TRUE
                ELSE FALSE
            END
            """
        ).fetchone()[0]

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
