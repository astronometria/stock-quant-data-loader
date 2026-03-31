"""
Build an enriched probe table for the highest-priority unresolved Stooq symbols.

Why this job exists:
- The normalization pipeline is already in a good state.
- We do not want to keep adding broad normalization rules.
- We now want a compact, review-friendly probe table for the top unresolved symbols.

What this job does:
- takes the current unresolved Stooq backlog table
- keeps only HIGH_PRIORITY candidates
- enriches them with nearby matches already present in symbol_reference_history
- enriches them with current/raw Nasdaq symbol directory presence
- enriches them with targeted SEC identity presence
- produces one SQL-first review table

Important:
- this is a probe / analyst-review table
- it does NOT mutate the main reference tables
- it does NOT create new instruments
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Build the high-priority unresolved symbol probe table.

    Output table:
    - high_priority_unresolved_symbol_probe

    The goal is to centralize the evidence needed before deciding:
    - create a new reference identity
    - attach to an existing identity
    - leave unresolved for now
    """
    configure_logging()
    LOGGER.info("build-high-priority-unresolved-symbol-probe started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS high_priority_unresolved_symbol_probe")

        conn.execute(
            """
            CREATE TABLE high_priority_unresolved_symbol_probe AS
            WITH priority_candidates AS (
                SELECT
                    raw_symbol,
                    unresolved_row_count,
                    min_price_date,
                    max_price_date,
                    candidate_family,
                    suggested_action,
                    recency_bucket
                FROM symbol_reference_candidates_from_unresolved_stooq
                WHERE suggested_action = 'REVIEW_FOR_REFERENCE_IDENTITY_CREATION_HIGH_PRIORITY'
            ),

            current_exact_reference AS (
                SELECT
                    pc.raw_symbol,
                    srh.instrument_id AS exact_current_instrument_id,
                    srh.symbol AS exact_current_symbol,
                    srh.exchange AS exact_current_exchange
                FROM priority_candidates AS pc
                LEFT JOIN symbol_reference_history AS srh
                  ON srh.symbol = pc.raw_symbol
                 AND srh.effective_to IS NULL
            ),

            nearby_reference AS (
                SELECT
                    pc.raw_symbol,
                    string_agg(
                        DISTINCT (
                            srh.symbol
                            || ' @ '
                            || COALESCE(srh.exchange, 'UNKNOWN')
                            || ' #'
                            || CAST(srh.instrument_id AS VARCHAR)
                        ),
                        ' | '
                        ORDER BY (
                            srh.symbol
                            || ' @ '
                            || COALESCE(srh.exchange, 'UNKNOWN')
                            || ' #'
                            || CAST(srh.instrument_id AS VARCHAR)
                        )
                    ) FILTER (WHERE srh.symbol IS NOT NULL) AS nearby_reference_matches
                FROM priority_candidates AS pc
                LEFT JOIN symbol_reference_history AS srh
                  ON srh.symbol LIKE (
                        CASE
                            WHEN POSITION('-' IN pc.raw_symbol) > 0 THEN split_part(pc.raw_symbol, '-', 1) || '%'
                            WHEN POSITION('_' IN pc.raw_symbol) > 0 THEN split_part(pc.raw_symbol, '_', 1) || '%'
                            ELSE pc.raw_symbol || '%'
                        END
                     )
                GROUP BY pc.raw_symbol
            ),

            latest_nasdaq_snapshot AS (
                SELECT snapshot_id
                FROM nasdaq_symbol_directory_raw
                ORDER BY snapshot_id DESC
                LIMIT 1
            ),

            nasdaq_exact AS (
                SELECT
                    pc.raw_symbol,
                    MAX(
                        CASE
                            WHEN nsd.symbol IS NOT NULL THEN 1
                            ELSE 0
                        END
                    ) AS in_latest_nasdaq_raw,
                    string_agg(
                        DISTINCT (
                            nsd.symbol
                            || ' @ '
                            || COALESCE(nsd.exchange_code, 'UNKNOWN')
                            || ' / '
                            || COALESCE(nsd.security_name, '')
                        ),
                        ' | '
                        ORDER BY (
                            nsd.symbol
                            || ' @ '
                            || COALESCE(nsd.exchange_code, 'UNKNOWN')
                            || ' / '
                            || COALESCE(nsd.security_name, '')
                        )
                    ) FILTER (WHERE nsd.symbol IS NOT NULL) AS nasdaq_exact_matches
                FROM priority_candidates AS pc
                CROSS JOIN latest_nasdaq_snapshot AS lns
                LEFT JOIN nasdaq_symbol_directory_raw AS nsd
                  ON nsd.snapshot_id = lns.snapshot_id
                 AND nsd.symbol = pc.raw_symbol
                GROUP BY pc.raw_symbol
            ),

            nasdaq_nearby AS (
                SELECT
                    pc.raw_symbol,
                    string_agg(
                        DISTINCT (
                            nsd.symbol
                            || ' @ '
                            || COALESCE(nsd.exchange_code, 'UNKNOWN')
                            || ' / '
                            || COALESCE(nsd.security_name, '')
                        ),
                        ' | '
                        ORDER BY (
                            nsd.symbol
                            || ' @ '
                            || COALESCE(nsd.exchange_code, 'UNKNOWN')
                            || ' / '
                            || COALESCE(nsd.security_name, '')
                        )
                    ) FILTER (WHERE nsd.symbol IS NOT NULL) AS nasdaq_nearby_matches
                FROM priority_candidates AS pc
                CROSS JOIN latest_nasdaq_snapshot AS lns
                LEFT JOIN nasdaq_symbol_directory_raw AS nsd
                  ON nsd.snapshot_id = lns.snapshot_id
                 AND nsd.symbol LIKE (
                        CASE
                            WHEN POSITION('-' IN pc.raw_symbol) > 0 THEN split_part(pc.raw_symbol, '-', 1) || '%'
                            WHEN POSITION('_' IN pc.raw_symbol) > 0 THEN split_part(pc.raw_symbol, '_', 1) || '%'
                            ELSE pc.raw_symbol || '%'
                        END
                     )
                GROUP BY pc.raw_symbol
            ),

            sec_exact AS (
                SELECT
                    pc.raw_symbol,
                    MAX(
                        CASE
                            WHEN ss.symbol IS NOT NULL THEN 1
                            ELSE 0
                        END
                    ) AS in_targeted_sec_symbols,
                    string_agg(
                        DISTINCT (
                            ss.symbol
                            || ' @ CIK '
                            || COALESCE(ss.cik, '')
                            || ' / '
                            || COALESCE(ss.company_name, '')
                            || ' / '
                            || COALESCE(ss.exchange, 'UNKNOWN')
                        ),
                        ' | '
                        ORDER BY (
                            ss.symbol
                            || ' @ CIK '
                            || COALESCE(ss.cik, '')
                            || ' / '
                            || COALESCE(ss.company_name, '')
                            || ' / '
                            || COALESCE(ss.exchange, 'UNKNOWN')
                        )
                    ) FILTER (WHERE ss.symbol IS NOT NULL) AS sec_exact_matches
                FROM priority_candidates AS pc
                LEFT JOIN sec_symbol_company_map_targeted AS ss
                  ON ss.symbol = pc.raw_symbol
                GROUP BY pc.raw_symbol
            )

            SELECT
                pc.raw_symbol,
                pc.unresolved_row_count,
                pc.min_price_date,
                pc.max_price_date,
                pc.candidate_family,
                pc.suggested_action,
                pc.recency_bucket,

                cer.exact_current_instrument_id,
                cer.exact_current_symbol,
                cer.exact_current_exchange,

                COALESCE(nr.nearby_reference_matches, '') AS nearby_reference_matches,

                COALESCE(ne.in_latest_nasdaq_raw, 0) AS in_latest_nasdaq_raw,
                COALESCE(ne.nasdaq_exact_matches, '') AS nasdaq_exact_matches,
                COALESCE(nn.nasdaq_nearby_matches, '') AS nasdaq_nearby_matches,

                COALESCE(se.in_targeted_sec_symbols, 0) AS in_targeted_sec_symbols,
                COALESCE(se.sec_exact_matches, '') AS sec_exact_matches,

                CASE
                    WHEN cer.exact_current_instrument_id IS NOT NULL
                        THEN 'CHECK_WHY_DIRECT_MATCH_STILL_UNRESOLVED'
                    WHEN COALESCE(ne.in_latest_nasdaq_raw, 0) = 1
                        THEN 'LIKELY_CREATE_REFERENCE_FROM_NASDAQ'
                    WHEN COALESCE(se.in_targeted_sec_symbols, 0) = 1
                        THEN 'LIKELY_CREATE_REFERENCE_FROM_SEC'
                    WHEN COALESCE(nr.nearby_reference_matches, '') <> ''
                        THEN 'REVIEW_NEARBY_REFERENCE_MATCHES'
                    ELSE 'MANUAL_RESEARCH_NEEDED'
                END AS probe_recommendation,

                CURRENT_TIMESTAMP AS built_at
            FROM priority_candidates AS pc
            LEFT JOIN current_exact_reference AS cer
              ON cer.raw_symbol = pc.raw_symbol
            LEFT JOIN nearby_reference AS nr
              ON nr.raw_symbol = pc.raw_symbol
            LEFT JOIN nasdaq_exact AS ne
              ON ne.raw_symbol = pc.raw_symbol
            LEFT JOIN nasdaq_nearby AS nn
              ON nn.raw_symbol = pc.raw_symbol
            LEFT JOIN sec_exact AS se
              ON se.raw_symbol = pc.raw_symbol
            ORDER BY
                pc.unresolved_row_count DESC,
                pc.raw_symbol
            """
        )

        row_count = conn.execute(
            "SELECT COUNT(*) FROM high_priority_unresolved_symbol_probe"
        ).fetchone()[0]

        by_recommendation = conn.execute(
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
                "probe_row_count": row_count,
                "rows_by_recommendation": by_recommendation,
            }
        )
    finally:
        conn.close()

    LOGGER.info("build-high-priority-unresolved-symbol-probe finished")


if __name__ == "__main__":
    run()
