"""
Build a triage table for unresolved Stooq symbols.

Design:
- SQL-first
- derived only from the current normalized unresolved set
- stable canonical column names
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild symbol_reference_candidates_from_unresolved_stooq.
    """
    configure_logging()
    LOGGER.info("build-symbol-reference-candidates-from-unresolved-stooq started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS symbol_reference_candidates_from_unresolved_stooq")
        conn.execute(
            """
            CREATE TABLE symbol_reference_candidates_from_unresolved_stooq AS
            WITH unresolved AS (
                SELECT
                    raw_symbol,
                    COUNT(*) AS unresolved_row_count,
                    MIN(price_date) AS min_price_date,
                    MAX(price_date) AS max_price_date,
                    MIN(source_row_id) AS first_source_row_id,
                    MAX(source_row_id) AS last_source_row_id,
                    MIN(normalization_notes) AS normalization_notes_example
                FROM price_source_daily_normalized
                WHERE source_name = 'stooq'
                  AND symbol_resolution_status <> 'RESOLVED'
                GROUP BY raw_symbol
            )
            SELECT
                raw_symbol,
                unresolved_row_count,
                min_price_date,
                max_price_date,
                first_source_row_id,
                last_source_row_id,
                CASE
                    WHEN raw_symbol LIKE '%-WS' THEN 'WARRANT_DASH_WS'
                    WHEN raw_symbol LIKE '%-U'  THEN 'UNIT_DASH_U'
                    WHEN POSITION('_' IN raw_symbol) > 0 THEN 'UNDERSCORE_VARIANT'
                    ELSE 'PLAIN_ALNUM'
                END AS candidate_family,
                CASE
                    WHEN raw_symbol LIKE '%-WS' THEN 'REVIEW_FOR_SYMBOL_FORMAT_MAPPING'
                    WHEN raw_symbol LIKE '%-U'  THEN 'REVIEW_FOR_SYMBOL_FORMAT_MAPPING'
                    WHEN POSITION('_' IN raw_symbol) > 0 THEN 'REVIEW_FOR_SYMBOL_FORMAT_MAPPING'
                    WHEN unresolved_row_count >= 1000 THEN 'REVIEW_FOR_REFERENCE_IDENTITY_CREATION_HIGH_PRIORITY'
                    WHEN unresolved_row_count >= 100 THEN 'REVIEW_FOR_REFERENCE_IDENTITY_CREATION'
                    ELSE 'REVIEW_LATER_LOW_VOLUME'
                END AS suggested_action,
                CASE
                    WHEN max_price_date >= CURRENT_DATE - INTERVAL 120 DAY THEN 'RECENT'
                    WHEN max_price_date >= CURRENT_DATE - INTERVAL 365 DAY THEN 'MID'
                    ELSE 'OLD'
                END AS recency_bucket,
                normalization_notes_example,
                CURRENT_TIMESTAMP AS built_at
            FROM unresolved
            ORDER BY unresolved_row_count DESC, raw_symbol
            """
        )

        candidate_row_count = conn.execute(
            "SELECT COUNT(*) FROM symbol_reference_candidates_from_unresolved_stooq"
        ).fetchone()[0]

        rows_by_family = conn.execute(
            """
            SELECT
                candidate_family,
                COUNT(*)
            FROM symbol_reference_candidates_from_unresolved_stooq
            GROUP BY candidate_family
            ORDER BY candidate_family
            """
        ).fetchall()

        rows_by_suggested_action = conn.execute(
            """
            SELECT
                suggested_action,
                COUNT(*)
            FROM symbol_reference_candidates_from_unresolved_stooq
            GROUP BY suggested_action
            ORDER BY COUNT(*) DESC, suggested_action
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "build-symbol-reference-candidates-from-unresolved-stooq",
                "candidate_row_count": candidate_row_count,
                "rows_by_family": rows_by_family,
                "rows_by_suggested_action": rows_by_suggested_action,
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-symbol-reference-candidates-from-unresolved-stooq finished")


if __name__ == "__main__":
    run()
