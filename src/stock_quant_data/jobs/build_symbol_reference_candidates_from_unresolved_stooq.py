"""
Build candidate rows from unresolved normalized Stooq prices.

Canonical target:
- symbol_reference_candidates_from_unresolved_stooq
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild the unresolved symbol candidate table from current normalized prices.
    """
    configure_logging()
    LOGGER.info("build-symbol-reference-candidates-from-unresolved-stooq started")

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM symbol_reference_candidates_from_unresolved_stooq")

        conn.execute(
            """
            INSERT INTO symbol_reference_candidates_from_unresolved_stooq (
                raw_symbol,
                unresolved_row_count,
                min_price_date,
                max_price_date,
                first_source_row_id,
                last_source_row_id,
                candidate_family,
                suggested_action,
                recency_bucket,
                normalization_notes_example,
                built_at
            )
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
            ),
            classified AS (
                SELECT
                    u.*,
                    CASE
                        WHEN raw_symbol LIKE '%-WS' THEN 'WARRANT_DASH_WS'
                        WHEN raw_symbol LIKE '%-U' THEN 'UNIT_DASH_U'
                        WHEN raw_symbol LIKE '%\_%' ESCAPE '\' THEN 'UNDERSCORE_VARIANT'
                        ELSE 'PLAIN_ALNUM'
                    END AS candidate_family,
                    CASE
                        WHEN max_price_date >= DATE '2025-12-01' THEN 'RECENT'
                        WHEN max_price_date >= DATE '2025-01-01' THEN 'MID'
                        ELSE 'OLD'
                    END AS recency_bucket
                FROM unresolved AS u
            )
            SELECT
                raw_symbol,
                unresolved_row_count,
                min_price_date,
                max_price_date,
                first_source_row_id,
                last_source_row_id,
                candidate_family,
                CASE
                    WHEN candidate_family IN ('WARRANT_DASH_WS', 'UNIT_DASH_U', 'UNDERSCORE_VARIANT')
                        THEN 'REVIEW_FOR_SYMBOL_FORMAT_MAPPING'
                    WHEN unresolved_row_count >= 1000
                        THEN 'REVIEW_FOR_REFERENCE_IDENTITY_CREATION_HIGH_PRIORITY'
                    WHEN unresolved_row_count >= 100
                        THEN 'REVIEW_FOR_REFERENCE_IDENTITY_CREATION'
                    ELSE 'REVIEW_LATER_LOW_VOLUME'
                END AS suggested_action,
                recency_bucket,
                normalization_notes_example,
                CURRENT_TIMESTAMP
            FROM classified
            ORDER BY unresolved_row_count DESC, raw_symbol
            """
        )

        candidate_row_count = conn.execute(
            "SELECT COUNT(*) FROM symbol_reference_candidates_from_unresolved_stooq"
        ).fetchone()[0]

        rows_by_family = conn.execute(
            """
            SELECT candidate_family, COUNT(*)
            FROM symbol_reference_candidates_from_unresolved_stooq
            GROUP BY candidate_family
            ORDER BY COUNT(*) DESC, candidate_family
            """
        ).fetchall()

        rows_by_suggested_action = conn.execute(
            """
            SELECT suggested_action, COUNT(*)
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
