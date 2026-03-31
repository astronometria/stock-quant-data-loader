"""
Build a review table for unresolved Stooq symbols.

Why this job exists:
- The price normalization pipeline is now in good shape.
- The remaining unresolved symbols are no longer mostly a string-normalization problem.
- They are mostly a reference-management / master-data problem.

This job creates a compact candidate table so unresolved symbols can be reviewed
in a structured way without polluting the main normalization logic.

Design notes:
- SQL-first: all heavy aggregation and classification is done in DuckDB SQL.
- Python is intentionally thin and only orchestrates the build.
- The output table is meant for analyst / developer review, not direct serving.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Build a review table from unresolved Stooq rows.

    Output table:
    - symbol_reference_candidates_from_unresolved_stooq

    Main purpose:
    - group unresolved rows by raw_symbol
    - compute date range and row counts
    - propose a coarse candidate family
    - propose a suggested action for future reference-management work
    """
    configure_logging()
    LOGGER.info("build-symbol-reference-candidates-from-unresolved-stooq started")

    conn = connect_build_db()
    try:
        # Rebuild from scratch every time so the review table always reflects
        # the current normalization state.
        conn.execute("DROP TABLE IF EXISTS symbol_reference_candidates_from_unresolved_stooq")

        conn.execute(
            """
            CREATE TABLE symbol_reference_candidates_from_unresolved_stooq AS
            WITH unresolved AS (
                SELECT
                    raw_symbol,
                    source_row_id,
                    price_date,
                    normalization_notes
                FROM price_source_daily_normalized
                WHERE source_name = 'stooq'
                  AND symbol_resolution_status <> 'RESOLVED'
            ),
            aggregated AS (
                SELECT
                    raw_symbol,
                    COUNT(*) AS unresolved_row_count,
                    MIN(price_date) AS min_price_date,
                    MAX(price_date) AS max_price_date,
                    MIN(source_row_id) AS first_source_row_id,
                    MAX(source_row_id) AS last_source_row_id,
                    MIN(normalization_notes) AS normalization_notes_example
                FROM unresolved
                GROUP BY raw_symbol
            ),
            classified AS (
                SELECT
                    raw_symbol,
                    unresolved_row_count,
                    min_price_date,
                    max_price_date,
                    first_source_row_id,
                    last_source_row_id,
                    normalization_notes_example,

                    CASE
                        WHEN raw_symbol LIKE '%-WS' THEN 'WARRANT_DASH_WS'
                        WHEN raw_symbol LIKE '%-U' THEN 'UNIT_DASH_U'
                        WHEN raw_symbol LIKE '%-R' THEN 'RIGHT_DASH_R'
                        WHEN POSITION('_' IN raw_symbol) > 0 THEN 'UNDERSCORE_VARIANT'
                        WHEN raw_symbol LIKE '%.W' THEN 'WARRANT_DOT_W'
                        WHEN raw_symbol LIKE '%.U' THEN 'UNIT_DOT_U'
                        WHEN raw_symbol LIKE '%.R' THEN 'RIGHT_DOT_R'
                        WHEN regexp_matches(raw_symbol, '^[A-Z0-9]+$') THEN 'PLAIN_ALNUM'
                        WHEN POSITION('-' IN raw_symbol) > 0 THEN 'DASH_OTHER'
                        ELSE 'OTHER'
                    END AS candidate_family
                FROM aggregated
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
                    WHEN candidate_family IN ('WARRANT_DASH_WS', 'UNIT_DASH_U', 'RIGHT_DASH_R', 'UNDERSCORE_VARIANT', 'DASH_OTHER')
                        THEN 'REVIEW_FOR_SYMBOL_FORMAT_MAPPING'
                    WHEN unresolved_row_count >= 1000
                        THEN 'REVIEW_FOR_REFERENCE_IDENTITY_CREATION_HIGH_PRIORITY'
                    WHEN unresolved_row_count >= 100
                        THEN 'REVIEW_FOR_REFERENCE_IDENTITY_CREATION'
                    ELSE 'REVIEW_LATER_LOW_VOLUME'
                END AS suggested_action,

                CASE
                    WHEN max_price_date >= CURRENT_DATE - INTERVAL 120 DAY THEN 'RECENT'
                    WHEN max_price_date >= CURRENT_DATE - INTERVAL 365 DAY THEN 'MID'
                    ELSE 'OLD'
                END AS recency_bucket,

                normalization_notes_example,
                CURRENT_TIMESTAMP AS built_at
            FROM classified
            ORDER BY unresolved_row_count DESC, raw_symbol
            """
        )

        total_candidates = conn.execute(
            """
            SELECT COUNT(*)
            FROM symbol_reference_candidates_from_unresolved_stooq
            """
        ).fetchone()[0]

        by_family = conn.execute(
            """
            SELECT candidate_family, COUNT(*)
            FROM symbol_reference_candidates_from_unresolved_stooq
            GROUP BY candidate_family
            ORDER BY COUNT(*) DESC, candidate_family
            """
        ).fetchall()

        by_action = conn.execute(
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
                "candidate_row_count": total_candidates,
                "rows_by_family": by_family,
                "rows_by_suggested_action": by_action,
            }
        )
    finally:
        conn.close()

    LOGGER.info("build-symbol-reference-candidates-from-unresolved-stooq finished")


if __name__ == "__main__":
    run()
