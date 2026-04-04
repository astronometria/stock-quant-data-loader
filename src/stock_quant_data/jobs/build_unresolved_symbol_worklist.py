"""
Build a compact unresolved symbol worklist for targeted SEC enrichment.

Why this job exists:
- the old platform pipeline expected unresolved_symbol_worklist
- but no dedicated builder job existed for it
- targeted SEC loading should scan only the current unresolved candidate set

Design:
- SQL-first
- source table:
    symbol_reference_candidates_from_unresolved_stooq
- output table:
    unresolved_symbol_worklist
- only keep candidate rows that are relevant for reference-identity work

Important:
- this is a review / targeting worklist
- it does not mutate instrument or symbol_reference_history directly
- it exists to bridge the Stooq unresolved branch into the targeted SEC branch
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Build unresolved_symbol_worklist from unresolved Stooq candidate groups.
    """
    configure_logging()
    LOGGER.info("build-unresolved-symbol-worklist started")

    conn = connect_build_db()
    try:
        # Rebuild deterministically every run so the worklist always
        # reflects the current unresolved Stooq state.
        conn.execute("DROP TABLE IF EXISTS unresolved_symbol_worklist")

        conn.execute(
            """
            CREATE TABLE unresolved_symbol_worklist AS
            SELECT
                raw_symbol,
                unresolved_row_count,
                min_price_date,
                max_price_date,
                candidate_family,
                suggested_action,
                recency_bucket,
                CURRENT_TIMESTAMP AS built_at
            FROM symbol_reference_candidates_from_unresolved_stooq
            WHERE suggested_action IN (
                'REVIEW_FOR_REFERENCE_IDENTITY_CREATION_HIGH_PRIORITY',
                'REVIEW_FOR_REFERENCE_IDENTITY_CREATION'
            )
            ORDER BY
                CASE suggested_action
                    WHEN 'REVIEW_FOR_REFERENCE_IDENTITY_CREATION_HIGH_PRIORITY' THEN 1
                    WHEN 'REVIEW_FOR_REFERENCE_IDENTITY_CREATION' THEN 2
                    ELSE 3
                END,
                unresolved_row_count DESC,
                max_price_date DESC,
                raw_symbol
            """
        )

        worklist_count = conn.execute(
            "SELECT COUNT(*) FROM unresolved_symbol_worklist"
        ).fetchone()[0]

        action_breakdown = conn.execute(
            """
            SELECT suggested_action, COUNT(*)
            FROM unresolved_symbol_worklist
            GROUP BY suggested_action
            ORDER BY COUNT(*) DESC, suggested_action
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "build-unresolved-symbol-worklist",
                "worklist_count": worklist_count,
                "rows_by_suggested_action": action_breakdown,
            }
        )
    finally:
        conn.close()

    LOGGER.info("build-unresolved-symbol-worklist finished")


if __name__ == "__main__":
    run()
