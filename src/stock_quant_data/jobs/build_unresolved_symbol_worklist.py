"""
Build the current unresolved symbol worklist.

Current canonical input:
- symbol_reference_candidates_from_unresolved_stooq

Current canonical output:
- unresolved_symbol_worklist

The worklist intentionally excludes:
- pure format-only mapping tasks
- low-volume later-review tasks

It keeps:
- REVIEW_FOR_REFERENCE_IDENTITY_CREATION
- REVIEW_FOR_REFERENCE_IDENTITY_CREATION_HIGH_PRIORITY
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild unresolved_symbol_worklist from current candidate rows.
    """
    configure_logging()
    LOGGER.info("build-unresolved-symbol-worklist started")

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM unresolved_symbol_worklist")

        conn.execute(
            """
            INSERT INTO unresolved_symbol_worklist (
                raw_symbol,
                unresolved_row_count,
                min_price_date,
                max_price_date,
                candidate_family,
                suggested_action,
                recency_bucket,
                built_at
            )
            SELECT
                raw_symbol,
                unresolved_row_count,
                min_price_date,
                max_price_date,
                candidate_family,
                suggested_action,
                recency_bucket,
                NOW() AS built_at
            FROM symbol_reference_candidates_from_unresolved_stooq
            WHERE suggested_action IN (
                'REVIEW_FOR_REFERENCE_IDENTITY_CREATION',
                'REVIEW_FOR_REFERENCE_IDENTITY_CREATION_HIGH_PRIORITY'
            )
            ORDER BY unresolved_row_count DESC, raw_symbol
            """
        )

        worklist_count = conn.execute(
            "SELECT COUNT(*) FROM unresolved_symbol_worklist"
        ).fetchone()[0]

        rows_by_suggested_action = conn.execute(
            """
            SELECT
                suggested_action,
                COUNT(*)
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
                "rows_by_suggested_action": rows_by_suggested_action,
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-unresolved-symbol-worklist finished")


if __name__ == "__main__":
    run()
