"""
Insert a deliberate overlapping universe membership interval for testing.

This job is ONLY for controlled validation testing.
It creates a known-bad row that should make:
- sq validate-release detect an overlap
- sq publish-release refuse publication
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Insert one deterministic bad overlap row.

    Existing valid row for:
        universe_id=1
        instrument_id=1001
        effective_from=2000-01-01
        effective_to=NULL

    Bad row inserted here:
        universe_id=1
        instrument_id=1001
        effective_from=2020-01-01
        effective_to=2021-12-31

    This overlaps the open-ended active membership interval.
    """
    configure_logging()
    LOGGER.info("insert-invalid-universe-overlap-demo started")

    conn = connect_build_db()
    try:
        conn.execute(
            """
            INSERT INTO universe_membership_history (
                universe_membership_history_id,
                universe_id,
                instrument_id,
                membership_status,
                effective_from,
                effective_to,
                source_name
            )
            SELECT
                900001,
                1,
                1001,
                'ACTIVE',
                DATE '2020-01-01',
                DATE '2021-12-31',
                'invalid_overlap_demo'
            WHERE NOT EXISTS (
                SELECT 1
                FROM universe_membership_history
                WHERE universe_membership_history_id = 900001
            )
            """
        )

        rows = conn.execute(
            """
            SELECT
                universe_membership_history_id,
                universe_id,
                instrument_id,
                membership_status,
                effective_from,
                effective_to,
                source_name
            FROM universe_membership_history
            WHERE universe_id = 1
              AND instrument_id = 1001
            ORDER BY effective_from, universe_membership_history_id
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "insert-invalid-universe-overlap-demo",
                "rows_for_universe_1_instrument_1001": [
                    {
                        "universe_membership_history_id": row[0],
                        "universe_id": row[1],
                        "instrument_id": row[2],
                        "membership_status": row[3],
                        "effective_from": str(row[4]) if row[4] is not None else None,
                        "effective_to": str(row[5]) if row[5] is not None else None,
                        "source_name": row[6],
                    }
                    for row in rows
                ],
            }
        )
    finally:
        conn.close()

    LOGGER.info("insert-invalid-universe-overlap-demo finished")
