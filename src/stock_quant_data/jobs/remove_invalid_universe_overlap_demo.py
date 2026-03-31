"""
Remove the deliberate invalid overlap test row.

This restores the build DB to a publishable state after the negative test.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Delete the known test row inserted by the overlap demo job.
    """
    configure_logging()
    LOGGER.info("remove-invalid-universe-overlap-demo started")

    conn = connect_build_db()
    try:
        before_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM universe_membership_history
            WHERE universe_membership_history_id = 900001
            """
        ).fetchone()[0]

        conn.execute(
            """
            DELETE FROM universe_membership_history
            WHERE universe_membership_history_id = 900001
            """
        )

        after_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM universe_membership_history
            WHERE universe_membership_history_id = 900001
            """
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "remove-invalid-universe-overlap-demo",
                "before_count": before_count,
                "after_count": after_count,
            }
        )
    finally:
        conn.close()

    LOGGER.info("remove-invalid-universe-overlap-demo finished")
