"""
Seed a deterministic initial set of universe membership history rows.

This is bootstrap data to validate:
- historized universe membership
- as-of querying
- published serving snapshots
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Insert deterministic membership history rows.

    Rules used here:
    - US_LISTED_COMMON_STOCKS contains AAPL, MSFT, IBM
    - NASDAQ_LISTED contains AAPL, MSFT
    - NYSE_LISTED contains IBM and SPY
    - US_LISTED_ETFS contains SPY

    We keep one closed interval example for IBM in US_LISTED_COMMON_STOCKS
    to prove that as-of filtering works.
    """
    configure_logging()
    LOGGER.info("seed-universe-membership-history started")

    rows_to_seed = [
        # universe_membership_history_id, universe_id, instrument_id, status, effective_from, effective_to, source_name
        (2001, 1, 1001, "ACTIVE", "2000-01-01", None, "seed_v1"),
        (2002, 1, 1002, "ACTIVE", "2000-01-01", None, "seed_v1"),
        (2003, 1, 1003, "ACTIVE", "2000-01-01", "2022-12-31", "seed_v1"),
        (2004, 2, 1001, "ACTIVE", "2000-01-01", None, "seed_v1"),
        (2005, 2, 1002, "ACTIVE", "2000-01-01", None, "seed_v1"),
        (2006, 3, 1003, "ACTIVE", "2000-01-01", None, "seed_v1"),
        (2007, 3, 1004, "ACTIVE", "2000-01-01", None, "seed_v1"),
        (2008, 4, 1004, "ACTIVE", "2000-01-01", None, "seed_v1"),
    ]

    conn = connect_build_db()
    try:
        conn.executemany(
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
            SELECT ?, ?, ?, ?, CAST(? AS DATE), CAST(? AS DATE), ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM universe_membership_history
                WHERE universe_membership_history_id = ?
            )
            """,
            [
                (
                    row_id,
                    universe_id,
                    instrument_id,
                    membership_status,
                    effective_from,
                    effective_to,
                    source_name,
                    row_id,
                )
                for row_id, universe_id, instrument_id, membership_status, effective_from, effective_to, source_name in rows_to_seed
            ],
        )

        count_row = conn.execute(
            "SELECT COUNT(*) FROM universe_membership_history"
        ).fetchone()

        print(
            {
                "status": "ok",
                "job": "seed-universe-membership-history",
                "universe_membership_history_count": count_row[0],
            }
        )
    finally:
        conn.close()

    LOGGER.info("seed-universe-membership-history finished")
