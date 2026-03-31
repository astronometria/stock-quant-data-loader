"""
Seed a deterministic minimal listing status history dataset.

This bootstrap data demonstrates:
- active listing lifecycle
- a renamed ticker / listing continuity example
- status history published separately from symbol reference history
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Insert deterministic listing status rows.

    Current examples:
    - AAPL active from 2000 onward
    - MSFT active from 2000 onward
    - IBM active from 2000 onward
    - SPY active from 2000 onward
    - FB active from 2012-05-18 to 2022-06-08
    - META active from 2022-06-09 onward

    This keeps the same instrument identity for FB/META while changing
    the listing symbol timeline.
    """
    configure_logging()
    LOGGER.info("seed-listing-status-history started")

    conn = connect_build_db()
    try:
        rows_to_seed = [
            # id, instrument_id, symbol, listing_status, event_type, effective_from, effective_to, source_name
            (4001, 1001, "AAPL", "ACTIVE", "LISTED", "2000-01-01", None, "seed_v1"),
            (4002, 1002, "MSFT", "ACTIVE", "LISTED", "2000-01-01", None, "seed_v1"),
            (4003, 1003, "IBM", "ACTIVE", "LISTED", "2000-01-01", None, "seed_v1"),
            (4004, 1004, "SPY", "ACTIVE", "LISTED", "2000-01-01", None, "seed_v1"),
            (4005, 1005, "FB", "ACTIVE", "LISTED", "2012-05-18", "2022-06-08", "seed_v1"),
            (4006, 1005, "META", "ACTIVE", "RENAMED", "2022-06-09", None, "seed_v1"),
        ]

        conn.executemany(
            """
            INSERT INTO listing_status_history (
                listing_status_history_id,
                instrument_id,
                symbol,
                listing_status,
                event_type,
                effective_from,
                effective_to,
                source_name
            )
            SELECT ?, ?, ?, ?, ?, CAST(? AS DATE), CAST(? AS DATE), ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM listing_status_history
                WHERE listing_status_history_id = ?
            )
            """,
            [
                (
                    row_id,
                    instrument_id,
                    symbol,
                    listing_status,
                    event_type,
                    effective_from,
                    effective_to,
                    source_name,
                    row_id,
                )
                for row_id, instrument_id, symbol, listing_status, event_type, effective_from, effective_to, source_name in rows_to_seed
            ],
        )

        count_row = conn.execute(
            "SELECT COUNT(*) FROM listing_status_history"
        ).fetchone()

        print(
            {
                "status": "ok",
                "job": "seed-listing-status-history",
                "listing_status_history_count": count_row[0],
            }
        )
    finally:
        conn.close()

    LOGGER.info("seed-listing-status-history finished")
