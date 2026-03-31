"""
Seed a small deterministic set of instruments into the build database.

These rows are only bootstrap data for the scientific platform scaffold.
They are not intended to be a full market loader.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Insert a deterministic initial set of instruments.

    We use explicit IDs so the early API outputs remain reproducible.
    """
    configure_logging()
    LOGGER.info("seed-instruments started")

    rows_to_seed = [
        (1001, "COMMON_STOCK", "COMP_AAPL", "AAPL", "NASDAQ"),
        (1002, "COMMON_STOCK", "COMP_MSFT", "MSFT", "NASDAQ"),
        (1003, "COMMON_STOCK", "COMP_IBM", "IBM", "NYSE"),
        (1004, "ETF", "FUND_SPY", "SPY", "NYSE"),
    ]

    conn = connect_build_db()
    try:
        conn.executemany(
            """
            INSERT INTO instrument (
                instrument_id,
                security_type,
                company_id,
                primary_ticker,
                primary_exchange
            )
            SELECT ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM instrument
                WHERE instrument_id = ?
            )
            """,
            [
                (iid, stype, cid, ticker, exch, iid)
                for iid, stype, cid, ticker, exch in rows_to_seed
            ],
        )

        count_row = conn.execute(
            "SELECT COUNT(*) FROM instrument"
        ).fetchone()

        print(
            {
                "status": "ok",
                "job": "seed-instruments",
                "instrument_count": count_row[0],
            }
        )
    finally:
        conn.close()

    LOGGER.info("seed-instruments finished")
