"""
Seed deterministic raw Stooq and Yahoo price demo data.

This job simulates what a downloader would normally land into raw tables.
It is intentionally small but preserves the source split:
- Stooq raw
- Yahoo raw
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.jobs.init_price_raw_tables import run as run_init_price_raw_tables
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Seed raw demo rows into Stooq and Yahoo raw tables.
    """
    configure_logging()
    LOGGER.info("seed-price-raw-demo started")

    run_init_price_raw_tables()

    stooq_rows = [
        # raw_stooq_id, raw_symbol, price_date, open, high, low, close, volume, source_file
        (6001, "AAPL", "2024-06-27", 210.00, 213.00, 209.00, 212.50, 55000000, "stooq_demo.csv"),
        (6002, "AAPL", "2024-06-28", 212.60, 214.20, 211.70, 213.90, 53000000, "stooq_demo.csv"),
        (6003, "MSFT", "2024-06-27", 445.00, 449.00, 443.50, 448.10, 21000000, "stooq_demo.csv"),
        (6004, "MSFT", "2024-06-28", 448.30, 451.20, 447.90, 450.70, 20500000, "stooq_demo.csv"),
        (6005, "IBM", "2024-06-27", 171.00, 172.50, 170.30, 171.90, 4800000, "stooq_demo.csv"),
        (6006, "IBM", "2024-06-28", 172.00, 173.20, 171.40, 172.80, 5000000, "stooq_demo.csv"),
    ]

    yahoo_rows = [
        # raw_yahoo_id, raw_symbol, price_date, open, high, low, close, adj_close, volume
        (7001, "SPY", "2024-06-27", 545.00, 547.20, 544.40, 546.80, 546.80, 65000000),
        (7002, "SPY", "2024-06-28", 547.00, 548.60, 545.80, 548.10, 548.10, 63000000),
        (7003, "META", "2024-06-27", 500.00, 507.50, 498.50, 506.20, 506.20, 14500000),
        (7004, "META", "2024-06-28", 506.50, 510.20, 505.10, 509.70, 509.70, 14100000),
    ]

    conn = connect_build_db()
    try:
        conn.executemany(
            """
            INSERT INTO price_source_daily_raw_stooq (
                raw_stooq_id,
                raw_symbol,
                price_date,
                open,
                high,
                low,
                close,
                volume,
                source_file
            )
            SELECT ?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM price_source_daily_raw_stooq
                WHERE raw_stooq_id = ?
            )
            """,
            [
                (*row, row[0])
                for row in stooq_rows
            ],
        )

        conn.executemany(
            """
            INSERT INTO price_source_daily_raw_yahoo (
                raw_yahoo_id,
                raw_symbol,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume
            )
            SELECT ?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM price_source_daily_raw_yahoo
                WHERE raw_yahoo_id = ?
            )
            """,
            [
                (*row, row[0])
                for row in yahoo_rows
            ],
        )

        stooq_count = conn.execute(
            "SELECT COUNT(*) FROM price_source_daily_raw_stooq"
        ).fetchone()[0]

        yahoo_count = conn.execute(
            "SELECT COUNT(*) FROM price_source_daily_raw_yahoo"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "seed-price-raw-demo",
                "price_source_daily_raw_stooq_count": stooq_count,
                "price_source_daily_raw_yahoo_count": yahoo_count,
            }
        )
    finally:
        conn.close()

    LOGGER.info("seed-price-raw-demo finished")
