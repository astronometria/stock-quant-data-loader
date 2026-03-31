"""
Seed a deterministic minimal daily price history dataset.

This bootstrap dataset is only for validating:
- published price history serving
- historical range queries
- latest price queries
- core OHLCV validation checks
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Insert a deterministic initial price history sample.

    Notes:
    - one row per (instrument_id, price_date)
    - values are synthetic but internally coherent
    - includes META only, not FB, because the sample dates are post-rename
    """
    configure_logging()
    LOGGER.info("seed-price-history started")

    rows_to_seed = [
        # price_history_id, instrument_id, price_date, open, high, low, close, adj_close, volume, source_name
        (5001, 1001, "2024-06-27", 210.00, 213.00, 209.00, 212.50, 212.50, 55000000, "seed_v1"),
        (5002, 1001, "2024-06-28", 212.60, 214.20, 211.70, 213.90, 213.90, 53000000, "seed_v1"),

        (5003, 1002, "2024-06-27", 445.00, 449.00, 443.50, 448.10, 448.10, 21000000, "seed_v1"),
        (5004, 1002, "2024-06-28", 448.30, 451.20, 447.90, 450.70, 450.70, 20500000, "seed_v1"),

        (5005, 1003, "2024-06-27", 171.00, 172.50, 170.30, 171.90, 171.90, 4800000, "seed_v1"),
        (5006, 1003, "2024-06-28", 172.00, 173.20, 171.40, 172.80, 172.80, 5000000, "seed_v1"),

        (5007, 1004, "2024-06-27", 545.00, 547.20, 544.40, 546.80, 546.80, 65000000, "seed_v1"),
        (5008, 1004, "2024-06-28", 547.00, 548.60, 545.80, 548.10, 548.10, 63000000, "seed_v1"),

        (5009, 1005, "2024-06-27", 500.00, 507.50, 498.50, 506.20, 506.20, 14500000, "seed_v1"),
        (5010, 1005, "2024-06-28", 506.50, 510.20, 505.10, 509.70, 509.70, 14100000, "seed_v1"),
    ]

    conn = connect_build_db()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_history (
                price_history_id BIGINT PRIMARY KEY,
                instrument_id BIGINT NOT NULL,
                price_date DATE NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                adj_close DOUBLE,
                volume BIGINT NOT NULL,
                source_name VARCHAR,
                observed_at TIMESTAMP,
                ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.executemany(
            """
            INSERT INTO price_history (
                price_history_id,
                instrument_id,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                source_name
            )
            SELECT
                ?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM price_history
                WHERE price_history_id = ?
            )
            """,
            [
                (
                    row_id,
                    instrument_id,
                    price_date,
                    open_,
                    high,
                    low,
                    close,
                    adj_close,
                    volume,
                    source_name,
                    row_id,
                )
                for row_id, instrument_id, price_date, open_, high, low, close, adj_close, volume, source_name in rows_to_seed
            ],
        )

        count_row = conn.execute(
            "SELECT COUNT(*) FROM price_history"
        ).fetchone()

        print(
            {
                "status": "ok",
                "job": "seed-price-history",
                "price_history_count": count_row[0],
            }
        )
    finally:
        conn.close()

    LOGGER.info("seed-price-history finished")
