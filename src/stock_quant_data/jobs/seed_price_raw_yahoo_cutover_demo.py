"""
Seed additional Yahoo raw rows beyond the current Stooq max date.

Purpose:
- prove that the canonical selection policy switches to Yahoo
  after the Stooq coverage horizon
- keep the test deterministic and easy to inspect

Current expectation:
- existing Stooq max date is 2024-06-28
- these rows extend Yahoo to 2024-07-01
- canonical price_history should therefore select Yahoo
  for dates after 2024-06-28
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Insert deterministic Yahoo cutover demo rows.

    We add:
    - SPY  on 2024-07-01
    - META on 2024-07-01

    These dates are after the current Stooq max date in the demo dataset.
    """
    configure_logging()
    LOGGER.info("seed-price-raw-yahoo-cutover-demo started")

    rows_to_seed = [
        # raw_yahoo_id, raw_symbol, price_date, open, high, low, close, adj_close, volume
        (7005, "SPY", "2024-07-01", 548.30, 549.90, 547.80, 549.20, 549.20, 62000000),
        (7006, "META", "2024-07-01", 510.10, 514.60, 509.40, 513.80, 513.80, 13900000),
    ]

    conn = connect_build_db()
    try:
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
                for row in rows_to_seed
            ],
        )

        rows = conn.execute(
            """
            SELECT
                raw_yahoo_id,
                raw_symbol,
                price_date,
                close,
                adj_close,
                volume
            FROM price_source_daily_raw_yahoo
            WHERE price_date >= DATE '2024-07-01'
            ORDER BY raw_symbol, price_date, raw_yahoo_id
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "seed-price-raw-yahoo-cutover-demo",
                "rows_on_or_after_2024_07_01": [
                    {
                        "raw_yahoo_id": row[0],
                        "raw_symbol": row[1],
                        "price_date": str(row[2]) if row[2] is not None else None,
                        "close": row[3],
                        "adj_close": row[4],
                        "volume": row[5],
                    }
                    for row in rows
                ],
            }
        )
    finally:
        conn.close()

    LOGGER.info("seed-price-raw-yahoo-cutover-demo finished")
