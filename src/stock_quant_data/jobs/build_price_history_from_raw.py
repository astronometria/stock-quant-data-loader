"""
Build the canonical price_history table from the normalized source table.

Design:
- SQL-first
- one canonical serving/build table for downstream consumers
- only resolved rows enter price_history
- keep the schema narrow and stable

Important:
- this table is the canonical price table for downstream joins
- unresolved symbols remain in price_source_daily_normalized for triage only
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild price_history from resolved rows in price_source_daily_normalized.
    """
    configure_logging()
    LOGGER.info("build-price-history-from-raw started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS price_history")
        conn.execute(
            """
            CREATE TABLE price_history AS
            SELECT
                normalized_price_id AS price_history_id,
                instrument_id,
                raw_symbol AS symbol,
                source_name,
                source_row_id,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                normalized_at AS built_at
            FROM price_source_daily_normalized
            WHERE symbol_resolution_status = 'RESOLVED'
              AND instrument_id IS NOT NULL
            """
        )

        total_count = conn.execute(
            "SELECT COUNT(*) FROM price_history"
        ).fetchone()[0]

        by_source = conn.execute(
            """
            SELECT
                source_name,
                COUNT(*)
            FROM price_history
            GROUP BY source_name
            ORDER BY source_name
            """
        ).fetchall()

        date_range = conn.execute(
            """
            SELECT
                MIN(price_date),
                MAX(price_date)
            FROM price_history
            """
        ).fetchone()

        print(
            {
                "status": "ok",
                "job": "build-price-history-from-raw",
                "price_history_count": total_count,
                "rows_by_source": by_source,
                "min_price_date": date_range[0],
                "max_price_date": date_range[1],
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-price-history-from-raw finished")


if __name__ == "__main__":
    run()
