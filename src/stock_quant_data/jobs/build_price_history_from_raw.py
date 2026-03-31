"""
Build canonical price_history from normalized raw price staging.

SQL-first design:
- normalized staging is the only input
- canonical price_history is rebuilt deterministically

Canonical source selection policy v2:
- compute global stooq_max_date from resolved normalized rows
- if price_date <= stooq_max_date:
    prefer stooq, fallback yahoo
- if price_date > stooq_max_date:
    prefer yahoo, fallback stooq

This keeps Stooq as the deep-history anchor and Yahoo as the
recent-date source, while still allowing fallback if the preferred
source is missing for a specific instrument/date.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.jobs.build_price_normalized_from_raw import (
    run as run_build_price_normalized_from_raw,
)
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild canonical price_history from normalized raw staging.

    Selection policy:
    1. Rebuild normalized staging first.
    2. Compute global stooq_max_date from resolved rows.
    3. Rank candidate rows per (instrument_id, price_date):
       - on/before stooq_max_date: stooq first, yahoo second
       - after stooq_max_date: yahoo first, stooq second
    4. Insert the top-ranked row into canonical price_history.
    """
    configure_logging()
    LOGGER.info("build-price-history-from-raw started")

    # ------------------------------------------------------------------
    # Always rebuild normalized staging first so canonical selection works
    # from a fresh, deterministic intermediate layer.
    # ------------------------------------------------------------------
    run_build_price_normalized_from_raw()

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Canonical published price table.
        # This remains the only truth served by the API.
        # ------------------------------------------------------------------
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

        # ------------------------------------------------------------------
        # Compute the global Stooq coverage horizon from resolved rows only.
        # This is the dynamic cutoff used by the canonical selection policy.
        # ------------------------------------------------------------------
        stooq_max_date = conn.execute(
            """
            SELECT MAX(price_date)
            FROM price_source_daily_normalized
            WHERE source_name = 'stooq'
              AND symbol_resolution_status = 'RESOLVED'
            """
        ).fetchone()[0]

        # ------------------------------------------------------------------
        # Rebuild canonical table from scratch in this deterministic version.
        # ------------------------------------------------------------------
        conn.execute("DELETE FROM price_history")

        # ------------------------------------------------------------------
        # If Stooq has no resolved rows at all, default to Yahoo-first ranking.
        # Otherwise apply the dynamic cutoff policy described above.
        #
        # Ranking notes:
        # - exactly one canonical row per (instrument_id, price_date)
        # - preferred source gets priority rank 1
        # - fallback source gets priority rank 2
        # - any unknown source goes last
        # ------------------------------------------------------------------
        conn.execute(
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
            WITH ranked AS (
                SELECT
                    ROW_NUMBER() OVER (
                        PARTITION BY instrument_id, price_date
                        ORDER BY
                            CASE
                                WHEN ? IS NULL THEN
                                    CASE source_name
                                        WHEN 'yahoo' THEN 1
                                        WHEN 'stooq' THEN 2
                                        ELSE 99
                                    END
                                WHEN price_date <= CAST(? AS DATE) THEN
                                    CASE source_name
                                        WHEN 'stooq' THEN 1
                                        WHEN 'yahoo' THEN 2
                                        ELSE 99
                                    END
                                ELSE
                                    CASE source_name
                                        WHEN 'yahoo' THEN 1
                                        WHEN 'stooq' THEN 2
                                        ELSE 99
                                    END
                            END,
                            normalized_price_id
                    ) AS row_rank,
                    instrument_id,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    adj_close,
                    volume,
                    source_name
                FROM price_source_daily_normalized
                WHERE symbol_resolution_status = 'RESOLVED'
            )
            SELECT
                3000000 + ROW_NUMBER() OVER (ORDER BY instrument_id, price_date) AS price_history_id,
                instrument_id,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                source_name
            FROM ranked
            WHERE row_rank = 1
            ORDER BY instrument_id, price_date
            """,
            [stooq_max_date, stooq_max_date],
        )

        price_history_count = conn.execute(
            "SELECT COUNT(*) FROM price_history"
        ).fetchone()[0]

        by_source = conn.execute(
            """
            SELECT source_name, COUNT(*)
            FROM price_history
            GROUP BY source_name
            ORDER BY source_name
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "build-price-history-from-raw",
                "stooq_max_date": str(stooq_max_date) if stooq_max_date is not None else None,
                "price_history_count": price_history_count,
                "price_history_rows_by_source": by_source,
            }
        )
    finally:
        conn.close()

    LOGGER.info("build-price-history-from-raw finished")


if __name__ == "__main__":
    run()
