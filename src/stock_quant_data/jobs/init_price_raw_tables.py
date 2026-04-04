"""
Initialize price raw / normalized / canonical price tables.

Design:
- SQL-first
- this job is intentionally idempotent
- it creates the price tables if missing but does not try to rebuild contents
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Ensure the price tables exist with canonical schemas.
    """
    configure_logging()
    LOGGER.info("init-price-raw-tables started")

    conn = connect_build_db()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_source_daily_raw_stooq (
                source_row_id BIGINT,
                raw_symbol VARCHAR,
                price_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_source_daily_raw_yahoo (
                source_row_id BIGINT,
                raw_symbol VARCHAR,
                price_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                adj_close DOUBLE,
                volume BIGINT
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_source_daily_normalized (
                normalized_price_id BIGINT PRIMARY KEY,
                source_name VARCHAR NOT NULL,
                source_row_id BIGINT NOT NULL,
                raw_symbol VARCHAR NOT NULL,
                instrument_id BIGINT,
                price_date DATE NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                adj_close DOUBLE,
                volume BIGINT NOT NULL,
                symbol_resolution_status VARCHAR NOT NULL,
                normalization_notes VARCHAR,
                normalized_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_history (
                price_history_id BIGINT PRIMARY KEY,
                instrument_id BIGINT,
                symbol VARCHAR,
                source_name VARCHAR,
                source_row_id BIGINT,
                price_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                adj_close DOUBLE,
                volume BIGINT,
                built_at TIMESTAMP
            )
            """
        )

        print(
            {
                "status": "ok",
                "job": "init-price-raw-tables",
                "tables": [
                    "price_source_daily_raw_stooq",
                    "price_source_daily_raw_yahoo",
                    "price_source_daily_normalized",
                    "price_history",
                ],
            }
        )
    finally:
        conn.close()
        LOGGER.info("init-price-raw-tables finished")


if __name__ == "__main__":
    run()
