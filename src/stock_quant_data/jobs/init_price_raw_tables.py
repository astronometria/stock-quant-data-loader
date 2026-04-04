"""
Create canonical raw + normalized price tables.

This file is intentionally tiny and explicit.
It owns only the price tables that the current loader repo is supposed to
create and maintain.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Ensure current canonical price tables exist.

    Canonical tables:
    - price_source_daily_raw_stooq
    - price_source_daily_raw_yahoo
    - price_source_daily_normalized
    """
    configure_logging()
    LOGGER.info("init-price-raw-tables started")

    conn = connect_build_db()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_source_daily_raw_stooq (
                raw_price_id BIGINT PRIMARY KEY,
                raw_symbol VARCHAR NOT NULL,
                price_date DATE NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                volume BIGINT NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_source_daily_raw_yahoo (
                raw_price_id BIGINT PRIMARY KEY,
                raw_symbol VARCHAR NOT NULL,
                price_date DATE NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                adj_close DOUBLE,
                volume BIGINT NOT NULL
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

        print(
            {
                "status": "ok",
                "job": "init-price-raw-tables",
                "tables": [
                    "price_source_daily_raw_stooq",
                    "price_source_daily_raw_yahoo",
                    "price_source_daily_normalized",
                ],
            }
        )
    finally:
        conn.close()
        LOGGER.info("init-price-raw-tables finished")


if __name__ == "__main__":
    run()
