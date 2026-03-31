"""
Initialize raw and normalized price tables.

Design goals:
- keep raw tables source-specific
- preserve downloader / landed-disk raw as faithfully as practical
- keep normalized table source-agnostic
- keep canonical price_history separate from raw and normalized layers
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Create price raw and normalized tables if they do not already exist.

    Important Stooq-specific note:
    - local landed Stooq files contain:
      <TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>
    - VOL can be decimal in the raw files
    - therefore the raw Stooq table preserves raw_volume as DOUBLE
    """
    configure_logging()
    LOGGER.info("init-price-raw-tables started")

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Raw Stooq landing table.
        #
        # This table is intentionally wider than the previous version because
        # the real landed Stooq files on disk contain:
        # - raw ticker
        # - period code
        # - date
        # - time
        # - OHLC
        # - volume (which may be decimal)
        # - open interest
        #
        # We also preserve file path and source category from the directory
        # name (e.g. "nasdaq stocks", "nyse etfs").
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_source_daily_raw_stooq (
                raw_stooq_id BIGINT PRIMARY KEY,
                raw_symbol VARCHAR NOT NULL,
                raw_ticker VARCHAR,
                raw_per VARCHAR,
                raw_time VARCHAR,
                price_date DATE NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                raw_volume DOUBLE,
                raw_open_interest DOUBLE,
                source_file VARCHAR,
                source_category VARCHAR,
                landed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # ------------------------------------------------------------------
        # Backward-compatible schema evolution for older local DBs.
        # If the table already existed from a previous scaffold, add the new
        # columns if they are missing.
        # ------------------------------------------------------------------
        for alter_sql in [
            "ALTER TABLE price_source_daily_raw_stooq ADD COLUMN raw_ticker VARCHAR",
            "ALTER TABLE price_source_daily_raw_stooq ADD COLUMN raw_per VARCHAR",
            "ALTER TABLE price_source_daily_raw_stooq ADD COLUMN raw_time VARCHAR",
            "ALTER TABLE price_source_daily_raw_stooq ADD COLUMN raw_volume DOUBLE",
            "ALTER TABLE price_source_daily_raw_stooq ADD COLUMN raw_open_interest DOUBLE",
            "ALTER TABLE price_source_daily_raw_stooq ADD COLUMN source_category VARCHAR",
        ]:
            try:
                conn.execute(alter_sql)
            except Exception:
                # Column likely already exists; keep the initializer idempotent.
                pass

        # ------------------------------------------------------------------
        # Raw Yahoo landing table.
        # Yahoo often carries adj_close directly.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_source_daily_raw_yahoo (
                raw_yahoo_id BIGINT PRIMARY KEY,
                raw_symbol VARCHAR NOT NULL,
                price_date DATE NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                adj_close DOUBLE,
                volume BIGINT NOT NULL,
                landed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # ------------------------------------------------------------------
        # Unified normalized multi-source staging table.
        # This is the source-agnostic shape used before canonical selection.
        # ------------------------------------------------------------------
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
