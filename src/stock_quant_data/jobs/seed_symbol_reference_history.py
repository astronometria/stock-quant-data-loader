"""
Seed a deterministic minimal symbol reference history.

This job bootstraps historical symbol identity mapping so the platform can:
- resolve a symbol to an instrument
- expose history for a symbol
- prepare for future as-of symbol resolution
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Insert a small deterministic symbol history dataset.

    Current rows:
    - AAPL -> instrument 1001
    - MSFT -> instrument 1002
    - IBM  -> instrument 1003
    - SPY  -> instrument 1004
    - FB   -> instrument 1005 until 2022-06-08
    - META -> instrument 1005 from 2022-06-09 onward

    This demonstrates ticker rename history with stable instrument identity.
    """
    configure_logging()
    LOGGER.info("seed-symbol-reference-history started")

    conn = connect_build_db()
    try:
        # Ensure the META/FB demo instrument exists.
        conn.execute(
            """
            INSERT INTO instrument (
                instrument_id,
                security_type,
                company_id,
                primary_ticker,
                primary_exchange
            )
            SELECT
                1005,
                'COMMON_STOCK',
                'COMP_META',
                'META',
                'NASDAQ'
            WHERE NOT EXISTS (
                SELECT 1
                FROM instrument
                WHERE instrument_id = 1005
            )
            """
        )

        rows_to_seed = [
            # id, instrument_id, symbol, exchange, is_primary, effective_from, effective_to
            (3001, 1001, "AAPL", "NASDAQ", True, "2000-01-01", None),
            (3002, 1002, "MSFT", "NASDAQ", True, "2000-01-01", None),
            (3003, 1003, "IBM", "NYSE", True, "2000-01-01", None),
            (3004, 1004, "SPY", "NYSE", True, "2000-01-01", None),
            (3005, 1005, "FB", "NASDAQ", True, "2012-05-18", "2022-06-08"),
            (3006, 1005, "META", "NASDAQ", True, "2022-06-09", None),
        ]

        conn.executemany(
            """
            INSERT INTO symbol_reference_history (
                symbol_reference_history_id,
                instrument_id,
                symbol,
                exchange,
                is_primary,
                effective_from,
                effective_to
            )
            SELECT ?, ?, ?, ?, ?, CAST(? AS DATE), CAST(? AS DATE)
            WHERE NOT EXISTS (
                SELECT 1
                FROM symbol_reference_history
                WHERE symbol_reference_history_id = ?
            )
            """,
            [
                (
                    row_id,
                    instrument_id,
                    symbol,
                    exchange,
                    is_primary,
                    effective_from,
                    effective_to,
                    row_id,
                )
                for row_id, instrument_id, symbol, exchange, is_primary, effective_from, effective_to in rows_to_seed
            ],
        )

        count_row = conn.execute(
            "SELECT COUNT(*) FROM symbol_reference_history"
        ).fetchone()

        print(
            {
                "status": "ok",
                "job": "seed-symbol-reference-history",
                "symbol_reference_history_count": count_row[0],
            }
        )
    finally:
        conn.close()

    LOGGER.info("seed-symbol-reference-history finished")
