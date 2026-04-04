"""
Check the critical loader invariants after reference and normalization jobs.

Design:
- SQL-first
- small explicit checks
- fail fast on corruption-prone issues
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Validate the critical loader invariants.
    """
    configure_logging()
    LOGGER.info("check-master-data-invariants started")

    conn = connect_build_db()
    try:
        duplicate_open_ended_symbol_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT
                    symbol
                FROM symbol_reference_history
                WHERE effective_to IS NULL
                GROUP BY symbol
                HAVING COUNT(*) > 1
            ) AS t
            """
        ).fetchone()[0]

        duplicate_primary_ticker_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT
                    primary_ticker
                FROM instrument
                GROUP BY primary_ticker
                HAVING COUNT(*) > 1
            ) AS t
            """
        ).fetchone()[0]

        duplicated_source_row_id_groups = conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT
                    source_name,
                    source_row_id
                FROM price_source_daily_normalized
                GROUP BY source_name, source_row_id
                HAVING COUNT(*) > 1
            ) AS t
            """
        ).fetchone()[0]

        payload = {
            "status": "ok",
            "job": "check-master-data-invariants",
            "duplicate_open_ended_symbol_count": duplicate_open_ended_symbol_count,
            "duplicate_primary_ticker_count": duplicate_primary_ticker_count,
            "duplicated_source_row_id_groups": duplicated_source_row_id_groups,
        }

        print(payload)

        if (
            duplicate_open_ended_symbol_count > 0
            or duplicate_primary_ticker_count > 0
            or duplicated_source_row_id_groups > 0
        ):
            raise RuntimeError(f"Invariant failure: {payload}")
    finally:
        conn.close()
        LOGGER.info("check-master-data-invariants finished")


if __name__ == "__main__":
    run()
