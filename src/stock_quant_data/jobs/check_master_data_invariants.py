"""
Check master-data invariants for the loader build database.

Design:
- SQL-first
- read-only validation
- fail loudly when invariant violations are found

Current invariants:
1. no duplicate open-ended symbol references
2. no duplicate primary_ticker instruments
3. no duplicate normalized rows per (source_name, source_row_id)
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Validate key identity and normalization invariants.

    This job intentionally raises RuntimeError on violation so the rebuild
    summary shows exactly where the database stopped being trustworthy.
    """
    configure_logging()
    LOGGER.info("check-master-data-invariants started")

    conn = connect_build_db()
    try:
        duplicate_open_ended_symbol_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT symbol
                FROM symbol_reference_history
                WHERE effective_to IS NULL
                GROUP BY symbol
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]

        duplicate_primary_ticker_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT primary_ticker
                FROM instrument
                GROUP BY primary_ticker
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]

        duplicated_source_row_id_groups = conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT source_name, source_row_id
                FROM price_source_daily_normalized
                GROUP BY source_name, source_row_id
                HAVING COUNT(*) > 1
            )
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

        violations = []
        if duplicate_open_ended_symbol_count != 0:
            violations.append(
                f"duplicate_open_ended_symbol_count={duplicate_open_ended_symbol_count}"
            )
        if duplicate_primary_ticker_count != 0:
            violations.append(
                f"duplicate_primary_ticker_count={duplicate_primary_ticker_count}"
            )
        if duplicated_source_row_id_groups != 0:
            violations.append(
                f"duplicated_source_row_id_groups={duplicated_source_row_id_groups}"
            )

        if violations:
            raise RuntimeError(" ; ".join(violations))
    finally:
        conn.close()

    LOGGER.info("check-master-data-invariants finished")


if __name__ == "__main__":
    run()
