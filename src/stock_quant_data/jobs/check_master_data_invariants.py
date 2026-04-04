"""
Check master-data invariants for the current loader schema.

Current canonical invariants:
- symbol_reference_history must not contain duplicate open-ended rows for the same symbol
- instrument must not contain duplicate primary_ticker rows
- price_source_daily_normalized must not contain duplicate (source_name, source_row_id) pairs

Important:
- this job must only reference tables/columns that exist in the current DB
- this job is intentionally read-only
- this job prints a compact machine-readable payload for orchestration scripts
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Run invariant checks against the current canonical loader schema.
    """
    configure_logging()
    LOGGER.info("check-master-data-invariants started")

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Duplicate open-ended symbol references are forbidden because they
        # make symbol resolution non-deterministic.
        # ------------------------------------------------------------------
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
            )
            """
        ).fetchone()[0]

        # ------------------------------------------------------------------
        # Duplicate instrument.primary_ticker rows are forbidden because the
        # current loader uses primary_ticker as the core identity join key for
        # many bootstrap stages.
        # ------------------------------------------------------------------
        duplicate_primary_ticker_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT
                    primary_ticker
                FROM instrument
                GROUP BY primary_ticker
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]

        # ------------------------------------------------------------------
        # The normalized price table must be unique per source record.
        #
        # NOTE:
        # We intentionally check (source_name, source_row_id), not only
        # source_row_id, because source_row_id uniqueness is source-scoped.
        # ------------------------------------------------------------------
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
            )
            """
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "check-master-data-invariants",
                "duplicate_open_ended_symbol_count": duplicate_open_ended_symbol_count,
                "duplicate_primary_ticker_count": duplicate_primary_ticker_count,
                "duplicated_source_row_id_groups": duplicated_source_row_id_groups,
            }
        )
    finally:
        conn.close()
        LOGGER.info("check-master-data-invariants finished")


if __name__ == "__main__":
    run()
