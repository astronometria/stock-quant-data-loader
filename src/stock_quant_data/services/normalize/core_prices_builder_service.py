"""
Core prices builder service.

Design goals:
- SQL-first canonicalization
- thin Python orchestration
- resolve raw source_symbol to instrument_id through historical symbol mapping
- avoid duplicates on repeated runs
"""

from __future__ import annotations

import logging

from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def build_core_prices_from_raw() -> dict:
    """
    Build canonical core.price_history rows from raw.price_source_daily.

    Mapping rule:
    - join raw source_symbol to core.symbol_reference_history
    - require the symbol mapping to be active on the raw price_date

    Duplicate rule:
    - do not insert if (instrument_id, price_date) already exists in core.price_history
    """
    connection = connect_build_db()

    try:
        before_count = connection.execute(
            "SELECT COUNT(*) FROM core.price_history"
        ).fetchone()[0]

        LOGGER.info("Building core.price_history from raw.price_source_daily")

        connection.execute(
            """
            INSERT INTO core.price_history (
                price_history_id,
                instrument_id,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                source_name,
                observed_at
            )
            WITH max_id AS (
                SELECT COALESCE(MAX(price_history_id), 0) AS current_max_id
                FROM core.price_history
            ),
            candidate_rows AS (
                SELECT
                    srh.instrument_id,
                    rpsd.price_date,
                    rpsd.open,
                    rpsd.high,
                    rpsd.low,
                    rpsd.close,
                    rpsd.adj_close,
                    rpsd.volume,
                    rpsd.source_name,
                    rpsd.observed_at
                FROM raw.price_source_daily AS rpsd
                JOIN core.symbol_reference_history AS srh
                  ON UPPER(srh.symbol) = UPPER(rpsd.source_symbol)
                 AND srh.effective_from <= rpsd.price_date
                 AND (
                        srh.effective_to IS NULL
                        OR srh.effective_to > rpsd.price_date
                     )
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM core.price_history existing
                    WHERE existing.instrument_id = srh.instrument_id
                      AND existing.price_date = rpsd.price_date
                )
            ),
            numbered_rows AS (
                SELECT
                    (SELECT current_max_id FROM max_id)
                    + ROW_NUMBER() OVER (
                        ORDER BY instrument_id, price_date, source_name
                    ) AS price_history_id,
                    instrument_id,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    adj_close,
                    volume,
                    source_name,
                    observed_at
                FROM candidate_rows
            )
            SELECT
                price_history_id,
                instrument_id,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                source_name,
                observed_at
            FROM numbered_rows
            """
        )

        after_count = connection.execute(
            "SELECT COUNT(*) FROM core.price_history"
        ).fetchone()[0]

        inserted_rows = int(after_count - before_count)

        unresolved_rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.price_source_daily AS rpsd
            WHERE NOT EXISTS (
                SELECT 1
                FROM core.symbol_reference_history AS srh
                WHERE UPPER(srh.symbol) = UPPER(rpsd.source_symbol)
                  AND srh.effective_from <= rpsd.price_date
                  AND (
                        srh.effective_to IS NULL
                        OR srh.effective_to > rpsd.price_date
                      )
            )
            """
        ).fetchone()[0]

        return {
            "inserted_rows": inserted_rows,
            "core_total_rows": int(after_count),
            "unresolved_raw_rows": int(unresolved_rows),
        }
    finally:
        connection.close()
