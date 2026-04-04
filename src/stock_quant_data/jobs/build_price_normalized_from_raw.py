"""
Build canonical normalized price rows from the raw price layer.

This job only uses:
- price_source_daily_raw_stooq
- price_source_daily_raw_yahoo
- symbol_reference_history
- stooq_symbol_normalization_map

Output:
- price_source_daily_normalized
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db
from stock_quant_data.jobs.init_price_raw_tables import run as run_init_price_raw_tables

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild the canonical normalized price table.

    Matching order for Stooq rows:
    1) exact raw_symbol -> open symbol_reference_history symbol
    2) mapped normalized_symbol via stooq_symbol_normalization_map -> open symbol_reference_history symbol

    A row is RESOLVED only when an instrument_id is found.
    """
    configure_logging()
    LOGGER.info("build-price-normalized-from-raw started")

    # Keep the target table present even if this job is invoked alone.
    run_init_price_raw_tables()

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM price_source_daily_normalized")

        # --------------------------------------------------------------
        # Canonical Stooq normalization pipeline.
        # --------------------------------------------------------------
        conn.execute(
            """
            INSERT INTO price_source_daily_normalized (
                normalized_price_id,
                source_name,
                source_row_id,
                raw_symbol,
                instrument_id,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                symbol_resolution_status,
                normalization_notes,
                normalized_at
            )
            WITH open_refs AS (
                SELECT
                    symbol,
                    instrument_id
                FROM symbol_reference_history
                WHERE effective_to IS NULL
            ),
            stooq_base AS (
                SELECT
                    r.raw_id AS source_row_id,
                    r.symbol AS raw_symbol,
                    r.price_date,
                    r.open,
                    r.high,
                    r.low,
                    r.close,
                    r.adj_close,
                    r.volume,
                    exact.instrument_id AS exact_instrument_id,
                    map.normalized_symbol,
                    mapped.instrument_id AS mapped_instrument_id,
                    map.rule_name
                FROM price_source_daily_raw_stooq r
                LEFT JOIN open_refs exact
                  ON exact.symbol = r.symbol
                LEFT JOIN stooq_symbol_normalization_map map
                  ON map.raw_symbol = r.symbol
                LEFT JOIN open_refs mapped
                  ON mapped.symbol = map.normalized_symbol
            ),
            staged AS (
                SELECT
                    ROW_NUMBER() OVER (ORDER BY source_row_id) AS normalized_price_id,
                    'stooq' AS source_name,
                    source_row_id,
                    raw_symbol,
                    COALESCE(exact_instrument_id, mapped_instrument_id) AS instrument_id,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    adj_close,
                    volume,
                    CASE
                        WHEN COALESCE(exact_instrument_id, mapped_instrument_id) IS NOT NULL
                            THEN 'RESOLVED'
                        ELSE 'UNRESOLVED'
                    END AS symbol_resolution_status,
                    CASE
                        WHEN exact_instrument_id IS NOT NULL
                            THEN 'exact raw symbol matched open symbol reference'
                        WHEN mapped_instrument_id IS NOT NULL
                            THEN 'mapped via ' || COALESCE(rule_name, 'stooq_symbol_normalization_map')
                        ELSE 'no matching symbol mapping found'
                    END AS normalization_notes,
                    CURRENT_TIMESTAMP AS normalized_at
                FROM stooq_base
            )
            SELECT
                normalized_price_id,
                source_name,
                source_row_id,
                raw_symbol,
                instrument_id,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                symbol_resolution_status,
                normalization_notes,
                normalized_at
            FROM staged
            """
        )

        price_source_daily_normalized_count = conn.execute(
            "SELECT COUNT(*) FROM price_source_daily_normalized"
        ).fetchone()[0]

        unresolved_symbol_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM price_source_daily_normalized
            WHERE symbol_resolution_status <> 'RESOLVED'
            """
        ).fetchone()[0]

        rows_by_source = conn.execute(
            """
            SELECT source_name, COUNT(*)
            FROM price_source_daily_normalized
            GROUP BY source_name
            ORDER BY source_name
            """
        ).fetchall()

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

        used_normalization_map_table = conn.execute(
            """
            SELECT CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'main'
                      AND table_name = 'stooq_symbol_normalization_map'
                )
                THEN TRUE
                ELSE FALSE
            END
            """
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "build-price-normalized-from-raw",
                "price_source_daily_normalized_count": price_source_daily_normalized_count,
                "unresolved_symbol_count": unresolved_symbol_count,
                "rows_by_source": rows_by_source,
                "duplicated_source_row_id_groups": duplicated_source_row_id_groups,
                "used_normalization_map_table": used_normalization_map_table,
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-price-normalized-from-raw finished")


if __name__ == "__main__":
    run()
