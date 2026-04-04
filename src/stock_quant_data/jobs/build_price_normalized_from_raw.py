"""
Build normalized prices from canonical raw tables.

Key rules:
- use only current canonical table names
- resolve through symbol_reference_history open intervals first
- if a normalization map transforms a raw symbol into a canonical symbol,
  resolve using the transformed symbol
- preserve raw_symbol in the normalized output for auditability
- never introduce duplicate source_row_id groups for a given source
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db
from stock_quant_data.jobs.init_price_raw_tables import run as run_init_price_raw_tables

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild price_source_daily_normalized from raw source tables.

    Current implementation normalizes Stooq data and keeps the schema aligned
    with the current repo contract.
    """
    configure_logging()
    LOGGER.info("build-price-normalized-from-raw started")

    # Keep the table creation dependency explicit and local.
    run_init_price_raw_tables()

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM price_source_daily_normalized")

        # --------------------------------------------------------------
        # Stage open symbol reference intervals only once.
        # This keeps the main insert query simpler and easier to audit.
        # --------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_open_symbol_reference")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_open_symbol_reference AS
            SELECT
                symbol,
                instrument_id,
                exchange,
                effective_from,
                effective_to
            FROM symbol_reference_history
            WHERE effective_to IS NULL
            """
        )

        # --------------------------------------------------------------
        # Stage normalization map.
        # A missing row means raw_symbol should be attempted as-is.
        # --------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_stooq_norm_map")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_stooq_norm_map AS
            SELECT
                raw_symbol,
                normalized_symbol,
                rule_name
            FROM stooq_symbol_normalization_map
            """
        )

        # --------------------------------------------------------------
        # Build normalized Stooq rows.
        #
        # Resolution order:
        # 1) mapped symbol from normalization map
        # 2) raw symbol as-is
        #
        # Open-interval resolution:
        # - match on current open refs only
        # - respect effective_from in case an open ref was back-extended
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
            WITH stooq_base AS (
                SELECT
                    raw_price_id AS source_row_id,
                    raw_symbol,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    CAST(NULL AS DOUBLE) AS adj_close,
                    volume
                FROM price_source_daily_raw_stooq
            ),
            staged AS (
                SELECT
                    b.source_row_id,
                    b.raw_symbol,
                    b.price_date,
                    b.open,
                    b.high,
                    b.low,
                    b.close,
                    b.adj_close,
                    b.volume,
                    m.normalized_symbol,
                    m.rule_name,
                    COALESCE(m.normalized_symbol, b.raw_symbol) AS attempted_symbol
                FROM stooq_base b
                LEFT JOIN tmp_stooq_norm_map m
                    ON m.raw_symbol = b.raw_symbol
            ),
            resolved AS (
                SELECT
                    s.source_row_id,
                    s.raw_symbol,
                    s.price_date,
                    s.open,
                    s.high,
                    s.low,
                    s.close,
                    s.adj_close,
                    s.volume,
                    s.normalized_symbol,
                    s.rule_name,
                    r.instrument_id,
                    CASE
                        WHEN r.instrument_id IS NOT NULL THEN 'RESOLVED'
                        ELSE 'UNRESOLVED'
                    END AS symbol_resolution_status,
                    CASE
                        WHEN r.instrument_id IS NOT NULL AND s.normalized_symbol IS NOT NULL
                            THEN 'mapped via ' || s.rule_name || ' to ' || s.normalized_symbol
                        WHEN r.instrument_id IS NOT NULL
                            THEN NULL
                        WHEN s.normalized_symbol IS NOT NULL
                            THEN 'mapped to ' || s.normalized_symbol || ' but no matching symbol reference found'
                        ELSE 'no matching symbol mapping found'
                    END AS normalization_notes
                FROM staged s
                LEFT JOIN tmp_open_symbol_reference r
                    ON r.symbol = s.attempted_symbol
                   AND s.price_date >= r.effective_from
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY source_row_id) AS normalized_price_id,
                'stooq' AS source_name,
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
                CURRENT_TIMESTAMP AS normalized_at
            FROM resolved
            """
        )

        normalized_count = conn.execute(
            "SELECT COUNT(*) FROM price_source_daily_normalized"
        ).fetchone()[0]

        unresolved_count = conn.execute(
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

        duplicate_source_row_groups = conn.execute(
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
                "job": "build-price-normalized-from-raw",
                "price_source_daily_normalized_count": normalized_count,
                "unresolved_symbol_count": unresolved_count,
                "rows_by_source": rows_by_source,
                "duplicated_source_row_id_groups": duplicate_source_row_groups,
                "used_normalization_map_table": True,
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-price-normalized-from-raw finished")


if __name__ == "__main__":
    run()
