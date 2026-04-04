"""
Build the canonical normalized price table from raw sources.

Important:
- current source tables only
- current symbol resolution tables only
- current normalization map only
- no legacy aliases
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db
from stock_quant_data.jobs.init_price_raw_tables import run as run_init_price_raw_tables

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild price_source_daily_normalized from canonical raw tables.

    Current behavior:
    - load from Stooq raw table
    - apply direct raw symbol match first
    - apply stooq_symbol_normalization_map second
    - resolve only against current open-ended symbol_reference_history rows
    """
    configure_logging()
    LOGGER.info("build-price-normalized-from-raw started")

    # Keep the table bootstrap local and deterministic.
    run_init_price_raw_tables()

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM price_source_daily_normalized")

        # ------------------------------------------------------------------
        # Rebuild normalized rows in one SQL pass.
        #
        # Resolution order:
        # 1. exact raw symbol -> open symbol reference
        # 2. mapped normalized symbol -> open symbol reference
        # 3. unresolved
        #
        # normalization_notes is deliberately human-readable because probes use
        # it for debugging and review.
        # ------------------------------------------------------------------
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
                normalization_notes
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
            mapped AS (
                SELECT
                    b.*,
                    m.normalized_symbol
                FROM stooq_base AS b
                LEFT JOIN stooq_symbol_normalization_map AS m
                    ON m.raw_symbol = b.raw_symbol
            ),
            exact_ref AS (
                SELECT
                    m.source_row_id,
                    srh.instrument_id,
                    srh.symbol,
                    srh.exchange
                FROM mapped AS m
                JOIN symbol_reference_history AS srh
                    ON srh.symbol = m.raw_symbol
                   AND srh.effective_to IS NULL
            ),
            mapped_ref AS (
                SELECT
                    m.source_row_id,
                    srh.instrument_id,
                    srh.symbol,
                    srh.exchange
                FROM mapped AS m
                JOIN symbol_reference_history AS srh
                    ON srh.symbol = m.normalized_symbol
                   AND srh.effective_to IS NULL
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY b.source_row_id) AS normalized_price_id,
                'stooq' AS source_name,
                b.source_row_id,
                b.raw_symbol,
                COALESCE(er.instrument_id, mr.instrument_id) AS instrument_id,
                b.price_date,
                b.open,
                b.high,
                b.low,
                b.close,
                b.adj_close,
                b.volume,
                CASE
                    WHEN er.instrument_id IS NOT NULL THEN 'RESOLVED'
                    WHEN mr.instrument_id IS NOT NULL THEN 'RESOLVED'
                    ELSE 'UNRESOLVED'
                END AS symbol_resolution_status,
                CASE
                    WHEN er.instrument_id IS NOT NULL THEN
                        'resolved by exact raw symbol match'
                    WHEN mr.instrument_id IS NOT NULL THEN
                        'resolved via normalization map: ' || b.raw_symbol || ' -> ' || b.normalized_symbol
                    ELSE
                        'no matching symbol mapping found'
                END AS normalization_notes
            FROM mapped AS b
            LEFT JOIN exact_ref AS er
                ON er.source_row_id = b.source_row_id
            LEFT JOIN mapped_ref AS mr
                ON mr.source_row_id = b.source_row_id
            ORDER BY b.source_row_id
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
                SELECT source_name, source_row_id
                FROM price_source_daily_normalized
                GROUP BY source_name, source_row_id
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]

        used_normalization_map_table = conn.execute(
            """
            SELECT COUNT(*) > 0
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = 'stooq_symbol_normalization_map'
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
                "used_normalization_map_table": bool(used_normalization_map_table),
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-price-normalized-from-raw finished")


if __name__ == "__main__":
    run()
