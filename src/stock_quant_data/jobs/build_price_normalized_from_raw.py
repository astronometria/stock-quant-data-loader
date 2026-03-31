"""
Build the unified normalized daily price staging table from raw source tables.

Resolution order for Stooq:
1. direct symbol_reference_history
2. stooq_symbol_normalization_map
3. symbol_manual_override_map
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.jobs.init_price_raw_tables import run as run_init_price_raw_tables
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)

STOOQ_ID_OFFSET = 1_000_000_000_000
YAHOO_ID_OFFSET = 2_000_000_000_000


def run() -> None:
    configure_logging()
    LOGGER.info("build-price-normalized-from-raw started")

    run_init_price_raw_tables()

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS price_source_daily_normalized")

        conn.execute(
            """
            CREATE TABLE price_source_daily_normalized (
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

        conn.execute(
            f"""
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
            WITH staged AS (
                SELECT
                    rs.raw_stooq_id AS source_row_id,
                    rs.raw_symbol,
                    srh_direct.instrument_id AS direct_instrument_id,
                    srh_norm.instrument_id AS normalized_instrument_id,
                    srh_manual.instrument_id AS manual_instrument_id,
                    nm.normalized_symbol,
                    mo.mapped_symbol AS manual_symbol,
                    rs.price_date,
                    rs.open,
                    rs.high,
                    rs.low,
                    rs.close,
                    rs.close AS adj_close,
                    CAST(ROUND(COALESCE(rs.raw_volume, 0)) AS BIGINT) AS volume,
                    ROW_NUMBER() OVER (
                        ORDER BY rs.raw_symbol, rs.price_date, rs.source_file, rs.raw_time, rs.raw_ticker
                    ) AS rn
                FROM price_source_daily_raw_stooq AS rs
                LEFT JOIN symbol_reference_history AS srh_direct
                  ON srh_direct.symbol = rs.raw_symbol
                 AND srh_direct.effective_to IS NULL
                LEFT JOIN stooq_symbol_normalization_map AS nm
                  ON nm.raw_symbol = rs.raw_symbol
                LEFT JOIN symbol_reference_history AS srh_norm
                  ON srh_norm.symbol = nm.normalized_symbol
                 AND srh_norm.effective_to IS NULL
                LEFT JOIN symbol_manual_override_map AS mo
                  ON mo.raw_symbol = rs.raw_symbol
                LEFT JOIN symbol_reference_history AS srh_manual
                  ON srh_manual.symbol = mo.mapped_symbol
                 AND srh_manual.effective_to IS NULL
            )
            SELECT
                {STOOQ_ID_OFFSET} + rn AS normalized_price_id,
                'stooq' AS source_name,
                source_row_id,
                raw_symbol,
                COALESCE(direct_instrument_id, normalized_instrument_id, manual_instrument_id) AS instrument_id,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                CASE
                    WHEN COALESCE(direct_instrument_id, normalized_instrument_id, manual_instrument_id) IS NOT NULL THEN 'RESOLVED'
                    ELSE 'UNRESOLVED'
                END AS symbol_resolution_status,
                CASE
                    WHEN direct_instrument_id IS NOT NULL THEN 'resolved via direct symbol_reference_history'
                    WHEN normalized_instrument_id IS NOT NULL THEN 'resolved via stooq_symbol_normalization_map'
                    WHEN manual_instrument_id IS NOT NULL THEN 'resolved via symbol_manual_override_map'
                    ELSE 'no matching symbol mapping found'
                END AS normalization_notes
            FROM staged
            """
        )

        conn.execute(
            f"""
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
            WITH staged AS (
                SELECT
                    ry.raw_yahoo_id AS source_row_id,
                    ry.raw_symbol,
                    srh.instrument_id,
                    ry.price_date,
                    ry.open,
                    ry.high,
                    ry.low,
                    ry.close,
                    ry.adj_close,
                    ry.volume,
                    ROW_NUMBER() OVER (
                        ORDER BY ry.raw_symbol, ry.price_date, ry.raw_yahoo_id
                    ) AS rn
                FROM price_source_daily_raw_yahoo AS ry
                LEFT JOIN symbol_reference_history AS srh
                  ON srh.symbol = ry.raw_symbol
                 AND srh.effective_to IS NULL
            )
            SELECT
                {YAHOO_ID_OFFSET} + rn AS normalized_price_id,
                'yahoo' AS source_name,
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
                CASE
                    WHEN instrument_id IS NOT NULL THEN 'RESOLVED'
                    ELSE 'UNRESOLVED'
                END AS symbol_resolution_status,
                CASE
                    WHEN instrument_id IS NOT NULL THEN 'resolved via symbol_reference_history'
                    ELSE 'no matching open-ended symbol mapping found'
                END AS normalization_notes
            FROM staged
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

        by_source = conn.execute(
            """
            SELECT source_name, COUNT(*)
            FROM price_source_daily_normalized
            GROUP BY source_name
            ORDER BY source_name
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "build-price-normalized-from-raw",
                "price_source_daily_normalized_count": normalized_count,
                "unresolved_symbol_count": unresolved_count,
                "rows_by_source": by_source,
            }
        )
    finally:
        conn.close()

    LOGGER.info("build-price-normalized-from-raw finished")


if __name__ == "__main__":
    run()
