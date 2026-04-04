"""
Build the unified normalized daily price staging table from raw source tables.

Resolution order for Stooq:
1. direct symbol_reference_history
2. stooq_symbol_normalization_map
3. symbol_manual_override_map

Important hardening rule:
- Never join directly against raw reference tables that may contain duplicate
  open-ended rows for the same symbol.
- We first build deterministic one-row-per-key temp tables.
- This prevents row multiplication in price_source_daily_normalized even if
  upstream reference builders temporarily emit duplicate open-ended rows.

This job remains SQL-first:
- Python only orchestrates setup and reporting.
- DuckDB performs the actual staging, deduplication, and inserts.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.jobs.init_price_raw_tables import run as run_init_price_raw_tables
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)

STOOQ_ID_OFFSET = 1_000_000_000_000
YAHOO_ID_OFFSET = 2_000_000_000_000


def _table_exists(conn, qualified_table_name: str) -> bool:
    """
    Check table existence using information_schema.

    Accepted formats:
    - main.table_name
    - table_name
    """
    if "." in qualified_table_name:
        schema_name, table_name = qualified_table_name.split(".", 1)
    else:
        schema_name = "main"
        table_name = qualified_table_name

    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_name = ?
        """,
        [schema_name, table_name],
    ).fetchone()

    return bool(row and row[0] > 0)


def run() -> None:
    """
    Rebuild price_source_daily_normalized.

    Main safety properties:
    - deterministic one-row-per-source-row output
    - defensive deduplication of reference joins
    - optional use of stooq_symbol_normalization_map when the table exists
    """
    configure_logging()
    LOGGER.info("build-price-normalized-from-raw started")

    # Ensure the raw/normalized price tables exist before rebuilding.
    run_init_price_raw_tables()

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Probe whether the normalization map exists.
        #
        # This lets the rebuild pipeline run a "pre-norm" pass safely before
        # the normalization map has been materialized, then a second pass after
        # the map exists.
        # ------------------------------------------------------------------
        has_normalization_map_table = _table_exists(conn, "main.stooq_symbol_normalization_map")

        # ------------------------------------------------------------------
        # Build a deterministic one-row-per-symbol current open reference view.
        #
        # Why:
        # - symbol_reference_history may temporarily contain duplicate
        #   open-ended rows for the same symbol
        # - direct joins against it can multiply price rows
        #
        # Selection rule:
        # - prefer primary rows
        # - then most recent effective_from
        # - then largest history id / instrument id as deterministic tie-break
        # ------------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_current_open_symbol_reference_one")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_current_open_symbol_reference_one AS
            SELECT
                instrument_id,
                symbol,
                exchange,
                is_primary,
                effective_from,
                effective_to,
                symbol_reference_history_id
            FROM symbol_reference_history
            WHERE effective_to IS NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY symbol
                ORDER BY
                    CASE WHEN is_primary THEN 1 ELSE 0 END DESC,
                    effective_from DESC NULLS LAST,
                    symbol_reference_history_id DESC,
                    instrument_id DESC
            ) = 1
            """
        )

        # ------------------------------------------------------------------
        # Build a deterministic one-row-per-raw-symbol manual override view.
        #
        # Why:
        # - even if manual override data is accidentally duplicated later,
        #   normalization should still stay one-row-per-source-row.
        # ------------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_symbol_manual_override_one")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_symbol_manual_override_one AS
            SELECT
                raw_symbol,
                mapped_symbol
            FROM symbol_manual_override_map
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY raw_symbol
                ORDER BY mapped_symbol
            ) = 1
            """
        )

        # ------------------------------------------------------------------
        # Build a deterministic one-row-per-raw-symbol normalization map view.
        #
        # If the table does not exist yet, create an empty compatible temp
        # table so the rest of the SQL remains stable.
        # ------------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_stooq_symbol_normalization_map_one")
        if has_normalization_map_table:
            conn.execute(
                """
                CREATE TEMP TABLE tmp_stooq_symbol_normalization_map_one AS
                SELECT
                    raw_symbol,
                    normalized_symbol,
                    rule_name
                FROM stooq_symbol_normalization_map
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY raw_symbol
                    ORDER BY
                        rule_name,
                        normalized_symbol
                ) = 1
                """
            )
        else:
            conn.execute(
                """
                CREATE TEMP TABLE tmp_stooq_symbol_normalization_map_one AS
                SELECT
                    CAST(NULL AS VARCHAR) AS raw_symbol,
                    CAST(NULL AS VARCHAR) AS normalized_symbol,
                    CAST(NULL AS VARCHAR) AS rule_name
                WHERE FALSE
                """
            )

        # ------------------------------------------------------------------
        # Rebuild normalized table from scratch.
        # ------------------------------------------------------------------
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

        # ------------------------------------------------------------------
        # Insert Stooq rows.
        #
        # Note:
        # - every join target here is already deduplicated
        # - this prevents source_row_id multiplication
        # ------------------------------------------------------------------
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
                        ORDER BY
                            rs.raw_symbol,
                            rs.price_date,
                            rs.source_file,
                            rs.raw_time,
                            rs.raw_ticker,
                            rs.raw_stooq_id
                    ) AS rn
                FROM price_source_daily_raw_stooq AS rs

                LEFT JOIN tmp_current_open_symbol_reference_one AS srh_direct
                  ON srh_direct.symbol = rs.raw_symbol

                LEFT JOIN tmp_stooq_symbol_normalization_map_one AS nm
                  ON nm.raw_symbol = rs.raw_symbol

                LEFT JOIN tmp_current_open_symbol_reference_one AS srh_norm
                  ON srh_norm.symbol = nm.normalized_symbol

                LEFT JOIN tmp_symbol_manual_override_one AS mo
                  ON mo.raw_symbol = rs.raw_symbol

                LEFT JOIN tmp_current_open_symbol_reference_one AS srh_manual
                  ON srh_manual.symbol = mo.mapped_symbol
            )
            SELECT
                {STOOQ_ID_OFFSET} + rn AS normalized_price_id,
                'stooq' AS source_name,
                source_row_id,
                raw_symbol,
                COALESCE(
                    direct_instrument_id,
                    normalized_instrument_id,
                    manual_instrument_id
                ) AS instrument_id,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                CASE
                    WHEN COALESCE(
                        direct_instrument_id,
                        normalized_instrument_id,
                        manual_instrument_id
                    ) IS NOT NULL THEN 'RESOLVED'
                    ELSE 'UNRESOLVED'
                END AS symbol_resolution_status,
                CASE
                    WHEN direct_instrument_id IS NOT NULL
                        THEN 'resolved via direct symbol_reference_history'
                    WHEN normalized_instrument_id IS NOT NULL
                        THEN 'resolved via stooq_symbol_normalization_map'
                    WHEN manual_instrument_id IS NOT NULL
                        THEN 'resolved via symbol_manual_override_map'
                    ELSE 'no matching symbol mapping found'
                END AS normalization_notes
            FROM staged
            """
        )

        # ------------------------------------------------------------------
        # Insert Yahoo rows.
        #
        # Same hardening rule: only join against the deduplicated current
        # reference view.
        # ------------------------------------------------------------------
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
                        ORDER BY
                            ry.raw_symbol,
                            ry.price_date,
                            ry.raw_yahoo_id
                    ) AS rn
                FROM price_source_daily_raw_yahoo AS ry
                LEFT JOIN tmp_current_open_symbol_reference_one AS srh
                  ON srh.symbol = ry.raw_symbol
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
                    WHEN instrument_id IS NOT NULL
                        THEN 'resolved via symbol_reference_history'
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

        duplicated_source_row_id_groups = conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT source_name, source_row_id
                FROM price_source_daily_normalized
                GROUP BY 1, 2
                HAVING COUNT(*) > 1
            ) AS t
            """
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "build-price-normalized-from-raw",
                "price_source_daily_normalized_count": normalized_count,
                "unresolved_symbol_count": unresolved_count,
                "rows_by_source": by_source,
                "duplicated_source_row_id_groups": duplicated_source_row_id_groups,
                "used_normalization_map_table": has_normalization_map_table,
            }
        )
    finally:
        conn.close()

    LOGGER.info("build-price-normalized-from-raw finished")


if __name__ == "__main__":
    run()
