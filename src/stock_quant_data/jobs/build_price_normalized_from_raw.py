"""
Build canonical normalized price rows from the raw price layer.

This job uses:
- price_source_daily_raw_stooq
- symbol_reference_history
- stooq_symbol_normalization_map

Output:
- price_source_daily_normalized

Important schema notes:
- current raw Stooq table uses raw_price_id and raw_symbol
- current raw Stooq table does not contain adj_close
- current normalized table does contain adj_close, so we populate it with close
  for Stooq until a true adjusted source is introduced

Resolution strategy:
1) Direct exact match on raw_symbol against currently open symbol references
2) Generic Stooq normalization:
   - uppercase
   - trim
   - strip trailing .US
   - replace underscore with dash
   Then match against currently open symbol references
3) Explicit normalization-map fallback for special symbols
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db
from stock_quant_data.jobs.init_price_raw_tables import run as run_init_price_raw_tables

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """Rebuild the canonical normalized price table."""
    configure_logging()
    LOGGER.info("build-price-normalized-from-raw started")

    run_init_price_raw_tables()

    conn = connect_build_db()
    try:
        # Rebuild from scratch for deterministic output.
        conn.execute("DELETE FROM price_source_daily_normalized")

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
                    upper(trim(symbol)) AS normalized_open_symbol,
                    symbol,
                    instrument_id
                FROM symbol_reference_history
                WHERE effective_to IS NULL
            ),
            stooq_base AS (
                SELECT
                    r.raw_price_id AS source_row_id,
                    r.raw_symbol AS raw_symbol,
                    r.price_date,
                    r.open,
                    r.high,
                    r.low,
                    r.close,
                    r.close AS adj_close,
                    r.volume,

                    -- Exact raw match first. This can help if the raw symbol
                    -- already matches the open symbol reference convention.
                    exact.instrument_id AS exact_instrument_id,

                    -- Generic SQL-first normalization for native Stooq symbols.
                    -- Examples:
                    --   AAPL.US   -> AAPL
                    --   ABR_D.US  -> ABR-D
                    upper(
                        replace(
                            replace(trim(r.raw_symbol), '.US', ''),
                            '_',
                            '-'
                        )
                    ) AS normalized_probe_symbol,

                    generic.instrument_id AS generic_instrument_id,

                    -- Specialized fallback map for preferred / special series.
                    map.normalized_symbol,
                    upper(
                        replace(
                            replace(trim(map.normalized_symbol), '.US', ''),
                            '_',
                            '-'
                        )
                    ) AS normalized_map_symbol,
                    mapped.instrument_id AS mapped_instrument_id,
                    map.rule_name
                FROM price_source_daily_raw_stooq r
                LEFT JOIN open_refs exact
                  ON exact.normalized_open_symbol = upper(trim(r.raw_symbol))
                LEFT JOIN open_refs generic
                  ON generic.normalized_open_symbol = upper(
                        replace(
                            replace(trim(r.raw_symbol), '.US', ''),
                            '_',
                            '-'
                        )
                     )
                LEFT JOIN stooq_symbol_normalization_map map
                  ON map.raw_symbol = r.raw_symbol
                LEFT JOIN open_refs mapped
                  ON mapped.normalized_open_symbol = upper(
                        replace(
                            replace(trim(map.normalized_symbol), '.US', ''),
                            '_',
                            '-'
                        )
                     )
            ),
            staged AS (
                SELECT
                    ROW_NUMBER() OVER (ORDER BY source_row_id) AS normalized_price_id,
                    'stooq' AS source_name,
                    source_row_id,
                    raw_symbol,
                    COALESCE(
                        exact_instrument_id,
                        generic_instrument_id,
                        mapped_instrument_id
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
                            exact_instrument_id,
                            generic_instrument_id,
                            mapped_instrument_id
                        ) IS NOT NULL
                            THEN 'RESOLVED'
                        ELSE 'UNRESOLVED'
                    END AS symbol_resolution_status,
                    CASE
                        WHEN exact_instrument_id IS NOT NULL
                            THEN 'exact raw symbol matched open symbol reference'
                        WHEN generic_instrument_id IS NOT NULL
                            THEN 'generic stooq normalization matched open symbol reference'
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
