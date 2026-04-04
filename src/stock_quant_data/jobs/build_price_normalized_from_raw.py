"""
Build the canonical normalized price table from raw price sources.

Current canonical source tables:
- price_source_daily_raw_stooq
- price_source_daily_raw_yahoo

Current canonical output table:
- price_source_daily_normalized

Current canonical normalization map:
- stooq_symbol_normalization_map(raw_symbol, normalized_symbol, rule_name, built_at)

Current canonical identity table:
- symbol_reference_history(symbol, instrument_id, exchange, effective_from, effective_to)

Design:
- SQL-first
- deterministic rebuild of normalized table
- no references to legacy table names
- no assumptions about old columns that do not exist anymore
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db
from stock_quant_data.jobs.init_price_raw_tables import run as run_init_price_raw_tables

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild price_source_daily_normalized from the current raw source tables.
    """
    configure_logging()
    LOGGER.info("build-price-normalized-from-raw started")

    # ----------------------------------------------------------------------
    # Ensure required raw/normalized tables exist before the rebuild.
    # This keeps orchestration simpler and makes the job idempotent.
    # ----------------------------------------------------------------------
    run_init_price_raw_tables()

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Full rebuild for deterministic results.
        # ------------------------------------------------------------------
        conn.execute("DELETE FROM price_source_daily_normalized")

        # ------------------------------------------------------------------
        # Canonical Stooq normalization:
        # 1) start from raw_symbol
        # 2) apply explicit symbol-format mapping when one exists
        # 3) resolve instrument_id by matching the normalized symbol against
        #    an open-ended symbol_reference_history row
        #
        # IMPORTANT:
        # - we only use open-ended references for the current loader design
        # - historical PIT price identity can be upgraded later, but this
        #   current build stays consistent with the present schema
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
            WITH mapped AS (
                SELECT
                    r.raw_price_id AS source_row_id,
                    'stooq' AS source_name,
                    r.raw_symbol,
                    COALESCE(m.normalized_symbol, r.raw_symbol) AS candidate_symbol,
                    r.price_date,
                    r.open,
                    r.high,
                    r.low,
                    r.close,
                    CAST(NULL AS DOUBLE) AS adj_close,
                    r.volume,
                    CASE
                        WHEN m.normalized_symbol IS NOT NULL THEN
                            'mapped via stooq_symbol_normalization_map (' || m.rule_name || ')'
                        ELSE
                            'no matching symbol mapping found'
                    END AS normalization_notes
                FROM price_source_daily_raw_stooq AS r
                LEFT JOIN stooq_symbol_normalization_map AS m
                    ON m.raw_symbol = r.raw_symbol
            ),
            resolved AS (
                SELECT
                    source_name,
                    source_row_id,
                    raw_symbol,
                    srh.instrument_id,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    adj_close,
                    volume,
                    CASE
                        WHEN srh.instrument_id IS NOT NULL THEN 'RESOLVED'
                        ELSE 'UNRESOLVED'
                    END AS symbol_resolution_status,
                    normalization_notes
                FROM mapped AS m
                LEFT JOIN symbol_reference_history AS srh
                    ON srh.symbol = m.candidate_symbol
                   AND srh.effective_to IS NULL
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY source_name, source_row_id) AS normalized_price_id,
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
            FROM resolved
            """
        )

        # ------------------------------------------------------------------
        # Optional Yahoo branch:
        # keep the table contract ready even if current runs are Stooq-only.
        # The loader should remain future-proof and schema-consistent.
        # ------------------------------------------------------------------
        yahoo_raw_count = conn.execute(
            "SELECT COUNT(*) FROM price_source_daily_raw_yahoo"
        ).fetchone()[0]

        if yahoo_raw_count > 0:
            current_max_id = conn.execute(
                "SELECT COALESCE(MAX(normalized_price_id), 0) FROM price_source_daily_normalized"
            ).fetchone()[0]

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
                WITH resolved AS (
                    SELECT
                        'yahoo' AS source_name,
                        y.raw_price_id AS source_row_id,
                        y.raw_symbol,
                        srh.instrument_id,
                        y.price_date,
                        y.open,
                        y.high,
                        y.low,
                        y.close,
                        y.adj_close,
                        y.volume,
                        CASE
                            WHEN srh.instrument_id IS NOT NULL THEN 'RESOLVED'
                            ELSE 'UNRESOLVED'
                        END AS symbol_resolution_status,
                        'direct raw_symbol match against open symbol_reference_history' AS normalization_notes
                    FROM price_source_daily_raw_yahoo AS y
                    LEFT JOIN symbol_reference_history AS srh
                        ON srh.symbol = y.raw_symbol
                       AND srh.effective_to IS NULL
                )
                SELECT
                    ? + ROW_NUMBER() OVER (ORDER BY source_name, source_row_id) AS normalized_price_id,
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
                FROM resolved
                """,
                [current_max_id],
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
            SELECT
                source_name,
                COUNT(*) AS row_count
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
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = 'stooq_symbol_normalization_map'
            """
        ).fetchone()[0] > 0

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
