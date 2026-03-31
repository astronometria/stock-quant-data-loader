"""
Build a conservative Stooq symbol normalization map.

Important design rule:
- this map must be built from raw Stooq symbols directly
- it must NOT depend on the previous price_source_daily_normalized state

Rules:
1. ABC_A -> ABC$A when target exists
2. ABC-WS -> ABC.W when target exists
3. ABC-U -> ABC.U when target exists
4. ABC-R -> ABC.R when target exists
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    configure_logging()
    LOGGER.info("build-stooq-symbol-normalization-map started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS stooq_symbol_normalization_map")
        conn.execute(
            """
            CREATE TABLE stooq_symbol_normalization_map AS
            WITH raw_symbols AS (
                SELECT DISTINCT raw_symbol
                FROM price_source_daily_raw_stooq
            ),
            candidates_underscore AS (
                SELECT
                    raw_symbol,
                    replace(raw_symbol, '_', '$') AS normalized_symbol,
                    'underscore_to_dollar_if_target_exists' AS normalization_rule,
                    'HIGH' AS confidence
                FROM raw_symbols
                WHERE POSITION('_' IN raw_symbol) > 0
            ),
            candidates_ws AS (
                SELECT
                    raw_symbol,
                    replace(raw_symbol, '-WS', '.W') AS normalized_symbol,
                    'dash_ws_to_dot_w_if_target_exists' AS normalization_rule,
                    'HIGH' AS confidence
                FROM raw_symbols
                WHERE raw_symbol LIKE '%-WS'
            ),
            candidates_u AS (
                SELECT
                    raw_symbol,
                    replace(raw_symbol, '-U', '.U') AS normalized_symbol,
                    'dash_u_to_dot_u_if_target_exists' AS normalization_rule,
                    'HIGH' AS confidence
                FROM raw_symbols
                WHERE raw_symbol LIKE '%-U'
            ),
            candidates_r AS (
                SELECT
                    raw_symbol,
                    replace(raw_symbol, '-R', '.R') AS normalized_symbol,
                    'dash_r_to_dot_r_if_target_exists' AS normalization_rule,
                    'HIGH' AS confidence
                FROM raw_symbols
                WHERE raw_symbol LIKE '%-R'
            ),
            all_candidates AS (
                SELECT * FROM candidates_underscore
                UNION ALL
                SELECT * FROM candidates_ws
                UNION ALL
                SELECT * FROM candidates_u
                UNION ALL
                SELECT * FROM candidates_r
            )
            SELECT
                c.raw_symbol,
                c.normalized_symbol,
                c.normalization_rule,
                c.confidence
            FROM all_candidates AS c
            JOIN (
                SELECT DISTINCT symbol
                FROM symbol_reference_history
            ) AS srh
              ON srh.symbol = c.normalized_symbol
            """
        )

        count_rows = conn.execute(
            "SELECT COUNT(*) FROM stooq_symbol_normalization_map"
        ).fetchone()[0]

        by_rule = conn.execute(
            """
            SELECT normalization_rule, COUNT(*)
            FROM stooq_symbol_normalization_map
            GROUP BY normalization_rule
            ORDER BY normalization_rule
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "build-stooq-symbol-normalization-map",
                "normalization_row_count": count_rows,
                "rows_by_rule": by_rule,
            }
        )
    finally:
        conn.close()

    LOGGER.info("build-stooq-symbol-normalization-map finished")


if __name__ == "__main__":
    run()
