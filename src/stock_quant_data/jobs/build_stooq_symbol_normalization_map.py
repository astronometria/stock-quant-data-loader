"""
Build deterministic Stooq symbol normalization rules.

Canonical output table:
- stooq_symbol_normalization_map(raw_symbol, normalized_symbol, rule_name, built_at)

This job derives general-format rules from unresolved raw symbols and keeps
the schema aligned with the active codebase.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild the normalization map from currently unresolved Stooq raw symbols.

    Initial rule families:
    - SYMBOL-WS -> SYMBOL.W
    - SYMBOL-U  -> SYMBOL.U
    - SYMBOL_R  -> SYMBOL.R
    - CLASS-A / CLASS-B style -> .A / .B
    - UNDERSCORE variants -> dollar-style preferred symbol
    """
    configure_logging()
    LOGGER.info("build-stooq-symbol-normalization-map started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS stooq_symbol_normalization_map")
        conn.execute(
            """
            CREATE TABLE stooq_symbol_normalization_map AS
            WITH unresolved_symbols AS (
                SELECT DISTINCT
                    raw_symbol
                FROM price_source_daily_normalized
                WHERE symbol_resolution_status <> 'RESOLVED'
            ),
            mapped AS (
                SELECT
                    raw_symbol,
                    CASE
                        WHEN raw_symbol LIKE '%-WS'
                            THEN regexp_replace(raw_symbol, '-WS$', '.W')
                        WHEN raw_symbol LIKE '%-U'
                            THEN regexp_replace(raw_symbol, '-U$', '.U')
                        WHEN raw_symbol LIKE '%-R'
                            THEN regexp_replace(raw_symbol, '-R$', '.R')
                        WHEN regexp_matches(raw_symbol, '^[A-Z0-9]+-[A-Z]$')
                            THEN regexp_replace(raw_symbol, '-([A-Z])$', '.\\1')
                        WHEN raw_symbol LIKE '%\_%' ESCAPE '\\'
                            THEN regexp_replace(raw_symbol, '_', '$')
                        ELSE NULL
                    END AS normalized_symbol,
                    CASE
                        WHEN raw_symbol LIKE '%-WS' THEN 'DASH_WS_TO_DOT_W'
                        WHEN raw_symbol LIKE '%-U' THEN 'DASH_U_TO_DOT_U'
                        WHEN raw_symbol LIKE '%-R' THEN 'DASH_R_TO_DOT_R'
                        WHEN regexp_matches(raw_symbol, '^[A-Z0-9]+-[A-Z]$') THEN 'DASH_CLASS_TO_DOT_CLASS'
                        WHEN raw_symbol LIKE '%\_%' ESCAPE '\\' THEN 'UNDERSCORE_TO_DOLLAR'
                        ELSE NULL
                    END AS rule_name
                FROM unresolved_symbols
            )
            SELECT
                raw_symbol,
                normalized_symbol,
                rule_name,
                CURRENT_TIMESTAMP AS built_at
            FROM mapped
            WHERE normalized_symbol IS NOT NULL
              AND rule_name IS NOT NULL
            ORDER BY raw_symbol
            """
        )

        total_map_count = conn.execute(
            "SELECT COUNT(*) FROM stooq_symbol_normalization_map"
        ).fetchone()[0]

        rows_by_rule = conn.execute(
            """
            SELECT rule_name, COUNT(*)
            FROM stooq_symbol_normalization_map
            GROUP BY rule_name
            ORDER BY COUNT(*) DESC, rule_name
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "build-stooq-symbol-normalization-map",
                "total_map_count": total_map_count,
                "rows_by_rule": rows_by_rule,
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-stooq-symbol-normalization-map finished")


if __name__ == "__main__":
    run()
