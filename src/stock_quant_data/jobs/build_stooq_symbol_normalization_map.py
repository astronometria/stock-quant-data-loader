"""
Build the base mechanical Stooq normalization map.

Canonical target schema:
- stooq_symbol_normalization_map(raw_symbol, normalized_symbol, rule_name, built_at)

Important:
- this job owns only mechanical format conversions
- it must not invent identity mappings
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild mechanical normalization rules from currently unresolved raw symbols.
    """
    configure_logging()
    LOGGER.info("build-stooq-symbol-normalization-map started")

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM stooq_symbol_normalization_map")

        # ------------------------------------------------------------------
        # Rule families:
        # - underscore preferred-style -> dollar preferred-style
        # - warrant suffix -WS -> .W
        # - unit suffix -U -> .U
        #
        # We only insert a rule when the normalized symbol already exists as an
        # open-ended reference symbol. This prevents bad speculative mappings.
        # ------------------------------------------------------------------
        conn.execute(
            """
            INSERT INTO stooq_symbol_normalization_map (
                raw_symbol,
                normalized_symbol,
                rule_name,
                built_at
            )
            WITH unresolved AS (
                SELECT DISTINCT raw_symbol
                FROM price_source_daily_raw_stooq
            ),
            staged AS (
                SELECT
                    raw_symbol,
                    CASE
                        WHEN raw_symbol LIKE '%\_%' ESCAPE '\' THEN replace(raw_symbol, '_', '$')
                        WHEN raw_symbol LIKE '%-WS' THEN replace(raw_symbol, '-WS', '.W')
                        WHEN raw_symbol LIKE '%-U' THEN replace(raw_symbol, '-U', '.U')
                        ELSE NULL
                    END AS normalized_symbol,
                    CASE
                        WHEN raw_symbol LIKE '%\_%' ESCAPE '\' THEN 'UNDERSCORE_TO_DOLLAR'
                        WHEN raw_symbol LIKE '%-WS' THEN 'DASH_WS_TO_DOT_W'
                        WHEN raw_symbol LIKE '%-U' THEN 'DASH_U_TO_DOT_U'
                        ELSE NULL
                    END AS rule_name
                FROM unresolved
            )
            SELECT
                s.raw_symbol,
                s.normalized_symbol,
                s.rule_name,
                CURRENT_TIMESTAMP
            FROM staged AS s
            JOIN symbol_reference_history AS srh
                ON srh.symbol = s.normalized_symbol
               AND srh.effective_to IS NULL
            WHERE s.normalized_symbol IS NOT NULL
            """
        )

        row_count = conn.execute(
            "SELECT COUNT(*) FROM stooq_symbol_normalization_map"
        ).fetchone()[0]

        by_rule_name = conn.execute(
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
                "row_count": row_count,
                "rows_by_rule_name": by_rule_name,
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-stooq-symbol-normalization-map finished")


if __name__ == "__main__":
    run()
