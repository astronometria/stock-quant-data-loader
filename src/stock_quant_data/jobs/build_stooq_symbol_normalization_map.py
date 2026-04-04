"""
Build the canonical Stooq symbol normalization map.

Canonical target schema:
- stooq_symbol_normalization_map(
    raw_symbol,
    normalized_symbol,
    rule_name,
    built_at
  )

Important:
- only current column names are used
- this job performs a deterministic rebuild from unresolved Stooq symbols
- explicit manually-added rows from later enrichment jobs remain possible, but
  this job itself seeds the broad mechanical rules
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild the broad mechanical Stooq normalization map.
    """
    configure_logging()
    LOGGER.info("build-stooq-symbol-normalization-map started")

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM stooq_symbol_normalization_map")

        # ------------------------------------------------------------------
        # Seed broad rules directly from unresolved normalized raw symbols.
        # ------------------------------------------------------------------
        conn.execute(
            """
            INSERT INTO stooq_symbol_normalization_map (
                raw_symbol,
                normalized_symbol,
                rule_name,
                built_at
            )
            WITH unresolved_symbols AS (
                SELECT DISTINCT raw_symbol
                FROM price_source_daily_normalized
                WHERE source_name = 'stooq'
                  AND symbol_resolution_status <> 'RESOLVED'
            ),
            staged AS (
                SELECT
                    raw_symbol,
                    replace(raw_symbol, '_', '$') AS normalized_symbol,
                    'UNDERSCORE_TO_DOLLAR' AS rule_name
                FROM unresolved_symbols
                WHERE raw_symbol LIKE '%\_%' ESCAPE '\'

                UNION ALL

                SELECT
                    raw_symbol,
                    replace(raw_symbol, '-WS', '.W') AS normalized_symbol,
                    'DASH_WS_TO_DOT_W' AS rule_name
                FROM unresolved_symbols
                WHERE raw_symbol LIKE '%-WS'

                UNION ALL

                SELECT
                    raw_symbol,
                    replace(raw_symbol, '-U', '.U') AS normalized_symbol,
                    'DASH_U_TO_DOT_U' AS rule_name
                FROM unresolved_symbols
                WHERE raw_symbol LIKE '%-U'

                UNION ALL

                SELECT
                    raw_symbol,
                    replace(raw_symbol, '-R', '.R') AS normalized_symbol,
                    'DASH_R_TO_DOT_R' AS rule_name
                FROM unresolved_symbols
                WHERE raw_symbol LIKE '%-R'

                UNION ALL

                SELECT
                    raw_symbol,
                    replace(raw_symbol, '-', '.') AS normalized_symbol,
                    'DASH_CLASS_TO_DOT_CLASS' AS rule_name
                FROM unresolved_symbols
                WHERE raw_symbol LIKE '%-%'
                  AND raw_symbol NOT LIKE '%-WS'
                  AND raw_symbol NOT LIKE '%-U'
                  AND raw_symbol NOT LIKE '%-R'
            )
            SELECT DISTINCT
                raw_symbol,
                normalized_symbol,
                rule_name,
                NOW() AS built_at
            FROM staged
            """
        )

        map_count = conn.execute(
            "SELECT COUNT(*) FROM stooq_symbol_normalization_map"
        ).fetchone()[0]

        rows_by_rule = conn.execute(
            """
            SELECT
                rule_name,
                COUNT(*)
            FROM stooq_symbol_normalization_map
            GROUP BY rule_name
            ORDER BY COUNT(*) DESC, rule_name
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "build-stooq-symbol-normalization-map",
                "map_count": map_count,
                "rows_by_rule": rows_by_rule,
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-stooq-symbol-normalization-map finished")


if __name__ == "__main__":
    run()
