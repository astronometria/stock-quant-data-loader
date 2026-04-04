"""
Build the Stooq symbol normalization map.

This map only handles deterministic formatting transforms.
It must remain separate from identity creation logic.

Canonical output schema:
- raw_symbol
- normalized_symbol
- rule_name
- built_at
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild deterministic normalization rules for currently unresolved Stooq symbols.
    """
    configure_logging()
    LOGGER.info("build-stooq-symbol-normalization-map started")

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM stooq_symbol_normalization_map")

        # --------------------------------------------------------------
        # Rule 1: underscore preferred-share style -> dollar form
        # Example: SCE_K -> SCE$K
        # --------------------------------------------------------------
        conn.execute(
            """
            INSERT INTO stooq_symbol_normalization_map (
                raw_symbol,
                normalized_symbol,
                rule_name,
                built_at
            )
            SELECT DISTINCT
                raw_symbol,
                regexp_replace(raw_symbol, '_', '$') AS normalized_symbol,
                'UNDERSCORE_TO_DOLLAR' AS rule_name,
                CURRENT_TIMESTAMP
            FROM price_source_daily_normalized
            WHERE symbol_resolution_status <> 'RESOLVED'
              AND raw_symbol LIKE '%\_%' ESCAPE '\'
            """
        )

        # --------------------------------------------------------------
        # Rule 2: warrant dash-WS -> dot-W
        # Example: NOTE-WS -> NOTE.W
        # --------------------------------------------------------------
        conn.execute(
            """
            INSERT INTO stooq_symbol_normalization_map (
                raw_symbol,
                normalized_symbol,
                rule_name,
                built_at
            )
            SELECT DISTINCT
                raw_symbol,
                regexp_replace(raw_symbol, '-WS$', '.W') AS normalized_symbol,
                'DASH_WS_TO_DOT_W' AS rule_name,
                CURRENT_TIMESTAMP
            FROM price_source_daily_normalized
            WHERE symbol_resolution_status <> 'RESOLVED'
              AND raw_symbol LIKE '%-WS'
            """
        )

        # --------------------------------------------------------------
        # Rule 3: unit dash-U -> dot-U
        # Example: GRP-U -> GRP.U
        # --------------------------------------------------------------
        conn.execute(
            """
            INSERT INTO stooq_symbol_normalization_map (
                raw_symbol,
                normalized_symbol,
                rule_name,
                built_at
            )
            SELECT DISTINCT
                raw_symbol,
                regexp_replace(raw_symbol, '-U$', '.U') AS normalized_symbol,
                'DASH_U_TO_DOT_U' AS rule_name,
                CURRENT_TIMESTAMP
            FROM price_source_daily_normalized
            WHERE symbol_resolution_status <> 'RESOLVED'
              AND raw_symbol LIKE '%-U'
            """
        )

        # --------------------------------------------------------------
        # Rule 4: rights dash-R -> dot-R
        # --------------------------------------------------------------
        conn.execute(
            """
            INSERT INTO stooq_symbol_normalization_map (
                raw_symbol,
                normalized_symbol,
                rule_name,
                built_at
            )
            SELECT DISTINCT
                raw_symbol,
                regexp_replace(raw_symbol, '-R$', '.R') AS normalized_symbol,
                'DASH_R_TO_DOT_R' AS rule_name,
                CURRENT_TIMESTAMP
            FROM price_source_daily_normalized
            WHERE symbol_resolution_status <> 'RESOLVED'
              AND raw_symbol LIKE '%-R'
            """
        )

        # --------------------------------------------------------------
        # Rule 5: class share dash-A/B/C -> dot-A/B/C
        # This is intentionally conservative: one trailing class segment only.
        # --------------------------------------------------------------
        conn.execute(
            """
            INSERT INTO stooq_symbol_normalization_map (
                raw_symbol,
                normalized_symbol,
                rule_name,
                built_at
            )
            SELECT DISTINCT
                raw_symbol,
                regexp_replace(raw_symbol, '-([A-Z])$', '.\\1') AS normalized_symbol,
                'DASH_CLASS_TO_DOT_CLASS' AS rule_name,
                CURRENT_TIMESTAMP
            FROM price_source_daily_normalized
            WHERE symbol_resolution_status <> 'RESOLVED'
              AND regexp_matches(raw_symbol, '-[A-Z]$')
            """
        )

        # --------------------------------------------------------------
        # Remove no-op and duplicate rows after all rule inserts.
        # --------------------------------------------------------------
        conn.execute(
            """
            DELETE FROM stooq_symbol_normalization_map
            WHERE normalized_symbol IS NULL
               OR normalized_symbol = raw_symbol
            """
        )

        conn.execute(
            """
            CREATE OR REPLACE TEMP TABLE tmp_norm_map_dedup AS
            SELECT
                raw_symbol,
                normalized_symbol,
                rule_name,
                MIN(built_at) AS built_at
            FROM stooq_symbol_normalization_map
            GROUP BY raw_symbol, normalized_symbol, rule_name
            """
        )

        conn.execute("DELETE FROM stooq_symbol_normalization_map")
        conn.execute(
            """
            INSERT INTO stooq_symbol_normalization_map (
                raw_symbol,
                normalized_symbol,
                rule_name,
                built_at
            )
            SELECT
                raw_symbol,
                normalized_symbol,
                rule_name,
                built_at
            FROM tmp_norm_map_dedup
            """
        )

        total_count = conn.execute(
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
                "row_count": total_count,
                "rows_by_rule": rows_by_rule,
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-stooq-symbol-normalization-map finished")


if __name__ == "__main__":
    run()
