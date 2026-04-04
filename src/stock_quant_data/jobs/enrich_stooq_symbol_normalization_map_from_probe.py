"""
Enrich stooq_symbol_normalization_map from current probe/candidate output.

Current canonical target table:
- stooq_symbol_normalization_map(raw_symbol, normalized_symbol, rule_name, built_at)

This job only handles current format-mapping families already recognized by the
loader:
- WARRANT_DASH_WS  -> SYMBOL-WS => SYMBOL.W
- UNIT_DASH_U      -> SYMBOL-U  => SYMBOL.U
- UNDERSCORE_VARIANT -> SYMBOL_X => SYMBOL$X

This job does NOT create reference identities.
It only adds symbol-format normalization mappings.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Insert missing normalization-map rows inferred from current candidate data.
    """
    configure_logging()
    LOGGER.info("enrich-stooq-symbol-normalization-map-from-probe started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS tmp_probe_format_rows")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_probe_format_rows AS
            SELECT
                raw_symbol,
                CASE
                    WHEN candidate_family = 'WARRANT_DASH_WS'
                        THEN replace(raw_symbol, '-WS', '.W')
                    WHEN candidate_family = 'UNIT_DASH_U'
                        THEN replace(raw_symbol, '-U', '.U')
                    WHEN candidate_family = 'UNDERSCORE_VARIANT'
                        THEN replace(raw_symbol, '_', '$')
                    ELSE NULL
                END AS normalized_symbol,
                CASE
                    WHEN candidate_family = 'WARRANT_DASH_WS'
                        THEN 'DASH_WS_TO_DOT_W'
                    WHEN candidate_family = 'UNIT_DASH_U'
                        THEN 'DASH_U_TO_DOT_U'
                    WHEN candidate_family = 'UNDERSCORE_VARIANT'
                        THEN 'UNDERSCORE_TO_DOLLAR'
                    ELSE NULL
                END AS rule_name
            FROM symbol_reference_candidates_from_unresolved_stooq
            WHERE suggested_action = 'REVIEW_FOR_SYMBOL_FORMAT_MAPPING'
            """
        )

        staged_row_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM tmp_probe_format_rows
            WHERE normalized_symbol IS NOT NULL
              AND rule_name IS NOT NULL
            """
        ).fetchone()[0]

        conn.execute(
            """
            INSERT INTO stooq_symbol_normalization_map (
                raw_symbol,
                normalized_symbol,
                rule_name,
                built_at
            )
            SELECT
                t.raw_symbol,
                t.normalized_symbol,
                t.rule_name,
                NOW() AS built_at
            FROM tmp_probe_format_rows AS t
            LEFT JOIN stooq_symbol_normalization_map AS m
                ON m.raw_symbol = t.raw_symbol
            WHERE t.normalized_symbol IS NOT NULL
              AND t.rule_name IS NOT NULL
              AND m.raw_symbol IS NULL
            """
        )

        inserted_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM stooq_symbol_normalization_map
            WHERE raw_symbol IN (
                SELECT raw_symbol
                FROM tmp_probe_format_rows
            )
            """
        ).fetchone()[0]

        total_map_count = conn.execute(
            "SELECT COUNT(*) FROM stooq_symbol_normalization_map"
        ).fetchone()[0]

        rows_now_present = conn.execute(
            """
            SELECT
                raw_symbol,
                normalized_symbol,
                rule_name
            FROM stooq_symbol_normalization_map
            WHERE raw_symbol IN (
                SELECT raw_symbol
                FROM tmp_probe_format_rows
            )
            ORDER BY raw_symbol
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "enrich-stooq-symbol-normalization-map-from-probe",
                "staged_row_count": staged_row_count,
                "inserted_count": max(0, inserted_count),
                "total_map_count": total_map_count,
                "rows_now_present": rows_now_present,
            }
        )
    finally:
        conn.close()
        LOGGER.info("enrich-stooq-symbol-normalization-map-from-probe finished")


if __name__ == "__main__":
    run()
