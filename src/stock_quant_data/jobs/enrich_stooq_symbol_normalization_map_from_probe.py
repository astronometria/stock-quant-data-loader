"""
Append deterministic Stooq normalization rules from probe/candidate tables.

This module only inserts rows that match the canonical
stooq_symbol_normalization_map schema:

- raw_symbol
- normalized_symbol
- rule_name
- built_at

It does not assume any legacy columns such as mapped_symbol.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Add deterministic format-mapping rows discovered by the unresolved probe.

    Current supported candidate families:
    - WARRANT_DASH_WS  -> .W
    - UNIT_DASH_U      -> .U
    - UNDERSCORE_VARIANT -> $
    """
    configure_logging()
    LOGGER.info("enrich-stooq-symbol-normalization-map-from-probe started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS tmp_probe_format_map")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_probe_format_map AS
            SELECT
                raw_symbol,
                CASE
                    WHEN candidate_family = 'WARRANT_DASH_WS'
                        THEN regexp_replace(raw_symbol, '-WS$', '.W')
                    WHEN candidate_family = 'UNIT_DASH_U'
                        THEN regexp_replace(raw_symbol, '-U$', '.U')
                    WHEN candidate_family = 'UNDERSCORE_VARIANT'
                        THEN regexp_replace(raw_symbol, '_', '$')
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
            FROM tmp_probe_format_map
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
                p.raw_symbol,
                p.normalized_symbol,
                p.rule_name,
                CURRENT_TIMESTAMP
            FROM tmp_probe_format_map p
            LEFT JOIN stooq_symbol_normalization_map m
              ON m.raw_symbol = p.raw_symbol
             AND m.normalized_symbol = p.normalized_symbol
             AND m.rule_name = p.rule_name
            WHERE p.normalized_symbol IS NOT NULL
              AND p.rule_name IS NOT NULL
              AND m.raw_symbol IS NULL
            """
        )

        inserted_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM tmp_probe_format_map p
            JOIN stooq_symbol_normalization_map m
              ON m.raw_symbol = p.raw_symbol
             AND m.normalized_symbol = p.normalized_symbol
             AND m.rule_name = p.rule_name
            WHERE p.normalized_symbol IS NOT NULL
              AND p.rule_name IS NOT NULL
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
                'ALUR-WS', 'DC-WS', 'FTW-U', 'GRP-U', 'NOTE-WS', 'SCE_K'
            )
            ORDER BY raw_symbol
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "enrich-stooq-symbol-normalization-map-from-probe",
                "staged_row_count": staged_row_count,
                "inserted_count": inserted_count,
                "total_map_count": total_map_count,
                "rows_now_present": rows_now_present,
            }
        )
    finally:
        conn.close()
        LOGGER.info("enrich-stooq-symbol-normalization-map-from-probe finished")


if __name__ == "__main__":
    run()
