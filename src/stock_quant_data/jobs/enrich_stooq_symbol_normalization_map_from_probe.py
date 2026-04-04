"""
Enrich stooq_symbol_normalization_map from the unresolved symbol probe.

Why this job exists
-------------------
The earlier version used the wrong target column name when inserting into
stooq_symbol_normalization_map.

The real schema is:

    raw_symbol
    normalized_symbol
    rule_name
    built_at

This version writes against the real schema and only inserts deterministic
format-mapping candidates that are already surfaced by the probe layer.

Current deterministic mappings handled here
-------------------------------------------
- UNIT_DASH_U        : GRP-U   -> GRP.U
- WARRANT_DASH_WS    : NOTE-WS -> NOTE.W
- UNDERSCORE_VARIANT : SCE_K   -> SCE$K

We keep this job intentionally narrow and explicit:
- only rows already classified by the candidate/probe layer
- only deterministic transformations
- no fuzzy guessing
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Insert deterministic Stooq symbol normalization mappings from the candidate table.
    """
    configure_logging()
    LOGGER.info("enrich-stooq-symbol-normalization-map-from-probe started")

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Stage deterministic mappings from the candidate table.
        #
        # We purposely use the already-built candidate classification rather
        # than re-inventing parsing logic again in Python.
        # ------------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_probe_format_mappings")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_probe_format_mappings AS
            SELECT
                raw_symbol,
                CASE
                    WHEN candidate_family = 'UNIT_DASH_U'
                        THEN replace(raw_symbol, '-U', '.U')
                    WHEN candidate_family = 'WARRANT_DASH_WS'
                        THEN replace(raw_symbol, '-WS', '.W')
                    WHEN candidate_family = 'UNDERSCORE_VARIANT'
                        THEN replace(raw_symbol, '_', '$')
                    ELSE NULL
                END AS normalized_symbol,
                CASE
                    WHEN candidate_family = 'UNIT_DASH_U'
                        THEN 'DASH_U_TO_DOT_U'
                    WHEN candidate_family = 'WARRANT_DASH_WS'
                        THEN 'DASH_WS_TO_DOT_W'
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
            FROM tmp_probe_format_mappings
            WHERE normalized_symbol IS NOT NULL
              AND rule_name IS NOT NULL
            """
        ).fetchone()[0]

        # ------------------------------------------------------------------
        # Insert only mappings not already present.
        #
        # The key is the raw_symbol here because the map is conceptually a
        # single normalization decision per raw Stooq symbol.
        # ------------------------------------------------------------------
        inserted_count = conn.execute(
            """
            INSERT INTO stooq_symbol_normalization_map (
                raw_symbol,
                normalized_symbol,
                rule_name,
                built_at
            )
            SELECT
                s.raw_symbol,
                s.normalized_symbol,
                s.rule_name,
                CURRENT_TIMESTAMP
            FROM tmp_probe_format_mappings s
            LEFT JOIN stooq_symbol_normalization_map existing
                ON existing.raw_symbol = s.raw_symbol
            WHERE s.normalized_symbol IS NOT NULL
              AND s.rule_name IS NOT NULL
              AND existing.raw_symbol IS NULL
            RETURNING raw_symbol
            """
        ).fetchall()

        total_map_count = conn.execute(
            "SELECT COUNT(*) FROM stooq_symbol_normalization_map"
        ).fetchone()[0]

        inserted_rows = conn.execute(
            """
            SELECT
                raw_symbol,
                normalized_symbol,
                rule_name
            FROM stooq_symbol_normalization_map
            WHERE raw_symbol IN (
                SELECT raw_symbol
                FROM tmp_probe_format_mappings
            )
            ORDER BY raw_symbol
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "enrich-stooq-symbol-normalization-map-from-probe",
                "staged_row_count": staged_row_count,
                "inserted_count": len(inserted_count),
                "total_map_count": total_map_count,
                "rows_now_present": inserted_rows,
            }
        )
    finally:
        conn.close()
        LOGGER.info("enrich-stooq-symbol-normalization-map-from-probe finished")


if __name__ == "__main__":
    run()
