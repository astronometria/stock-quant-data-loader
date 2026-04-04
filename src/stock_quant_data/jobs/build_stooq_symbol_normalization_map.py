"""
Build a SQL-first normalization map for unresolved Stooq symbols.

Why this job exists:
- We want symbol-shape normalization rules to live in one place.
- We only create mappings when the proposed target symbol already exists
  in the current open-ended symbol_reference_history.
- This keeps the normalization layer conservative and avoids inventing
  identities that are not yet present in master data.

Current supported safe rule families:
- UNDERSCORE_TO_DOLLAR
    Example: BAC_E -> BAC$E
    Example: TY_   -> TY$
- DASH_CLASS_TO_DOT_CLASS
    Example: BRK-B -> BRK.B
- DASH_WS_TO_DOT_W
    Example: ACHR-WS -> ACHR.W
- DASH_U_TO_DOT_U
    Example: SOME-U -> SOME.U   (only if target exists)
- DASH_R_TO_DOT_R
    Example: SOME-R -> SOME.R   (only if target exists)

Important design rule:
- We do NOT create speculative mappings for trailing no-separator suffixes
  like AUUDW -> AUUD.W unless we later add a separate verified rule family.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild the Stooq normalization map from current unresolved symbols.

    SQL-first design:
    - Python only orchestrates the rebuild.
    - DuckDB performs:
      - unresolved symbol extraction
      - candidate generation
      - target existence validation
      - final map materialization
    """
    configure_logging()
    LOGGER.info("build-stooq-symbol-normalization-map started")

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Rebuild from scratch every time so the normalization map always
        # reflects the current reference layer.
        # ------------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS stooq_symbol_normalization_map")

        conn.execute(
            """
            CREATE TABLE stooq_symbol_normalization_map AS
            WITH unresolved AS (
                SELECT DISTINCT
                    raw_symbol
                FROM price_source_daily_normalized
                WHERE source_name = 'stooq'
                  AND symbol_resolution_status <> 'RESOLVED'
                  AND raw_symbol IS NOT NULL
                  AND raw_symbol <> ''
            ),

            -- --------------------------------------------------------------
            -- Rule family 1:
            -- Any underscore variant maps to a dollar variant, but only when
            -- the target already exists as an open-ended reference symbol.
            --
            -- Examples:
            --   BAC_E  -> BAC$E
            --   TY_    -> TY$
            --   ETI_   -> ETI$
            -- --------------------------------------------------------------
            underscore_to_dollar AS (
                SELECT DISTINCT
                    u.raw_symbol,
                    replace(u.raw_symbol, '_', '$') AS normalized_symbol,
                    'UNDERSCORE_TO_DOLLAR' AS rule_name
                FROM unresolved AS u
                JOIN symbol_reference_history AS srh
                  ON srh.symbol = replace(u.raw_symbol, '_', '$')
                 AND srh.effective_to IS NULL
                WHERE POSITION('_' IN u.raw_symbol) > 0
            ),

            -- --------------------------------------------------------------
            -- Rule family 2:
            -- Dash share-class / series symbol to dot share-class / series.
            --
            -- Examples:
            --   BRK-A -> BRK.A
            --   BF-B  -> BF.B
            --
            -- We explicitly exclude the known structural suffix families
            -- handled elsewhere: -WS, -U, -R.
            -- --------------------------------------------------------------
            dash_class_to_dot_class AS (
                SELECT DISTINCT
                    u.raw_symbol,
                    regexp_replace(u.raw_symbol, '-([A-Z0-9]+)$', '.\\1') AS normalized_symbol,
                    'DASH_CLASS_TO_DOT_CLASS' AS rule_name
                FROM unresolved AS u
                JOIN symbol_reference_history AS srh
                  ON srh.symbol = regexp_replace(u.raw_symbol, '-([A-Z0-9]+)$', '.\\1')
                 AND srh.effective_to IS NULL
                WHERE POSITION('-' IN u.raw_symbol) > 0
                  AND u.raw_symbol NOT LIKE '%-WS'
                  AND u.raw_symbol NOT LIKE '%-U'
                  AND u.raw_symbol NOT LIKE '%-R'
            ),

            -- --------------------------------------------------------------
            -- Rule family 3:
            -- Warrant class notation.
            --
            -- Example:
            --   ACHR-WS -> ACHR.W
            -- --------------------------------------------------------------
            dash_ws_to_dot_w AS (
                SELECT DISTINCT
                    u.raw_symbol,
                    replace(u.raw_symbol, '-WS', '.W') AS normalized_symbol,
                    'DASH_WS_TO_DOT_W' AS rule_name
                FROM unresolved AS u
                JOIN symbol_reference_history AS srh
                  ON srh.symbol = replace(u.raw_symbol, '-WS', '.W')
                 AND srh.effective_to IS NULL
                WHERE u.raw_symbol LIKE '%-WS'
            ),

            -- --------------------------------------------------------------
            -- Rule family 4:
            -- Unit notation.
            --
            -- Example:
            --   FTW-U -> FTW.U
            --
            -- This currently may produce zero rows, and that is fine.
            -- --------------------------------------------------------------
            dash_u_to_dot_u AS (
                SELECT DISTINCT
                    u.raw_symbol,
                    replace(u.raw_symbol, '-U', '.U') AS normalized_symbol,
                    'DASH_U_TO_DOT_U' AS rule_name
                FROM unresolved AS u
                JOIN symbol_reference_history AS srh
                  ON srh.symbol = replace(u.raw_symbol, '-U', '.U')
                 AND srh.effective_to IS NULL
                WHERE u.raw_symbol LIKE '%-U'
            ),

            -- --------------------------------------------------------------
            -- Rule family 5:
            -- Right notation.
            --
            -- Example:
            --   SOME-R -> SOME.R
            --
            -- This currently may produce zero rows, and that is fine.
            -- --------------------------------------------------------------
            dash_r_to_dot_r AS (
                SELECT DISTINCT
                    u.raw_symbol,
                    replace(u.raw_symbol, '-R', '.R') AS normalized_symbol,
                    'DASH_R_TO_DOT_R' AS rule_name
                FROM unresolved AS u
                JOIN symbol_reference_history AS srh
                  ON srh.symbol = replace(u.raw_symbol, '-R', '.R')
                 AND srh.effective_to IS NULL
                WHERE u.raw_symbol LIKE '%-R'
            ),

            combined AS (
                SELECT * FROM underscore_to_dollar
                UNION ALL
                SELECT * FROM dash_class_to_dot_class
                UNION ALL
                SELECT * FROM dash_ws_to_dot_w
                UNION ALL
                SELECT * FROM dash_u_to_dot_u
                UNION ALL
                SELECT * FROM dash_r_to_dot_r
            )

            SELECT
                raw_symbol,
                normalized_symbol,
                rule_name,
                CURRENT_TIMESTAMP AS built_at
            FROM combined
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY raw_symbol
                ORDER BY
                    CASE rule_name
                        WHEN 'UNDERSCORE_TO_DOLLAR' THEN 1
                        WHEN 'DASH_CLASS_TO_DOT_CLASS' THEN 2
                        WHEN 'DASH_WS_TO_DOT_W' THEN 3
                        WHEN 'DASH_U_TO_DOT_U' THEN 4
                        WHEN 'DASH_R_TO_DOT_R' THEN 5
                        ELSE 999
                    END,
                    normalized_symbol
            ) = 1
            """
        )

        normalization_row_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM stooq_symbol_normalization_map
            """
        ).fetchone()[0]

        rows_by_rule = conn.execute(
            """
            SELECT
                rule_name,
                COUNT(*) AS row_count
            FROM stooq_symbol_normalization_map
            GROUP BY rule_name
            ORDER BY row_count DESC, rule_name
            """
        ).fetchall()

        sample_rows = conn.execute(
            """
            SELECT
                raw_symbol,
                normalized_symbol,
                rule_name
            FROM stooq_symbol_normalization_map
            ORDER BY raw_symbol
            LIMIT 100
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "build-stooq-symbol-normalization-map",
                "normalization_row_count": normalization_row_count,
                "rows_by_rule": rows_by_rule,
                "sample_rows": sample_rows,
            }
        )
    finally:
        conn.close()

    LOGGER.info("build-stooq-symbol-normalization-map finished")


if __name__ == "__main__":
    run()
