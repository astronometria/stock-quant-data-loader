"""
Enrich the Stooq symbol normalization map from the current unresolved-symbol probe.

Design:
- SQL-first
- additive only
- conservative: only insert mappings whose normalized target symbol already exists
  as an open-ended row in symbol_reference_history
- use the CURRENT real schema of stooq_symbol_normalization_map:
    raw_symbol
    normalized_symbol
    rule_name
    built_at

Why this job exists:
- the broad builder already handles generic safe rule families
- this job adds a very small, explicit batch of probe-confirmed format mappings
- it must never invent identities that do not exist in master data

Important:
- this job only handles the pure symbol-format mappings discovered by the probe
- reference-identity creation remains a separate concern
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Insert conservative, probe-confirmed symbol-format mappings into
    stooq_symbol_normalization_map.

    Current intended mappings:
    - GRP-U   -> GRP.U
    - FTW-U   -> FTW.U
    - NOTE-WS -> NOTE.W
    - DC-WS   -> DC.W
    - ALUR-WS -> ALUR.W
    - SCE_K   -> SCE$K

    Safety rule:
    - only insert a row when the normalized target symbol already exists in the
      open-ended reference layer
    """
    configure_logging()
    LOGGER.info("enrich-stooq-symbol-normalization-map-from-probe started")

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Ensure the destination table exists with the canonical schema.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stooq_symbol_normalization_map (
                raw_symbol VARCHAR,
                normalized_symbol VARCHAR,
                rule_name VARCHAR,
                built_at TIMESTAMP WITH TIME ZONE
            )
            """
        )

        # ------------------------------------------------------------------
        # Stage the small explicit set of probe-confirmed format mappings.
        #
        # These are symbol-shape transforms only. They are NOT new identity rows.
        # ------------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_probe_format_symbol_map")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_probe_format_symbol_map AS
            SELECT *
            FROM (
                VALUES
                    ('ALUR-WS', 'ALUR.W', 'DASH_WS_TO_DOT_W'),
                    ('DC-WS',   'DC.W',   'DASH_WS_TO_DOT_W'),
                    ('FTW-U',   'FTW.U',  'DASH_U_TO_DOT_U'),
                    ('GRP-U',   'GRP.U',  'DASH_U_TO_DOT_U'),
                    ('NOTE-WS', 'NOTE.W', 'DASH_WS_TO_DOT_W'),
                    ('SCE_K',   'SCE$K',  'UNDERSCORE_TO_DOLLAR')
            ) AS t(raw_symbol, normalized_symbol, rule_name)
            """
        )

        # ------------------------------------------------------------------
        # Insert only mappings whose target symbol exists in the current
        # open-ended reference layer, and only if the raw_symbol is not already
        # present in the normalization map.
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
            FROM tmp_probe_format_symbol_map AS s
            JOIN symbol_reference_history AS srh
              ON srh.symbol = s.normalized_symbol
             AND srh.effective_to IS NULL
            LEFT JOIN stooq_symbol_normalization_map AS existing
              ON existing.raw_symbol = s.raw_symbol
            WHERE existing.raw_symbol IS NULL
            RETURNING 1
            """
        ).fetchall()

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
            WHERE raw_symbol IN ('ALUR-WS', 'DC-WS', 'FTW-U', 'GRP-U', 'NOTE-WS', 'SCE_K')
            ORDER BY raw_symbol
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "enrich-stooq-symbol-normalization-map-from-probe",
                "staged_row_count": 6,
                "inserted_count": len(inserted_count),
                "total_map_count": total_map_count,
                "rows_now_present": rows_now_present,
            }
        )
    finally:
        conn.close()
        LOGGER.info("enrich-stooq-symbol-normalization-map-from-probe finished")


if __name__ == "__main__":
    run()
