"""
Build the current explicit manual override map.

The point of this table is not to be "smart". It is to be explicit, auditable,
and tiny. If a symbol has been manually reviewed and we know what current
reference identity it should map to, it belongs here.

Current canonical target table:
- symbol_manual_override_map(
    raw_symbol,
    mapped_symbol,
    source_name,
    mapping_note,
    priority_level,
    built_at
  )
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild the manual override map from the currently approved explicit rows.
    """
    configure_logging()
    LOGGER.info("build-symbol-manual-override-map started")

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM symbol_manual_override_map")

        # ------------------------------------------------------------------
        # Current explicit seeds.
        #
        # Notes:
        # - Keep these rows extremely explicit.
        # - Do not infer more than what has already been manually confirmed.
        # - We only use current-schema column names.
        # ------------------------------------------------------------------
        rows = [
            ("AHH", "AHH", "manual_seed_v5", "common equity ticker absent from current reference layer", "REVIEW"),
            ("AHH_A", "AHH", "manual_seed_v5", "probe confirmed AHH_A maps to AHH", "HIGH"),
            ("ATXS", "ATXS", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("BFIN", "BFIN", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("BFK", "BFK", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("BFZ", "BFZ", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("BKN", "BKN", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("BNY", "BNY", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("BRY", "BRY", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("BSCP", "BSCP", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("BSJP", "BSJP", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("BTA", "BTA", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("CDTX", "CDTX", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("CIL", "CIL", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("CIO", "CIO", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("CIVI", "CIVI", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("DENN", "DENN", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("EFC_A", "EFC_A", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("ELP", "ELP", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("ETHZ", "ETHZ", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("GIFI", "GIFI", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("GMRE_A", "GMRE_A", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("HSII", "HSII", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("IAS", "IAS", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("IBTF", "IBTF", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("IMG", "IMG", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("MGIC", "MGIC", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("MHN", "MHN", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("MQT", "MQT", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("MRUS", "MRUS", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("MUE", "MUE", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("MVF", "MVF", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("MVT", "MVT", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("MYD", "MYD", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("NINEQ", "NINEQ", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("NXC", "NXC", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("NXN", "NXN", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("ODP", "ODP", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("PLYM", "PLYM", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("REVG", "REVG", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("RFEU", "RFEU", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("RPTX", "RPTX", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("SFYX", "SFYX", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("SOHO", "SOHO", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("SPMV", "SPMV", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("SPNS", "SPNS", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("TRUE", "TRUE", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("XRLV", "XRLV", "manual_seed_v5", "manual unresolved review seed", "REVIEW"),
            ("META", "META", "manual_seed_v5", "current public company symbol", "HIGH"),
            ("FB", "META", "manual_seed_v5", "legacy renamed ticker maps to current instrument identity", "HIGH"),
            ("BXMX", "BXMX", "manual_seed_v5", "explicit SEC-confirmed symbol", "HIGH"),
            ("DIAX", "DIAX", "manual_seed_v5", "explicit SEC-confirmed symbol", "HIGH"),
        ]

        conn.executemany(
            """
            INSERT INTO symbol_manual_override_map (
                raw_symbol,
                mapped_symbol,
                source_name,
                mapping_note,
                priority_level
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )

        row_count = conn.execute(
            "SELECT COUNT(*) FROM symbol_manual_override_map"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "build-symbol-manual-override-map",
                "row_count": row_count,
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-symbol-manual-override-map finished")


if __name__ == "__main__":
    run()
