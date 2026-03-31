"""
Build a manual symbol override map for difficult residual unresolved symbols.

This table is intentionally explicit and conservative.
Only high-confidence manual mappings should be placed here.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    configure_logging()
    LOGGER.info("build-symbol-manual-override-map started")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS symbol_manual_override_map")
        conn.execute(
            """
            CREATE TABLE symbol_manual_override_map (
                raw_symbol VARCHAR NOT NULL,
                mapped_symbol VARCHAR NOT NULL,
                mapping_source VARCHAR NOT NULL,
                notes VARCHAR,
                confidence VARCHAR NOT NULL
            )
            """
        )

        conn.execute(
            """
            INSERT INTO symbol_manual_override_map (
                raw_symbol,
                mapped_symbol,
                mapping_source,
                notes,
                confidence
            )
            VALUES
                ('ODP',  'ODP',  'manual_seed_v5', 'legacy/high-volume unresolved symbol', 'REVIEW'),
                ('SOHO', 'SOHO', 'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('DENN', 'DENN', 'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('GIFI', 'GIFI', 'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('NXC',  'NXC',  'manual_seed_v5', 'common equity / fund symbol absent from current reference layer', 'REVIEW'),
                ('ELP',  'ELP',  'manual_seed_v5', 'common equity / ADR symbol absent from current reference layer', 'REVIEW'),
                ('HSII', 'HSII', 'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('SPNS', 'SPNS', 'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('CIVI', 'CIVI', 'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('AHH',  'AHH',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('MGIC', 'MGIC', 'manual_seed_v5', 'large unresolved common equity', 'REVIEW'),
                ('NXN',  'NXN',  'manual_seed_v5', 'large unresolved fund/common symbol', 'REVIEW'),
                ('CIO',  'CIO',  'manual_seed_v5', 'large unresolved common equity', 'REVIEW'),
                ('TRUE', 'TRUE', 'manual_seed_v5', 'large unresolved common equity', 'REVIEW'),
                ('CDTX', 'CDTX', 'manual_seed_v5', 'large unresolved common equity', 'REVIEW'),
                ('ATXS', 'ATXS', 'manual_seed_v5', 'large unresolved common equity', 'REVIEW'),
                ('XRLV', 'XRLV', 'manual_seed_v5', 'large unresolved ETF/fund symbol', 'REVIEW'),
                ('MRUS', 'MRUS', 'manual_seed_v5', 'large unresolved common equity', 'REVIEW'),
                ('REVG', 'REVG', 'manual_seed_v5', 'large unresolved common equity', 'REVIEW'),
                ('PLYM', 'PLYM', 'manual_seed_v5', 'large unresolved common equity', 'REVIEW'),
                ('RFEU', 'RFEU', 'manual_seed_v5', 'large unresolved ETF/fund symbol', 'REVIEW'),
                ('IAS',  'IAS',  'manual_seed_v5', 'large unresolved common equity', 'REVIEW'),
                ('BKN',  'BKN',  'manual_seed_v5', 'large unresolved closed-end fund', 'REVIEW'),
                ('MQT',  'MQT',  'manual_seed_v5', 'large unresolved closed-end fund', 'REVIEW'),
                ('MVF',  'MVF',  'manual_seed_v5', 'large unresolved closed-end fund', 'REVIEW'),
                ('MVT',  'MVT',  'manual_seed_v5', 'large unresolved closed-end fund', 'REVIEW'),
                ('MYD',  'MYD',  'manual_seed_v5', 'large unresolved closed-end fund', 'REVIEW'),
                ('BFK',  'BFK',  'manual_seed_v5', 'large unresolved closed-end fund', 'REVIEW'),
                ('BFZ',  'BFZ',  'manual_seed_v5', 'large unresolved closed-end fund', 'REVIEW'),
                ('BNY',  'BNY',  'manual_seed_v5', 'large unresolved closed-end fund', 'REVIEW'),
                ('MHN',  'MHN',  'manual_seed_v5', 'large unresolved closed-end fund', 'REVIEW'),
                ('MUE',  'MUE',  'manual_seed_v5', 'large unresolved closed-end fund', 'REVIEW'),
                ('GAINL', 'GAIN', 'manual_seed_v5', 'probe confirmed GAINL maps to GAIN', 'HIGH'),
                ('PNFPP', 'PNFP', 'manual_seed_v5', 'probe confirmed PNFPP maps to PNFP', 'HIGH'),
                ('NE-WS-A', 'NE.A', 'manual_seed_v5', 'probe confirmed NE-WS-A maps to NE.A', 'HIGH'),
                ('AHH_A', 'AHH', 'manual_seed_v5', 'probe confirmed AHH_A maps to AHH', 'HIGH'),
                ('SPNT_B', 'SPNT', 'manual_seed_v5', 'probe confirmed SPNT_B maps to SPNT', 'HIGH'),

                ('BFIN', 'BFIN', 'manual_seed_v5', 'next-wave high-volume unresolved symbol', 'REVIEW'),
                ('BTA', 'BTA', 'manual_seed_v5', 'next-wave high-volume unresolved symbol', 'REVIEW'),
                ('BSCP', 'BSCP', 'manual_seed_v5', 'next-wave high-volume unresolved symbol', 'REVIEW'),
                ('CIL', 'CIL', 'manual_seed_v5', 'next-wave high-volume unresolved symbol', 'REVIEW'),
                ('GMRE_A', 'GMRE_A', 'manual_seed_v5', 'next-wave unresolved preferred-like symbol; keep explicit pending stronger transform rule', 'REVIEW'),
                ('BSJP', 'BSJP', 'manual_seed_v5', 'next-wave high-volume unresolved symbol', 'REVIEW'),
                ('NINEQ', 'NINEQ', 'manual_seed_v5', 'next-wave high-volume unresolved symbol', 'REVIEW'),
                ('SPMV', 'SPMV', 'manual_seed_v5', 'next-wave high-volume unresolved symbol', 'REVIEW'),
                ('BRY', 'BRY', 'manual_seed_v5', 'next-wave high-volume unresolved symbol', 'REVIEW'),
                ('SFYX', 'SFYX', 'manual_seed_v5', 'next-wave high-volume unresolved symbol', 'REVIEW'),
                ('EFC_A', 'EFC_A', 'manual_seed_v5', 'next-wave unresolved preferred-like symbol; keep explicit pending stronger transform rule', 'REVIEW'),
                ('IMG', 'IMG', 'manual_seed_v5', 'next-wave high-volume unresolved symbol', 'REVIEW'),
                ('ETHZ', 'ETHZ', 'manual_seed_v5', 'next-wave high-volume unresolved symbol', 'REVIEW'),
                ('IBTF', 'IBTF', 'manual_seed_v5', 'next-wave high-volume unresolved symbol', 'REVIEW'),
                ('RPTX', 'RPTX', 'manual_seed_v5', 'next-wave high-volume unresolved symbol', 'REVIEW')
            """
        )

        count_rows = conn.execute(
            "SELECT COUNT(*) FROM symbol_manual_override_map"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "build-symbol-manual-override-map",
                "override_row_count": count_rows,
            }
        )
    finally:
        conn.close()

    LOGGER.info("build-symbol-manual-override-map finished")


if __name__ == "__main__":
    run()
