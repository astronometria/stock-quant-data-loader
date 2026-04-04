"""
Build the explicit manual override map for unresolved Stooq symbols.

Important:
- This table is only a mapping table.
- It does NOT directly create instruments or symbol_reference rows.
- That responsibility belongs to dedicated enrichment jobs.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild the canonical manual override mapping table.

    Table contract:
    - raw_symbol: symbol as found in source raw data
    - mapped_symbol: canonical symbol to resolve toward
    - source_name: provenance of the manual mapping
    - mapping_rationale: human-readable reason
    - confidence_level: REVIEW / HIGH / etc.
    """
    configure_logging()
    LOGGER.info("build-symbol-manual-override-map started")

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM symbol_manual_override_map")

        conn.execute(
            """
            INSERT INTO symbol_manual_override_map (
                raw_symbol,
                mapped_symbol,
                source_name,
                mapping_rationale,
                confidence_level
            )
            VALUES
                ('AHH',   'AHH',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('ATXS',  'ATXS',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('BFIN',  'BFIN',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('BFK',   'BFK',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('BFZ',   'BFZ',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('BKN',   'BKN',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('BNY',   'BNY',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('BRY',   'BRY',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('BSCP',  'BSCP',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('BSJP',  'BSJP',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('BTA',   'BTA',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('CDTX',  'CDTX',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('CIL',   'CIL',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('CIO',   'CIO',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('CIVI',  'CIVI',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('DENN',  'DENN',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('EFC_A', 'EFC.A', 'manual_seed_v5', 'preferred share underscore to dot class normalization', 'HIGH'),
                ('ELP',   'ELP',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('ETHZ',  'ETHZ',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('GIFI',  'GIFI',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('GMRE_A','GMRE.A','manual_seed_v5', 'preferred share underscore to dot class normalization', 'HIGH'),
                ('HSII',  'HSII',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('IAS',   'IAS',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('IBTF',  'IBTF',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('IMG',   'IMG',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('MGIC',  'MGIC',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('MHN',   'MHN',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('MQT',   'MQT',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('MRUS',  'MRUS',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('MUE',   'MUE',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('MVF',   'MVF',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('MVT',   'MVT',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('MYD',   'MYD',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('NINEQ', 'NINEQ', 'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('NXC',   'NXC',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('NXN',   'NXN',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('ODP',   'ODP',   'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('PLYM',  'PLYM',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('REVG',  'REVG',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('RFEU',  'RFEU',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('RPTX',  'RPTX',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('SFYX',  'SFYX',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('SOHO',  'SOHO',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('SPMV',  'SPMV',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('SPNS',  'SPNS',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('TRUE',  'TRUE',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('XRLV',  'XRLV',  'manual_seed_v5', 'common equity ticker absent from current reference layer', 'REVIEW'),
                ('AHH_A', 'AHH',   'manual_seed_v5', 'probe confirmed AHH_A maps to AHH', 'HIGH'),
                ('GRP-U', 'GRP.U', 'manual_seed_v5', 'unit dash-U formatting normalized to dot-U', 'HIGH'),
                ('FTW-U', 'FTW.U', 'manual_seed_v5', 'unit dash-U formatting normalized to dot-U', 'HIGH'),
                ('SCE_K', 'SCE$K', 'manual_seed_v5', 'underscore preferred-share suffix normalized to dollar form', 'HIGH'),
                ('NOTE-WS','NOTE.W','manual_seed_v5', 'warrant dash-WS formatting normalized to dot-W', 'HIGH'),
                ('DC-WS', 'DC.W',  'manual_seed_v5', 'warrant dash-WS formatting normalized to dot-W', 'HIGH'),
                ('ALUR-WS','ALUR.W','manual_seed_v5', 'warrant dash-WS formatting normalized to dot-W', 'HIGH')
            """
        )

        count = conn.execute("SELECT COUNT(*) FROM symbol_manual_override_map").fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "build-symbol-manual-override-map",
                "row_count": count,
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-symbol-manual-override-map finished")


if __name__ == "__main__":
    run()
