"""
Seed initial logical universes into the mutable build database.

Design goals:
- keep seeding explicit and deterministic
- make the initial serving API useful immediately
- avoid hidden bootstrap data
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Insert a small deterministic set of initial universe definitions.

    We use explicit IDs in v1 to keep bootstrapping reproducible and easy
    to inspect. This can later evolve into migration-driven reference data.
    """
    configure_logging()
    LOGGER.info("seed-universes started")

    rows_to_seed = [
        (1, "US_LISTED_COMMON_STOCKS", "US listed common stocks universe"),
        (2, "NASDAQ_LISTED", "NASDAQ listed instruments universe"),
        (3, "NYSE_LISTED", "NYSE listed instruments universe"),
        (4, "US_LISTED_ETFS", "US listed ETFs universe"),
    ]

    conn = connect_build_db()
    try:
        conn.executemany(
            """
            INSERT INTO universe_definition (
                universe_id,
                universe_name,
                description
            )
            SELECT ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM universe_definition
                WHERE universe_id = ?
                   OR universe_name = ?
            )
            """,
            [
                (uid, uname, desc, uid, uname)
                for uid, uname, desc in rows_to_seed
            ],
        )

        count_row = conn.execute(
            "SELECT COUNT(*) FROM universe_definition"
        ).fetchone()

        print(
            {
                "status": "ok",
                "job": "seed-universes",
                "universe_definition_count": count_row[0],
            }
        )
    finally:
        conn.close()

    LOGGER.info("seed-universes finished")
