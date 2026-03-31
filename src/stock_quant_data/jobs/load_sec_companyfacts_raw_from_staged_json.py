"""
Load staged SEC companyfacts JSON files into DuckDB using a SQL-first approach.

Why this job exists:
- DuckDB is very good at reading JSON and multiple files directly in SQL.
- We want Python to remain thin here.
- The staging job already moved ZIP complexity out of the ingestion step.

Design:
- Read all staged JSON files from:
    data/staging/sec/companyfacts/*/*.json
- Build sec_companyfacts_raw in DuckDB from read_json_auto().
- Preserve full lineage:
    - source_snapshot_id
    - source_json_path
- Preserve the nested facts object as JSON text in facts_json.

Important:
- This is still a RAW layer.
- It does not explode facts into a fact-level table yet.
- The table is rebuilt on each run for deterministic behavior during refactor.
"""

from __future__ import annotations

import logging
from pathlib import Path

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)

STAGING_GLOB = (
    "/home/marty/stock-quant-data-platform/data/staging/sec/companyfacts/*/*.json"
)

STAGING_ROOT = Path(
    "/home/marty/stock-quant-data-platform/data/staging/sec/companyfacts"
)

DUCKDB_THREADS = 8


def run() -> None:
    """
    Build sec_companyfacts_raw from staged JSON files using DuckDB SQL.
    """
    configure_logging()
    LOGGER.info("load-sec-companyfacts-raw-from-staged-json started")

    if not STAGING_ROOT.exists():
        raise FileNotFoundError(
            f"Staging root does not exist: {STAGING_ROOT}"
        )

    conn = connect_build_db()
    try:
        conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
        conn.execute("DROP TABLE IF EXISTS sec_companyfacts_raw")

        # We use read_json_auto() directly in SQL so the ingestion logic stays SQL-first.
        # filename=true adds source path lineage per row.
        conn.execute(
            f"""
            CREATE TABLE sec_companyfacts_raw AS
            WITH staged AS (
                SELECT
                    filename AS source_json_path,
                    regexp_extract(
                        filename,
                        '.*/companyfacts/([^/]+)/[^/]+\\.json$',
                        1
                    ) AS source_snapshot_id,
                    regexp_extract(
                        filename,
                        '.*/([^/]+\\.json)$',
                        1
                    ) AS json_member_name,
                    CAST(cik AS VARCHAR) AS cik,
                    CAST(entityName AS VARCHAR) AS entity_name,
                    CAST(to_json(facts) AS VARCHAR) AS facts_json
                FROM read_json_auto(
                    '{STAGING_GLOB}',
                    filename = true,
                    union_by_name = true
                )
            )
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY source_snapshot_id, json_member_name, source_json_path
                ) AS raw_id,
                source_snapshot_id,
                source_json_path,
                json_member_name,
                COALESCE(cik, '') AS cik,
                COALESCE(entity_name, '') AS entity_name,
                COALESCE(facts_json, '{{}}') AS facts_json,
                CURRENT_TIMESTAMP AS loaded_at
            FROM staged
            """
        )

        row_count = conn.execute(
            "SELECT COUNT(*) FROM sec_companyfacts_raw"
        ).fetchone()[0]

        distinct_cik_count = conn.execute(
            "SELECT COUNT(DISTINCT cik) FROM sec_companyfacts_raw"
        ).fetchone()[0]

        snapshot_count = conn.execute(
            "SELECT COUNT(DISTINCT source_snapshot_id) FROM sec_companyfacts_raw"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "load-sec-companyfacts-raw-from-staged-json",
                "staging_glob": STAGING_GLOB,
                "row_count": row_count,
                "distinct_cik_count": distinct_cik_count,
                "snapshot_count": snapshot_count,
                "duckdb_threads": DUCKDB_THREADS,
            }
        )
    finally:
        conn.close()

    LOGGER.info("load-sec-companyfacts-raw-from-staged-json finished")


if __name__ == "__main__":
    run()
