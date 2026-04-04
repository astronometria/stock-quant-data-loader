"""
Load SEC companyfacts raw JSON members from downloader ZIP archives into the build DuckDB.

Why this version exists:
- the earlier version reopened the ZIP archive for every single JSON member
- that causes massive overhead on large SEC bulk archives
- this replacement keeps the process simple, deterministic, and much faster

Design choices:
- still a RAW landing table
- still rebuilds the raw table each run for deterministic behavior
- process one ZIP at a time
- open each ZIP only once
- insert rows into DuckDB in moderate chunks
- keep progress visible with tqdm without flooding the terminal

Important:
- facts_json remains raw JSON text
- no fact-level normalization happens here
- this job is intentionally SQL-light because archive walking is better kept in thin Python
"""

from __future__ import annotations

import json
import logging
import zipfile
from pathlib import Path

from tqdm import tqdm

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Downloader source root.
# This is the handoff boundary from downloader -> loader.
# ----------------------------------------------------------------------
DOWNLOADER_COMPANYFACTS_ROOT = Path(
    "/home/marty/stock-quant-data-downloader/data/sec/companyfacts"
)

# ----------------------------------------------------------------------
# Insert chunk size.
# We keep this much smaller than the previous effective work unit so:
# - progress advances more often
# - memory stays more stable
# - failure radius is smaller
# ----------------------------------------------------------------------
INSERT_CHUNK_SIZE = 100

# Ask DuckDB to use multiple threads where DB-side work can benefit.
DUCKDB_THREADS = 8


def _iter_zip_paths(root: Path) -> list[Path]:
    """
    Return companyfacts ZIP paths in stable sorted order.
    """
    return sorted(path for path in root.glob("*.zip") if path.is_file())


def _json_member_names(zf: zipfile.ZipFile) -> list[str]:
    """
    Return JSON member names in stable sorted order.

    Keeping order stable makes runs easier to compare.
    """
    return sorted(
        member_name
        for member_name in zf.namelist()
        if member_name.lower().endswith(".json")
    )


def _flush_rows(conn, insert_sql: str, rows: list[tuple]) -> int:
    """
    Flush one buffered insert batch to DuckDB.

    Returns:
    - number of inserted rows
    """
    if not rows:
        return 0
    conn.executemany(insert_sql, rows)
    inserted = len(rows)
    rows.clear()
    return inserted


def run() -> None:
    """
    Main job entry point.
    """
    configure_logging()
    LOGGER.info("load-sec-companyfacts-raw-from-downloader started")

    root = DOWNLOADER_COMPANYFACTS_ROOT
    if not root.exists():
        raise FileNotFoundError(
            f"Downloader companyfacts root does not exist: {root}"
        )

    zip_paths = _iter_zip_paths(root)
    if not zip_paths:
        raise RuntimeError(f"No companyfacts zip found under {root}")

    conn = connect_build_db()
    try:
        # --------------------------------------------------------------
        # Conservative DB pragmas.
        # --------------------------------------------------------------
        conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")

        # --------------------------------------------------------------
        # Deterministic rebuild of the raw landing table.
        # --------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS sec_companyfacts_raw")

        conn.execute(
            """
            CREATE TABLE sec_companyfacts_raw (
                raw_id BIGINT PRIMARY KEY,
                source_zip_path VARCHAR,
                json_member_name VARCHAR,
                cik VARCHAR NOT NULL,
                entity_name VARCHAR NOT NULL,
                facts_json VARCHAR NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        insert_sql = """
            INSERT INTO sec_companyfacts_raw (
                raw_id,
                source_zip_path,
                json_member_name,
                cik,
                entity_name,
                facts_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """

        total_inserted = 0
        total_member_count = 0
        raw_id = 1

        # --------------------------------------------------------------
        # Progress by ZIP first.
        # This gives immediate visible progress at the outer level.
        # --------------------------------------------------------------
        for zip_path in tqdm(
            zip_paths,
            desc="sec_companyfacts_zip",
            unit="zip",
            dynamic_ncols=True,
            leave=True,
        ):
            with zipfile.ZipFile(zip_path, "r") as zf:
                member_names = _json_member_names(zf)
                total_member_count += len(member_names)

                # ------------------------------------------------------
                # Keep one ZIP open and stream all JSON members from it.
                # This is the main performance fix.
                # ------------------------------------------------------
                insert_rows: list[tuple] = []

                for member_name in tqdm(
                    member_names,
                    desc=f"companyfacts:{zip_path.name}",
                    unit="json",
                    dynamic_ncols=True,
                    leave=False,
                ):
                    with zf.open(member_name) as fh:
                        payload = json.load(fh)

                    cik_value = payload.get("cik")
                    entity_name_value = payload.get("entityName")
                    facts_value = payload.get("facts", {})

                    cik = "" if cik_value is None else str(cik_value)
                    entity_name = "" if entity_name_value is None else str(entity_name_value)

                    # Raw layer:
                    # keep only the nested facts object as compact JSON text.
                    facts_json = json.dumps(
                        facts_value,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=False,
                    )

                    insert_rows.append(
                        (
                            raw_id,
                            str(zip_path),
                            member_name,
                            cik,
                            entity_name,
                            facts_json,
                        )
                    )
                    raw_id += 1

                    # --------------------------------------------------
                    # Flush in moderate chunks.
                    # This keeps memory bounded and progress smoother.
                    # --------------------------------------------------
                    if len(insert_rows) >= INSERT_CHUNK_SIZE:
                        total_inserted += _flush_rows(conn, insert_sql, insert_rows)

                # Flush trailing rows for the current ZIP.
                total_inserted += _flush_rows(conn, insert_sql, insert_rows)

        # --------------------------------------------------------------
        # Final validation metrics.
        # --------------------------------------------------------------
        row_count = conn.execute(
            "SELECT COUNT(*) FROM sec_companyfacts_raw"
        ).fetchone()[0]

        distinct_cik_count = conn.execute(
            "SELECT COUNT(DISTINCT cik) FROM sec_companyfacts_raw"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "load-sec-companyfacts-raw-from-downloader",
                "root": str(root),
                "zip_count": len(zip_paths),
                "json_member_count": total_member_count,
                "row_count": row_count,
                "distinct_cik_count": distinct_cik_count,
                "insert_chunk_size": INSERT_CHUNK_SIZE,
                "duckdb_threads": DUCKDB_THREADS,
                "total_inserted": total_inserted,
            }
        )
    finally:
        conn.close()

    LOGGER.info("load-sec-companyfacts-raw-from-downloader finished")


if __name__ == "__main__":
    run()
