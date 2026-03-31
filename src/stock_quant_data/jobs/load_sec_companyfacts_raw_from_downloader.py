"""
Load SEC companyfacts raw JSON members from downloader ZIP archives into the build DuckDB.

Why this job exists:
- The downloader repo already stores SEC companyfacts ZIP archives on disk.
- The platform repo needs a conservative raw landing table before any deeper normalization.
- We want a version that is still easy to debug, but faster than tiny-row inserts.

Design choices:
- One row per JSON member inside each companyfacts ZIP archive.
- Keep the nested `facts` object as raw JSON text in `facts_json`.
- Extract only a few top-level identity fields now:
  - cik
  - entity_name
- Preserve full source lineage:
  - source_zip_path
  - json_member_name

Performance choices:
- Single-process only for simplicity and easier debugging.
- Large batched inserts with executemany().
- Avoid sort_keys=True because raw landing does not need canonical key ordering.
- Keep tqdm progress at the JSON-member level.

Important:
- This is intentionally a RAW layer.
- It does not explode all companyfacts into fact-level rows yet.
- It rebuilds the table on each run for deterministic behavior during refactor stage.
"""

from __future__ import annotations

import json
import logging
import zipfile
from pathlib import Path
from typing import Iterator

from tqdm import tqdm

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)

# Explicit path to the downloader output tree.
# Kept hard-coded for now because this repo is still being stabilized operationally.
DOWNLOADER_COMPANYFACTS_ROOT = Path(
    "/home/marty/stock-quant-data-downloader/data/sec/companyfacts"
)

# Larger batch size than the first version so inserts are meaningfully more efficient.
# This is still conservative enough to avoid ridiculous memory spikes.
INSERT_CHUNK_SIZE = 10_000

# Ask DuckDB to use multiple threads where it can.
# Even though the Python loop is single-process, this can still help some DB-side work.
DUCKDB_THREADS = 8


def iter_companyfacts_members(root: Path) -> Iterator[tuple[Path, str]]:
    """
    Yield (zip_path, json_member_name) pairs in a stable order.

    We keep sorting so runs are reproducible and easier to compare in logs.
    """
    zip_paths = sorted(
        path for path in root.glob("*.zip") if path.is_file()
    )

    for zip_path in zip_paths:
        with zipfile.ZipFile(zip_path, "r") as zf:
            for member_name in sorted(zf.namelist()):
                if member_name.lower().endswith(".json"):
                    yield zip_path, member_name


def chunked(iterator, chunk_size: int):
    """
    Group an iterator into lists of at most `chunk_size` items.

    This helper is used twice:
    - once for JSON work units
    - once indirectly for insert batches
    """
    batch = []
    for item in iterator:
        batch.append(item)
        if len(batch) >= chunk_size:
            yield batch
            batch = []
    if batch:
        yield batch


def build_insert_rows(member_batch: list[tuple[int, Path, str]]) -> list[tuple[int, str, str, str, str, str]]:
    """
    Convert a batch of ZIP JSON members into DB insert tuples.

    Input row:
    - raw_id
    - zip_path
    - member_name

    Output row:
    - raw_id
    - source_zip_path
    - json_member_name
    - cik
    - entity_name
    - facts_json
    """
    rows: list[tuple[int, str, str, str, str, str]] = []

    for raw_id, zip_path, member_name in member_batch:
        with zipfile.ZipFile(zip_path, "r") as zf:
            with zf.open(member_name) as fh:
                payload = json.load(fh)

        cik_value = payload.get("cik")
        entity_name_value = payload.get("entityName")
        facts_value = payload.get("facts", {})

        # Raw layer: keep types simple and explicit as strings.
        cik = "" if cik_value is None else str(cik_value)
        entity_name = "" if entity_name_value is None else str(entity_name_value)

        # Important optimization:
        # this is a raw landing layer, so we do NOT need sorted keys.
        facts_json = json.dumps(
            facts_value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=False,
        )

        rows.append(
            (
                raw_id,
                str(zip_path),
                member_name,
                cik,
                entity_name,
                facts_json,
            )
        )

    return rows


def run() -> None:
    """
    Main CLI entry point for the loader.
    """
    configure_logging()
    LOGGER.info("load-sec-companyfacts-raw-from-downloader started")

    root = DOWNLOADER_COMPANYFACTS_ROOT

    if not root.exists():
        raise FileNotFoundError(
            f"Downloader companyfacts root does not exist: {root}"
        )

    # Materialize the worklist once so tqdm has a stable total and logs are reproducible.
    member_pairs = list(iter_companyfacts_members(root))

    indexed_members: list[tuple[int, Path, str]] = [
        (raw_id, zip_path, member_name)
        for raw_id, (zip_path, member_name) in enumerate(member_pairs, start=1)
    ]

    conn = connect_build_db()
    try:
        # Conservative DB pragmas.
        conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")

        # Rebuild raw landing table each run for deterministic behavior.
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

        # Process the workload in large batches.
        # This keeps Python overhead lower and DB inserts much more efficient.
        for member_batch in tqdm(
            chunked(indexed_members, INSERT_CHUNK_SIZE),
            total=(len(indexed_members) + INSERT_CHUNK_SIZE - 1) // INSERT_CHUNK_SIZE,
            desc="sec_companyfacts_batches",
            unit="batch",
            dynamic_ncols=True,
            leave=True,
        ):
            rows = build_insert_rows(member_batch)
            conn.executemany(insert_sql, rows)
            total_inserted += len(rows)

        # Final validation metrics.
        row_count = conn.execute(
            "SELECT COUNT(*) FROM sec_companyfacts_raw"
        ).fetchone()[0]

        distinct_cik_count = conn.execute(
            "SELECT COUNT(DISTINCT cik) FROM sec_companyfacts_raw"
        ).fetchone()[0]

        zip_count = len(sorted(root.glob("*.zip")))

        print(
            {
                "status": "ok",
                "job": "load-sec-companyfacts-raw-from-downloader",
                "root": str(root),
                "zip_count": zip_count,
                "json_member_count": len(indexed_members),
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
