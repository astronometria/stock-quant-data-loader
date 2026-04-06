"""
Build a Parquet facts dataset from staged SEC companyfacts JSON.

SQL-first, but batch-oriented:
- avoids one giant JSON explosion over all files at once
- writes one Parquet part per file batch
- future rebuilds can load from Parquet much faster than reparsing JSON

Why this exists:
- A single read_json_auto('*.json') over ~19k SEC companyfacts files can
  require huge intermediate state and fail with DuckDB temp/OOM pressure.
- Batching keeps the transformation SQL-first while bounding the working set.

Why this patched version exists:
- The previous batch size was still too large for the deep json_each(...)
  explosion done by companyfacts.
- DuckDB was failing inside conn.execute(sql_text) with OutOfMemoryException.
- This version reduces pressure by:
  1) lowering the default file batch size,
  2) lowering the DuckDB thread count,
  3) creating a fresh DuckDB connection for each batch so memory can be
     released more aggressively between batches.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from tqdm import tqdm

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.config.settings import get_settings
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Conservative defaults.
#
# They can be overridden at runtime without editing the file again:
#   BUILD_SEC_COMPANYFACTS_FILE_BATCH_SIZE=5
#   BUILD_SEC_COMPANYFACTS_DUCKDB_THREADS=1
# -----------------------------------------------------------------------------
FILE_BATCH_SIZE = int(os.getenv("BUILD_SEC_COMPANYFACTS_FILE_BATCH_SIZE", "5"))
DUCKDB_THREADS = int(os.getenv("BUILD_SEC_COMPANYFACTS_DUCKDB_THREADS", "1"))


def _latest_stage_dir(stage_root: Path) -> Path:
    """Resolve the latest staged companyfacts batch directory."""
    candidates = sorted([p for p in stage_root.iterdir() if p.is_dir()])
    if not candidates:
        raise FileNotFoundError(
            f"No staged companyfacts batch found under {stage_root}"
        )
    return candidates[-1]


def _chunked(items: list[Path], chunk_size: int) -> list[list[Path]]:
    """Split a list into fixed-size chunks."""
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def _quote_sql_string(value: str) -> str:
    """Safely quote a string literal for SQL embedding."""
    return "'" + value.replace("'", "''") + "'"


def _build_batch_sql(batch_files: list[Path], parquet_output_path: Path) -> str:
    """
    Build one SQL-first JSON -> Parquet export for a specific file batch.

    Notes:
    - The file list is embedded as SQL string literals.
    - JSON field paths must remain SQL string literals too.
    - This function returns one COPY ... TO parquet statement.
    """
    file_list_sql = ", ".join(_quote_sql_string(str(p)) for p in batch_files)
    parquet_sql = _quote_sql_string(str(parquet_output_path))

    return f"""
    COPY (
        WITH
        src AS (
            SELECT
                filename,
                regexp_extract(filename, '[^/]+$') AS json_member_name,
                trim(cik) AS cik,
                facts
            FROM read_json_auto(
                [{file_list_sql}],
                columns = {{cik: 'VARCHAR', facts: 'JSON'}},
                union_by_name = true,
                filename = true
            )
        ),
        fact_namespaces AS (
            SELECT
                filename,
                json_member_name,
                cik,
                ns.key AS fact_namespace,
                ns.value AS namespace_obj
            FROM src,
            json_each(src.facts) AS ns
        ),
        fact_definitions AS (
            SELECT
                filename,
                json_member_name,
                cik,
                fact_namespace,
                fd.key AS fact_name,
                fd.value AS fact_obj
            FROM fact_namespaces,
            json_each(fact_namespaces.namespace_obj) AS fd
        ),
        fact_units AS (
            SELECT
                filename,
                json_member_name,
                cik,
                fact_namespace,
                fact_name,
                unit_entry.key AS unit_name,
                unit_entry.value AS observations_json
            FROM fact_definitions,
            json_each(json_extract(fact_obj, '$.units')) AS unit_entry
        ),
        fact_observations AS (
            SELECT
                filename,
                json_member_name,
                cik,
                fact_namespace,
                fact_name,
                unit_name,
                obs.value AS obs_json
            FROM fact_units,
            json_each(fact_units.observations_json) AS obs
        )
        SELECT
            cik,
            fact_namespace,
            fact_name,
            CASE
                WHEN try_cast(json_extract_string(obs_json, '$.val') AS DOUBLE) IS NOT NULL
                    THEN try_cast(json_extract_string(obs_json, '$.val') AS DOUBLE)
                ELSE NULL
            END AS fact_value_double,
            CASE
                WHEN try_cast(json_extract_string(obs_json, '$.val') AS DOUBLE) IS NULL
                    THEN json_extract_string(obs_json, '$.val')
                ELSE NULL
            END AS fact_value_text,
            unit_name,
            try_cast(json_extract_string(obs_json, '$.end') AS DATE) AS period_end,
            try_cast(json_extract_string(obs_json, '$.filed') AS DATE) AS filing_date,
            json_extract_string(obs_json, '$.accn') AS accession_number,
            filename AS source_file_path,
            json_member_name
        FROM fact_observations
    )
    TO {parquet_sql}
    (FORMAT PARQUET, COMPRESSION ZSTD)
    """


def _configure_connection_for_batch(temp_dir: Path):
    """
    Open and configure one fresh DuckDB connection.

    A fresh connection per batch helps prevent allocator / working-set buildup
    across the whole run.
    """
    conn = connect_build_db()

    # Escape apostrophes once before embedding into SQL.
    temp_dir_sql = str(temp_dir).replace("'", "''")

    # Conservative settings to reduce RAM pressure.
    conn.execute(f"SET threads={DUCKDB_THREADS}")
    conn.execute("SET preserve_insertion_order=false")
    conn.execute(f"SET temp_directory='{temp_dir_sql}'")

    return conn


def run() -> None:
    """
    Build a Parquet facts dataset from the latest staged SEC companyfacts batch.

    Runtime design:
    - batch file list
    - JSON explosion stays in DuckDB SQL
    - each batch writes one Parquet file
    - final verification reads the produced Parquet set

    Memory-control design:
    - small FILE_BATCH_SIZE,
    - fresh DuckDB connection for each batch,
    - separate verification connection at the end.
    """
    configure_logging()
    LOGGER.info("build-sec-companyfacts-parquet-from-staged-json started")

    settings = get_settings()

    stage_root = Path(settings.data_root) / "staging" / "sec" / "companyfacts"
    if not stage_root.exists():
        raise FileNotFoundError(f"Staging root not found: {stage_root}")

    stage_dir = _latest_stage_dir(stage_root)
    json_files = sorted(stage_dir.glob("*.json"))
    if not json_files:
        raise FileNotFoundError(
            f"No staged companyfacts json files found in {stage_dir}"
        )

    derived_root = (
        Path(settings.data_root)
        / "derived"
        / "sec"
        / "companyfacts_parquet"
        / stage_dir.name
    )
    derived_root.mkdir(parents=True, exist_ok=True)

    # Clean old parquet parts before rebuilding.
    for existing in derived_root.glob("*.parquet"):
        existing.unlink()

    file_batches = _chunked(json_files, FILE_BATCH_SIZE)

    temp_dir = Path(settings.data_root) / "build" / "duckdb_temp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    progress = tqdm(
        file_batches,
        desc="sec_companyfacts_to_parquet_batches",
        unit="batch",
    )
    progress.set_postfix(
        stage_dir=stage_dir.name,
        json_files=len(json_files),
        batch_size=FILE_BATCH_SIZE,
    )

    # -------------------------------------------------------------------------
    # Execute one batch at a time with a fresh connection.
    # -------------------------------------------------------------------------
    for batch_index, batch_files in enumerate(progress, start=1):
        parquet_output_path = derived_root / f"part-{batch_index:05d}.parquet"
        sql_text = _build_batch_sql(batch_files, parquet_output_path)

        conn = _configure_connection_for_batch(temp_dir)
        try:
            conn.execute(sql_text)
        finally:
            conn.close()

        progress.set_postfix(
            batch=batch_index,
            total_batches=len(file_batches),
            files_in_batch=len(batch_files),
            parquet_file=parquet_output_path.name,
        )

    parquet_files = sorted(derived_root.glob("*.parquet"))
    if not parquet_files:
        raise RuntimeError(f"No parquet files were written to {derived_root}")

    parquet_glob = str(derived_root / "*.parquet").replace("'", "''")

    # Final verification on a fresh connection.
    verify_conn = _configure_connection_for_batch(temp_dir)
    try:
        parquet_row_count = verify_conn.execute(
            f"""
            SELECT COUNT(*)
            FROM read_parquet('{parquet_glob}')
            """
        ).fetchone()[0]
    finally:
        verify_conn.close()

    print(
        {
            "status": "ok",
            "job": "build-sec-companyfacts-parquet-from-staged-json",
            "stage_dir": str(stage_dir),
            "json_file_count": len(json_files),
            "file_batch_size": FILE_BATCH_SIZE,
            "batch_count": len(file_batches),
            "derived_root": str(derived_root),
            "parquet_file_count": len(parquet_files),
            "parquet_row_count": parquet_row_count,
            "duckdb_threads": DUCKDB_THREADS,
            "temp_directory": str(temp_dir),
        }
    )

    LOGGER.info("build-sec-companyfacts-parquet-from-staged-json finished")


if __name__ == "__main__":
    run()
