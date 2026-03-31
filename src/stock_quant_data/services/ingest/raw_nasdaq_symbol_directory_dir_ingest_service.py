"""
Ingest raw Nasdaq symbol directory snapshot files into DuckDB.

Design goals:
- raw-only ingestion
- preserve provenance
- incremental skip by source_file_path
- SQL-first load path
- parameterized SQL for file metadata values
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from tqdm import tqdm

from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def ingest_raw_nasdaq_symbol_directory_dir(root_dir: str) -> dict:
    """
    Ingest all *.txt Nasdaq symbol directory snapshots found under root_dir.
    """
    root_path = Path(root_dir).expanduser().resolve()
    if not root_path.exists():
        raise FileNotFoundError(f"Nasdaq symbol directory root not found: {root_path}")

    txt_files = sorted([p for p in root_path.glob("*.txt") if p.is_file()])
    connection = connect_build_db()

    try:
        raw_rows_before = connection.execute(
            "SELECT COUNT(*) FROM raw.nasdaq_symbol_directory_snapshot"
        ).fetchone()[0]

        inserted_files = 0
        skipped_existing_files = 0
        rows_inserted_total = 0

        for file_path in tqdm(txt_files, desc="nasdaq_symdir_files", unit="file"):
            file_path_str = str(file_path)

            existing_count = connection.execute(
                """
                SELECT COUNT(*)
                FROM raw.nasdaq_symbol_directory_snapshot
                WHERE source_file_path = ?
                """,
                [file_path_str],
            ).fetchone()[0]

            if existing_count > 0:
                skipped_existing_files += 1
                continue

            lower_name = file_path.name.lower()
            if "nasdaqlisted" in lower_name:
                snapshot_file_type = "nasdaqlisted"
            elif "otherlisted" in lower_name:
                snapshot_file_type = "otherlisted"
            else:
                snapshot_file_type = "unknown"

            before_count = connection.execute(
                "SELECT COUNT(*) FROM raw.nasdaq_symbol_directory_snapshot"
            ).fetchone()[0]

            # IMPORTANT:
            # - keep file path passed as a parameter to read_csv(?)
            # - keep source metadata values passed as parameters
            # - avoid embedding JSON/double-quoted strings directly into SQL
            insert_sql = """
                INSERT INTO raw.nasdaq_symbol_directory_snapshot (
                    raw_nasdaq_symbol_directory_snapshot_id,
                    source_name,
                    source_root_dir,
                    source_file_path,
                    source_file_name,
                    snapshot_file_type,
                    source_line_number,
                    row_raw,
                    col_01_raw,
                    col_02_raw,
                    col_03_raw,
                    col_04_raw,
                    col_05_raw,
                    col_06_raw,
                    col_07_raw,
                    col_08_raw
                )
                SELECT
                    nextval('raw_nasdaq_symbol_directory_snapshot_id_seq'),
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    row_number() OVER () AS source_line_number,
                    array_to_string(
                        [column0, column1, column2, column3, column4, column5, column6, column7],
                        '|'
                    ) AS row_raw,
                    column0,
                    column1,
                    column2,
                    column3,
                    column4,
                    column5,
                    column6,
                    column7
                FROM read_csv(
                    ?,
                    delim='|',
                    header=false,
                    columns={
                        'column0': 'VARCHAR',
                        'column1': 'VARCHAR',
                        'column2': 'VARCHAR',
                        'column3': 'VARCHAR',
                        'column4': 'VARCHAR',
                        'column5': 'VARCHAR',
                        'column6': 'VARCHAR',
                        'column7': 'VARCHAR'
                    },
                    ignore_errors=true,
                    strict_mode=false
                )
            """

            params = [
                "nasdaq_symbol_directory",
                str(root_path),
                file_path_str,
                file_path.name,
                snapshot_file_type,
                file_path_str,
            ]

            connection.execute(insert_sql, params)

            after_count = connection.execute(
                "SELECT COUNT(*) FROM raw.nasdaq_symbol_directory_snapshot"
            ).fetchone()[0]

            file_rows_inserted = after_count - before_count
            inserted_files += 1
            rows_inserted_total += file_rows_inserted

            LOGGER.info(
                "Nasdaq symbol directory file ingested | file=%s | snapshot_file_type=%s | inserted_rows=%s",
                file_path.name,
                snapshot_file_type,
                file_rows_inserted,
            )

        raw_rows_after = connection.execute(
            "SELECT COUNT(*) FROM raw.nasdaq_symbol_directory_snapshot"
        ).fetchone()[0]

        result = {
            "root_dir": str(root_path),
            "files_discovered": len(txt_files),
            "inserted_files": inserted_files,
            "skipped_existing_files": skipped_existing_files,
            "raw_rows_before": raw_rows_before,
            "raw_rows_after": raw_rows_after,
            "rows_inserted": rows_inserted_total,
        }

        LOGGER.info("Nasdaq symbol directory ingest summary | %s", json.dumps(result))
        return result
    finally:
        connection.close()
