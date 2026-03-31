"""
Raw Stooq directory ingestion service.

IMPORTANT ARCHITECTURE RULE:
- This service preserves Stooq rows exactly as they appear in the files.
- No ticker standardization is performed here.
- No exchange normalization is performed here.
- No security-type inference is performed here.
- No type casting is performed here for the market data fields.

PERFORMANCE DESIGN:
- This version is SQL-first.
- DuckDB reads the CSV files directly.
- We ingest by subdirectory, not row-by-row in Python.
- Already-ingested files are skipped at the file-path level.
"""

from __future__ import annotations

from pathlib import Path
import logging

from tqdm import tqdm

from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def _discover_stooq_subdirs(root_dir: Path) -> list[Path]:
    """
    Discover first-level Stooq subdirectories that contain the market/type splits.
    """
    return sorted(path for path in root_dir.iterdir() if path.is_dir())


def _count_txt_files_recursive(path: Path) -> int:
    """
    Count .txt files recursively under a path.
    """
    return sum(1 for item in path.rglob("*.txt") if item.is_file())


def ingest_raw_prices_stooq_dir(root_dir: str) -> dict:
    """
    Ingest a full local Stooq daily directory tree into raw.price_source_daily_stooq.

    Parameters
    ----------
    root_dir:
        Root directory containing the unpacked Stooq tree.

    Returns
    -------
    dict
        Basic ingestion metrics.

    Notes
    -----
    - The source file content is preserved as text.
    - Duplicate detection is file-path based.
    - If a file is already present, it is skipped entirely.
    - Loading is executed by DuckDB directly from the source files.
    """
    root_path = Path(root_dir).expanduser().resolve()

    if not root_path.exists():
        raise FileNotFoundError(f"Stooq root directory not found: {root_path}")

    if not root_path.is_dir():
        raise NotADirectoryError(f"Stooq root path is not a directory: {root_path}")

    subdirs = _discover_stooq_subdirs(root_path)

    if not subdirs:
        raise FileNotFoundError(f"No Stooq subdirectories found under: {root_path}")

    total_files_discovered = sum(_count_txt_files_recursive(subdir) for subdir in subdirs)

    connection = connect_build_db()

    try:
        before_count = connection.execute(
            "SELECT COUNT(*) FROM raw.price_source_daily_stooq"
        ).fetchone()[0]

        current_max_id = connection.execute(
            "SELECT COALESCE(MAX(raw_price_source_daily_stooq_id), 0) FROM raw.price_source_daily_stooq"
        ).fetchone()[0]

        next_id_start = int(current_max_id) + 1

        inserted_rows_total = 0
        inserted_files_total = 0
        skipped_existing_files_total = 0
        subdirs_processed = 0
        subdirs_skipped_empty = 0

        progress = tqdm(
            subdirs,
            desc="stooq_raw_dirs",
            unit="dir",
            leave=True,
        )

        for subdir in progress:
            txt_files_in_subdir = _count_txt_files_recursive(subdir)

            if txt_files_in_subdir == 0:
                subdirs_skipped_empty += 1
                LOGGER.info("Skipping empty Stooq subdir: %s", subdir)
                continue

            subdirs_processed += 1

            # IMPORTANT:
            # some Stooq subdirs contain nested folders like:
            #   nasdaq stocks/1
            #   nasdaq stocks/2
            # so we must use a recursive glob.
            subdir_glob = str(subdir / "**" / "*.txt").replace("'", "''")
            subdir_path_str = str(subdir)
            root_path_str = str(root_path)

            before_subdir_count = connection.execute(
                "SELECT COUNT(*) FROM raw.price_source_daily_stooq"
            ).fetchone()[0]

            existing_files_before = connection.execute(
                """
                SELECT COUNT(DISTINCT source_file_path)
                FROM raw.price_source_daily_stooq
                WHERE source_root_dir = ?
                  AND source_file_path LIKE ?
                """,
                [root_path_str, f"{subdir_path_str}/%"],
            ).fetchone()[0]

            insert_sql = f"""
                INSERT INTO raw.price_source_daily_stooq (
                    raw_price_source_daily_stooq_id,
                    source_name,
                    source_root_dir,
                    source_file_path,
                    source_file_name,
                    source_line_number,
                    ticker_raw,
                    per_raw,
                    date_raw,
                    time_raw,
                    open_raw,
                    high_raw,
                    low_raw,
                    close_raw,
                    vol_raw,
                    openint_raw,
                    row_raw
                )
                WITH staged AS (
                    SELECT
                        filename AS source_file_path,
                        regexp_extract(filename, '[^/]+$') AS source_file_name,
                        ticker_raw,
                        per_raw,
                        date_raw,
                        time_raw,
                        open_raw,
                        high_raw,
                        low_raw,
                        close_raw,
                        vol_raw,
                        openint_raw,
                        concat_ws(
                            ',',
                            ticker_raw,
                            per_raw,
                            date_raw,
                            time_raw,
                            open_raw,
                            high_raw,
                            low_raw,
                            close_raw,
                            vol_raw,
                            openint_raw
                        ) AS row_raw
                    FROM read_csv(
                        '{subdir_glob}',
                        delim=',',
                        header=false,
                        skip=1,
                        filename=true,
                        columns={{
                            'ticker_raw': 'VARCHAR',
                            'per_raw': 'VARCHAR',
                            'date_raw': 'VARCHAR',
                            'time_raw': 'VARCHAR',
                            'open_raw': 'VARCHAR',
                            'high_raw': 'VARCHAR',
                            'low_raw': 'VARCHAR',
                            'close_raw': 'VARCHAR',
                            'vol_raw': 'VARCHAR',
                            'openint_raw': 'VARCHAR'
                        }}
                    )
                ),
                filtered AS (
                    SELECT *
                    FROM staged
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM raw.price_source_daily_stooq existing
                        WHERE existing.source_file_path = staged.source_file_path
                    )
                ),
                numbered AS (
                    SELECT
                        {next_id_start} + ROW_NUMBER() OVER (
                            ORDER BY source_file_path, date_raw, time_raw, row_raw
                        ) - 1 AS raw_price_source_daily_stooq_id,
                        *
                    FROM filtered
                )
                SELECT
                    raw_price_source_daily_stooq_id,
                    'stooq' AS source_name,
                    '{root_path_str.replace("'", "''")}' AS source_root_dir,
                    source_file_path,
                    source_file_name,
                    NULL AS source_line_number,
                    ticker_raw,
                    per_raw,
                    date_raw,
                    time_raw,
                    open_raw,
                    high_raw,
                    low_raw,
                    close_raw,
                    vol_raw,
                    openint_raw,
                    row_raw
                FROM numbered
            """

            connection.execute(insert_sql)

            after_subdir_count = connection.execute(
                "SELECT COUNT(*) FROM raw.price_source_daily_stooq"
            ).fetchone()[0]

            inserted_rows_subdir = int(after_subdir_count - before_subdir_count)
            inserted_rows_total += inserted_rows_subdir
            next_id_start += inserted_rows_subdir

            existing_files_after = connection.execute(
                """
                SELECT COUNT(DISTINCT source_file_path)
                FROM raw.price_source_daily_stooq
                WHERE source_root_dir = ?
                  AND source_file_path LIKE ?
                """,
                [root_path_str, f"{subdir_path_str}/%"],
            ).fetchone()[0]

            inserted_files_subdir = int(existing_files_after - existing_files_before)
            inserted_files_subdir = max(inserted_files_subdir, 0)
            inserted_files_total += inserted_files_subdir

            skipped_existing_files_subdir = int(txt_files_in_subdir - inserted_files_subdir)
            skipped_existing_files_subdir = max(skipped_existing_files_subdir, 0)
            skipped_existing_files_total += skipped_existing_files_subdir

            LOGGER.info(
                "Stooq subdir ingested | subdir=%s | txt_files=%s | inserted_files=%s | skipped_existing_files=%s | inserted_rows=%s",
                subdir.name,
                txt_files_in_subdir,
                inserted_files_subdir,
                skipped_existing_files_subdir,
                inserted_rows_subdir,
            )

        after_count = connection.execute(
            "SELECT COUNT(*) FROM raw.price_source_daily_stooq"
        ).fetchone()[0]

        distinct_files = connection.execute(
            """
            SELECT COUNT(DISTINCT source_file_path)
            FROM raw.price_source_daily_stooq
            WHERE source_root_dir = ?
            """,
            [str(root_path)],
        ).fetchone()[0]

        return {
            "root_dir": str(root_path),
            "subdirs_discovered": len(subdirs),
            "subdirs_processed": int(subdirs_processed),
            "subdirs_skipped_empty": int(subdirs_skipped_empty),
            "files_discovered": int(total_files_discovered),
            "inserted_files": int(inserted_files_total),
            "skipped_existing_files": int(skipped_existing_files_total),
            "distinct_files_loaded_for_root": int(distinct_files),
            "rows_inserted": int(inserted_rows_total),
            "raw_rows_before": int(before_count),
            "raw_rows_after": int(after_count),
            "raw_total_rows": int(after_count),
        }
    finally:
        connection.close()
