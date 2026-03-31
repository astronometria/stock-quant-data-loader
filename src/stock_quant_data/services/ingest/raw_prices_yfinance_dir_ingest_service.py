"""
Raw yfinance directory ingestion service.

IMPORTANT ARCHITECTURE RULE:
- This service preserves yfinance rows exactly as they appear in the CSV files.
- No ticker standardization is performed here.
- No exchange normalization is performed here.
- No instrument mapping is performed here.
- No business typing/casting is performed here for the market data fields.

PERFORMANCE DESIGN:
- SQL-first ingestion through DuckDB read_csv_auto.
- Recursive file discovery by DuckDB-friendly glob.
- Duplicate detection is file-path based.
- Already ingested files are skipped at the file-path level.

OBSERVED CSV FORMAT:
- Date
- Open
- High
- Low
- Close
- Adj Close
- Volume
- Dividends
- Stock Splits
"""

from __future__ import annotations

from pathlib import Path
import logging

from tqdm import tqdm

from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def _discover_symbol_dirs(root_dir: Path) -> list[Path]:
    """
    Discover first-level symbol directories under the yfinance daily root.
    """
    return sorted(path for path in root_dir.iterdir() if path.is_dir())


def _count_csv_files_recursive(path: Path) -> int:
    """
    Count .csv files recursively under a path.
    """
    return sum(1 for item in path.rglob("*.csv") if item.is_file())


def ingest_raw_prices_yfinance_dir(root_dir: str) -> dict:
    """
    Ingest a local yfinance daily directory tree into raw.price_source_daily_yfinance.
    """
    root_path = Path(root_dir).expanduser().resolve()

    if not root_path.exists():
        raise FileNotFoundError(f"yfinance root directory not found: {root_path}")

    if not root_path.is_dir():
        raise NotADirectoryError(f"yfinance root path is not a directory: {root_path}")

    symbol_dirs = _discover_symbol_dirs(root_path)

    if not symbol_dirs:
        raise FileNotFoundError(f"No symbol subdirectories found under: {root_path}")

    total_files_discovered = sum(_count_csv_files_recursive(symbol_dir) for symbol_dir in symbol_dirs)

    connection = connect_build_db()

    try:
        before_count = connection.execute(
            "SELECT COUNT(*) FROM raw.price_source_daily_yfinance"
        ).fetchone()[0]

        current_max_id = connection.execute(
            "SELECT COALESCE(MAX(raw_price_source_daily_yfinance_id), 0) FROM raw.price_source_daily_yfinance"
        ).fetchone()[0]

        next_id_start = int(current_max_id) + 1

        inserted_rows_total = 0
        inserted_files_total = 0
        skipped_existing_files_total = 0
        symbol_dirs_processed = 0
        symbol_dirs_skipped_empty = 0

        progress = tqdm(
            symbol_dirs,
            desc="yfinance_raw_dirs",
            unit="dir",
            leave=True,
        )

        for symbol_dir in progress:
            csv_files_in_dir = _count_csv_files_recursive(symbol_dir)

            if csv_files_in_dir == 0:
                symbol_dirs_skipped_empty += 1
                LOGGER.info("Skipping empty yfinance symbol dir: %s", symbol_dir)
                continue

            symbol_dirs_processed += 1

            symbol_dir_glob = str(symbol_dir / "**" / "*.csv").replace("'", "''")
            symbol_dir_path_str = str(symbol_dir)
            root_path_str = str(root_path)

            before_dir_count = connection.execute(
                "SELECT COUNT(*) FROM raw.price_source_daily_yfinance"
            ).fetchone()[0]

            existing_files_before = connection.execute(
                """
                SELECT COUNT(DISTINCT source_file_path)
                FROM raw.price_source_daily_yfinance
                WHERE source_root_dir = ?
                  AND source_file_path LIKE ?
                """,
                [root_path_str, f"{symbol_dir_path_str}/%"],
            ).fetchone()[0]

            insert_sql = f"""
                INSERT INTO raw.price_source_daily_yfinance (
                    raw_price_source_daily_yfinance_id,
                    source_name,
                    source_root_dir,
                    source_file_path,
                    source_file_name,
                    source_line_number,
                    date_raw,
                    open_raw,
                    high_raw,
                    low_raw,
                    close_raw,
                    adj_close_raw,
                    volume_raw,
                    dividends_raw,
                    stock_splits_raw,
                    row_raw
                )
                WITH staged AS (
                    SELECT
                        filename AS source_file_path,
                        regexp_extract(filename, '[^/]+$') AS source_file_name,
                        "Date" AS date_raw,
                        "Open" AS open_raw,
                        "High" AS high_raw,
                        "Low" AS low_raw,
                        "Close" AS close_raw,
                        "Adj Close" AS adj_close_raw,
                        "Volume" AS volume_raw,
                        "Dividends" AS dividends_raw,
                        "Stock Splits" AS stock_splits_raw,
                        concat_ws(
                            ',',
                            "Date",
                            "Open",
                            "High",
                            "Low",
                            "Close",
                            "Adj Close",
                            "Volume",
                            "Dividends",
                            "Stock Splits"
                        ) AS row_raw
                    FROM read_csv_auto(
                        '{symbol_dir_glob}',
                        header=true,
                        all_varchar=true,
                        filename=true,
                        union_by_name=true
                    )
                ),
                filtered AS (
                    SELECT *
                    FROM staged
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM raw.price_source_daily_yfinance existing
                        WHERE existing.source_file_path = staged.source_file_path
                    )
                ),
                numbered AS (
                    SELECT
                        {next_id_start} + ROW_NUMBER() OVER (
                            ORDER BY source_file_path, date_raw, row_raw
                        ) - 1 AS raw_price_source_daily_yfinance_id,
                        *
                    FROM filtered
                )
                SELECT
                    raw_price_source_daily_yfinance_id,
                    'yfinance' AS source_name,
                    '{root_path_str.replace("'", "''")}' AS source_root_dir,
                    source_file_path,
                    source_file_name,
                    NULL AS source_line_number,
                    date_raw,
                    open_raw,
                    high_raw,
                    low_raw,
                    close_raw,
                    adj_close_raw,
                    volume_raw,
                    dividends_raw,
                    stock_splits_raw,
                    row_raw
                FROM numbered
            """

            connection.execute(insert_sql)

            after_dir_count = connection.execute(
                "SELECT COUNT(*) FROM raw.price_source_daily_yfinance"
            ).fetchone()[0]

            inserted_rows_dir = int(after_dir_count - before_dir_count)
            inserted_rows_total += inserted_rows_dir
            next_id_start += inserted_rows_dir

            existing_files_after = connection.execute(
                """
                SELECT COUNT(DISTINCT source_file_path)
                FROM raw.price_source_daily_yfinance
                WHERE source_root_dir = ?
                  AND source_file_path LIKE ?
                """,
                [root_path_str, f"{symbol_dir_path_str}/%"],
            ).fetchone()[0]

            inserted_files_dir = int(existing_files_after - existing_files_before)
            inserted_files_dir = max(inserted_files_dir, 0)
            inserted_files_total += inserted_files_dir

            skipped_existing_files_dir = int(csv_files_in_dir - inserted_files_dir)
            skipped_existing_files_dir = max(skipped_existing_files_dir, 0)
            skipped_existing_files_total += skipped_existing_files_dir

            LOGGER.info(
                "yfinance symbol dir ingested | symbol_dir=%s | csv_files=%s | inserted_files=%s | skipped_existing_files=%s | inserted_rows=%s",
                symbol_dir.name,
                csv_files_in_dir,
                inserted_files_dir,
                skipped_existing_files_dir,
                inserted_rows_dir,
            )

        after_count = connection.execute(
            "SELECT COUNT(*) FROM raw.price_source_daily_yfinance"
        ).fetchone()[0]

        distinct_files = connection.execute(
            """
            SELECT COUNT(DISTINCT source_file_path)
            FROM raw.price_source_daily_yfinance
            WHERE source_root_dir = ?
            """,
            [str(root_path)],
        ).fetchone()[0]

        return {
            "root_dir": str(root_path),
            "symbol_dirs_discovered": len(symbol_dirs),
            "symbol_dirs_processed": int(symbol_dirs_processed),
            "symbol_dirs_skipped_empty": int(symbol_dirs_skipped_empty),
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
