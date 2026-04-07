"""
Fast bulk loader for Stooq daily files into price_source_daily_raw_stooq.

Design goals:
- Replace slow Python row-by-row CSV parsing with DuckDB bulk CSV ingestion.
- Keep a resumable incremental mode.
- Use SQL-first loading for much better throughput.
- Keep Python thin: discover files, batch them, execute SQL.

Supported source roots:
- downloader/data/prices/stooq/daily/us
- local data/stooq
- legacy ~/stock-quant-oop-raw/data/raw/stooq

Modes:
- default / --full-refresh:
    rebuild the target raw table from scratch
- --incremental:
    ingest only files whose source_file_path is not already present
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from tqdm import tqdm

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.config.settings import get_settings
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Tune this if needed.
# Larger batches reduce SQL overhead, but too large can increase memory usage.
# -----------------------------------------------------------------------------
FILE_BATCH_SIZE = 250


def _discover_stooq_files() -> tuple[Path, list[Path]]:
    """Find the first candidate Stooq root that actually contains files."""
    settings = get_settings()

    candidate_roots = [
        Path(settings.downloader_data_dir) / "prices" / "stooq" / "daily" / "us",
        Path(settings.data_root) / "stooq",
        Path.home() / "stock-quant-oop-raw" / "data" / "raw" / "stooq",
    ]

    for root in candidate_roots:
        if not root.exists():
            continue

        files = sorted(root.rglob("*.txt")) + sorted(root.rglob("*.csv"))
        if files:
            return root, files

    searched = ", ".join(str(p) for p in candidate_roots)
    raise FileNotFoundError(
        f"No Stooq CSV/TXT files found under any candidate root: {searched}"
    )


def _chunked(items: list[Path], chunk_size: int) -> list[list[Path]]:
    """Split a list into fixed-size chunks."""
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def _quote_sql_string(value: str) -> str:
    """Safely quote one SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def _ensure_checkpoint_table(conn) -> None:
    """Create the file checkpoint table if needed."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stooq_ingested_files (
            source_file_path VARCHAR PRIMARY KEY,
            file_size_bytes BIGINT,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _clear_checkpoint_table(conn) -> None:
    """Clear checkpoint state for a full refresh."""
    conn.execute("DELETE FROM stooq_ingested_files")


def _existing_checkpoint_files(conn) -> set[str]:
    """Return already checkpointed source_file_path values."""
    rows = conn.execute(
        "SELECT source_file_path FROM stooq_ingested_files"
    ).fetchall()
    return {row[0] for row in rows if row[0] is not None}


def _next_raw_price_id(conn) -> int:
    """Continue raw_price_id allocation after current max."""
    return conn.execute(
        "SELECT COALESCE(MAX(raw_price_id), 0) FROM price_source_daily_raw_stooq"
    ).fetchone()[0]


def _mark_batch_completed(conn, batch_files: list[Path]) -> None:
    """Mark all files in one successfully loaded batch as completed."""
    rows = [(str(p), p.stat().st_size) for p in batch_files]
    conn.executemany(
        """
        INSERT OR REPLACE INTO stooq_ingested_files (
            source_file_path,
            file_size_bytes,
            loaded_at
        )
        VALUES (?, ?, CURRENT_TIMESTAMP)
        """,
        rows,
    )


def _build_batch_sql(batch_files: list[Path], start_raw_price_id: int) -> str:
    """
    Build one SQL-first bulk insert for a batch of Stooq files.

    Notes:
    - We read native Stooq headers directly with DuckDB.
    - filename=true gives us source_file_path.
    - We normalize DATE and VOL in SQL.
    - raw_price_id is generated deterministically inside the batch using
      ROW_NUMBER() plus the supplied offset.
    """
    file_list_sql = ", ".join(_quote_sql_string(str(p)) for p in batch_files)

    return f"""
    INSERT INTO price_source_daily_raw_stooq (
        raw_price_id,
        raw_symbol,
        price_date,
        open,
        high,
        low,
        close,
        volume,
        source_file_path
    )
    WITH src AS (
        SELECT
            filename AS source_file_path,
            "<TICKER>" AS raw_symbol,
            "<DATE>" AS raw_date_text,
            "<OPEN>" AS open_text,
            "<HIGH>" AS high_text,
            "<LOW>" AS low_text,
            "<CLOSE>" AS close_text,
            "<VOL>" AS volume_text
        FROM read_csv(
            [{file_list_sql}],
            auto_detect=true,
            header=true,
            delim=',',
            filename=true
        )
    ),
    cleaned AS (
        SELECT
            source_file_path,
            trim(raw_symbol) AS raw_symbol,
            CASE
                WHEN raw_date_text IS NOT NULL
                     AND length(trim(CAST(raw_date_text AS VARCHAR))) = 8
                THEN strptime(trim(CAST(raw_date_text AS VARCHAR)), '%Y%m%d')::DATE
                ELSE NULL
            END AS price_date,
            try_cast(open_text AS DOUBLE) AS open,
            try_cast(high_text AS DOUBLE) AS high,
            try_cast(low_text AS DOUBLE) AS low,
            try_cast(close_text AS DOUBLE) AS close,
            try_cast(round(try_cast(volume_text AS DOUBLE)) AS BIGINT) AS volume
        FROM src
    ),
    valid_rows AS (
        SELECT
            source_file_path,
            raw_symbol,
            price_date,
            open,
            high,
            low,
            close,
            volume
        FROM cleaned
        WHERE raw_symbol IS NOT NULL
          AND raw_symbol <> ''
          AND price_date IS NOT NULL
          AND NOT (open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL)
    ),
    numbered AS (
        SELECT
            {start_raw_price_id} + ROW_NUMBER() OVER (
                ORDER BY source_file_path, raw_symbol, price_date
            ) AS raw_price_id,
            raw_symbol,
            price_date,
            open,
            high,
            low,
            close,
            volume,
            source_file_path
        FROM valid_rows
    )
    SELECT
        raw_price_id,
        raw_symbol,
        price_date,
        open,
        high,
        low,
        close,
        volume,
        source_file_path
    FROM numbered
    """


def parse_args() -> argparse.Namespace:
    """Parse CLI flags."""
    parser = argparse.ArgumentParser(
        description="Fast bulk load raw Stooq daily files into price_source_daily_raw_stooq."
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Ingest only files not yet checkpointed in stooq_ingested_files.",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Delete target content and checkpoint state before rebuilding.",
    )
    parser.add_argument(
        "--file-batch-size",
        type=int,
        default=FILE_BATCH_SIZE,
        help="Number of files per DuckDB bulk load batch.",
    )
    return parser.parse_args()


def run() -> None:
    """
    Load Stooq raw daily rows into price_source_daily_raw_stooq using bulk SQL.
    """
    args = parse_args()

    if args.incremental and args.full_refresh:
        raise ValueError("Use either --incremental or --full-refresh, not both.")

    configure_logging()
    LOGGER.info("load-price-source-daily-raw-stooq-from-disk started")

    source_root, files = _discover_stooq_files()

    conn = connect_build_db()
    try:
        _ensure_checkpoint_table(conn)

        mode = "incremental" if args.incremental else "full_refresh"

        if args.incremental:
            checkpointed_files = _existing_checkpoint_files(conn)
        else:
            conn.execute("DELETE FROM price_source_daily_raw_stooq")
            _clear_checkpoint_table(conn)
            checkpointed_files = set()

        files_to_process = [p for p in files if str(p) not in checkpointed_files]
        skipped_existing_files = len(files) - len(files_to_process)

        file_batches = _chunked(files_to_process, args.file_batch_size)
        inserted_rows_runtime = 0
        processed_file_count = 0

        progress = tqdm(file_batches, desc="stooq_raw_file_batches", unit="batch")
        progress.set_postfix(
            mode=mode,
            file_batch_size=args.file_batch_size,
            discovered_file_count=len(files),
            to_process=len(files_to_process),
            skipped_existing_files=skipped_existing_files,
        )

        for batch_index, batch_files in enumerate(progress, start=1):
            start_raw_price_id = _next_raw_price_id(conn)
            before_count = conn.execute(
                "SELECT COUNT(*) FROM price_source_daily_raw_stooq"
            ).fetchone()[0]

            sql_text = _build_batch_sql(batch_files, start_raw_price_id)
            conn.execute(sql_text)

            _mark_batch_completed(conn, batch_files)
            conn.commit()

            after_count = conn.execute(
                "SELECT COUNT(*) FROM price_source_daily_raw_stooq"
            ).fetchone()[0]

            inserted_this_batch = after_count - before_count
            inserted_rows_runtime += inserted_this_batch
            processed_file_count += len(batch_files)

            progress.set_postfix(
                mode=mode,
                batch=batch_index,
                total_batches=len(file_batches),
                processed_file_count=processed_file_count,
                remaining_file_count=len(files_to_process) - processed_file_count,
                inserted_rows=inserted_rows_runtime,
                inserted_this_batch=inserted_this_batch,
                current_last_file=batch_files[-1].name,
                file_batch_size=args.file_batch_size,
                skipped_existing_files=skipped_existing_files,
            )

        print(
            {
                "status": "ok",
                "job": "load-price-source-daily-raw-stooq-from-disk",
                "mode": mode,
                "source_root": str(source_root),
                "discovered_file_count": len(files),
                "processed_file_count": len(files_to_process),
                "skipped_existing_files": skipped_existing_files,
                "checkpointed_file_count": conn.execute(
                    "SELECT COUNT(*) FROM stooq_ingested_files"
                ).fetchone()[0],
                "row_count": conn.execute(
                    "SELECT COUNT(*) FROM price_source_daily_raw_stooq"
                ).fetchone()[0],
                "inserted_rows_runtime": inserted_rows_runtime,
                "file_batch_size": args.file_batch_size,
            }
        )
    finally:
        conn.close()
        LOGGER.info("load-price-source-daily-raw-stooq-from-disk finished")


if __name__ == "__main__":
    run()
