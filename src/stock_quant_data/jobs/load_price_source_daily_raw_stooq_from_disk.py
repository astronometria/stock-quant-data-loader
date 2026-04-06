"""
Load raw Stooq daily files from disk into price_source_daily_raw_stooq.

Supported source roots:
- downloader/data/prices/stooq/daily/us
- local data/stooq
- legacy ~/stock-quant-oop-raw/data/raw/stooq
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from tqdm import tqdm

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.config.settings import get_settings
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def _discover_stooq_files() -> tuple[Path, list[Path]]:
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
    raise FileNotFoundError(f"No Stooq CSV files found under any candidate root: {searched}")


def _row_value(row: dict, *names: str):
    for name in names:
        if name in row and row[name] not in (None, ""):
            return row[name]
    return None


def run() -> None:
    configure_logging()
    LOGGER.info("load-price-source-daily-raw-stooq-from-disk started")

    source_root, files = _discover_stooq_files()

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM price_source_daily_raw_stooq")

        rows: list[tuple] = []
        raw_price_id = 0

        for file_path in tqdm(files, desc="stooq_raw_files", unit="file"):
            delimiter = "," if file_path.suffix.lower() == ".csv" else ","
            with file_path.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh, delimiter=delimiter)
                for row in reader:
                    raw_symbol = (
                        _row_value(row, "symbol", "Symbol")
                        or file_path.stem.split(".")[0]
                    )
                    price_date = _row_value(row, "date", "Date")
                    open_ = _row_value(row, "open", "Open")
                    high = _row_value(row, "high", "High")
                    low = _row_value(row, "low", "Low")
                    close = _row_value(row, "close", "Close")
                    volume = _row_value(row, "volume", "Volume")

                    if not raw_symbol or not price_date:
                        continue

                    raw_price_id += 1
                    rows.append(
                        (
                            raw_price_id,
                            raw_symbol,
                            price_date,
                            open_,
                            high,
                            low,
                            close,
                            volume,
                            str(file_path),
                        )
                    )

        if rows:
            conn.executemany(
                """
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

        print(
            {
                "status": "ok",
                "job": "load-price-source-daily-raw-stooq-from-disk",
                "source_root": str(source_root),
                "file_count": len(files),
                "row_count": conn.execute("SELECT COUNT(*) FROM price_source_daily_raw_stooq").fetchone()[0],
            }
        )
    finally:
        conn.close()
        LOGGER.info("load-price-source-daily-raw-stooq-from-disk finished")


if __name__ == "__main__":
    run()
