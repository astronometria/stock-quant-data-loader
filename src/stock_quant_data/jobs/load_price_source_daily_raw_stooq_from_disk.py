"""
Load Stooq daily raw prices from disk into the canonical raw table.

Current canonical target:
- price_source_daily_raw_stooq

The loader expects a local disk mirror already available. This repo should not
perform external downloading here.
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


def _find_stooq_csv_files(root: Path) -> list[Path]:
    """
    Find all csv files under the local stooq mirror.
    """
    files = sorted(root.rglob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No Stooq CSV files found under {root}")
    return files


def run() -> None:
    """
    Load Stooq raw daily price rows into price_source_daily_raw_stooq.
    """
    configure_logging()
    LOGGER.info("load-price-source-daily-raw-stooq-from-disk started")

    settings = get_settings()

    # Prefer colocated local raw mirror if configured under current repo data.
    stooq_root = Path(settings.data_root) / "stooq"
    if not stooq_root.exists():
        # Fallback to a common sibling runtime/raw location pattern.
        stooq_root = Path.home() / "stock-quant-oop-raw" / "data" / "raw" / "stooq"

    csv_files = _find_stooq_csv_files(stooq_root)

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM price_source_daily_raw_stooq")

        rows: list[tuple] = []
        raw_price_id = 0

        for csv_path in tqdm(csv_files, desc="stooq_csv_files", unit="file"):
            with csv_path.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)

                for row in reader:
                    raw_symbol = (
                        row.get("symbol")
                        or row.get("Symbol")
                        or csv_path.stem.upper()
                    )

                    price_date = row.get("date") or row.get("Date")
                    open_ = row.get("open") or row.get("Open")
                    high = row.get("high") or row.get("High")
                    low = row.get("low") or row.get("Low")
                    close = row.get("close") or row.get("Close")
                    volume = row.get("volume") or row.get("Volume") or 0

                    # Skip obviously malformed rows cleanly.
                    if not raw_symbol or not price_date or open_ in (None, "") or high in (None, "") or low in (None, "") or close in (None, ""):
                        continue

                    raw_price_id += 1
                    rows.append(
                        (
                            raw_price_id,
                            raw_symbol,
                            price_date,
                            float(open_),
                            float(high),
                            float(low),
                            float(close),
                            int(float(volume)),
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
                    volume
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

        row_count = conn.execute(
            "SELECT COUNT(*) FROM price_source_daily_raw_stooq"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "load-price-source-daily-raw-stooq-from-disk",
                "csv_file_count": len(csv_files),
                "row_count": row_count,
            }
        )
    finally:
        conn.close()
        LOGGER.info("load-price-source-daily-raw-stooq-from-disk finished")


if __name__ == "__main__":
    run()
