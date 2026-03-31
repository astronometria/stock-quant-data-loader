"""
Fast batch SQL-first Stooq loader.

Approach:
- list files in Python (thin orchestration only)
- load batches of files with one DuckDB SQL statement per batch
- avoid per-row Python parsing
"""

from __future__ import annotations

import logging
from pathlib import Path

from tqdm import tqdm

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)

DEFAULT_ROOT = "/home/marty/stock-quant-oop-raw/data/raw/stooq/daily/us"
BATCH_SIZE = 250


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_file_list_sql(files: list[str]) -> str:
    return "[" + ", ".join(sql_quote(f) for f in files) + "]"


def run(root_path: str | None = None) -> None:
    configure_logging()
    LOGGER.info("load-price-source-daily-raw-stooq-from-disk started")

    root = Path(root_path or DEFAULT_ROOT)
    if not root.exists():
        raise FileNotFoundError(f"Stooq root does not exist: {root}")

    files = sorted(str(p) for p in root.rglob("*.txt") if p.is_file())
    if not files:
        raise RuntimeError(f"No .txt files found under {root}")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS price_source_daily_raw_stooq")
        conn.execute(
            """
            CREATE TABLE price_source_daily_raw_stooq (
                raw_stooq_id BIGINT,
                raw_symbol VARCHAR NOT NULL,
                raw_ticker VARCHAR,
                raw_per VARCHAR,
                raw_time VARCHAR,
                price_date DATE NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                raw_volume DOUBLE,
                raw_open_interest DOUBLE,
                source_file VARCHAR,
                source_category VARCHAR,
                landed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        next_id = 1

        for start in tqdm(range(0, len(files), BATCH_SIZE), desc="stooq_raw_batches", unit="batch"):
            batch_files = files[start:start + BATCH_SIZE]
            file_list_sql = build_file_list_sql(batch_files)

            sql = f"""
            INSERT INTO price_source_daily_raw_stooq (
                raw_stooq_id,
                raw_symbol,
                raw_ticker,
                raw_per,
                raw_time,
                price_date,
                open,
                high,
                low,
                close,
                raw_volume,
                raw_open_interest,
                source_file,
                source_category,
                landed_at
            )
            WITH raw AS (
                SELECT
                    filename,
                    "<TICKER>" AS raw_ticker,
                    "<PER>" AS raw_per,
                    "<DATE>" AS raw_date,
                    "<TIME>" AS raw_time,
                    "<OPEN>"::DOUBLE AS open,
                    "<HIGH>"::DOUBLE AS high,
                    "<LOW>"::DOUBLE AS low,
                    "<CLOSE>"::DOUBLE AS close,
                    "<VOL>"::DOUBLE AS raw_volume,
                    "<OPENINT>"::DOUBLE AS raw_open_interest
                FROM read_csv(
                    {file_list_sql},
                    delim=',',
                    header=true,
                    auto_detect=false,
                    columns={{
                        '<TICKER>': 'VARCHAR',
                        '<PER>': 'VARCHAR',
                        '<DATE>': 'VARCHAR',
                        '<TIME>': 'VARCHAR',
                        '<OPEN>': 'VARCHAR',
                        '<HIGH>': 'VARCHAR',
                        '<LOW>': 'VARCHAR',
                        '<CLOSE>': 'VARCHAR',
                        '<VOL>': 'VARCHAR',
                        '<OPENINT>': 'VARCHAR'
                    }},
                    filename=true
                )
            )
            SELECT
                {next_id} - 1 + row_number() OVER () AS raw_stooq_id,
                upper(replace(replace(raw_ticker, '.US', ''), '.us', '')) AS raw_symbol,
                raw_ticker,
                raw_per,
                raw_time,
                CAST(substr(raw_date, 1, 4) || '-' || substr(raw_date, 5, 2) || '-' || substr(raw_date, 7, 2) AS DATE) AS price_date,
                open,
                high,
                low,
                close,
                raw_volume,
                raw_open_interest,
                filename AS source_file,
                regexp_extract(filename, '/us/([^/]+)/', 1) AS source_category,
                current_timestamp AS landed_at
            FROM raw
            WHERE raw_per = 'D'
            """
            conn.execute(sql)

            inserted = conn.execute("SELECT COUNT(*) FROM price_source_daily_raw_stooq").fetchone()[0]
            next_id = inserted + 1

        total_rows = conn.execute(
            "SELECT COUNT(*) FROM price_source_daily_raw_stooq"
        ).fetchone()[0]

        min_max_dates = conn.execute(
            "SELECT MIN(price_date), MAX(price_date) FROM price_source_daily_raw_stooq"
        ).fetchone()

        by_category = conn.execute(
            """
            SELECT source_category, COUNT(*)
            FROM price_source_daily_raw_stooq
            GROUP BY source_category
            ORDER BY source_category
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "load-price-source-daily-raw-stooq-from-disk",
                "root": str(root),
                "files_count": len(files),
                "raw_table_total_rows": total_rows,
                "raw_table_min_price_date": str(min_max_dates[0]) if min_max_dates[0] is not None else None,
                "raw_table_max_price_date": str(min_max_dates[1]) if min_max_dates[1] is not None else None,
                "rows_by_category": by_category,
            }
        )
    finally:
        conn.close()

    LOGGER.info("load-price-source-daily-raw-stooq-from-disk finished")


if __name__ == "__main__":
    run()
