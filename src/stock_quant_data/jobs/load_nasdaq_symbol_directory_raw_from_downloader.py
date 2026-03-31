"""
Fast SQL-first loader for Nasdaq Trader symbol directory snapshots.
"""

from __future__ import annotations

import logging
from pathlib import Path

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)

DEFAULT_ROOT = "/home/marty/stock-quant-data-downloader/data/nasdaq/symdir"


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def run(root_path: str | None = None) -> None:
    configure_logging()
    LOGGER.info("load-nasdaq-symbol-directory-raw-from-downloader started")

    root = Path(root_path or DEFAULT_ROOT)
    if not root.exists():
        raise FileNotFoundError(f"Nasdaq symdir root does not exist: {root}")

    nasdaqlisted_files = sorted(
        str(p) for p in root.iterdir()
        if p.is_file() and p.name.endswith("_nasdaqlisted.txt")
    )
    otherlisted_files = sorted(
        str(p) for p in root.iterdir()
        if p.is_file() and p.name.endswith("_otherlisted.txt")
    )

    if not nasdaqlisted_files and not otherlisted_files:
        raise RuntimeError(f"No nasdaqlisted/otherlisted files found under {root}")

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS nasdaq_symbol_directory_raw")

        conn.execute(
            """
            CREATE TABLE nasdaq_symbol_directory_raw (
                raw_id BIGINT,
                snapshot_file VARCHAR NOT NULL,
                snapshot_id VARCHAR NOT NULL,
                source_kind VARCHAR NOT NULL,
                symbol VARCHAR,
                security_name VARCHAR,
                exchange_code VARCHAR,
                market_category VARCHAR,
                cqs_symbol VARCHAR,
                nasdaq_symbol VARCHAR,
                financial_status VARCHAR,
                round_lot_size BIGINT,
                etf_flag VARCHAR,
                nextshares_flag VARCHAR,
                test_issue_flag VARCHAR,
                raw_lineage VARCHAR,
                loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        next_id = 1

        if nasdaqlisted_files:
            files_sql = "[" + ", ".join(sql_quote(p) for p in nasdaqlisted_files) + "]"
            sql = f"""
            INSERT INTO nasdaq_symbol_directory_raw (
                raw_id,
                snapshot_file,
                snapshot_id,
                source_kind,
                symbol,
                security_name,
                exchange_code,
                market_category,
                cqs_symbol,
                nasdaq_symbol,
                financial_status,
                round_lot_size,
                etf_flag,
                nextshares_flag,
                test_issue_flag,
                raw_lineage,
                loaded_at
            )
            SELECT
                {next_id} - 1 + row_number() OVER () AS raw_id,
                filename AS snapshot_file,
                regexp_extract(filename, '/([^/]+)_(nasdaqlisted|otherlisted)\\.txt$', 1) AS snapshot_id,
                'nasdaqlisted' AS source_kind,
                "Symbol" AS symbol,
                "Security Name" AS security_name,
                'Q' AS exchange_code,
                "Market Category" AS market_category,
                NULL AS cqs_symbol,
                "Symbol" AS nasdaq_symbol,
                "Financial Status" AS financial_status,
                TRY_CAST("Round Lot Size" AS BIGINT) AS round_lot_size,
                "ETF" AS etf_flag,
                "NextShares" AS nextshares_flag,
                "Test Issue" AS test_issue_flag,
                'nasdaqlisted' AS raw_lineage,
                current_timestamp AS loaded_at
            FROM read_csv(
                {files_sql},
                delim='|',
                header=true,
                auto_detect=false,
                filename=true,
                ignore_errors=true,
                null_padding=true,
                columns={{
                    'Symbol': 'VARCHAR',
                    'Security Name': 'VARCHAR',
                    'Market Category': 'VARCHAR',
                    'Test Issue': 'VARCHAR',
                    'Financial Status': 'VARCHAR',
                    'Round Lot Size': 'VARCHAR',
                    'ETF': 'VARCHAR',
                    'NextShares': 'VARCHAR'
                }}
            )
            WHERE "Symbol" IS NOT NULL
              AND "Symbol" <> ''
              AND "Symbol" <> 'File Creation Time'
            """
            conn.execute(sql)
            next_id = conn.execute("SELECT COALESCE(MAX(raw_id), 0) + 1 FROM nasdaq_symbol_directory_raw").fetchone()[0]

        if otherlisted_files:
            files_sql = "[" + ", ".join(sql_quote(p) for p in otherlisted_files) + "]"
            sql = f"""
            INSERT INTO nasdaq_symbol_directory_raw (
                raw_id,
                snapshot_file,
                snapshot_id,
                source_kind,
                symbol,
                security_name,
                exchange_code,
                market_category,
                cqs_symbol,
                nasdaq_symbol,
                financial_status,
                round_lot_size,
                etf_flag,
                nextshares_flag,
                test_issue_flag,
                raw_lineage,
                loaded_at
            )
            SELECT
                {next_id} - 1 + row_number() OVER () AS raw_id,
                filename AS snapshot_file,
                regexp_extract(filename, '/([^/]+)_(nasdaqlisted|otherlisted)\\.txt$', 1) AS snapshot_id,
                'otherlisted' AS source_kind,
                "ACT Symbol" AS symbol,
                "Security Name" AS security_name,
                "Exchange" AS exchange_code,
                NULL AS market_category,
                "CQS Symbol" AS cqs_symbol,
                "NASDAQ Symbol" AS nasdaq_symbol,
                NULL AS financial_status,
                TRY_CAST("Round Lot Size" AS BIGINT) AS round_lot_size,
                "ETF" AS etf_flag,
                NULL AS nextshares_flag,
                "Test Issue" AS test_issue_flag,
                'otherlisted' AS raw_lineage,
                current_timestamp AS loaded_at
            FROM read_csv(
                {files_sql},
                delim='|',
                header=true,
                auto_detect=false,
                filename=true,
                ignore_errors=true,
                null_padding=true,
                columns={{
                    'ACT Symbol': 'VARCHAR',
                    'Security Name': 'VARCHAR',
                    'Exchange': 'VARCHAR',
                    'CQS Symbol': 'VARCHAR',
                    'ETF': 'VARCHAR',
                    'Round Lot Size': 'VARCHAR',
                    'Test Issue': 'VARCHAR',
                    'NASDAQ Symbol': 'VARCHAR'
                }}
            )
            WHERE "ACT Symbol" IS NOT NULL
              AND "ACT Symbol" <> ''
              AND "ACT Symbol" <> 'File Creation Time'
            """
            conn.execute(sql)

        total_rows = conn.execute(
            "SELECT COUNT(*) FROM nasdaq_symbol_directory_raw"
        ).fetchone()[0]

        by_kind = conn.execute(
            """
            SELECT source_kind, COUNT(*)
            FROM nasdaq_symbol_directory_raw
            GROUP BY source_kind
            ORDER BY source_kind
            """
        ).fetchall()

        snapshots = conn.execute(
            """
            SELECT snapshot_id, source_kind, COUNT(*)
            FROM nasdaq_symbol_directory_raw
            GROUP BY snapshot_id, source_kind
            ORDER BY snapshot_id, source_kind
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "load-nasdaq-symbol-directory-raw-from-downloader",
                "root": str(root),
                "nasdaqlisted_files": len(nasdaqlisted_files),
                "otherlisted_files": len(otherlisted_files),
                "total_rows": total_rows,
                "rows_by_kind": by_kind,
                "rows_by_snapshot": snapshots,
            }
        )
    finally:
        conn.close()

    LOGGER.info("load-nasdaq-symbol-directory-raw-from-downloader finished")


if __name__ == "__main__":
    run()
