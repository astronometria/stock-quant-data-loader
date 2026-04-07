"""
Load canonical SEC companyfacts raw rows from a derived Parquet facts dataset.

Why this version exists:
- Keep the ingestion path SQL-first.
- Keep Python thin and deterministic.
- Reuse the SQL file under sql/etl/sec/ instead of embedding the full query
  inline in Python.
- Preserve the existing target contract into sec_companyfacts_raw.

Runtime flow:
1) Resolve the latest parquet batch directory.
2) Count parquet files for logging.
3) Load SQL template from sql/etl/sec/load_sec_companyfacts_raw_from_parquet.sql
4) Replace the parquet glob placeholder.
5) Execute the SQL in DuckDB.
6) Verify final target row count.

Notes:
- This loader intentionally does not reparse SEC JSON.
- It assumes the parquet builder already produced the canonical flattened
  companyfacts dataset.
"""

from __future__ import annotations

import logging
from pathlib import Path

from tqdm import tqdm

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.config.settings import get_settings
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def _latest_parquet_dir(parquet_root: Path) -> Path:
    """Resolve the latest Parquet batch directory."""
    candidates = sorted([p for p in parquet_root.iterdir() if p.is_dir()])
    if not candidates:
        raise FileNotFoundError(
            f"No Parquet companyfacts batch found under {parquet_root}"
        )
    return candidates[-1]


def _load_sql_template(repo_root: Path) -> str:
    """
    Read the SQL template from disk.

    Keeping SQL in a dedicated file makes the loader easier to audit and keeps
    the Python orchestration layer small.
    """
    sql_path = (
        repo_root
        / "sql"
        / "etl"
        / "sec"
        / "load_sec_companyfacts_raw_from_parquet.sql"
    )
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    return sql_path.read_text(encoding="utf-8")


def run() -> None:
    """Load sec_companyfacts_raw from the latest Parquet batch."""
    configure_logging()
    LOGGER.info("load-sec-companyfacts-raw-from-parquet started")

    settings = get_settings()

    repo_root = Path(__file__).resolve().parents[3]
    parquet_root = Path(settings.data_root) / "derived" / "sec" / "companyfacts_parquet"

    if not parquet_root.exists():
        raise FileNotFoundError(f"Parquet root not found: {parquet_root}")

    parquet_dir = _latest_parquet_dir(parquet_root)
    parquet_files = sorted(parquet_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found in {parquet_dir}")

    # Escape apostrophes once before injecting into SQL.
    parquet_glob = str(parquet_dir / "*.parquet").replace("'", "''")

    sql_template = _load_sql_template(repo_root)
    sql_text = sql_template.replace("__PARQUET_GLOB__", parquet_glob)

    progress = tqdm(total=3, desc="sec_companyfacts_load_parquet", unit="step")
    progress.set_postfix(parquet_dir=parquet_dir.name, parquet_files=len(parquet_files))

    conn = connect_build_db()
    try:
        progress.update(1)

        progress.set_postfix(stage="execute_sql")
        conn.execute(sql_text)

        progress.update(1)

        final_row_count = conn.execute(
            "SELECT COUNT(*) FROM sec_companyfacts_raw"
        ).fetchone()[0]

        progress.set_postfix(stage="done", final_row_count=final_row_count)
        progress.update(1)

        print(
            {
                "status": "ok",
                "job": "load-sec-companyfacts-raw-from-parquet",
                "parquet_dir": str(parquet_dir),
                "parquet_file_count": len(parquet_files),
                "final_row_count": final_row_count,
            }
        )
    finally:
        progress.close()
        conn.close()
        LOGGER.info("load-sec-companyfacts-raw-from-parquet finished")


if __name__ == "__main__":
    run()
