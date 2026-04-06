"""
Load canonical SEC companyfacts raw rows from a derived Parquet facts dataset.

Design goals:
- SQL-first
- much faster than reparsing staged JSON each time
- deterministic row_id assignment at load time
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
    """
    Resolve the latest Parquet batch directory.
    """
    candidates = sorted([p for p in parquet_root.iterdir() if p.is_dir()])
    if not candidates:
        raise FileNotFoundError(f"No Parquet companyfacts batch found under {parquet_root}")
    return candidates[-1]


def run() -> None:
    """
    Load sec_companyfacts_raw from the latest Parquet batch.
    """
    configure_logging()
    LOGGER.info("load-sec-companyfacts-raw-from-parquet started")

    settings = get_settings()

    parquet_root = Path(settings.data_root) / "derived" / "sec" / "companyfacts_parquet"
    if not parquet_root.exists():
        raise FileNotFoundError(f"Parquet root not found: {parquet_root}")

    parquet_dir = _latest_parquet_dir(parquet_root)
    parquet_files = sorted(parquet_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found in {parquet_dir}")

    parquet_glob = str(parquet_dir / "*.parquet").replace("'", "''")

    progress = tqdm(total=3, desc="sec_companyfacts_load_parquet", unit="step")
    progress.set_postfix(parquet_dir=parquet_dir.name, parquet_files=len(parquet_files))

    conn = connect_build_db()
    try:
        progress.update(1)
        conn.execute("DELETE FROM sec_companyfacts_raw")

        progress.set_postfix(stage="insert_from_parquet")

        conn.execute(
            f"""
            INSERT INTO sec_companyfacts_raw (
                raw_id,
                cik,
                fact_namespace,
                fact_name,
                fact_value_double,
                fact_value_text,
                unit_name,
                period_end,
                filing_date,
                accession_number,
                source_zip_path,
                json_member_name
            )
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY
                        cik,
                        fact_namespace,
                        fact_name,
                        unit_name,
                        source_file_path,
                        accession_number,
                        filing_date,
                        period_end
                ) AS raw_id,
                cik,
                fact_namespace,
                fact_name,
                fact_value_double,
                fact_value_text,
                unit_name,
                period_end,
                filing_date,
                accession_number,
                source_file_path AS source_zip_path,
                json_member_name
            FROM read_parquet('{parquet_glob}')
            """
        )

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
