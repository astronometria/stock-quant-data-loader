"""
CLI job for raw Stooq directory ingestion.
"""

from __future__ import annotations

from stock_quant_data.services.ingest.raw_prices_stooq_dir_ingest_service import (
    ingest_raw_prices_stooq_dir,
)


def run_ingest_raw_prices_stooq_dir(root_dir: str) -> dict:
    """
    Ingest one unpacked local Stooq directory tree into raw.price_source_daily_stooq.
    """
    return ingest_raw_prices_stooq_dir(root_dir=root_dir)
