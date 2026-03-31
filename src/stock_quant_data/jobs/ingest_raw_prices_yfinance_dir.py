"""
CLI job for raw yfinance directory ingestion.
"""

from __future__ import annotations

from stock_quant_data.services.ingest.raw_prices_yfinance_dir_ingest_service import (
    ingest_raw_prices_yfinance_dir,
)


def run_ingest_raw_prices_yfinance_dir(root_dir: str) -> dict:
    """
    Ingest one downloader-produced yfinance directory tree into raw.price_source_daily_yfinance.
    """
    return ingest_raw_prices_yfinance_dir(root_dir=root_dir)
