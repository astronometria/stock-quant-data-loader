"""
CLI job for raw local CSV price ingestion.
"""

from __future__ import annotations

from stock_quant_data.services.ingest.raw_prices_csv_ingest_service import (
    ingest_raw_prices_csv,
)


def run_ingest_raw_prices_csv(csv_path: str) -> dict:
    """
    Ingest one local CSV file into raw.price_source_daily.
    """
    return ingest_raw_prices_csv(csv_path)
