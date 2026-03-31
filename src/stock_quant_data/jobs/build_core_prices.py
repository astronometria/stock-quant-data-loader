"""
CLI job for building canonical core prices from raw landed data.
"""

from __future__ import annotations

from stock_quant_data.services.normalize.core_prices_builder_service import (
    build_core_prices_from_raw,
)


def run_build_core_prices() -> dict:
    """
    Build canonical core.price_history from raw.price_source_daily.
    """
    return build_core_prices_from_raw()
