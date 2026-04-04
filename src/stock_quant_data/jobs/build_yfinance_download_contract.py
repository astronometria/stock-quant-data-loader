"""
CLI-style job wrapper for building the Yahoo downloader contract.
"""

from __future__ import annotations

import json
import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.services.contracts.yfinance_contract_builder_service import (
    build_yfinance_download_contract,
)

LOGGER = logging.getLogger(__name__)


def run() -> dict:
    """
    Build Yahoo downloader contract artifacts and return the summary payload.
    """
    configure_logging()
    LOGGER.info("build-yfinance-download-contract started")

    payload = build_yfinance_download_contract()

    LOGGER.info(
        "build-yfinance-download-contract finished: %s",
        json.dumps(payload, default=str),
    )
    return payload


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
