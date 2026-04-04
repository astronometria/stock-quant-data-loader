"""
Convenience wrapper that stages companyfacts JSON from downloader artifacts and
then loads them into sec_companyfacts_raw.

This keeps orchestration simple and preserves one canonical target table:
- sec_companyfacts_raw
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.jobs.load_sec_companyfacts_raw_from_staged_json import run as run_load_from_staged
from stock_quant_data.jobs.stage_sec_companyfacts_json_from_downloader import run as run_stage_from_downloader

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Stage then load SEC companyfacts raw.
    """
    configure_logging()
    LOGGER.info("load-sec-companyfacts-raw-from-downloader started")
    try:
        run_stage_from_downloader()
        run_load_from_staged()
        print(
            {
                "status": "ok",
                "job": "load-sec-companyfacts-raw-from-downloader",
            }
        )
    finally:
        LOGGER.info("load-sec-companyfacts-raw-from-downloader finished")


if __name__ == "__main__":
    run()
