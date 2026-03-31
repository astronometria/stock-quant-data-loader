"""
Job wrapper for raw Nasdaq symbol directory ingest.
"""

from __future__ import annotations

from stock_quant_data.services.ingest.raw_nasdaq_symbol_directory_dir_ingest_service import (
    ingest_raw_nasdaq_symbol_directory_dir,
)


def run_ingest_raw_nasdaq_symbol_directory_dir(root_dir: str) -> dict:
    return ingest_raw_nasdaq_symbol_directory_dir(root_dir=root_dir)


if __name__ == "__main__":
    import json
    import sys

    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m stock_quant_data.jobs.ingest_raw_nasdaq_symbol_directory_dir <root_dir>")

    print(json.dumps(run_ingest_raw_nasdaq_symbol_directory_dir(sys.argv[1]), indent=2))
