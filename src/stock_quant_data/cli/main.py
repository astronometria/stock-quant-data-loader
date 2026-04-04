"""
Main CLI entrypoint for stock-quant-data-loader.

This file deliberately imports only CURRENT loader jobs.
Any reference to old repos or deprecated package paths should be removed here.
"""

from __future__ import annotations

import argparse
from typing import Callable

from stock_quant_data.jobs.build_high_priority_unresolved_symbol_probe import (
    run as run_build_high_priority_unresolved_symbol_probe,
)
from stock_quant_data.jobs.build_price_normalized_from_raw import (
    run as run_build_price_normalized_from_raw,
)
from stock_quant_data.jobs.build_stooq_symbol_normalization_map import (
    run as run_build_stooq_symbol_normalization_map,
)
from stock_quant_data.jobs.build_symbol_reference_candidates_from_unresolved_stooq import (
    run as run_build_symbol_reference_candidates_from_unresolved_stooq,
)
from stock_quant_data.jobs.build_symbol_reference_from_nasdaq_latest import (
    run as run_build_symbol_reference_from_nasdaq_latest,
)
from stock_quant_data.jobs.build_symbol_reference_history_from_nasdaq_snapshots import (
    run as run_build_symbol_reference_history_from_nasdaq_snapshots,
)
from stock_quant_data.jobs.build_symbol_manual_override_map import (
    run as run_build_symbol_manual_override_map,
)
from stock_quant_data.jobs.build_unresolved_symbol_worklist import (
    run as run_build_unresolved_symbol_worklist,
)
from stock_quant_data.jobs.check_master_data_invariants import (
    run as run_check_master_data_invariants,
)
from stock_quant_data.jobs.enrich_stooq_symbol_normalization_map_from_probe import (
    run as run_enrich_stooq_symbol_normalization_map_from_probe,
)
from stock_quant_data.jobs.enrich_symbol_reference_from_high_priority_sec_probe import (
    run as run_enrich_symbol_reference_from_high_priority_sec_probe,
)
from stock_quant_data.jobs.enrich_symbol_reference_from_manual_overrides import (
    run as run_enrich_symbol_reference_from_manual_overrides,
)
from stock_quant_data.jobs.enrich_symbol_reference_from_sec_targeted import (
    run as run_enrich_symbol_reference_from_sec_targeted,
)
from stock_quant_data.jobs.init_db import run as run_init_db
from stock_quant_data.jobs.init_price_raw_tables import run as run_init_price_raw_tables
from stock_quant_data.jobs.load_nasdaq_symbol_directory_raw_from_downloader import (
    run as run_load_nasdaq_symbol_directory_raw_from_downloader,
)
from stock_quant_data.jobs.load_price_source_daily_raw_stooq_from_disk import (
    run as run_load_price_source_daily_raw_stooq_from_disk,
)
from stock_quant_data.jobs.load_sec_companyfacts_raw_from_staged_json import (
    run as run_load_sec_companyfacts_raw_from_staged_json,
)
from stock_quant_data.jobs.load_sec_submissions_identity_from_downloader import (
    run as run_load_sec_submissions_identity_from_downloader,
)
from stock_quant_data.jobs.load_sec_submissions_identity_targeted import (
    run as run_load_sec_submissions_identity_targeted,
)
from stock_quant_data.jobs.stage_sec_companyfacts_json_from_downloader import (
    run as run_stage_sec_companyfacts_json_from_downloader,
)


JOB_MAP: dict[str, Callable[[], None]] = {
    "init_db": run_init_db,
    "init_price_raw_tables": run_init_price_raw_tables,
    "build_symbol_manual_override_map": run_build_symbol_manual_override_map,
    "load_nasdaq_symbol_directory_raw_from_downloader": run_load_nasdaq_symbol_directory_raw_from_downloader,
    "stage_sec_companyfacts_json_from_downloader": run_stage_sec_companyfacts_json_from_downloader,
    "load_sec_companyfacts_raw_from_staged_json": run_load_sec_companyfacts_raw_from_staged_json,
    "load_sec_submissions_identity_from_downloader": run_load_sec_submissions_identity_from_downloader,
    "load_price_source_daily_raw_stooq_from_disk": run_load_price_source_daily_raw_stooq_from_disk,
    "build_symbol_reference_from_nasdaq_latest": run_build_symbol_reference_from_nasdaq_latest,
    "build_symbol_reference_history_from_nasdaq_snapshots": run_build_symbol_reference_history_from_nasdaq_snapshots,
    "enrich_symbol_reference_from_manual_overrides": run_enrich_symbol_reference_from_manual_overrides,
    "check_master_data_invariants": run_check_master_data_invariants,
    "build_price_normalized_from_raw": run_build_price_normalized_from_raw,
    "build_stooq_symbol_normalization_map": run_build_stooq_symbol_normalization_map,
    "build_symbol_reference_candidates_from_unresolved_stooq": run_build_symbol_reference_candidates_from_unresolved_stooq,
    "build_unresolved_symbol_worklist": run_build_unresolved_symbol_worklist,
    "load_sec_submissions_identity_targeted": run_load_sec_submissions_identity_targeted,
    "enrich_symbol_reference_from_sec_targeted": run_enrich_symbol_reference_from_sec_targeted,
    "build_high_priority_unresolved_symbol_probe": run_build_high_priority_unresolved_symbol_probe,
    "enrich_symbol_reference_from_high_priority_sec_probe": run_enrich_symbol_reference_from_high_priority_sec_probe,
    "enrich_stooq_symbol_normalization_map_from_probe": run_enrich_stooq_symbol_normalization_map_from_probe,
}


def main() -> None:
    """
    Small explicit dispatcher.

    Example:
        python -m stock_quant_data.cli.main build_price_normalized_from_raw
    """
    parser = argparse.ArgumentParser(description="stock-quant-data-loader CLI")
    parser.add_argument("job", choices=sorted(JOB_MAP.keys()))
    args = parser.parse_args()
    JOB_MAP[args.job]()


if __name__ == "__main__":
    main()
