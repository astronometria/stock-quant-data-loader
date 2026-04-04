"""
Thin CLI dispatcher for the current loader repo.

Design:
- thin Python only
- one canonical namespace: stock_quant_data.jobs.*
- no legacy repo references
- no hidden fallback aliases
"""

from __future__ import annotations

import argparse

from stock_quant_data.jobs.build_high_priority_unresolved_symbol_probe import (
    run as run_build_high_priority_unresolved_symbol_probe,
)
from stock_quant_data.jobs.build_price_history_from_raw import (
    run as run_build_price_history_from_raw,
)
from stock_quant_data.jobs.build_price_normalized_from_raw import (
    run as run_build_price_normalized_from_raw,
)
from stock_quant_data.jobs.build_stooq_symbol_normalization_map import (
    run as run_build_stooq_symbol_normalization_map,
)
from stock_quant_data.jobs.build_symbol_manual_override_map import (
    run as run_build_symbol_manual_override_map,
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


def build_parser() -> argparse.ArgumentParser:
    """
    Build the CLI parser.

    Each subcommand maps 1:1 to one job.
    That makes operations, troubleshooting, and repo grep much cleaner.
    """
    parser = argparse.ArgumentParser(prog="sqload")
    subparsers = parser.add_subparsers(dest="command", required=True)

    commands = [
        "init-db",
        "init-price-raw-tables",
        "build-symbol-manual-override-map",
        "load-nasdaq-symbol-directory-raw-from-downloader",
        "stage-sec-companyfacts-json-from-downloader",
        "load-sec-companyfacts-raw-from-staged-json",
        "load-sec-submissions-identity-from-downloader",
        "load-price-source-daily-raw-stooq-from-disk",
        "build-symbol-reference-from-nasdaq-latest",
        "build-symbol-reference-history-from-nasdaq-snapshots",
        "enrich-symbol-reference-from-manual-overrides",
        "check-master-data-invariants",
        "build-stooq-symbol-normalization-map",
        "enrich-stooq-symbol-normalization-map-from-probe",
        "build-price-normalized-from-raw",
        "build-price-history-from-raw",
        "build-symbol-reference-candidates-from-unresolved-stooq",
        "build-unresolved-symbol-worklist",
        "load-sec-submissions-identity-targeted",
        "enrich-symbol-reference-from-sec-targeted",
        "enrich-symbol-reference-from-high-priority-sec-probe",
        "build-high-priority-unresolved-symbol-probe",
    ]

    for command in commands:
        subparsers.add_parser(command)

    return parser


def main() -> None:
    """
    Dispatch subcommands to the canonical current job implementations.
    """
    parser = build_parser()
    args = parser.parse_args()

    command_map = {
        "init-db": run_init_db,
        "init-price-raw-tables": run_init_price_raw_tables,
        "build-symbol-manual-override-map": run_build_symbol_manual_override_map,
        "load-nasdaq-symbol-directory-raw-from-downloader": run_load_nasdaq_symbol_directory_raw_from_downloader,
        "stage-sec-companyfacts-json-from-downloader": run_stage_sec_companyfacts_json_from_downloader,
        "load-sec-companyfacts-raw-from-staged-json": run_load_sec_companyfacts_raw_from_staged_json,
        "load-sec-submissions-identity-from-downloader": run_load_sec_submissions_identity_from_downloader,
        "load-price-source-daily-raw-stooq-from-disk": run_load_price_source_daily_raw_stooq_from_disk,
        "build-symbol-reference-from-nasdaq-latest": run_build_symbol_reference_from_nasdaq_latest,
        "build-symbol-reference-history-from-nasdaq-snapshots": run_build_symbol_reference_history_from_nasdaq_snapshots,
        "enrich-symbol-reference-from-manual-overrides": run_enrich_symbol_reference_from_manual_overrides,
        "check-master-data-invariants": run_check_master_data_invariants,
        "build-stooq-symbol-normalization-map": run_build_stooq_symbol_normalization_map,
        "enrich-stooq-symbol-normalization-map-from-probe": run_enrich_stooq_symbol_normalization_map_from_probe,
        "build-price-normalized-from-raw": run_build_price_normalized_from_raw,
        "build-price-history-from-raw": run_build_price_history_from_raw,
        "build-symbol-reference-candidates-from-unresolved-stooq": run_build_symbol_reference_candidates_from_unresolved_stooq,
        "build-unresolved-symbol-worklist": run_build_unresolved_symbol_worklist,
        "load-sec-submissions-identity-targeted": run_load_sec_submissions_identity_targeted,
        "enrich-symbol-reference-from-sec-targeted": run_enrich_symbol_reference_from_sec_targeted,
        "enrich-symbol-reference-from-high-priority-sec-probe": run_enrich_symbol_reference_from_high_priority_sec_probe,
        "build-high-priority-unresolved-symbol-probe": run_build_high_priority_unresolved_symbol_probe,
    }

    command_map[args.command]()


if __name__ == "__main__":
    main()
