"""
Full loader DB rebuild orchestration script.

Goals:
- use only current package/module names
- rebuild the DB in a deterministic order
- print a structured step summary
- avoid any references to legacy repos or stale module paths

Important:
- this script only orchestrates current jobs
- it does not create backups
- it assumes the caller intentionally wants a fresh rebuild
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from typing import Callable


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

# --------------------------------------------------------------------------
# Ensure the current repo package is importable even when the script is run
# directly with /usr/bin/python3 scripts/rebuild_loader_db.py.
# --------------------------------------------------------------------------
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def _print_json(payload: object) -> None:
    """
    Pretty JSON printer for consistent rebuild logs.
    """
    print(json.dumps(payload, indent=2, default=str))


def _load_run(module_name: str) -> Callable[[], None]:
    """
    Import a module and return its run() function.

    This keeps orchestration compact while making import failures obvious.
    """
    module = importlib.import_module(module_name)
    run_fn = getattr(module, "run", None)
    if run_fn is None:
        raise AttributeError(f"{module_name} does not expose run()")
    return run_fn


def _probe_required_tables() -> dict[str, dict[str, object]]:
    """
    Probe required current-schema tables after the rebuild.
    """
    from stock_quant_data.db.connections import connect_build_db

    required_tables = [
        "symbol_manual_override_map",
        "nasdaq_symbol_directory_raw",
        "sec_companyfacts_raw",
        "sec_submissions_company_raw",
        "price_source_daily_raw_stooq",
        "stooq_symbol_normalization_map",
        "price_source_daily_normalized",
        "symbol_reference_candidates_from_unresolved_stooq",
        "unresolved_symbol_worklist",
        "sec_symbol_company_map",
        "sec_symbol_company_map_targeted",
        "high_priority_unresolved_symbol_probe",
        "instrument",
        "symbol_reference_history",
    ]

    conn = connect_build_db()
    try:
        results: dict[str, dict[str, object]] = {}
        for table_name in required_tables:
            qualified_name = f"main.{table_name}"
            try:
                row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                results[qualified_name] = {"status": "ok", "count": row_count}
            except Exception as exc:  # noqa: BLE001
                results[qualified_name] = {"status": "error", "detail": f"{type(exc).__name__}: {exc}"}
        return results
    finally:
        conn.close()


def main() -> None:
    """
    Execute the current canonical rebuild sequence.
    """
    from stock_quant_data.config.settings import get_settings

    settings = get_settings()
    db_path = settings.build_db_path

    print("===== REBUILD LOADER DB =====")
    print(f"REPO_ROOT: {REPO_ROOT}")
    print(f"DB_PATH: {db_path}")

    # ----------------------------------------------------------------------
    # Fresh rebuild means removing any old DB file up front.
    # We intentionally do not create backups here.
    # ----------------------------------------------------------------------
    if db_path.exists():
        db_path.unlink()
        print(f"REMOVED_DB: {db_path}")

    wal_path = Path(str(db_path) + ".wal")
    if wal_path.exists():
        wal_path.unlink()
        print(f"REMOVED_WAL: {wal_path}")

    steps = [
        ("init_db", "stock_quant_data.jobs.init_db"),
        ("init_price_raw_tables", "stock_quant_data.jobs.init_price_raw_tables"),
        ("build_symbol_manual_override_map", "stock_quant_data.jobs.build_symbol_manual_override_map"),
        ("load_nasdaq_symbol_directory_raw_from_downloader", "stock_quant_data.jobs.load_nasdaq_symbol_directory_raw_from_downloader"),
        ("stage_sec_companyfacts_json_from_downloader", "stock_quant_data.jobs.stage_sec_companyfacts_json_from_downloader"),
        ("load_sec_companyfacts_raw_from_staged_json", "stock_quant_data.jobs.load_sec_companyfacts_raw_from_staged_json"),
        ("load_sec_submissions_identity_from_downloader", "stock_quant_data.jobs.load_sec_submissions_identity_from_downloader"),
        ("load_price_source_daily_raw_stooq_from_disk", "stock_quant_data.jobs.load_price_source_daily_raw_stooq_from_disk"),
        ("build_symbol_reference_from_nasdaq_latest", "stock_quant_data.jobs.build_symbol_reference_from_nasdaq_latest"),
        ("build_symbol_reference_history_from_nasdaq_snapshots", "stock_quant_data.jobs.build_symbol_reference_history_from_nasdaq_snapshots"),
        ("enrich_symbol_reference_from_manual_overrides", "stock_quant_data.jobs.enrich_symbol_reference_from_manual_overrides"),
        ("check_master_data_invariants_after_manual", "stock_quant_data.jobs.check_master_data_invariants"),
        ("build_price_normalized_from_raw_pre_norm", "stock_quant_data.jobs.build_price_normalized_from_raw"),
        ("build_stooq_symbol_normalization_map", "stock_quant_data.jobs.build_stooq_symbol_normalization_map"),
        ("build_price_normalized_from_raw_post_norm", "stock_quant_data.jobs.build_price_normalized_from_raw"),
        ("check_master_data_invariants_after_post_norm", "stock_quant_data.jobs.check_master_data_invariants"),
        ("build_symbol_reference_candidates_from_unresolved_stooq", "stock_quant_data.jobs.build_symbol_reference_candidates_from_unresolved_stooq"),
        ("build_unresolved_symbol_worklist", "stock_quant_data.jobs.build_unresolved_symbol_worklist"),
        ("load_sec_submissions_identity_targeted", "stock_quant_data.jobs.load_sec_submissions_identity_targeted"),
        ("enrich_symbol_reference_from_sec_targeted", "stock_quant_data.jobs.enrich_symbol_reference_from_sec_targeted"),
        ("build_price_normalized_from_raw_post_sec", "stock_quant_data.jobs.build_price_normalized_from_raw"),
        ("check_master_data_invariants_after_post_sec", "stock_quant_data.jobs.check_master_data_invariants"),
        ("build_symbol_reference_candidates_from_unresolved_stooq_post_sec", "stock_quant_data.jobs.build_symbol_reference_candidates_from_unresolved_stooq"),
        ("build_unresolved_symbol_worklist_post_sec", "stock_quant_data.jobs.build_unresolved_symbol_worklist"),
        ("build_high_priority_unresolved_symbol_probe", "stock_quant_data.jobs.build_high_priority_unresolved_symbol_probe"),
    ]

    step_results: list[dict[str, str]] = []

    for step_name, module_name in steps:
        print(f"===== STEP: {step_name} =====")
        try:
            run_fn = _load_run(module_name)
            run_fn()
            step_results.append(
                {
                    "name": step_name,
                    "status": "ok",
                    "detail": "completed",
                }
            )
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR: {step_name} failed")
            step_results.append(
                {
                    "name": step_name,
                    "status": "error",
                    "detail": f"{type(exc).__name__}: {exc}",
                }
            )

    print("===== FINAL STEP SUMMARY =====")
    _print_json(step_results)

    print("===== REQUIRED TABLE PROBE =====")
    _print_json(_probe_required_tables())


if __name__ == "__main__":
    main()
