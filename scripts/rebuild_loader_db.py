"""
Full rebuild orchestration script for stock-quant-data-loader.

Important design choices:
- no backup logic
- no destructive file rename logic
- explicit CURRENT job list only
- import through the current package only
- stable JSON-ish printed summaries for shell inspection
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    # ------------------------------------------------------------------
    # Critical fix:
    # running "python scripts/rebuild_loader_db.py" from the repo root or
    # elsewhere must still be able to import stock_quant_data from ./src.
    # ------------------------------------------------------------------
    sys.path.insert(0, str(SRC_DIR))


def _print_json(payload: Any) -> None:
    """
    Print deterministic pretty JSON for shell logs.
    """
    print(json.dumps(payload, indent=2, default=str))


def _load_run(module_name: str):
    """
    Import a module and return its run() function.
    """
    module = importlib.import_module(module_name)
    run_fn = getattr(module, "run", None)
    if run_fn is None:
        raise AttributeError(f"Module {module_name} has no run()")
    return run_fn


def _run_step(name: str, module_name: str) -> dict[str, str]:
    """
    Execute one job and capture a compact status record.
    """
    print(f"===== STEP: {name} =====")
    try:
        run_fn = _load_run(module_name)
        run_fn()
        return {"name": name, "status": "ok", "detail": "completed"}
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {name} failed")
        return {"name": name, "status": "error", "detail": f"{type(exc).__name__}: {exc}"}


def _probe_required_tables() -> dict[str, dict[str, object]]:
    """
    Probe required current tables after rebuild.

    Keep this list aligned with CURRENT jobs only.
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

    conn = connect_build_db(read_only=True)
    try:
        result: dict[str, dict[str, object]] = {}
        for table_name in required_tables:
            fq_name = f"main.{table_name}"
            exists = conn.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'main'
                  AND table_name = ?
                """,
                [table_name],
            ).fetchone()[0]

            if not exists:
                result[fq_name] = {"status": "missing"}
                continue

            count = conn.execute(f"SELECT COUNT(*) FROM {fq_name}").fetchone()[0]
            result[fq_name] = {"status": "ok", "count": count}

        return result
    finally:
        conn.close()


def main() -> None:
    """
    Run the full current loader rebuild.

    Notes:
    - this script does NOT delete backups
    - this script does NOT create backups
    - it only orchestrates the current canonical jobs
    """
    steps: list[tuple[str, str]] = [
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

    print("===== REBUILD LOADER DB =====")
    print(f"REPO_ROOT: {REPO_ROOT}")

    results: list[dict[str, str]] = []
    for step_name, module_name in steps:
        results.append(_run_step(step_name, module_name))

    print("===== FINAL STEP SUMMARY =====")
    _print_json(results)

    print("===== REQUIRED TABLE PROBE =====")
    _print_json(_probe_required_tables())


if __name__ == "__main__":
    main()
