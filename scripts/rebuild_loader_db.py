#!/usr/bin/env python3
"""
Rebuild the stock-quant-data-loader build database using loader-native jobs.

Design goals:
- keep orchestration thin and explicit
- keep SQL-first where possible
- continue after failures so the operator gets a full failure map
- run explicit invariant checks so master-data corruption is caught quickly
- do NOT create automatic backup files

Important runtime rule:
- this script bootstraps the repo-local src/ path itself
- it must work even if the shell forgot to activate .venv
"""

from __future__ import annotations

import importlib
import json
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

# ----------------------------------------------------------------------
# Make repo-local package imports deterministic.
# Why:
# - the user's shell may not have the repo venv activated
# - editable install may not be active in the current interpreter
# - we want local source imports to work directly from this repo
# ----------------------------------------------------------------------
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

DB_PATH = REPO_ROOT / "data" / "build" / "market_build.duckdb"
DB_WAL_PATH = REPO_ROOT / "data" / "build" / "market_build.duckdb.wal"


@dataclass
class StepResult:
    name: str
    status: str
    detail: str


def _print_json(payload: object) -> None:
    """Pretty-print JSON payloads with deterministic formatting."""
    print(json.dumps(payload, indent=2, default=str))


def _load_run(module_name: str):
    """
    Dynamically import a loader job module and return its run() function.

    We keep the orchestration layer intentionally thin:
    each step is just a Python module exposing run().
    """
    module = importlib.import_module(module_name)
    run_fn = getattr(module, "run", None)
    if run_fn is None:
        raise AttributeError(f"Module '{module_name}' does not expose run()")
    return run_fn


def _reset_db_without_backup() -> None:
    """
    Remove the mutable build DB and WAL so the rebuild starts from a clean state.

    Important:
    - no backup files are created here
    - the caller is explicitly choosing a full clean rebuild
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    if DB_PATH.exists():
        DB_PATH.unlink()
        print(f"REMOVED_DB: {DB_PATH}")

    if DB_WAL_PATH.exists():
        DB_WAL_PATH.unlink()
        print(f"REMOVED_WAL: {DB_WAL_PATH}")


def _probe_required_tables() -> dict:
    """
    Return a status map for key tables expected in the build DB.

    This is intentionally explicit so operators can immediately see whether the
    canonical tables required by downstream steps exist and have rows.
    """
    from stock_quant_data.db.connections import connect_build_db

    wanted = [
        "main.symbol_manual_override_map",
        "main.nasdaq_symbol_directory_raw",
        "main.sec_companyfacts_raw",
        "main.sec_submissions_company_raw",
        "main.price_source_daily_raw_stooq",
        "main.stooq_symbol_normalization_map",
        "main.price_source_daily_normalized",
        "main.symbol_reference_candidates_from_unresolved_stooq",
        "main.unresolved_symbol_worklist",
        "main.sec_symbol_company_map",
        "main.sec_symbol_company_map_targeted",
        "main.high_priority_unresolved_symbol_probe",
        "main.instrument",
        "main.symbol_reference_history",
        "main.listing_status_history",
        "main.price_history",
    ]

    probe: dict[str, dict] = {}
    conn = connect_build_db()
    try:
        for table_name in wanted:
            try:
                row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                probe[table_name] = {"status": "ok", "count": row[0]}
            except Exception as exc:
                probe[table_name] = {
                    "status": "error",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
    finally:
        conn.close()

    return probe


def _run_step(step_name: str, module_name: str) -> StepResult:
    """
    Execute one loader job and capture success/failure without stopping the rebuild.

    This is useful operationally because one broken step should not hide the status
    of the later steps and probes.
    """
    print(f"===== STEP: {step_name} =====")
    try:
        run_fn = _load_run(module_name)
        payload = run_fn()
        if payload is not None:
            _print_json(payload)
        return StepResult(name=step_name, status="ok", detail="completed")
    except Exception as exc:
        traceback.print_exc()
        print(f"ERROR: {step_name} failed")
        return StepResult(
            name=step_name,
            status="error",
            detail=f"{type(exc).__name__}: {exc}",
        )


def main() -> None:
    """
    Full loader-native rebuild entrypoint.

    Order matters:
    - raw/source identity layers first
    - reference/master reconstruction next
    - price normalization after reference identity exists
    - unresolved triage after normalization
    - canonical price_history near the end
    """
    print("===== REBUILD LOADER DB =====")
    print(f"REPO_ROOT: {REPO_ROOT}")
    print(f"SRC_ROOT: {SRC_ROOT}")
    print(f"DB_PATH: {DB_PATH}")
    print(f"PYTHON_EXECUTABLE: {sys.executable}")
    print(f"SYS_PATH_HEAD: {sys.path[:5]}")

    _reset_db_without_backup()

    step_results: list[StepResult] = []

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
        ("build_price_history_from_raw", "stock_quant_data.jobs.build_price_history_from_raw"),
        ("check_master_data_invariants_final", "stock_quant_data.jobs.check_master_data_invariants"),
    ]

    for step_name, module_name in steps:
        step_results.append(_run_step(step_name, module_name))

    print("===== FINAL STEP SUMMARY =====")
    _print_json([result.__dict__ for result in step_results])

    print("===== REQUIRED TABLE PROBE =====")
    _print_json(_probe_required_tables())


if __name__ == "__main__":
    main()
