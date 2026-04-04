"""
Rebuild the loader DuckDB database from scratch using the current canonical jobs.

Important constraints for this script:
- no backup logic
- no legacy table names
- no alternate repo imports
- explicit, readable step list
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from typing import Callable

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"

# --------------------------------------------------------------------
# Make the local src/ tree importable even when the editable install is
# not active yet. This keeps the rebuild script deterministic when it is
# launched directly via:
#   ./.venv/bin/python scripts/rebuild_loader_db.py
# --------------------------------------------------------------------
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from stock_quant_data.config.settings import settings  # noqa: E402


def _print_json(payload: object) -> None:
    """Pretty-print JSON for shell logs."""
    print(json.dumps(payload, indent=2, default=str))


def _load_run(module_name: str) -> Callable[[], None]:
    """
    Import a module and return its `run` entrypoint.

    We keep this loader tiny and explicit so failures are easy to trace.
    """
    module = importlib.import_module(module_name)
    run_fn = getattr(module, "run", None)
    if run_fn is None:
        raise AttributeError(f"Module {module_name!r} does not expose run()")
    return run_fn


def _remove_existing_db_files() -> None:
    """
    Remove the active build DB and WAL before the clean rebuild.

    The user explicitly asked for a clean rebuild without backup handling.
    """
    db_path = settings.build_db_path
    wal_path = Path(str(db_path) + ".wal")

    print("===== REBUILD LOADER DB =====")
    print(f"REPO_ROOT: {REPO_ROOT}")
    print(f"DB_PATH: {db_path}")

    if db_path.exists():
        db_path.unlink()
        print(f"REMOVED_DB: {db_path}")

    if wal_path.exists():
        wal_path.unlink()
        print(f"REMOVED_WAL: {wal_path}")


def _probe_required_tables() -> dict[str, dict[str, object]]:
    """
    Probe the canonical tables that should exist after the rebuild.

    This is intentionally narrow and uses only current table names.
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
        out: dict[str, dict[str, object]] = {}
        for table_name in required_tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                out[f"main.{table_name}"] = {"status": "ok", "count": count}
            except Exception as exc:
                out[f"main.{table_name}"] = {
                    "status": "error",
                    "detail": f"{type(exc).__name__}: {exc}",
                }
        return out
    finally:
        conn.close()


def main() -> None:
    """
    Execute the full canonical rebuild in a fixed order.

    Order matters because later jobs depend on tables materialized by
    earlier jobs.
    """
    settings.ensure_directories()
    _remove_existing_db_files()

    steps: list[tuple[str, str]] = [
        ("init_db", "stock_quant_data.jobs.init_db"),
        ("init_price_raw_tables", "stock_quant_data.jobs.init_price_raw_tables"),
        ("build_symbol_manual_override_map", "stock_quant_data.jobs.build_symbol_manual_override_map"),
        (
            "load_nasdaq_symbol_directory_raw_from_downloader",
            "stock_quant_data.jobs.load_nasdaq_symbol_directory_raw_from_downloader",
        ),
        (
            "stage_sec_companyfacts_json_from_downloader",
            "stock_quant_data.jobs.stage_sec_companyfacts_json_from_downloader",
        ),
        (
            "load_sec_companyfacts_raw_from_staged_json",
            "stock_quant_data.jobs.load_sec_companyfacts_raw_from_staged_json",
        ),
        (
            "load_sec_submissions_identity_from_downloader",
            "stock_quant_data.jobs.load_sec_submissions_identity_from_downloader",
        ),
        (
            "load_price_source_daily_raw_stooq_from_disk",
            "stock_quant_data.jobs.load_price_source_daily_raw_stooq_from_disk",
        ),
        (
            "build_symbol_reference_from_nasdaq_latest",
            "stock_quant_data.jobs.build_symbol_reference_from_nasdaq_latest",
        ),
        (
            "build_symbol_reference_history_from_nasdaq_snapshots",
            "stock_quant_data.jobs.build_symbol_reference_history_from_nasdaq_snapshots",
        ),
        (
            "enrich_symbol_reference_from_manual_overrides",
            "stock_quant_data.jobs.enrich_symbol_reference_from_manual_overrides",
        ),
        (
            "check_master_data_invariants_after_manual",
            "stock_quant_data.jobs.check_master_data_invariants",
        ),
        (
            "build_price_normalized_from_raw_pre_norm",
            "stock_quant_data.jobs.build_price_normalized_from_raw",
        ),
        (
            "build_stooq_symbol_normalization_map",
            "stock_quant_data.jobs.build_stooq_symbol_normalization_map",
        ),
        (
            "build_price_normalized_from_raw_post_norm",
            "stock_quant_data.jobs.build_price_normalized_from_raw",
        ),
        (
            "check_master_data_invariants_after_post_norm",
            "stock_quant_data.jobs.check_master_data_invariants",
        ),
        (
            "build_symbol_reference_candidates_from_unresolved_stooq",
            "stock_quant_data.jobs.build_symbol_reference_candidates_from_unresolved_stooq",
        ),
        (
            "build_unresolved_symbol_worklist",
            "stock_quant_data.jobs.build_unresolved_symbol_worklist",
        ),
        (
            "load_sec_submissions_identity_targeted",
            "stock_quant_data.jobs.load_sec_submissions_identity_targeted",
        ),
        (
            "enrich_symbol_reference_from_sec_targeted",
            "stock_quant_data.jobs.enrich_symbol_reference_from_sec_targeted",
        ),
        (
            "build_price_normalized_from_raw_post_sec",
            "stock_quant_data.jobs.build_price_normalized_from_raw",
        ),
        (
            "check_master_data_invariants_after_post_sec",
            "stock_quant_data.jobs.check_master_data_invariants",
        ),
        (
            "build_symbol_reference_candidates_from_unresolved_stooq_post_sec",
            "stock_quant_data.jobs.build_symbol_reference_candidates_from_unresolved_stooq",
        ),
        (
            "build_unresolved_symbol_worklist_post_sec",
            "stock_quant_data.jobs.build_unresolved_symbol_worklist",
        ),
        (
            "build_high_priority_unresolved_symbol_probe",
            "stock_quant_data.jobs.build_high_priority_unresolved_symbol_probe",
        ),
    ]

    summary: list[dict[str, str]] = []

    for step_name, module_name in steps:
        print(f"===== STEP: {step_name} =====")
        try:
            run_fn = _load_run(module_name)
            run_fn()
            summary.append(
                {
                    "name": step_name,
                    "status": "ok",
                    "detail": "completed",
                }
            )
        except Exception as exc:
            summary.append(
                {
                    "name": step_name,
                    "status": "error",
                    "detail": f"{type(exc).__name__}: {exc}",
                }
            )
            print(f"ERROR: {step_name} failed")

    print("===== FINAL STEP SUMMARY =====")
    _print_json(summary)

    print("===== REQUIRED TABLE PROBE =====")
    _print_json(_probe_required_tables())


if __name__ == "__main__":
    main()
