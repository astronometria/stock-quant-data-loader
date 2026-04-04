#!/usr/bin/env python3
"""
Full clean rebuild orchestration for the loader build database.

Design:
- SQL-first orchestration, but Python remains thin for subprocess / sequencing
- no backups
- no "smart" fallback logic
- one deterministic ordered pipeline
- loud structured logs
- failure stops the pipeline immediately
- comments are intentionally extensive for future maintainers

Important operational note:
- this script only ORCHESTRATES jobs that already own their SQL logic
- it does not duplicate business logic from the jobs
- it assumes the environment/venv is already correct before execution
"""

from __future__ import annotations

import importlib
import json
import sys
import traceback
from pathlib import Path
from typing import Callable


# ---------------------------------------------------------------------------
# Keep repo-root discovery simple and explicit.
# This script lives in: scripts/rebuild_loader_db.py
# So repo root is one level above this file.
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]

# ---------------------------------------------------------------------------
# Ensure local editable-style imports work even if the caller forgot to use
# "python -m" or forgot to activate the editable install.
#
# This was the root cause of the previous "No module named stock_quant_data"
# failure shape seen in logs. By pushing src/ onto sys.path here, the rebuild
# script becomes robust to the exact shell entrypoint used.
# ---------------------------------------------------------------------------
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def _print_header(title: str) -> None:
    """Pretty terminal section header."""
    print(f"===== {title} =====")


def _print_json(payload: object) -> None:
    """Stable pretty JSON printer for logs."""
    print(json.dumps(payload, indent=2, default=str))


def _load_run(module_name: str) -> Callable[[], None]:
    """
    Import a job module and return its run() function.

    We keep this tiny and explicit:
    - import module by fully-qualified name
    - require a callable named run
    """
    module = importlib.import_module(module_name)
    run_fn = getattr(module, "run", None)
    if run_fn is None or not callable(run_fn):
        raise AttributeError(f"Module {module_name} does not expose callable run()")
    return run_fn


def _remove_build_db_files() -> dict[str, object]:
    """
    Delete the active build DB and WAL if they exist.

    No backups.
    No restore path.
    The user explicitly asked for a clean rebuild.
    """
    from stock_quant_data.config.settings import get_settings

    settings = get_settings()
    db_path = Path(settings.build_db_path)
    wal_path = Path(str(db_path) + ".wal")

    removed: list[str] = []

    if wal_path.exists():
        wal_path.unlink()
        removed.append(str(wal_path))

    if db_path.exists():
        db_path.unlink()
        removed.append(str(db_path))

    return {
        "db_path": str(db_path),
        "removed_files": removed,
    }


def _probe_required_tables() -> dict[str, dict[str, object]]:
    """
    Probe the key required tables after rebuild.

    This does not attempt deep validation. It is just a compact final smoke check
    so the operator can immediately see whether the canonical objects exist and
    are populated.
    """
    from stock_quant_data.db.connections import connect_build_db

    required_tables = [
        "main.symbol_manual_override_map",
        "main.nasdaq_symbol_directory_raw",
        "main.sec_companyfacts_raw",
        "main.sec_submissions_company_raw",
        "main.price_source_daily_raw_stooq",
        "main.stooq_symbol_normalization_map",
        "main.price_source_daily_normalized",
        "main.price_history",
        "main.symbol_reference_candidates_from_unresolved_stooq",
        "main.unresolved_symbol_worklist",
        "main.sec_symbol_company_map",
        "main.sec_symbol_company_map_targeted",
        "main.high_priority_unresolved_symbol_probe",
        "main.instrument",
        "main.symbol_reference_history",
    ]

    conn = connect_build_db()
    try:
        out: dict[str, dict[str, object]] = {}
        for table_name in required_tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                out[table_name] = {
                    "status": "ok",
                    "count": count,
                }
            except Exception as exc:  # pragma: no cover - operational probe path
                out[table_name] = {
                    "status": "error",
                    "detail": f"{type(exc).__name__}: {exc}",
                }
        return out
    finally:
        conn.close()


def _run_step(name: str, module_name: str) -> dict[str, str]:
    """
    Execute one job step.

    Returns a tiny structured summary used at the end.
    """
    _print_header(f"STEP: {name}")
    try:
        run_fn = _load_run(module_name)
        run_fn()
        return {
            "name": name,
            "status": "ok",
            "detail": "completed",
        }
    except Exception as exc:
        # Print full traceback so operators do not need a second repro just to
        # see where the failure happened.
        traceback.print_exc()
        return {
            "name": name,
            "status": "error",
            "detail": f"{type(exc).__name__}: {exc}",
        }


def main() -> None:
    """
    Run the full clean loader rebuild.

    Step ordering matters:
    1. create schemas / base tables
    2. load raw source tables
    3. build broad reference layer
    4. enrich with manual rules
    5. normalize prices
    6. build unresolved triage artifacts
    7. targeted SEC repair pass
    8. rebuild normalized/derived outputs
    9. final probes
    """
    _print_header("REBUILD LOADER DB")
    print(f"REPO_ROOT: {REPO_ROOT}")

    _print_header("REMOVE ACTIVE BUILD DB")
    _print_json(_remove_build_db_files())

    # ----------------------------------------------------------------------
    # Canonical pipeline.
    #
    # Only job names that belong to the CURRENT loader repo namespace.
    # No legacy repo names.
    # No stale module paths.
    # ----------------------------------------------------------------------
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
        ("build_price_history_from_raw_post_norm", "stock_quant_data.jobs.build_price_history_from_raw"),
        ("check_master_data_invariants_after_post_norm", "stock_quant_data.jobs.check_master_data_invariants"),
        ("build_symbol_reference_candidates_from_unresolved_stooq", "stock_quant_data.jobs.build_symbol_reference_candidates_from_unresolved_stooq"),
        ("build_unresolved_symbol_worklist", "stock_quant_data.jobs.build_unresolved_symbol_worklist"),
        ("load_sec_submissions_identity_targeted", "stock_quant_data.jobs.load_sec_submissions_identity_targeted"),
        ("enrich_symbol_reference_from_sec_targeted", "stock_quant_data.jobs.enrich_symbol_reference_from_sec_targeted"),
        ("enrich_symbol_reference_from_high_priority_sec_probe", "stock_quant_data.jobs.enrich_symbol_reference_from_high_priority_sec_probe"),
        ("enrich_stooq_symbol_normalization_map_from_probe", "stock_quant_data.jobs.enrich_stooq_symbol_normalization_map_from_probe"),
        ("build_price_normalized_from_raw_post_sec", "stock_quant_data.jobs.build_price_normalized_from_raw"),
        ("build_price_history_from_raw_post_sec", "stock_quant_data.jobs.build_price_history_from_raw"),
        ("check_master_data_invariants_after_post_sec", "stock_quant_data.jobs.check_master_data_invariants"),
        ("build_symbol_reference_candidates_from_unresolved_stooq_post_sec", "stock_quant_data.jobs.build_symbol_reference_candidates_from_unresolved_stooq"),
        ("build_unresolved_symbol_worklist_post_sec", "stock_quant_data.jobs.build_unresolved_symbol_worklist"),
        ("build_high_priority_unresolved_symbol_probe", "stock_quant_data.jobs.build_high_priority_unresolved_symbol_probe"),
    ]

    results: list[dict[str, str]] = []

    for step_name, module_name in steps:
        result = _run_step(step_name, module_name)
        results.append(result)
        if result["status"] != "ok":
            # Hard stop. Clean failure is better than cascading nonsense.
            break

    _print_header("FINAL STEP SUMMARY")
    _print_json(results)

    _print_header("REQUIRED TABLE PROBE")
    _print_json(_probe_required_tables())

    # Exit non-zero if any step failed.
    if any(r["status"] != "ok" for r in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
