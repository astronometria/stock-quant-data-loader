"""
DuckDB connection helpers for stock-quant-data-loader.

Design:
- one canonical build DB
- one obvious connection entrypoint
- no legacy path fallbacks
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from stock_quant_data.config.settings import settings


def _ensure_parent_dir(db_path: Path) -> None:
    """
    Ensure the parent directory for the DB exists.

    This is safe and idempotent.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)


def connect_build_db(*, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Connect to the canonical build DB for the current loader repo.

    Important:
    - every CURRENT loader job should use this function
    - scripts must not hardcode older repo paths
    """
    settings.ensure_directories()
    db_path = Path(settings.build_db_path)
    _ensure_parent_dir(db_path)

    conn = duckdb.connect(str(db_path), read_only=read_only)

    # ------------------------------------------------------------------
    # Keep runtime predictable and quiet for CLI usage.
    # ------------------------------------------------------------------
    conn.execute("PRAGMA enable_progress_bar=false")
    conn.execute("PRAGMA threads=4")

    return conn
