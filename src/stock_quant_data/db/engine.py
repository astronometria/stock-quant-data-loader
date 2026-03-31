"""
Small SQL execution helpers for DuckDB.

We keep this layer intentionally thin:
- execute SQL files
- avoid embedding large DDL blobs in Python
- make schema review easier for humans
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import duckdb


def read_sql_file(path: Path) -> str:
    """
    Read a SQL file as UTF-8 text.

    Keeping SQL on disk makes the data contract easier to review and version.
    """
    return path.read_text(encoding="utf-8")


def execute_sql_file(connection: duckdb.DuckDBPyConnection, path: Path) -> None:
    """
    Execute the full contents of a SQL file against the given DuckDB connection.
    """
    sql_text = read_sql_file(path)
    connection.execute(sql_text)


def execute_sql_files_in_order(
    connection: duckdb.DuckDBPyConnection,
    paths: Iterable[Path],
) -> None:
    """
    Execute multiple SQL files in the supplied order.

    Ordering is explicit because DDL dependencies matter.
    """
    for path in paths:
        execute_sql_file(connection, path)
