"""
DuckDB connection helpers.

Scientific design rule:
- mutable build DB for ingestion / normalization / checks
- immutable serving DB for API reads

The API must never read directly from the build DB.
"""

from pathlib import Path
import duckdb

from stock_quant_data.config.settings import get_settings


def connect_build_db() -> duckdb.DuckDBPyConnection:
    """
    Open the mutable build database.

    This connection is intended for jobs, not for the serving API.
    """
    settings = get_settings()
    settings.build_db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(settings.build_db_path))


def connect_serving_db_read_only() -> duckdb.DuckDBPyConnection:
    """
    Open the current serving database in read-only mode.

    We fail loudly if the current release does not exist,
    because serving without a published release would be misleading.
    """
    settings = get_settings()
    db_path: Path = settings.current_release_db_path

    if not db_path.exists():
        raise FileNotFoundError(
            f"Serving database not found at '{db_path}'. "
            "Publish a release before starting the API."
        )

    return duckdb.connect(str(db_path), read_only=True)
