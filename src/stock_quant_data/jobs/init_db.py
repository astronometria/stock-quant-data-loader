"""
Initialize the mutable build database.

This job is intentionally conservative:
- create the build database if missing
- apply the foundation schema
- register the schema migration record

We keep this job small and explicit for scientific reproducibility.
"""

from __future__ import annotations

from pathlib import Path
import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def read_sql_file(path: Path) -> str:
    """
    Read a SQL file as UTF-8 text.

    Keeping this helper small makes it easier to reuse later when
    additional migrations are added.
    """
    return path.read_text(encoding="utf-8")


def run() -> None:
    """
    Apply the v1 foundation schema to the build database.
    """
    configure_logging()
    repo_root = Path(__file__).resolve().parents[3]
    ddl_path = repo_root / "sql" / "ddl" / "001_foundation.sql"

    LOGGER.info("init-db started")
    LOGGER.info("ddl_path=%s", ddl_path)

    sql_text = read_sql_file(ddl_path)
    conn = connect_build_db()

    try:
        conn.execute(sql_text)

        # Record schema migration in an idempotent way.
        conn.execute(
            """
            INSERT INTO schema_migrations (version, applied_at, description)
            SELECT ?, CURRENT_TIMESTAMP, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM schema_migrations
                WHERE version = ?
            )
            """,
            [
                "001_foundation",
                "Initial scientific foundation schema",
                "001_foundation",
            ],
        )

        migration_count = conn.execute(
            "SELECT COUNT(*) FROM schema_migrations"
        ).fetchone()[0]

        LOGGER.info("init-db finished")
        LOGGER.info("schema_migrations_count=%s", migration_count)

        print(
            {
                "status": "ok",
                "job": "init-db",
                "ddl_path": str(ddl_path),
                "schema_migrations_count": migration_count,
            }
        )
    finally:
        conn.close()


if __name__ == "__main__":
    run()
