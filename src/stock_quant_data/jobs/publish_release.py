"""
Publish an immutable serving release.

Published objects in this version:
- serving_release_metadata
- serving_release_checks
- instrument
- universe_definition
- universe_membership_history
- symbol_reference_history
- listing_status_history
- price_history

Critical rule:
publication is blocked if validation checks fail.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import logging
import subprocess

import duckdb

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.config.settings import get_settings
from stock_quant_data.db.connections import connect_build_db
from stock_quant_data.db.publish import (
    create_release_dir,
    switch_current_release_symlink,
)
from stock_quant_data.jobs.validate_release import build_checks_payload

LOGGER = logging.getLogger(__name__)


def detect_git_commit(repo_root: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip() or None
    except Exception:
        return None


def read_table_rows(sql_text: str) -> list[tuple]:
    conn = connect_build_db()
    try:
        return conn.execute(sql_text).fetchall()
    finally:
        conn.close()


def table_exists() -> bool:
    conn = connect_build_db()
    try:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = 'price_history'
            """
        ).fetchone()
        return bool(row[0])
    finally:
        conn.close()


def build_manifest(
    repo_root: Path,
    release_id: str,
    instrument_count: int,
    universe_count: int,
    membership_count: int,
    symbol_reference_count: int,
    listing_status_count: int,
    price_history_count: int,
    checks_passed: bool,
) -> dict:
    now_utc = datetime.now(timezone.utc).isoformat()

    return {
        "release_id": release_id,
        "build_started_at": None,
        "build_finished_at": now_utc,
        "published_at": now_utc,
        "schema_version": "v1",
        "checks_passed": checks_passed,
        "build_git_commit": detect_git_commit(repo_root),
        "published_row_counts": {
            "instrument": instrument_count,
            "universe_definition": universe_count,
            "universe_membership_history": membership_count,
            "symbol_reference_history": symbol_reference_count,
            "listing_status_history": listing_status_count,
            "price_history": price_history_count,
        },
    }


def create_serving_db(
    release_dir: Path,
    manifest: dict,
    checks_payload: dict,
    instrument_rows: list[tuple],
    universe_rows: list[tuple],
    membership_rows: list[tuple],
    symbol_reference_rows: list[tuple],
    listing_status_rows: list[tuple],
    price_history_rows: list[tuple],
) -> Path:
    serving_db_path = release_dir / "serving.duckdb"
    conn = duckdb.connect(str(serving_db_path))

    try:
        conn.execute(
            """
            CREATE TABLE serving_release_metadata (
                release_id VARCHAR,
                schema_version VARCHAR,
                published_at VARCHAR,
                checks_passed BOOLEAN,
                build_git_commit VARCHAR
            )
            """
        )

        conn.execute(
            """
            INSERT INTO serving_release_metadata (
                release_id,
                schema_version,
                published_at,
                checks_passed,
                build_git_commit
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                manifest["release_id"],
                manifest["schema_version"],
                manifest["published_at"],
                manifest["checks_passed"],
                manifest["build_git_commit"],
            ],
        )

        conn.execute(
            """
            CREATE TABLE serving_release_checks (
                checks_json VARCHAR
            )
            """
        )

        conn.execute(
            """
            INSERT INTO serving_release_checks (checks_json)
            VALUES (?)
            """,
            [json.dumps(checks_payload, sort_keys=True)],
        )

        conn.execute(
            """
            CREATE TABLE instrument (
                instrument_id BIGINT,
                security_type VARCHAR,
                company_id VARCHAR,
                primary_ticker VARCHAR,
                primary_exchange VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
        if instrument_rows:
            conn.executemany(
                """
                INSERT INTO instrument (
                    instrument_id,
                    security_type,
                    company_id,
                    primary_ticker,
                    primary_exchange,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                instrument_rows,
            )

        conn.execute(
            """
            CREATE TABLE universe_definition (
                universe_id BIGINT,
                universe_name VARCHAR,
                description VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
        if universe_rows:
            conn.executemany(
                """
                INSERT INTO universe_definition (
                    universe_id,
                    universe_name,
                    description,
                    created_at
                )
                VALUES (?, ?, ?, ?)
                """,
                universe_rows,
            )

        conn.execute(
            """
            CREATE TABLE universe_membership_history (
                universe_membership_history_id BIGINT,
                universe_id BIGINT,
                instrument_id BIGINT,
                membership_status VARCHAR,
                effective_from DATE,
                effective_to DATE,
                source_name VARCHAR,
                observed_at TIMESTAMP,
                ingested_at TIMESTAMP
            )
            """
        )
        if membership_rows:
            conn.executemany(
                """
                INSERT INTO universe_membership_history (
                    universe_membership_history_id,
                    universe_id,
                    instrument_id,
                    membership_status,
                    effective_from,
                    effective_to,
                    source_name,
                    observed_at,
                    ingested_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                membership_rows,
            )

        conn.execute(
            """
            CREATE TABLE symbol_reference_history (
                symbol_reference_history_id BIGINT,
                instrument_id BIGINT,
                symbol VARCHAR,
                exchange VARCHAR,
                is_primary BOOLEAN,
                effective_from DATE,
                effective_to DATE,
                observed_at TIMESTAMP,
                ingested_at TIMESTAMP
            )
            """
        )
        if symbol_reference_rows:
            conn.executemany(
                """
                INSERT INTO symbol_reference_history (
                    symbol_reference_history_id,
                    instrument_id,
                    symbol,
                    exchange,
                    is_primary,
                    effective_from,
                    effective_to,
                    observed_at,
                    ingested_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                symbol_reference_rows,
            )

        conn.execute(
            """
            CREATE TABLE listing_status_history (
                listing_status_history_id BIGINT,
                instrument_id BIGINT,
                symbol VARCHAR,
                listing_status VARCHAR,
                event_type VARCHAR,
                effective_from DATE,
                effective_to DATE,
                source_name VARCHAR,
                observed_at TIMESTAMP,
                ingested_at TIMESTAMP
            )
            """
        )
        if listing_status_rows:
            conn.executemany(
                """
                INSERT INTO listing_status_history (
                    listing_status_history_id,
                    instrument_id,
                    symbol,
                    listing_status,
                    event_type,
                    effective_from,
                    effective_to,
                    source_name,
                    observed_at,
                    ingested_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                listing_status_rows,
            )

        conn.execute(
            """
            CREATE TABLE price_history (
                price_history_id BIGINT,
                instrument_id BIGINT,
                price_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                adj_close DOUBLE,
                volume BIGINT,
                source_name VARCHAR,
                observed_at TIMESTAMP,
                ingested_at TIMESTAMP
            )
            """
        )
        if price_history_rows:
            conn.executemany(
                """
                INSERT INTO price_history (
                    price_history_id,
                    instrument_id,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    adj_close,
                    volume,
                    source_name,
                    observed_at,
                    ingested_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                price_history_rows,
            )
    finally:
        conn.close()

    return serving_db_path


def run() -> None:
    configure_logging()
    settings = get_settings()
    repo_root = Path(__file__).resolve().parents[3]

    LOGGER.info("publish-release started")

    build_conn = connect_build_db()
    try:
        build_conn.execute("SELECT 1")
    finally:
        build_conn.close()

    checks_payload = build_checks_payload()
    if not checks_payload["checks_passed"]:
        raise RuntimeError(
            f"Refusing to publish because validation failed: {json.dumps(checks_payload, sort_keys=True)}"
        )

    instrument_rows = read_table_rows(
        """
        SELECT instrument_id, security_type, company_id, primary_ticker, primary_exchange, created_at
        FROM instrument
        ORDER BY instrument_id
        """
    )
    universe_rows = read_table_rows(
        """
        SELECT universe_id, universe_name, description, created_at
        FROM universe_definition
        ORDER BY universe_name
        """
    )
    membership_rows = read_table_rows(
        """
        SELECT universe_membership_history_id, universe_id, instrument_id, membership_status,
               effective_from, effective_to, source_name, observed_at, ingested_at
        FROM universe_membership_history
        ORDER BY universe_id, instrument_id, effective_from
        """
    )
    symbol_reference_rows = read_table_rows(
        """
        SELECT symbol_reference_history_id, instrument_id, symbol, exchange, is_primary,
               effective_from, effective_to, observed_at, ingested_at
        FROM symbol_reference_history
        ORDER BY symbol, effective_from, symbol_reference_history_id
        """
    )
    listing_status_rows = read_table_rows(
        """
        SELECT listing_status_history_id, instrument_id, symbol, listing_status, event_type,
               effective_from, effective_to, source_name, observed_at, ingested_at
        FROM listing_status_history
        ORDER BY instrument_id, effective_from, listing_status_history_id
        """
    )

    if table_exists():
        price_history_rows = read_table_rows(
            """
            SELECT price_history_id, instrument_id, price_date, open, high, low, close,
                   adj_close, volume, source_name, observed_at, ingested_at
            FROM price_history
            ORDER BY instrument_id, price_date, price_history_id
            """
        )
    else:
        price_history_rows = []

    release_dir = create_release_dir()
    release_id = release_dir.name

    manifest = build_manifest(
        repo_root=repo_root,
        release_id=release_id,
        instrument_count=len(instrument_rows),
        universe_count=len(universe_rows),
        membership_count=len(membership_rows),
        symbol_reference_count=len(symbol_reference_rows),
        listing_status_count=len(listing_status_rows),
        price_history_count=len(price_history_rows),
        checks_passed=checks_payload["checks_passed"],
    )

    serving_db_path = create_serving_db(
        release_dir=release_dir,
        manifest=manifest,
        checks_payload=checks_payload,
        instrument_rows=instrument_rows,
        universe_rows=universe_rows,
        membership_rows=membership_rows,
        symbol_reference_rows=symbol_reference_rows,
        listing_status_rows=listing_status_rows,
        price_history_rows=price_history_rows,
    )

    manifest_path = release_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    checks_path = release_dir / "checks.json"
    checks_path.write_text(
        json.dumps(checks_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    switch_current_release_symlink(release_dir)

    print(
        {
            "status": "ok",
            "job": "publish-release",
            "release_id": release_id,
            "release_dir": str(release_dir),
            "serving_db_path": str(serving_db_path),
            "manifest_path": str(manifest_path),
            "checks_path": str(checks_path),
            "current_release_link": str(settings.current_release_link),
            "published_instrument_rows": len(instrument_rows),
            "published_universe_definition_rows": len(universe_rows),
            "published_universe_membership_history_rows": len(membership_rows),
            "published_symbol_reference_history_rows": len(symbol_reference_rows),
            "published_listing_status_history_rows": len(listing_status_rows),
            "published_price_history_rows": len(price_history_rows),
        }
    )


if __name__ == "__main__":
    run()
