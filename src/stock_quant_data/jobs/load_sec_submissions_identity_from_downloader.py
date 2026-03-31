"""
Load SEC submissions identity fields from the latest downloader submissions ZIP.

Scope of this first loader:
- read only the latest submissions.zip from downloader storage
- extract identity-relevant fields only
- write a raw company-level table
- explode a simple symbol map staging table

Why this shape:
- keep Python thin and focused on ZIP + JSON reading
- keep downstream joins and analysis SQL-first in DuckDB
"""

from __future__ import annotations

import json
import logging
import zipfile
from pathlib import Path

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)

DEFAULT_SUBMISSIONS_ROOT = Path("/home/marty/stock-quant-data-downloader/data/sec/submissions")


def run(submissions_root: str | None = None) -> None:
    configure_logging()
    LOGGER.info("load-sec-submissions-identity-from-downloader started")

    root = Path(submissions_root) if submissions_root else DEFAULT_SUBMISSIONS_ROOT
    if not root.exists():
        raise FileNotFoundError(f"SEC submissions root does not exist: {root}")

    zip_files = sorted(root.glob("*.zip"))
    if not zip_files:
        raise RuntimeError(f"No submissions zip found under {root}")

    latest_zip = zip_files[-1]

    conn = connect_build_db()
    try:
        # --------------------------------------------------------------
        # Raw company-level identity staging.
        # We keep JSON arrays serialized as JSON text to preserve raw shape.
        # --------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS sec_submissions_company_raw")
        conn.execute(
            """
            CREATE TABLE sec_submissions_company_raw (
                raw_id BIGINT,
                source_zip_path VARCHAR NOT NULL,
                json_member_name VARCHAR NOT NULL,
                cik VARCHAR,
                company_name VARCHAR,
                tickers_json VARCHAR,
                exchanges_json VARCHAR,
                former_names_json VARCHAR,
                sic VARCHAR,
                sic_description VARCHAR,
                entity_type VARCHAR,
                loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # --------------------------------------------------------------
        # Symbol-level exploded staging.
        # One row per (cik, symbol) from SEC submissions current tickers.
        # --------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS sec_symbol_company_map")
        conn.execute(
            """
            CREATE TABLE sec_symbol_company_map (
                raw_id BIGINT,
                cik VARCHAR,
                symbol VARCHAR,
                company_name VARCHAR,
                exchange VARCHAR,
                source_zip_path VARCHAR NOT NULL,
                json_member_name VARCHAR NOT NULL,
                loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        company_rows = []
        symbol_rows = []

        with zipfile.ZipFile(latest_zip) as zf:
            json_names = [n for n in zf.namelist() if n.endswith(".json")]

            raw_company_id = 1
            raw_symbol_id = 1

            for member_name in json_names:
                with zf.open(member_name) as fh:
                    try:
                        obj = json.load(fh)
                    except Exception:
                        # Skip malformed JSON members defensively in this first pass.
                        continue

                cik = str(obj.get("cik", "")).strip() or None
                company_name = obj.get("name")
                tickers = obj.get("tickers") or []
                exchanges = obj.get("exchanges") or []
                former_names = obj.get("formerNames") or []
                sic = obj.get("sic")
                sic_description = obj.get("sicDescription")
                entity_type = obj.get("entityType")

                company_rows.append(
                    (
                        raw_company_id,
                        str(latest_zip),
                        member_name,
                        cik,
                        company_name,
                        json.dumps(tickers, ensure_ascii=False),
                        json.dumps(exchanges, ensure_ascii=False),
                        json.dumps(former_names, ensure_ascii=False),
                        sic,
                        sic_description,
                        entity_type,
                    )
                )
                raw_company_id += 1

                # ------------------------------------------------------
                # Explode current tickers. Keep pairwise alignment with
                # exchanges when lengths match, else leave exchange NULL.
                # ------------------------------------------------------
                for i, symbol in enumerate(tickers):
                    symbol = str(symbol).strip()
                    if not symbol:
                        continue

                    exchange = None
                    if i < len(exchanges):
                        exchange = exchanges[i]

                    symbol_rows.append(
                        (
                            raw_symbol_id,
                            cik,
                            symbol,
                            company_name,
                            exchange,
                            str(latest_zip),
                            member_name,
                        )
                    )
                    raw_symbol_id += 1

        if company_rows:
            conn.executemany(
                """
                INSERT INTO sec_submissions_company_raw (
                    raw_id,
                    source_zip_path,
                    json_member_name,
                    cik,
                    company_name,
                    tickers_json,
                    exchanges_json,
                    former_names_json,
                    sic,
                    sic_description,
                    entity_type
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                company_rows,
            )

        if symbol_rows:
            conn.executemany(
                """
                INSERT INTO sec_symbol_company_map (
                    raw_id,
                    cik,
                    symbol,
                    company_name,
                    exchange,
                    source_zip_path,
                    json_member_name
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                symbol_rows,
            )

        company_count = conn.execute(
            "SELECT COUNT(*) FROM sec_submissions_company_raw"
        ).fetchone()[0]

        symbol_count = conn.execute(
            "SELECT COUNT(*) FROM sec_symbol_company_map"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "load-sec-submissions-identity-from-downloader",
                "latest_zip": str(latest_zip),
                "company_row_count": company_count,
                "symbol_row_count": symbol_count,
            }
        )
    finally:
        conn.close()

    LOGGER.info("load-sec-submissions-identity-from-downloader finished")


if __name__ == "__main__":
    run()
