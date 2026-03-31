"""
Targeted SEC submissions identity loader.

Purpose:
- scan the latest SEC submissions ZIP
- keep only companies whose current SEC tickers intersect the unresolved worklist
- preserve progress visibility with tqdm
- keep Python thin: ZIP/JSON read only, SQL used for downstream joins
"""

from __future__ import annotations

import json
import logging
import zipfile
from pathlib import Path

from tqdm import tqdm

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)

DEFAULT_SUBMISSIONS_ROOT = Path("/home/marty/stock-quant-data-downloader/data/sec/submissions")


def run(submissions_root: str | None = None) -> None:
    configure_logging()
    LOGGER.info("load-sec-submissions-identity-targeted started")

    root = Path(submissions_root) if submissions_root else DEFAULT_SUBMISSIONS_ROOT
    if not root.exists():
        raise FileNotFoundError(f"SEC submissions root does not exist: {root}")

    zip_files = sorted(root.glob("*.zip"))
    if not zip_files:
        raise RuntimeError(f"No submissions zip found under {root}")

    latest_zip = zip_files[-1]

    conn = connect_build_db()
    try:
        worklist_rows = conn.execute(
            """
            SELECT raw_symbol
            FROM unresolved_symbol_worklist
            """
        ).fetchall()

        worklist = {row[0] for row in worklist_rows}
        if not worklist:
            raise RuntimeError("unresolved_symbol_worklist is empty or missing")

        conn.execute("DROP TABLE IF EXISTS sec_submissions_company_raw_targeted")
        conn.execute(
            """
            CREATE TABLE sec_submissions_company_raw_targeted (
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

        conn.execute("DROP TABLE IF EXISTS sec_symbol_company_map_targeted")
        conn.execute(
            """
            CREATE TABLE sec_symbol_company_map_targeted (
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
            hits = 0

            for member_name in tqdm(json_names, desc="sec_submissions_json", unit="json"):
                with zf.open(member_name) as fh:
                    try:
                        obj = json.load(fh)
                    except Exception:
                        continue

                tickers = [str(x).strip().upper() for x in (obj.get("tickers") or []) if str(x).strip()]
                hit_tickers = sorted(set(tickers).intersection(worklist))
                if not hit_tickers:
                    continue

                hits += 1

                cik = str(obj.get("cik", "")).strip() or None
                company_name = obj.get("name")
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

                for i, symbol in enumerate(tickers):
                    if symbol not in worklist:
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

                if len(company_rows) >= 1000:
                    conn.executemany(
                        """
                        INSERT INTO sec_submissions_company_raw_targeted (
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
                    company_rows.clear()

                if len(symbol_rows) >= 5000:
                    conn.executemany(
                        """
                        INSERT INTO sec_symbol_company_map_targeted (
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
                    symbol_rows.clear()

        if company_rows:
            conn.executemany(
                """
                INSERT INTO sec_submissions_company_raw_targeted (
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
                INSERT INTO sec_symbol_company_map_targeted (
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
            "SELECT COUNT(*) FROM sec_submissions_company_raw_targeted"
        ).fetchone()[0]

        symbol_count = conn.execute(
            "SELECT COUNT(*) FROM sec_symbol_company_map_targeted"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "load-sec-submissions-identity-targeted",
                "latest_zip": str(latest_zip),
                "worklist_symbol_count": len(worklist),
                "company_row_count": company_count,
                "symbol_row_count": symbol_count,
            }
        )
    finally:
        conn.close()

    LOGGER.info("load-sec-submissions-identity-targeted finished")


if __name__ == "__main__":
    run()
