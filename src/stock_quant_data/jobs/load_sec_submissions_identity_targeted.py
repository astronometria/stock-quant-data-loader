"""
Load a targeted SEC submissions symbol/company map for the current unresolved worklist.

Design:
- SQL-first output tables
- Python stays thin for archive / JSON iteration
- write only to CURRENT canonical targeted tables:
    sec_submissions_company_raw_targeted
    sec_symbol_company_map_targeted
"""

from __future__ import annotations

import json
import logging
import zipfile
from pathlib import Path

from tqdm import tqdm

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.config.settings import get_settings
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def _find_latest_zip(root: Path) -> Path:
    """
    Return the latest submissions zip by modified time.

    We keep the rule simple and deterministic.
    """
    zips = sorted(root.glob("*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not zips:
        raise FileNotFoundError(f"No submissions zip found in {root}")
    return zips[0]


def run() -> None:
    """
    Build a targeted SEC mapping layer for unresolved worklist symbols only.
    """
    configure_logging()
    LOGGER.info("load-sec-submissions-identity-targeted started")

    settings = get_settings()
    conn = connect_build_db()

    try:
        submissions_root = Path(settings.downloader_data_root) / "sec" / "submissions"
        latest_zip = _find_latest_zip(submissions_root)

        # ------------------------------------------------------------------
        # Determine the exact set of symbols worth scanning from the current
        # unresolved worklist.
        # ------------------------------------------------------------------
        worklist_symbols = {
            row[0]
            for row in conn.execute(
                """
                SELECT DISTINCT raw_symbol
                FROM unresolved_symbol_worklist
                """
            ).fetchall()
        }

        # Clean rebuild of targeted tables each run.
        conn.execute("DELETE FROM sec_submissions_company_raw_targeted")
        conn.execute("DELETE FROM sec_symbol_company_map_targeted")

        raw_rows: list[tuple] = []
        map_rows: list[tuple] = []

        with zipfile.ZipFile(latest_zip, "r") as zf:
            member_names = [
                name for name in zf.namelist()
                if name.lower().endswith(".json")
            ]

            raw_id = 0
            for member_name in tqdm(member_names, desc="sec_submissions_json", unit="json"):
                with zf.open(member_name) as fh:
                    payload = json.load(fh)

                cik = str(payload.get("cik", "")).strip() or None
                company_name = payload.get("name")
                tickers = payload.get("tickers") or []
                exchanges = payload.get("exchanges") or []
                sic = payload.get("sic")
                sic_description = payload.get("sicDescription")
                entity_type = payload.get("entityType")

                # Skip files with no ticker overlap at all.
                if not any(ticker in worklist_symbols for ticker in tickers):
                    continue

                # Persist one company-level targeted raw row.
                raw_id += 1
                raw_rows.append(
                    (
                        raw_id,
                        cik,
                        company_name,
                        tickers[0] if tickers else None,
                        exchanges[0] if exchanges else None,
                        sic,
                        sic_description,
                        entity_type,
                        str(latest_zip),
                        member_name,
                    )
                )

                # Persist only ticker rows actually relevant to the worklist.
                for idx, ticker in enumerate(tickers):
                    if ticker not in worklist_symbols:
                        continue

                    exchange = exchanges[idx] if idx < len(exchanges) else None
                    map_rows.append(
                        (
                            raw_id,
                            cik,
                            ticker,
                            company_name,
                            exchange,
                            str(latest_zip),
                            member_name,
                        )
                    )

        if raw_rows:
            conn.executemany(
                """
                INSERT INTO sec_submissions_company_raw_targeted (
                    raw_id,
                    cik,
                    company_name,
                    ticker,
                    exchange,
                    sic,
                    sic_description,
                    entity_type,
                    source_zip_path,
                    json_member_name
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                raw_rows,
            )

        if map_rows:
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
                map_rows,
            )

        company_row_count = conn.execute(
            "SELECT COUNT(*) FROM sec_submissions_company_raw_targeted"
        ).fetchone()[0]

        symbol_row_count = conn.execute(
            "SELECT COUNT(*) FROM sec_symbol_company_map_targeted"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "load-sec-submissions-identity-targeted",
                "latest_zip": str(latest_zip),
                "worklist_symbol_count": len(worklist_symbols),
                "company_row_count": company_row_count,
                "symbol_row_count": symbol_row_count,
            }
        )
    finally:
        conn.close()
        LOGGER.info("load-sec-submissions-identity-targeted finished")


if __name__ == "__main__":
    run()
