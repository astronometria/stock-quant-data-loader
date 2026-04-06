"""
Load broad SEC submissions identity data from downloader artifacts.

Canonical outputs modified:
- sec_submissions_company_raw
- sec_symbol_company_map

The canonical raw company table stores the parsed top-level submission identity
fields, while sec_symbol_company_map stores one row per ticker mapping.
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


def _latest_zip_path(root: Path) -> Path:
    """Resolve the latest submissions zip from the downloader mirror."""
    candidates = sorted(root.glob("*.zip"))
    if not candidates:
        raise FileNotFoundError(f"No submissions zip found under {root}")
    return candidates[-1]


def run() -> None:
    """Load SEC submissions company rows and extracted symbol mappings."""
    configure_logging()
    LOGGER.info("load-sec-submissions-identity-from-downloader started")

    settings = get_settings()
    submissions_root = Path(settings.downloader_data_dir) / "sec" / "submissions"
    if not submissions_root.exists():
        submissions_root = Path(settings.data_root) / "sec" / "submissions"

    latest_zip = _latest_zip_path(submissions_root)

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM sec_submissions_company_raw")
        conn.execute("DELETE FROM sec_symbol_company_map")

        raw_rows: list[tuple] = []
        symbol_rows: list[tuple] = []

        with zipfile.ZipFile(latest_zip, "r") as zf:
            members = sorted(
                name for name in zf.namelist() if name.lower().endswith(".json")
            )

            raw_id = 0
            for member_name in tqdm(members, desc="sec_submissions_json", unit="json"):
                payload = json.loads(zf.read(member_name).decode("utf-8"))
                raw_id += 1

                cik = str(payload.get("cik", "")).strip()
                name = payload.get("name", "")
                tickers = payload.get("tickers") or []
                exchanges = payload.get("exchanges") or []

                raw_rows.append(
                    (
                        raw_id,
                        cik,
                        payload.get("entityType", ""),
                        payload.get("sic", ""),
                        payload.get("sicDescription", ""),
                        name,
                        json.dumps(tickers, separators=(",", ":")),
                        json.dumps(exchanges, separators=(",", ":")),
                        payload.get("ein", ""),
                        payload.get("description", ""),
                        payload.get("website", ""),
                        payload.get("investorWebsite", ""),
                        payload.get("fiscalYearEnd", ""),
                        str(latest_zip),
                        member_name,
                    )
                )

                for idx, symbol in enumerate(tickers):
                    if not symbol:
                        continue
                    exchange = exchanges[idx] if idx < len(exchanges) else None
                    symbol_rows.append(
                        (
                            raw_id,
                            cik,
                            str(symbol).strip(),
                            name,
                            exchange,
                            str(latest_zip),
                            member_name,
                        )
                    )

        if raw_rows:
            conn.executemany(
                """
                INSERT INTO sec_submissions_company_raw (
                    raw_id,
                    cik,
                    entity_type,
                    sic,
                    sic_description,
                    name,
                    tickers_json,
                    exchanges_json,
                    ein,
                    description,
                    website,
                    investor_website,
                    fiscal_year_end,
                    source_zip_path,
                    json_member_name
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                raw_rows,
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

        company_row_count = conn.execute(
            "SELECT COUNT(*) FROM sec_submissions_company_raw"
        ).fetchone()[0]
        symbol_row_count = conn.execute(
            "SELECT COUNT(*) FROM sec_symbol_company_map"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "load-sec-submissions-identity-from-downloader",
                "latest_zip": str(latest_zip),
                "company_row_count": company_row_count,
                "symbol_row_count": symbol_row_count,
            }
        )
    finally:
        conn.close()
        LOGGER.info("load-sec-submissions-identity-from-downloader finished")


if __name__ == "__main__":
    run()
