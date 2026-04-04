"""
Load targeted SEC submissions identity data only for current unresolved symbol
worklist / probe workflow.

Current canonical outputs modified:
- sec_submissions_company_raw_targeted
- sec_symbol_company_map_targeted

Current canonical input:
- unresolved_symbol_worklist

Important:
- this is intentionally a second narrower pass over the same latest zip
- only current schema names
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
    """
    Resolve the latest submissions zip from downloader mirror or local mirror.
    """
    candidates = sorted(root.glob("*.zip"))
    if not candidates:
        raise FileNotFoundError(f"No submissions zip found under {root}")
    return candidates[-1]


def run() -> None:
    """
    Load targeted SEC submissions identity rows for only currently relevant
    unresolved symbols.
    """
    configure_logging()
    LOGGER.info("load-sec-submissions-identity-targeted started")

    settings = get_settings()
    submissions_root = Path(settings.data_root).parent / "stock-quant-data-downloader" / "data" / "sec" / "submissions"
    if not submissions_root.exists():
        submissions_root = Path(settings.data_root) / "sec" / "submissions"

    latest_zip = _latest_zip_path(submissions_root)

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Target set comes from current unresolved worklist.
        # ------------------------------------------------------------------
        target_symbols = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT raw_symbol FROM unresolved_symbol_worklist"
            ).fetchall()
        }

        conn.execute("DELETE FROM sec_submissions_company_raw_targeted")
        conn.execute("DELETE FROM sec_symbol_company_map_targeted")

        raw_rows: list[tuple] = []
        symbol_rows: list[tuple] = []

        with zipfile.ZipFile(latest_zip, "r") as zf:
            members = sorted(
                [name for name in zf.namelist() if name.lower().endswith(".json")]
            )

            raw_id = 0
            for member_name in tqdm(members, desc="01sec_submissions_json", unit="json"):
                payload = json.loads(zf.read(member_name).decode("utf-8"))

                tickers = payload.get("tickers") or []
                exchanges = payload.get("exchanges") or []

                matched_symbols = [symbol for symbol in tickers if symbol in target_symbols]
                if not matched_symbols:
                    continue

                raw_id += 1
                cik = str(payload.get("cik", "")).strip()
                company_name = payload.get("name", "")

                raw_rows.append(
                    (
                        raw_id,
                        cik,
                        company_name,
                        str(latest_zip),
                        member_name,
                        json.dumps(payload, separators=(",", ":")),
                    )
                )

                for idx, symbol in enumerate(tickers):
                    if symbol not in target_symbols:
                        continue
                    exchange = exchanges[idx] if idx < len(exchanges) else None
                    symbol_rows.append(
                        (
                            raw_id,
                            cik,
                            symbol,
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
                    source_zip_path,
                    json_member_name,
                    raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                raw_rows,
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
                "worklist_symbol_count": len(target_symbols),
                "company_row_count": company_row_count,
                "symbol_row_count": symbol_row_count,
            }
        )
    finally:
        conn.close()
        LOGGER.info("load-sec-submissions-identity-targeted finished")


if __name__ == "__main__":
    run()
