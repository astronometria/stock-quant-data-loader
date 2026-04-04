"""
Load staged SEC companyfacts JSON files into the canonical raw table.

Current canonical target:
- sec_companyfacts_raw

Staging source:
- <data_root>/sec/companyfacts_staged/*.json
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from tqdm import tqdm

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.config.settings import get_settings
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Read staged companyfacts json files and load them into sec_companyfacts_raw.
    """
    configure_logging()
    LOGGER.info("load-sec-companyfacts-raw-from-staged-json started")

    settings = get_settings()
    stage_dir = Path(settings.data_root) / "sec" / "companyfacts_staged"

    if not stage_dir.exists():
        raise FileNotFoundError(f"Staging directory not found: {stage_dir}")

    files = sorted(stage_dir.glob("*.json"))

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM sec_companyfacts_raw")

        rows: list[tuple] = []
        raw_id = 0

        for file_path in tqdm(files, desc="sec_companyfacts_load", unit="json"):
            payload = json.loads(file_path.read_text(encoding="utf-8"))
            raw_id += 1

            rows.append(
                (
                    raw_id,
                    str(payload.get("cik", "")).strip(),
                    payload.get("entityName", "") or payload.get("entity_name", ""),
                    str(file_path),
                    file_path.name,
                    json.dumps(payload, separators=(",", ":")),
                )
            )

        if rows:
            conn.executemany(
                """
                INSERT INTO sec_companyfacts_raw (
                    raw_id,
                    cik,
                    entity_name,
                    source_zip_path,
                    json_member_name,
                    raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

        row_count = conn.execute(
            "SELECT COUNT(*) FROM sec_companyfacts_raw"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "load-sec-companyfacts-raw-from-staged-json",
                "row_count": row_count,
                "stage_dir": str(stage_dir),
            }
        )
    finally:
        conn.close()
        LOGGER.info("load-sec-companyfacts-raw-from-staged-json finished")


if __name__ == "__main__":
    run()
