"""
Load Nasdaq symbol directory raw snapshots from downloader artifacts.

Current canonical target:
- nasdaq_symbol_directory_raw

Expected downloader artifact shape:
- one or more CSV files and/or zip payloads placed under the downloader repo
  data mirror

This loader stays intentionally tolerant:
- it accepts .csv and .zip
- for .zip it loads every .csv member found inside
"""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from pathlib import Path

from tqdm import tqdm

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.config.settings import get_settings
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def _candidate_paths(root: Path) -> list[Path]:
    """
    Return all loadable Nasdaq raw artifacts in stable sorted order.
    """
    candidates = sorted(root.glob("*.csv")) + sorted(root.glob("*.zip"))
    if not candidates:
        raise FileNotFoundError(f"No Nasdaq symbol directory artifacts found under {root}")
    return candidates


def _snapshot_id_from_name(path_name: str) -> str:
    """
    Derive a snapshot_id from filename.

    We keep this simple and deterministic: the stem becomes the snapshot_id.
    """
    return Path(path_name).stem


def run() -> None:
    """
    Load raw Nasdaq symbol directory snapshot rows.
    """
    configure_logging()
    LOGGER.info("load-nasdaq-symbol-directory-raw-from-downloader started")

    settings = get_settings()
    downloader_root = Path(settings.data_root).parent / "stock-quant-data-downloader" / "data" / "nasdaq"
    if not downloader_root.exists():
        downloader_root = Path(settings.data_root) / "nasdaq"

    artifact_paths = _candidate_paths(downloader_root)

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM nasdaq_symbol_directory_raw")

        rows: list[tuple] = []
        raw_id = 0

        for artifact_path in artifact_paths:
            if artifact_path.suffix.lower() == ".csv":
                with artifact_path.open("r", encoding="utf-8", newline="") as fh:
                    reader = csv.DictReader(fh)
                    for row in tqdm(list(reader), desc=f"nasdaq_csv:{artifact_path.name}", unit="row"):
                        raw_id += 1
                        rows.append(
                            (
                                raw_id,
                                _snapshot_id_from_name(artifact_path.name),
                                row.get("source_kind", "csv"),
                                row.get("symbol"),
                                row.get("security_name") or row.get("Security Name"),
                                row.get("exchange_code") or row.get("Market Category"),
                                row.get("test_issue_flag") or row.get("Test Issue"),
                                row.get("etf_flag") or row.get("ETF"),
                                None,
                                str(row),
                            )
                        )

            elif artifact_path.suffix.lower() == ".zip":
                with zipfile.ZipFile(artifact_path, "r") as zf:
                    members = sorted([name for name in zf.namelist() if name.lower().endswith(".csv")])
                    for member_name in members:
                        reader = csv.DictReader(io.StringIO(zf.read(member_name).decode("utf-8")))
                        for row in tqdm(list(reader), desc=f"nasdaq_zip:{Path(member_name).name}", unit="row"):
                            raw_id += 1
                            rows.append(
                                (
                                    raw_id,
                                    _snapshot_id_from_name(artifact_path.name),
                                    row.get("source_kind", Path(member_name).stem),
                                    row.get("symbol"),
                                    row.get("security_name") or row.get("Security Name"),
                                    row.get("exchange_code") or row.get("Market Category"),
                                    row.get("test_issue_flag") or row.get("Test Issue"),
                                    row.get("etf_flag") or row.get("ETF"),
                                    None,
                                    str(row),
                                )
                            )

        if rows:
            conn.executemany(
                """
                INSERT INTO nasdaq_symbol_directory_raw (
                    raw_id,
                    snapshot_id,
                    source_kind,
                    symbol,
                    security_name,
                    exchange_code,
                    test_issue_flag,
                    etf_flag,
                    round_lot_size,
                    raw_payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

        row_count = conn.execute(
            "SELECT COUNT(*) FROM nasdaq_symbol_directory_raw"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "load-nasdaq-symbol-directory-raw-from-downloader",
                "artifact_count": len(artifact_paths),
                "row_count": row_count,
            }
        )
    finally:
        conn.close()
        LOGGER.info("load-nasdaq-symbol-directory-raw-from-downloader finished")


if __name__ == "__main__":
    run()
