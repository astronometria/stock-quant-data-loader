"""
Load Nasdaq symbol directory raw snapshots from downloader artifacts.

Supports:
- downloader/data/nasdaq/symdir/*.txt
- downloader/data/nasdaq/*.csv
- downloader/data/nasdaq/*.zip
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
    candidates: list[Path] = []

    # Current downloader layout
    symdir_root = root / "symdir"
    if symdir_root.exists():
        candidates.extend(sorted(symdir_root.glob("*.txt")))

    # Legacy/simple layouts
    candidates.extend(sorted(root.glob("*.csv")))
    candidates.extend(sorted(root.glob("*.zip")))

    if not candidates:
        raise FileNotFoundError(f"No Nasdaq symbol directory artifacts found under {root}")
    return candidates


def _snapshot_id_from_name(path_name: str) -> str:
    return Path(path_name).stem


def _iter_reader_rows(reader: csv.DictReader, desc: str):
    for row in tqdm(list(reader), desc=desc, unit="row"):
        yield row


def _open_pipe_txt_as_reader(path: Path) -> csv.DictReader:
    """
    Nasdaq Trader txt files are pipe-delimited and often end with summary/footer rows.
    """
    fh = path.open("r", encoding="utf-8", newline="")
    return csv.DictReader(fh, delimiter="|")


def run() -> None:
    configure_logging()
    LOGGER.info("load-nasdaq-symbol-directory-raw-from-downloader started")

    settings = get_settings()
    downloader_root = Path(settings.downloader_data_dir) / "nasdaq"
    if not downloader_root.exists():
        downloader_root = Path(settings.data_root) / "nasdaq"

    artifact_paths = _candidate_paths(downloader_root)

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM nasdaq_symbol_directory_raw")

        rows: list[tuple] = []
        raw_id = 0

        for artifact_path in artifact_paths:
            suffix = artifact_path.suffix.lower()

            if suffix == ".txt":
                with artifact_path.open("r", encoding="utf-8", newline="") as fh:
                    reader = csv.DictReader(fh, delimiter="|")
                    for row in _iter_reader_rows(reader, f"nasdaq_txt:{artifact_path.name}"):
                        symbol = (row.get("Symbol") or row.get("symbol") or "").strip()
                        if not symbol or symbol.upper().startswith("FILE CREATION TIME") or symbol.upper() == "SYMBOL":
                            continue
                        if symbol.upper().startswith("TOTAL"):
                            continue

                        raw_id += 1
                        rows.append(
                            (
                                raw_id,
                                _snapshot_id_from_name(artifact_path.name),
                                "nasdaq_txt",
                                symbol,
                                (row.get("Security Name") or row.get("security_name") or "").strip() or None,
                                (row.get("Market Category") or row.get("exchange_code") or "").strip() or None,
                                (row.get("ETF") or row.get("etf_flag") or "").strip() or None,
                                (row.get("Test Issue") or row.get("test_issue_flag") or "").strip() or None,
                            )
                        )

            elif suffix == ".csv":
                with artifact_path.open("r", encoding="utf-8", newline="") as fh:
                    reader = csv.DictReader(fh)
                    for row in _iter_reader_rows(reader, f"nasdaq_csv:{artifact_path.name}"):
                        raw_id += 1
                        rows.append(
                            (
                                raw_id,
                                _snapshot_id_from_name(artifact_path.name),
                                row.get("source_kind", "csv"),
                                row.get("symbol"),
                                row.get("security_name") or row.get("Security Name"),
                                row.get("exchange_code") or row.get("Market Category"),
                                row.get("etf_flag") or row.get("ETF"),
                                row.get("test_issue_flag") or row.get("Test Issue"),
                            )
                        )

            elif suffix == ".zip":
                with zipfile.ZipFile(artifact_path, "r") as zf:
                    members = sorted(name for name in zf.namelist() if name.lower().endswith((".csv", ".txt")))
                    for member_name in members:
                        member_bytes = zf.read(member_name).decode("utf-8")
                        delimiter = "|" if member_name.lower().endswith(".txt") else ","
                        reader = csv.DictReader(io.StringIO(member_bytes), delimiter=delimiter)

                        for row in _iter_reader_rows(reader, f"nasdaq_zip:{Path(member_name).name}"):
                            symbol = (row.get("symbol") or row.get("Symbol") or "").strip()
                            if delimiter == "|" and (not symbol or symbol.upper().startswith("TOTAL")):
                                continue

                            raw_id += 1
                            rows.append(
                                (
                                    raw_id,
                                    _snapshot_id_from_name(artifact_path.name),
                                    Path(member_name).stem,
                                    symbol or None,
                                    row.get("security_name") or row.get("Security Name"),
                                    row.get("exchange_code") or row.get("Market Category"),
                                    row.get("etf_flag") or row.get("ETF"),
                                    row.get("test_issue_flag") or row.get("Test Issue"),
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
                    etf_flag,
                    test_issue_flag
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

        print(
            {
                "status": "ok",
                "job": "load-nasdaq-symbol-directory-raw-from-downloader",
                "artifact_count": len(artifact_paths),
                "row_count": conn.execute("SELECT COUNT(*) FROM nasdaq_symbol_directory_raw").fetchone()[0],
                "downloader_root": str(downloader_root),
            }
        )
    finally:
        conn.close()
        LOGGER.info("load-nasdaq-symbol-directory-raw-from-downloader finished")


if __name__ == "__main__":
    run()
