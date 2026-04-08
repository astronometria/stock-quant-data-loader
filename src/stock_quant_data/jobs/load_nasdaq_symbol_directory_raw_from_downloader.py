"""
Load Nasdaq symbol directory raw snapshots from downloader artifacts.

Supports:
- downloader/data/nasdaq/symdir/*.txt
- downloader/data/nasdaq/*.csv
- downloader/data/nasdaq/*.zip

Important:
- nasdaqlisted.txt and otherlisted.txt have different shapes
- this loader preserves enough structure in nasdaq_symbol_directory_raw
  for downstream symbol-reference builders
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

    symdir_root = root / "symdir"
    if symdir_root.exists():
        candidates.extend(sorted(symdir_root.glob("*.txt")))

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


def _txt_source_kind_from_name(file_name: str) -> str:
    lower_name = file_name.lower()
    if "nasdaqlisted" in lower_name:
        return "nasdaqlisted"
    if "otherlisted" in lower_name:
        return "otherlisted"
    return "nasdaq_txt"


def _parse_txt_row(row: dict, source_kind: str) -> tuple[str | None, str | None, str | None, str | None]:
    """
    Return:
    - symbol
    - security_name
    - exchange_code
    - etf_flag

    nasdaqlisted columns commonly include:
    Symbol | Security Name | Market Category | Test Issue | Financial Status | Round Lot Size | ETF | NextShares

    otherlisted columns commonly include:
    ACT Symbol | Security Name | Exchange | CQS Symbol | ETF | Round Lot Size | Test Issue | NASDAQ Symbol
    """
    if source_kind == "otherlisted":
        symbol = (row.get("ACT Symbol") or row.get("symbol") or "").strip() or None
        security_name = (row.get("Security Name") or row.get("security_name") or "").strip() or None
        exchange_code = (row.get("Exchange") or row.get("exchange_code") or "").strip() or None
        etf_flag = (row.get("ETF") or row.get("etf_flag") or "").strip() or None
        return symbol, security_name, exchange_code, etf_flag

    symbol = (row.get("Symbol") or row.get("symbol") or "").strip() or None
    security_name = (row.get("Security Name") or row.get("security_name") or "").strip() or None
    exchange_code = (row.get("Market Category") or row.get("exchange_code") or "").strip() or None
    etf_flag = (row.get("ETF") or row.get("etf_flag") or "").strip() or None
    return symbol, security_name, exchange_code, etf_flag


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
                source_kind = _txt_source_kind_from_name(artifact_path.name)

                with artifact_path.open("r", encoding="utf-8", newline="") as fh:
                    reader = csv.DictReader(fh, delimiter="|")

                    for row in _iter_reader_rows(reader, f"nasdaq_txt:{artifact_path.name}"):
                        symbol, security_name, exchange_code, etf_flag = _parse_txt_row(row, source_kind)

                        if not symbol:
                            continue

                        upper_symbol = symbol.upper()
                        if upper_symbol.startswith("FILE CREATION TIME"):
                            continue
                        if upper_symbol == "SYMBOL":
                            continue
                        if upper_symbol.startswith("TOTAL"):
                            continue

                        raw_id += 1
                        rows.append(
                            (
                                raw_id,
                                _snapshot_id_from_name(artifact_path.name),
                                source_kind,
                                symbol,
                                security_name,
                                exchange_code,
                                etf_flag,
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
                                row.get("exchange_code") or row.get("Market Category") or row.get("Exchange"),
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

                        member_source_kind = Path(member_name).stem
                        if delimiter == "|":
                            member_source_kind = _txt_source_kind_from_name(member_name)

                        for row in _iter_reader_rows(reader, f"nasdaq_zip:{Path(member_name).name}"):
                            if delimiter == "|":
                                symbol, security_name, exchange_code, etf_flag = _parse_txt_row(row, member_source_kind)
                            else:
                                symbol = (row.get("symbol") or row.get("Symbol") or "").strip() or None
                                security_name = row.get("security_name") or row.get("Security Name")
                                exchange_code = row.get("exchange_code") or row.get("Market Category") or row.get("Exchange")
                                etf_flag = row.get("etf_flag") or row.get("ETF")

                            if delimiter == "|" and (not symbol or str(symbol).upper().startswith("TOTAL")):
                                continue

                            raw_id += 1
                            rows.append(
                                (
                                    raw_id,
                                    _snapshot_id_from_name(artifact_path.name),
                                    member_source_kind,
                                    symbol or None,
                                    security_name,
                                    exchange_code,
                                    etf_flag,
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
