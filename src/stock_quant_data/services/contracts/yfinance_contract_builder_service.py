"""
Build Yahoo download contract files from the loader build database.

Design goals:
- SQL-first selection and normalization
- Python stays thin and only orchestrates I/O
- produce a downloader-friendly text file
- produce a richer CSV mapping file for audit/debug
- keep the first version conservative to reduce Yahoo failures

Important v1 rules:
- include only active / open-ended symbols from symbol_reference_history
- exclude obvious non-Yahoo-friendly symbols containing '$'
- exclude non-common instrument families:
  PREFERRED_STOCK, WARRANT, RIGHT, UNIT
- normalize class-share dots to Yahoo dashes:
  BRK.B -> BRK-B
  BF.B  -> BF-B
"""

from __future__ import annotations

import csv
from pathlib import Path

from stock_quant_data.db.connections import connect_build_db


def _repo_root() -> Path:
    """
    Resolve the repository root from this file location.
    """
    return Path(__file__).resolve().parents[4]


def _contracts_dir() -> Path:
    """
    Return the destination directory for downloader contract artifacts.
    """
    return _repo_root() / "data" / "contracts" / "yfinance"


def _normalize_symbol_for_yahoo(symbol: str) -> tuple[str, bool, str]:
    """
    Normalize one canonical symbol into a Yahoo-compatible symbol.

    Returns:
    - yahoo_symbol
    - is_eligible
    - eligibility_reason
    """
    symbol = (symbol or "").strip().upper()

    if not symbol:
        return "", False, "empty_symbol"

    if "$" in symbol:
        return "", False, "contains_dollar_not_supported"

    if "." in symbol:
        return symbol.replace(".", "-"), True, "class_share_dot_to_dash"

    return symbol, True, "exact_match"


def build_yfinance_download_contract() -> dict:
    """
    Build the Yahoo downloader contract from the loader database.

    Output files:
    - data/contracts/yfinance/current_symbols.txt
    - data/contracts/yfinance/symbol_map.csv
    """
    conn = connect_build_db()

    contracts_dir = _contracts_dir()
    contracts_dir.mkdir(parents=True, exist_ok=True)

    txt_path = contracts_dir / "current_symbols.txt"
    csv_path = contracts_dir / "symbol_map.csv"

    try:
        # ------------------------------------------------------------------
        # Use explicit main.* schema references.
        #
        # This avoids ambiguity when multiple schemas exist (main/core/api)
        # and keeps the first contract builder pinned to the loader's main
        # working tables that we already verified exist.
        # ------------------------------------------------------------------
        rows = conn.execute(
            """
            WITH current_rows AS (
                SELECT
                    srh.symbol,
                    srh.exchange,
                    srh.instrument_id,
                    srh.effective_from,
                    srh.effective_to,
                    i.security_type,
                    i.primary_ticker,
                    i.primary_exchange
                FROM main.symbol_reference_history AS srh
                JOIN main.instrument AS i
                  ON i.instrument_id = srh.instrument_id
                WHERE srh.effective_to IS NULL
                  AND srh.symbol IS NOT NULL
                  AND srh.symbol <> ''
            )
            SELECT
                UPPER(symbol) AS canonical_symbol,
                COALESCE(exchange, '') AS source_exchange,
                instrument_id,
                COALESCE(security_type, 'UNKNOWN') AS security_type,
                COALESCE(primary_ticker, '') AS primary_ticker,
                COALESCE(primary_exchange, '') AS primary_exchange,
                effective_from
            FROM current_rows
            ORDER BY canonical_symbol, instrument_id
            """
        ).fetchall()

        excluded_security_types = {
            "PREFERRED_STOCK",
            "WARRANT",
            "RIGHT",
            "UNIT",
        }

        symbol_map_rows: list[dict] = []
        eligible_symbols: list[str] = []

        for (
            canonical_symbol,
            source_exchange,
            instrument_id,
            security_type,
            primary_ticker,
            primary_exchange,
            effective_from,
        ) in rows:
            if security_type in excluded_security_types:
                symbol_map_rows.append(
                    {
                        "canonical_symbol": canonical_symbol,
                        "source_symbol": canonical_symbol,
                        "source_name": source_exchange,
                        "instrument_id": instrument_id,
                        "security_type": security_type,
                        "yahoo_symbol": "",
                        "is_active": True,
                        "is_yahoo_eligible": False,
                        "eligibility_reason": f"security_type_excluded:{security_type}",
                        "primary_ticker": primary_ticker,
                        "primary_exchange": primary_exchange,
                        "effective_from": effective_from,
                    }
                )
                continue

            yahoo_symbol, is_eligible, reason = _normalize_symbol_for_yahoo(canonical_symbol)

            symbol_map_rows.append(
                {
                    "canonical_symbol": canonical_symbol,
                    "source_symbol": canonical_symbol,
                    "source_name": source_exchange,
                    "instrument_id": instrument_id,
                    "security_type": security_type,
                    "yahoo_symbol": yahoo_symbol,
                    "is_active": True,
                    "is_yahoo_eligible": is_eligible,
                    "eligibility_reason": reason,
                    "primary_ticker": primary_ticker,
                    "primary_exchange": primary_exchange,
                    "effective_from": effective_from,
                }
            )

            if is_eligible and yahoo_symbol:
                eligible_symbols.append(yahoo_symbol)

        eligible_symbols = sorted(set(eligible_symbols))

        txt_path.write_text("".join(f"{symbol}\n" for symbol in eligible_symbols), encoding="utf-8")

        fieldnames = [
            "canonical_symbol",
            "source_symbol",
            "source_name",
            "instrument_id",
            "security_type",
            "yahoo_symbol",
            "is_active",
            "is_yahoo_eligible",
            "eligibility_reason",
            "primary_ticker",
            "primary_exchange",
            "effective_from",
        ]

        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(symbol_map_rows)

        return {
            "contracts_dir": str(contracts_dir),
            "current_symbols_txt_path": str(txt_path),
            "symbol_map_csv_path": str(csv_path),
            "total_current_rows_seen": len(rows),
            "eligible_yahoo_symbols_written": len(eligible_symbols),
            "symbol_map_rows_written": len(symbol_map_rows),
        }

    finally:
        conn.close()
