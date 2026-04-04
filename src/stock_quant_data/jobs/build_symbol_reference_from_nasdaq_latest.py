"""
Build a current-snapshot instrument + symbol reference layer from the latest
complete Nasdaq Trader raw snapshot.

Canonical behavior:
- keep instrument as the identity table
- rebuild only the current open-ended symbol_reference_history layer
- do not inject legacy demo rows
- do not duplicate currently open symbols
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """Build the current latest-snapshot identity layer."""
    configure_logging()
    LOGGER.info("build-symbol-reference-from-nasdaq-latest started")

    conn = connect_build_db()
    try:
        latest_snapshot_id_row = conn.execute(
            """
            WITH snapshot_counts AS (
                SELECT
                    snapshot_id,
                    COUNT(DISTINCT source_kind) AS kind_count
                FROM nasdaq_symbol_directory_raw
                GROUP BY snapshot_id
            )
            SELECT snapshot_id
            FROM snapshot_counts
            WHERE kind_count >= 1
            ORDER BY snapshot_id DESC
            LIMIT 1
            """
        ).fetchone()

        if latest_snapshot_id_row is None:
            raise RuntimeError(
                "No complete Nasdaq Trader snapshot found in nasdaq_symbol_directory_raw"
            )

        latest_snapshot_id = latest_snapshot_id_row[0]

        conn.execute("DROP TABLE IF EXISTS tmp_nasdaq_latest_symbol_universe")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_nasdaq_latest_symbol_universe AS
            SELECT
                symbol,
                security_name,
                exchange_code,
                etf_flag,
                source_kind,
                snapshot_id,
                CASE
                    WHEN etf_flag = 'Y' THEN 'ETF'
                    WHEN upper(security_name) LIKE '%WARRANT%' THEN 'WARRANT'
                    WHEN upper(security_name) LIKE '%RIGHT%' THEN 'RIGHT'
                    WHEN upper(security_name) LIKE '%UNIT%' THEN 'UNIT'
                    WHEN upper(security_name) LIKE '%PREFERRED%' THEN 'PREFERRED_STOCK'
                    ELSE 'COMMON_STOCK'
                END AS security_type
            FROM nasdaq_symbol_directory_raw
            WHERE snapshot_id = ?
              AND symbol IS NOT NULL
              AND symbol <> ''
              AND COALESCE(test_issue_flag, 'N') = 'N'
            """,
            [latest_snapshot_id],
        )

        conn.execute(
            """
            INSERT INTO instrument (
                instrument_id,
                security_type,
                company_id,
                primary_ticker,
                primary_exchange
            )
            WITH current_max AS (
                SELECT COALESCE(MAX(instrument_id), 0) AS max_id
                FROM instrument
            ),
            missing AS (
                SELECT
                    u.symbol,
                    u.security_type,
                    CASE
                        WHEN u.exchange_code = 'Q' THEN 'NASDAQ'
                        WHEN u.exchange_code = 'N' THEN 'NYSE'
                        WHEN u.exchange_code = 'A' THEN 'NYSEMKT'
                        WHEN u.exchange_code = 'P' THEN 'NYSEARCA'
                        WHEN u.exchange_code = 'Z' THEN 'BATS'
                        WHEN u.exchange_code = 'V' THEN 'IEX'
                        ELSE COALESCE(u.exchange_code, 'UNKNOWN')
                    END AS primary_exchange,
                    ROW_NUMBER() OVER (ORDER BY u.symbol) AS rn
                FROM tmp_nasdaq_latest_symbol_universe u
                LEFT JOIN instrument i
                  ON i.primary_ticker = u.symbol
                WHERE i.instrument_id IS NULL
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS instrument_id,
                security_type,
                'NASDAQTRADER_' || symbol AS company_id,
                symbol AS primary_ticker,
                primary_exchange
            FROM missing
            """
        )

        conn.execute("DROP TABLE IF EXISTS tmp_latest_open_symbol_refs")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_latest_open_symbol_refs AS
            SELECT
                i.instrument_id,
                u.symbol,
                CASE
                    WHEN u.exchange_code = 'Q' THEN 'NASDAQ'
                    WHEN u.exchange_code = 'N' THEN 'NYSE'
                    WHEN u.exchange_code = 'A' THEN 'NYSEMKT'
                    WHEN u.exchange_code = 'P' THEN 'NYSEARCA'
                    WHEN u.exchange_code = 'Z' THEN 'BATS'
                    WHEN u.exchange_code = 'V' THEN 'IEX'
                    ELSE COALESCE(u.exchange_code, 'UNKNOWN')
                END AS exchange_name
            FROM tmp_nasdaq_latest_symbol_universe u
            JOIN instrument i
              ON i.primary_ticker = u.symbol
            """
        )

        conn.execute(
            """
            UPDATE symbol_reference_history srh
            SET effective_to = DATE '2026-03-29'
            WHERE srh.effective_to IS NULL
              AND NOT EXISTS (
                    SELECT 1
                    FROM tmp_latest_open_symbol_refs t
                    WHERE t.symbol = srh.symbol
                      AND t.instrument_id = srh.instrument_id
              )
            """
        )

        conn.execute("DROP TABLE IF EXISTS tmp_missing_latest_open_refs")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_missing_latest_open_refs AS
            SELECT
                t.instrument_id,
                t.symbol,
                t.exchange_name
            FROM tmp_latest_open_symbol_refs t
            LEFT JOIN symbol_reference_history srh
              ON srh.instrument_id = t.instrument_id
             AND srh.symbol = t.symbol
             AND srh.effective_to IS NULL
            WHERE srh.symbol IS NULL
            """
        )

        conn.execute(
            """
            INSERT INTO symbol_reference_history (
                symbol_reference_history_id,
                instrument_id,
                symbol,
                exchange,
                is_primary,
                effective_from,
                effective_to
            )
            WITH current_max AS (
                SELECT COALESCE(MAX(symbol_reference_history_id), 0) AS max_id
                FROM symbol_reference_history
            ),
            staged AS (
                SELECT
                    instrument_id,
                    symbol,
                    exchange_name,
                    ROW_NUMBER() OVER (ORDER BY symbol, instrument_id) AS rn
                FROM tmp_missing_latest_open_refs
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS symbol_reference_history_id,
                instrument_id,
                symbol,
                exchange_name AS exchange,
                TRUE AS is_primary,
                DATE '2026-03-29' AS effective_from,
                NULL AS effective_to
            FROM staged
            """
        )

        instrument_count = conn.execute(
            "SELECT COUNT(*) FROM instrument"
        ).fetchone()[0]
        symbol_reference_count = conn.execute(
            "SELECT COUNT(*) FROM symbol_reference_history"
        ).fetchone()[0]
        open_symbol_reference_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM symbol_reference_history
            WHERE effective_to IS NULL
            """
        ).fetchone()[0]
        by_security_type = conn.execute(
            """
            SELECT security_type, COUNT(*)
            FROM instrument
            GROUP BY security_type
            ORDER BY security_type
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "build-symbol-reference-from-nasdaq-latest",
                "latest_snapshot_id": latest_snapshot_id,
                "instrument_count": instrument_count,
                "symbol_reference_history_count": symbol_reference_count,
                "open_symbol_reference_count": open_symbol_reference_count,
                "instrument_rows_by_security_type": by_security_type,
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-symbol-reference-from-nasdaq-latest finished")


if __name__ == "__main__":
    run()
