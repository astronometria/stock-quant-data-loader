"""
Build the current open-ended symbol reference layer from the latest Nasdaq
Trader snapshot.

Current canonical outputs modified:
- instrument
- symbol_reference_history

Important:
- this job rebuilds the current identity layer from latest raw snapshot data
- it preserves current loader schema names only
- it avoids legacy demo-only rows that caused duplicate open-ended identities
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild current instrument + open symbol_reference_history from the latest
    complete Nasdaq snapshot.
    """
    configure_logging()
    LOGGER.info("build-symbol-reference-from-nasdaq-latest started")

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Find the latest snapshot id. We prefer a snapshot that has both
        # source kinds when available.
        # ------------------------------------------------------------------
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
            ORDER BY
                CASE WHEN kind_count >= 2 THEN 0 ELSE 1 END,
                snapshot_id DESC
            LIMIT 1
            """
        ).fetchone()

        if latest_snapshot_id_row is None:
            raise RuntimeError("No Nasdaq snapshot found in nasdaq_symbol_directory_raw")

        latest_snapshot_id = latest_snapshot_id_row[0]

        # ------------------------------------------------------------------
        # Stage current universe rows from the latest snapshot.
        # ------------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_nasdaq_latest_symbol_universe")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_nasdaq_latest_symbol_universe AS
            SELECT DISTINCT
                symbol,
                security_name,
                exchange_code,
                etf_flag,
                source_kind,
                snapshot_id,
                CASE
                    WHEN etf_flag = 'Y' THEN 'ETF'
                    WHEN upper(COALESCE(security_name, '')) LIKE '%WARRANT%' THEN 'WARRANT'
                    WHEN upper(COALESCE(security_name, '')) LIKE '%RIGHT%' THEN 'RIGHT'
                    WHEN upper(COALESCE(security_name, '')) LIKE '%UNIT%' THEN 'UNIT'
                    WHEN upper(COALESCE(security_name, '')) LIKE '%PREFERRED%' THEN 'PREFERRED_STOCK'
                    ELSE 'COMMON_STOCK'
                END AS security_type,
                CASE
                    WHEN exchange_code = 'Q' THEN 'NASDAQ'
                    WHEN exchange_code = 'N' THEN 'NYSE'
                    WHEN exchange_code = 'A' THEN 'NYSEMKT'
                    WHEN exchange_code = 'P' THEN 'NYSEARCA'
                    WHEN exchange_code = 'Z' THEN 'BATS'
                    WHEN exchange_code = 'V' THEN 'IEX'
                    ELSE COALESCE(exchange_code, 'UNKNOWN')
                END AS exchange_name
            FROM nasdaq_symbol_directory_raw
            WHERE snapshot_id = ?
              AND symbol IS NOT NULL
              AND symbol <> ''
              AND COALESCE(test_issue_flag, 'N') = 'N'
            """,
            [latest_snapshot_id],
        )

        # ------------------------------------------------------------------
        # Rebuild instrument from the latest snapshot.
        #
        # This loader repo currently treats the latest Nasdaq snapshot as the
        # authoritative source for current exchange / security type metadata.
        # ------------------------------------------------------------------
        conn.execute("DELETE FROM instrument")
        conn.execute(
            """
            INSERT INTO instrument (
                instrument_id,
                security_type,
                company_id,
                primary_ticker,
                primary_exchange
            )
            SELECT
                1000 + ROW_NUMBER() OVER (ORDER BY symbol) AS instrument_id,
                security_type,
                'NASDAQTRADER_' || symbol AS company_id,
                symbol AS primary_ticker,
                exchange_name AS primary_exchange
            FROM tmp_nasdaq_latest_symbol_universe
            ORDER BY symbol
            """
        )

        # ------------------------------------------------------------------
        # Rebuild current open-ended symbol reference layer.
        # Important:
        # - effective_from is the snapshot date extracted from snapshot_id
        # - effective_to is NULL because this table represents the current layer
        # ------------------------------------------------------------------
        conn.execute("DELETE FROM symbol_reference_history")
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
            SELECT
                100000000 + ROW_NUMBER() OVER (ORDER BY u.symbol) AS symbol_reference_history_id,
                i.instrument_id,
                u.symbol,
                u.exchange_name,
                TRUE AS is_primary,
                CAST(substr(u.snapshot_id, 1, 10) AS DATE) AS effective_from,
                NULL AS effective_to
            FROM tmp_nasdaq_latest_symbol_universe AS u
            JOIN instrument AS i
                ON i.primary_ticker = u.symbol
            ORDER BY u.symbol
            """
        )

        instrument_count = conn.execute(
            "SELECT COUNT(*) FROM instrument"
        ).fetchone()[0]

        symbol_reference_history_count = conn.execute(
            "SELECT COUNT(*) FROM symbol_reference_history"
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
                "symbol_reference_history_count": symbol_reference_history_count,
                "instrument_rows_by_security_type": by_security_type,
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-symbol-reference-from-nasdaq-latest finished")


if __name__ == "__main__":
    run()
