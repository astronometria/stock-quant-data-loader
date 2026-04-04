"""
Build a broader symbol history layer from all loaded Nasdaq snapshots.

Current canonical outputs modified:
- instrument
- symbol_reference_history

Important:
- unlike the latest-only job, this one builds coarse first/last seen intervals
- it must use current schema names only
- it must not insert extra demo rows that create collisions later
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild instrument + symbol_reference_history from all available Nasdaq
    snapshots using simple first-seen / last-seen interval logic.
    """
    configure_logging()
    LOGGER.info("build-symbol-reference-history-from-nasdaq-snapshots started")

    conn = connect_build_db()
    try:
        latest_snapshot_id_row = conn.execute(
            """
            SELECT MAX(snapshot_id)
            FROM nasdaq_symbol_directory_raw
            """
        ).fetchone()

        if latest_snapshot_id_row is None or latest_snapshot_id_row[0] is None:
            raise RuntimeError("No Nasdaq Trader snapshots found in nasdaq_symbol_directory_raw")

        latest_snapshot_id = latest_snapshot_id_row[0]

        conn.execute("DROP TABLE IF EXISTS tmp_nasdaq_symbol_history_stage")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_nasdaq_symbol_history_stage AS
            WITH base AS (
                SELECT
                    snapshot_id,
                    CAST(substr(snapshot_id, 1, 10) AS DATE) AS snapshot_date,
                    symbol,
                    security_name,
                    exchange_code,
                    etf_flag
                FROM nasdaq_symbol_directory_raw
                WHERE symbol IS NOT NULL
                  AND symbol <> ''
                  AND COALESCE(test_issue_flag, 'N') = 'N'
            ),
            per_symbol AS (
                SELECT
                    symbol,
                    MIN(snapshot_date) AS first_seen_date,
                    MAX(snapshot_date) AS last_seen_date,
                    MAX(CASE WHEN snapshot_id = ? THEN 1 ELSE 0 END) AS seen_in_latest_snapshot
                FROM base
                GROUP BY symbol
            ),
            latest_attrs AS (
                SELECT
                    b.symbol,
                    b.security_name,
                    b.exchange_code,
                    b.etf_flag,
                    ROW_NUMBER() OVER (
                        PARTITION BY b.symbol
                        ORDER BY b.snapshot_date DESC, b.snapshot_id DESC
                    ) AS rn
                FROM base AS b
            )
            SELECT
                p.symbol,
                p.first_seen_date,
                p.last_seen_date,
                p.seen_in_latest_snapshot,
                CASE
                    WHEN a.etf_flag = 'Y' THEN 'ETF'
                    WHEN upper(COALESCE(a.security_name, '')) LIKE '%WARRANT%' THEN 'WARRANT'
                    WHEN upper(COALESCE(a.security_name, '')) LIKE '%RIGHT%' THEN 'RIGHT'
                    WHEN upper(COALESCE(a.security_name, '')) LIKE '%UNIT%' THEN 'UNIT'
                    WHEN upper(COALESCE(a.security_name, '')) LIKE '%PREFERRED%' THEN 'PREFERRED_STOCK'
                    ELSE 'COMMON_STOCK'
                END AS security_type,
                CASE
                    WHEN a.exchange_code = 'Q' THEN 'NASDAQ'
                    WHEN a.exchange_code = 'N' THEN 'NYSE'
                    WHEN a.exchange_code = 'A' THEN 'NYSEMKT'
                    WHEN a.exchange_code = 'P' THEN 'NYSEARCA'
                    WHEN a.exchange_code = 'Z' THEN 'BATS'
                    WHEN a.exchange_code = 'V' THEN 'IEX'
                    ELSE COALESCE(a.exchange_code, 'UNKNOWN')
                END AS exchange_name
            FROM per_symbol AS p
            JOIN latest_attrs AS a
                ON a.symbol = p.symbol
               AND a.rn = 1
            """,
            [latest_snapshot_id],
        )

        # ------------------------------------------------------------------
        # Rebuild instrument table from the broad history stage.
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
            FROM tmp_nasdaq_symbol_history_stage
            ORDER BY symbol
            """
        )

        # ------------------------------------------------------------------
        # Rebuild history intervals.
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
                100000000 + ROW_NUMBER() OVER (ORDER BY s.symbol) AS symbol_reference_history_id,
                i.instrument_id,
                s.symbol,
                s.exchange_name,
                TRUE AS is_primary,
                s.first_seen_date AS effective_from,
                CASE
                    WHEN s.seen_in_latest_snapshot = 1 THEN NULL
                    ELSE s.last_seen_date
                END AS effective_to
            FROM tmp_nasdaq_symbol_history_stage AS s
            JOIN instrument AS i
                ON i.primary_ticker = s.symbol
            ORDER BY s.symbol
            """
        )

        instrument_count = conn.execute(
            "SELECT COUNT(*) FROM instrument"
        ).fetchone()[0]

        symbol_reference_history_count = conn.execute(
            "SELECT COUNT(*) FROM symbol_reference_history"
        ).fetchone()[0]

        closed_interval_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM symbol_reference_history
            WHERE effective_to IS NOT NULL
            """
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "build-symbol-reference-history-from-nasdaq-snapshots",
                "latest_snapshot_id": latest_snapshot_id,
                "instrument_count": instrument_count,
                "symbol_reference_history_count": symbol_reference_history_count,
                "closed_interval_count": closed_interval_count,
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-symbol-reference-history-from-nasdaq-snapshots finished")


if __name__ == "__main__":
    run()
