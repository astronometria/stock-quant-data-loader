"""
Build a current snapshot instrument + symbol reference layer
from the latest Nasdaq snapshot.

Important table contract:
- instrument.primary_ticker is the canonical current symbol
- symbol_reference_history stores the historical/open interval mapping
- this job writes CURRENT snapshot rows only
- later enrichment jobs may back-extend effective_from dates
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Rebuild current symbol reference state from the latest Nasdaq snapshot.

    Notes:
    - We use current snapshot presence to seed current open intervals.
    - We do not create duplicate open intervals for the same symbol.
    - We intentionally do not preserve older demo-only FB/META seed rows here.
      The current repo should be data-driven first.
    """
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
            raise RuntimeError("No Nasdaq snapshot found in nasdaq_symbol_directory_raw")

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

        # --------------------------------------------------------------
        # Insert any missing instruments by canonical current ticker.
        # --------------------------------------------------------------
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
                    u.exchange_name,
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
                exchange_name AS primary_exchange
            FROM missing
            """
        )

        # --------------------------------------------------------------
        # Replace only the current open-ended reference layer.
        # Historical closed intervals are left to the broader snapshot job
        # or to later enrichment jobs.
        # --------------------------------------------------------------
        conn.execute("DELETE FROM symbol_reference_history WHERE effective_to IS NULL")

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
            base AS (
                SELECT
                    i.instrument_id,
                    u.symbol,
                    u.exchange_name,
                    ROW_NUMBER() OVER (ORDER BY u.symbol) AS rn
                FROM tmp_nasdaq_latest_symbol_universe u
                JOIN instrument i
                    ON i.primary_ticker = u.symbol
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS symbol_reference_history_id,
                instrument_id,
                symbol,
                exchange_name,
                TRUE AS is_primary,
                CAST(substr(?, 1, 10) AS DATE) AS effective_from,
                NULL AS effective_to
            FROM base
            """,
            [latest_snapshot_id],
        )

        instrument_count = conn.execute("SELECT COUNT(*) FROM instrument").fetchone()[0]
        symbol_reference_count = conn.execute(
            "SELECT COUNT(*) FROM symbol_reference_history"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "build-symbol-reference-from-nasdaq-latest",
                "latest_snapshot_id": latest_snapshot_id,
                "instrument_count": instrument_count,
                "symbol_reference_history_count": symbol_reference_count,
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-symbol-reference-from-nasdaq-latest finished")


if __name__ == "__main__":
    run()
