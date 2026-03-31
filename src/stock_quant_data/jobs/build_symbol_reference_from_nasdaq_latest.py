"""
Build a large current-snapshot instrument + symbol reference layer
from the latest Nasdaq Trader raw snapshot.

Design:
- SQL-first
- use only the latest complete Nasdaq Trader snapshot
- create missing instruments for symbols not already present
- rebuild symbol_reference_history as a large current open-ended layer
- preserve existing FB/META rename demo rows explicitly

Important limitation of this v1:
- this is a current snapshot identity layer, not a full PIT historical
  symbol identity reconstruction yet
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    configure_logging()
    LOGGER.info("build-symbol-reference-from-nasdaq-latest started")

    conn = connect_build_db()
    try:
        # --------------------------------------------------------------
        # Find the latest complete snapshot id seen in raw Nasdaq data.
        # --------------------------------------------------------------
        latest_snapshot_id = conn.execute(
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
            WHERE kind_count = 2
            ORDER BY snapshot_id DESC
            LIMIT 1
            """
        ).fetchone()

        if latest_snapshot_id is None:
            raise RuntimeError("No complete Nasdaq Trader snapshot found in nasdaq_symbol_directory_raw")

        latest_snapshot_id = latest_snapshot_id[0]

        # --------------------------------------------------------------
        # Build a temp current snapshot view with normalized fields.
        # --------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_nasdaq_latest_symbol_universe")
        conn.execute(
            f"""
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
              AND test_issue_flag = 'N'
            """,
            [latest_snapshot_id],
        )

        # --------------------------------------------------------------
        # Insert missing instruments.
        #
        # Existing instruments are reused when primary_ticker already matches.
        # Missing ones get synthetic IDs and company_ids.
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
                FROM tmp_nasdaq_latest_symbol_universe AS u
                LEFT JOIN instrument AS i
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

        # --------------------------------------------------------------
        # Rebuild symbol_reference_history from scratch as a large current
        # open-ended layer, then re-add the explicit FB/META demo rows.
        #
        # For price normalization today, this is enough to massively improve
        # symbol resolution.
        # --------------------------------------------------------------
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
            WITH base AS (
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
                    END AS exchange_name,
                    ROW_NUMBER() OVER (ORDER BY u.symbol) AS rn
                FROM tmp_nasdaq_latest_symbol_universe AS u
                JOIN instrument AS i
                  ON i.primary_ticker = u.symbol
            )
            SELECT
                100000000 + rn AS symbol_reference_history_id,
                instrument_id,
                symbol,
                exchange_name,
                TRUE AS is_primary,
                DATE '2026-03-29' AS effective_from,
                NULL AS effective_to
            FROM base
            """
        )

        # Preserve explicit FB/META rename example rows.
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
            VALUES
                (3005, 1005, 'FB',   'NASDAQ', TRUE, DATE '2012-05-18', DATE '2022-06-08'),
                (3006, 1005, 'META', 'NASDAQ', TRUE, DATE '2022-06-09', NULL)
            """
        )

        instrument_count = conn.execute(
            "SELECT COUNT(*) FROM instrument"
        ).fetchone()[0]

        symbol_reference_count = conn.execute(
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
                "symbol_reference_history_count": symbol_reference_count,
                "instrument_rows_by_security_type": by_security_type,
            }
        )
    finally:
        conn.close()

    LOGGER.info("build-symbol-reference-from-nasdaq-latest finished")


if __name__ == "__main__":
    run()
