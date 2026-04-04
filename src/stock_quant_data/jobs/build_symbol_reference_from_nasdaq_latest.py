"""
Build a current-snapshot instrument + symbol reference layer
from the latest Nasdaq Trader raw snapshot.

Design:
- SQL-first
- use only the latest complete snapshot
- deduplicate per symbol inside the snapshot
- never inject old demo rows into the production identity layer

Important production rule:
- this job must only materialize symbols really observed in Nasdaq raw data
- old FB/META demo rows from legacy experimentation are intentionally removed
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Build the latest current identity layer from Nasdaq raw data.

    Notes:
    - This job is still a current-snapshot builder, not the final PIT identity
      history builder.
    - It stays conservative and deterministic.
    """
    configure_logging()
    LOGGER.info("build-symbol-reference-from-nasdaq-latest started")

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Find the latest complete snapshot.
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
            WHERE kind_count = 2
            ORDER BY snapshot_id DESC
            LIMIT 1
            """
        ).fetchone()

        if latest_snapshot_id_row is None:
            raise RuntimeError("No complete Nasdaq Trader snapshot found in nasdaq_symbol_directory_raw")

        latest_snapshot_id = latest_snapshot_id_row[0]

        # ------------------------------------------------------------------
        # Build exactly one normalized row per symbol from the latest snapshot.
        #
        # The raw feed can contain multiple source kinds for the same symbol.
        # We collapse that here so downstream inserts stay one-row-per-symbol.
        # ------------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_nasdaq_latest_symbol_universe")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_nasdaq_latest_symbol_universe AS
            WITH raw_latest AS (
                SELECT
                    symbol,
                    security_name,
                    exchange_code,
                    etf_flag,
                    source_kind,
                    snapshot_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol
                        ORDER BY
                            CASE source_kind
                                WHEN 'nasdaqlisted' THEN 1
                                WHEN 'otherlisted' THEN 2
                                ELSE 99
                            END,
                            security_name
                    ) AS rn
                FROM nasdaq_symbol_directory_raw
                WHERE snapshot_id = ?
                  AND symbol IS NOT NULL
                  AND symbol <> ''
                  AND test_issue_flag = 'N'
            )
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
            FROM raw_latest
            WHERE rn = 1
            """,
            [latest_snapshot_id],
        )

        # ------------------------------------------------------------------
        # Insert missing instruments only.
        # ------------------------------------------------------------------
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

        # ------------------------------------------------------------------
        # Rebuild the current open-ended symbol layer from scratch.
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

        instrument_count = conn.execute(
            "SELECT COUNT(*) FROM instrument"
        ).fetchone()[0]

        symbol_reference_count = conn.execute(
            "SELECT COUNT(*) FROM symbol_reference_history"
        ).fetchone()[0]

        duplicate_open_ended_symbol_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT symbol
                FROM symbol_reference_history
                WHERE effective_to IS NULL
                GROUP BY symbol
                HAVING COUNT(*) > 1
            )
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
                "duplicate_open_ended_symbol_count": duplicate_open_ended_symbol_count,
                "instrument_rows_by_security_type": by_security_type,
            }
        )
    finally:
        conn.close()

    LOGGER.info("build-symbol-reference-from-nasdaq-latest finished")


if __name__ == "__main__":
    run()
