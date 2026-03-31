"""
Validate build-database scientific invariants before publication.

Current scope:
- universe_membership_history interval validity + overlap detection
- symbol_reference_history interval validity + overlap detection
- listing_status_history interval validity + overlap detection by instrument
- price_history uniqueness and OHLCV coherence checks
"""

from __future__ import annotations

from pathlib import Path
import json
import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def fetch_scalar(conn, sql_text: str):
    return conn.execute(sql_text).fetchone()[0]


def fetch_rows(conn, sql_text: str) -> list[tuple]:
    return conn.execute(sql_text).fetchall()


def table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row[0])


def build_checks_payload() -> dict:
    conn = connect_build_db()
    try:
        universe_invalid_interval_count = fetch_scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM universe_membership_history
            WHERE effective_to IS NOT NULL
              AND effective_to < effective_from
            """,
        )

        universe_invalid_interval_examples = fetch_rows(
            conn,
            """
            SELECT
                universe_membership_history_id,
                universe_id,
                instrument_id,
                membership_status,
                effective_from,
                effective_to
            FROM universe_membership_history
            WHERE effective_to IS NOT NULL
              AND effective_to < effective_from
            ORDER BY universe_id, instrument_id, effective_from
            LIMIT 20
            """,
        )

        universe_overlap_rows = fetch_rows(
            conn,
            """
            SELECT
                a.universe_membership_history_id AS left_id,
                b.universe_membership_history_id AS right_id,
                a.universe_id,
                a.instrument_id,
                a.effective_from AS left_effective_from,
                a.effective_to   AS left_effective_to,
                b.effective_from AS right_effective_from,
                b.effective_to   AS right_effective_to
            FROM universe_membership_history AS a
            JOIN universe_membership_history AS b
              ON a.universe_id = b.universe_id
             AND a.instrument_id = b.instrument_id
             AND a.universe_membership_history_id < b.universe_membership_history_id
             AND a.effective_from < COALESCE(b.effective_to, DATE '9999-12-31')
             AND b.effective_from < COALESCE(a.effective_to, DATE '9999-12-31')
            ORDER BY a.universe_id, a.instrument_id, a.effective_from, b.effective_from
            LIMIT 100
            """,
        )

        symbol_invalid_interval_count = fetch_scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM symbol_reference_history
            WHERE effective_to IS NOT NULL
              AND effective_to < effective_from
            """,
        )

        symbol_invalid_interval_examples = fetch_rows(
            conn,
            """
            SELECT
                symbol_reference_history_id,
                instrument_id,
                symbol,
                exchange,
                effective_from,
                effective_to
            FROM symbol_reference_history
            WHERE effective_to IS NOT NULL
              AND effective_to < effective_from
            ORDER BY symbol, instrument_id, effective_from
            LIMIT 20
            """,
        )

        symbol_overlap_rows = fetch_rows(
            conn,
            """
            SELECT
                a.symbol_reference_history_id AS left_id,
                b.symbol_reference_history_id AS right_id,
                a.symbol,
                a.instrument_id AS left_instrument_id,
                b.instrument_id AS right_instrument_id,
                a.effective_from AS left_effective_from,
                a.effective_to   AS left_effective_to,
                b.effective_from AS right_effective_from,
                b.effective_to   AS right_effective_to
            FROM symbol_reference_history AS a
            JOIN symbol_reference_history AS b
              ON a.symbol = b.symbol
             AND a.symbol_reference_history_id < b.symbol_reference_history_id
             AND a.effective_from < COALESCE(b.effective_to, DATE '9999-12-31')
             AND b.effective_from < COALESCE(a.effective_to, DATE '9999-12-31')
            ORDER BY a.symbol, a.effective_from, b.effective_from
            LIMIT 100
            """,
        )

        listing_invalid_interval_count = fetch_scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM listing_status_history
            WHERE effective_to IS NOT NULL
              AND effective_to < effective_from
            """,
        )

        listing_invalid_interval_examples = fetch_rows(
            conn,
            """
            SELECT
                listing_status_history_id,
                instrument_id,
                symbol,
                listing_status,
                event_type,
                effective_from,
                effective_to
            FROM listing_status_history
            WHERE effective_to IS NOT NULL
              AND effective_to < effective_from
            ORDER BY instrument_id, effective_from
            LIMIT 20
            """,
        )

        listing_overlap_rows = fetch_rows(
            conn,
            """
            SELECT
                a.listing_status_history_id AS left_id,
                b.listing_status_history_id AS right_id,
                a.instrument_id,
                a.symbol AS left_symbol,
                b.symbol AS right_symbol,
                a.effective_from AS left_effective_from,
                a.effective_to   AS left_effective_to,
                b.effective_from AS right_effective_from,
                b.effective_to   AS right_effective_to
            FROM listing_status_history AS a
            JOIN listing_status_history AS b
              ON a.instrument_id = b.instrument_id
             AND a.listing_status_history_id < b.listing_status_history_id
             AND a.effective_from < COALESCE(b.effective_to, DATE '9999-12-31')
             AND b.effective_from < COALESCE(a.effective_to, DATE '9999-12-31')
            ORDER BY a.instrument_id, a.effective_from, b.effective_from
            LIMIT 100
            """,
        )

        price_table_present = table_exists(conn, "price_history")

        if price_table_present:
            price_duplicate_key_rows = fetch_rows(
                conn,
                """
                SELECT
                    instrument_id,
                    price_date,
                    COUNT(*) AS dup_count
                FROM price_history
                GROUP BY instrument_id, price_date
                HAVING COUNT(*) > 1
                ORDER BY instrument_id, price_date
                LIMIT 100
                """,
            )

            price_bad_ohlc_rows = fetch_rows(
                conn,
                """
                SELECT
                    price_history_id,
                    instrument_id,
                    price_date,
                    open,
                    high,
                    low,
                    close
                FROM price_history
                WHERE high < low
                   OR open < low
                   OR open > high
                   OR close < low
                   OR close > high
                ORDER BY instrument_id, price_date
                LIMIT 100
                """,
            )

            price_negative_volume_rows = fetch_rows(
                conn,
                """
                SELECT
                    price_history_id,
                    instrument_id,
                    price_date,
                    volume
                FROM price_history
                WHERE volume < 0
                ORDER BY instrument_id, price_date
                LIMIT 100
                """,
            )
        else:
            price_duplicate_key_rows = []
            price_bad_ohlc_rows = []
            price_negative_volume_rows = []

        checks = {
            "universe_membership_history": {
                "invalid_interval_count": universe_invalid_interval_count,
                "invalid_interval_examples": [
                    {
                        "universe_membership_history_id": row[0],
                        "universe_id": row[1],
                        "instrument_id": row[2],
                        "membership_status": row[3],
                        "effective_from": str(row[4]) if row[4] is not None else None,
                        "effective_to": str(row[5]) if row[5] is not None else None,
                    }
                    for row in universe_invalid_interval_examples
                ],
                "overlap_count": len(universe_overlap_rows),
                "overlap_examples": [
                    {
                        "left_id": row[0],
                        "right_id": row[1],
                        "universe_id": row[2],
                        "instrument_id": row[3],
                        "left_effective_from": str(row[4]) if row[4] is not None else None,
                        "left_effective_to": str(row[5]) if row[5] is not None else None,
                        "right_effective_from": str(row[6]) if row[6] is not None else None,
                        "right_effective_to": str(row[7]) if row[7] is not None else None,
                    }
                    for row in universe_overlap_rows
                ],
            },
            "symbol_reference_history": {
                "invalid_interval_count": symbol_invalid_interval_count,
                "invalid_interval_examples": [
                    {
                        "symbol_reference_history_id": row[0],
                        "instrument_id": row[1],
                        "symbol": row[2],
                        "exchange": row[3],
                        "effective_from": str(row[4]) if row[4] is not None else None,
                        "effective_to": str(row[5]) if row[5] is not None else None,
                    }
                    for row in symbol_invalid_interval_examples
                ],
                "overlap_count": len(symbol_overlap_rows),
                "overlap_examples": [
                    {
                        "left_id": row[0],
                        "right_id": row[1],
                        "symbol": row[2],
                        "left_instrument_id": row[3],
                        "right_instrument_id": row[4],
                        "left_effective_from": str(row[5]) if row[5] is not None else None,
                        "left_effective_to": str(row[6]) if row[6] is not None else None,
                        "right_effective_from": str(row[7]) if row[7] is not None else None,
                        "right_effective_to": str(row[8]) if row[8] is not None else None,
                    }
                    for row in symbol_overlap_rows
                ],
            },
            "listing_status_history": {
                "invalid_interval_count": listing_invalid_interval_count,
                "invalid_interval_examples": [
                    {
                        "listing_status_history_id": row[0],
                        "instrument_id": row[1],
                        "symbol": row[2],
                        "listing_status": row[3],
                        "event_type": row[4],
                        "effective_from": str(row[5]) if row[5] is not None else None,
                        "effective_to": str(row[6]) if row[6] is not None else None,
                    }
                    for row in listing_invalid_interval_examples
                ],
                "overlap_count": len(listing_overlap_rows),
                "overlap_examples": [
                    {
                        "left_id": row[0],
                        "right_id": row[1],
                        "instrument_id": row[2],
                        "left_symbol": row[3],
                        "right_symbol": row[4],
                        "left_effective_from": str(row[5]) if row[5] is not None else None,
                        "left_effective_to": str(row[6]) if row[6] is not None else None,
                        "right_effective_from": str(row[7]) if row[7] is not None else None,
                        "right_effective_to": str(row[8]) if row[8] is not None else None,
                    }
                    for row in listing_overlap_rows
                ],
            },
            "price_history": {
                "table_present": price_table_present,
                "duplicate_key_count": len(price_duplicate_key_rows),
                "duplicate_key_examples": [
                    {
                        "instrument_id": row[0],
                        "price_date": str(row[1]) if row[1] is not None else None,
                        "dup_count": row[2],
                    }
                    for row in price_duplicate_key_rows
                ],
                "bad_ohlc_count": len(price_bad_ohlc_rows),
                "bad_ohlc_examples": [
                    {
                        "price_history_id": row[0],
                        "instrument_id": row[1],
                        "price_date": str(row[2]) if row[2] is not None else None,
                        "open": row[3],
                        "high": row[4],
                        "low": row[5],
                        "close": row[6],
                    }
                    for row in price_bad_ohlc_rows
                ],
                "negative_volume_count": len(price_negative_volume_rows),
                "negative_volume_examples": [
                    {
                        "price_history_id": row[0],
                        "instrument_id": row[1],
                        "price_date": str(row[2]) if row[2] is not None else None,
                        "volume": row[3],
                    }
                    for row in price_negative_volume_rows
                ],
            },
        }

        checks_passed = (
            checks["universe_membership_history"]["invalid_interval_count"] == 0
            and checks["universe_membership_history"]["overlap_count"] == 0
            and checks["symbol_reference_history"]["invalid_interval_count"] == 0
            and checks["symbol_reference_history"]["overlap_count"] == 0
            and checks["listing_status_history"]["invalid_interval_count"] == 0
            and checks["listing_status_history"]["overlap_count"] == 0
            and checks["price_history"]["duplicate_key_count"] == 0
            and checks["price_history"]["bad_ohlc_count"] == 0
            and checks["price_history"]["negative_volume_count"] == 0
        )

        return {
            "checks_passed": checks_passed,
            "checks": checks,
        }
    finally:
        conn.close()


def write_checks_file(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def run() -> None:
    configure_logging()
    LOGGER.info("validate-release started")
    payload = build_checks_payload()
    print(payload)
    LOGGER.info(
        "validate-release finished | checks_passed=%s",
        payload["checks_passed"],
    )


if __name__ == "__main__":
    run()
