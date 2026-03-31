"""
Prices repository.

This repository provides explicit read access to published price history.
"""

from __future__ import annotations

from typing import Any

import duckdb


class PricesRepository:
    """
    Thin read repository for price endpoints.
    """

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        """
        Store an already-open DuckDB connection.
        """
        self._connection = connection

    def get_price_history(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        """
        Return daily EOD prices for a symbol over a date range.
        """
        rows = self._connection.execute(
            """
            SELECT
                price_history_id,
                instrument_id,
                company_id,
                security_type,
                primary_ticker,
                primary_exchange,
                symbol,
                exchange,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                source_name,
                observed_at,
                ingested_at
            FROM api.price_eod_history
            WHERE UPPER(symbol) = UPPER(?)
              AND price_date >= CAST(? AS DATE)
              AND price_date <= CAST(? AS DATE)
            ORDER BY price_date
            """,
            [symbol, start_date, end_date],
        ).fetchall()

        items: list[dict[str, Any]] = []

        for row in rows:
            items.append(
                {
                    "price_history_id": int(row[0]),
                    "instrument_id": row[1],
                    "company_id": row[2],
                    "security_type": row[3],
                    "primary_ticker": row[4],
                    "primary_exchange": row[5],
                    "symbol": row[6],
                    "exchange": row[7],
                    "price_date": row[8].isoformat() if row[8] is not None else None,
                    "open": float(row[9]) if row[9] is not None else None,
                    "high": float(row[10]) if row[10] is not None else None,
                    "low": float(row[11]) if row[11] is not None else None,
                    "close": float(row[12]) if row[12] is not None else None,
                    "adj_close": float(row[13]) if row[13] is not None else None,
                    "volume": int(row[14]) if row[14] is not None else None,
                    "source_name": row[15],
                    "observed_at": row[16].isoformat() if row[16] is not None else None,
                    "ingested_at": row[17].isoformat() if row[17] is not None else None,
                }
            )

        return items

    def get_price_as_of(self, symbol: str, as_of_date: str) -> dict[str, Any] | None:
        """
        Return the latest available EOD price on or before the supplied date.
        """
        row = self._connection.execute(
            """
            SELECT
                price_history_id,
                instrument_id,
                company_id,
                security_type,
                primary_ticker,
                primary_exchange,
                symbol,
                exchange,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                source_name,
                observed_at,
                ingested_at
            FROM api.price_eod_history
            WHERE UPPER(symbol) = UPPER(?)
              AND price_date <= CAST(? AS DATE)
            ORDER BY
                price_date DESC,
                price_history_id DESC
            LIMIT 1
            """,
            [symbol, as_of_date],
        ).fetchone()

        if row is None:
            return None

        return {
            "price_history_id": int(row[0]),
            "instrument_id": row[1],
            "company_id": row[2],
            "security_type": row[3],
            "primary_ticker": row[4],
            "primary_exchange": row[5],
            "symbol": row[6],
            "exchange": row[7],
            "price_date": row[8].isoformat() if row[8] is not None else None,
            "open": float(row[9]) if row[9] is not None else None,
            "high": float(row[10]) if row[10] is not None else None,
            "low": float(row[11]) if row[11] is not None else None,
            "close": float(row[12]) if row[12] is not None else None,
            "adj_close": float(row[13]) if row[13] is not None else None,
            "volume": int(row[14]) if row[14] is not None else None,
            "source_name": row[15],
            "observed_at": row[16].isoformat() if row[16] is not None else None,
            "ingested_at": row[17].isoformat() if row[17] is not None else None,
        }
