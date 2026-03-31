"""
Symbols repository.

This repository provides explicit read access to published symbol history.
"""

from __future__ import annotations

from typing import Any

import duckdb


class SymbolsRepository:
    """
    Thin read repository for symbol-history endpoints.
    """

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        """
        Store an already-open DuckDB connection.
        """
        self._connection = connection

    def get_symbol_history(self, symbol: str) -> list[dict[str, Any]]:
        """
        Return the full historical mapping timeline for one symbol.

        Results are ordered chronologically by effective_from.
        """
        rows = self._connection.execute(
            """
            SELECT
                symbol_reference_history_id,
                instrument_id,
                company_id,
                security_type,
                primary_ticker,
                primary_exchange,
                symbol,
                exchange,
                mic,
                is_primary,
                effective_from,
                effective_to,
                observed_at,
                ingested_at,
                source_name,
                record_status
            FROM api.symbol_history
            WHERE UPPER(symbol) = UPPER(?)
            ORDER BY effective_from, symbol_reference_history_id
            """,
            [symbol],
        ).fetchall()

        items: list[dict[str, Any]] = []

        for row in rows:
            items.append(
                {
                    "symbol_reference_history_id": int(row[0]),
                    "instrument_id": row[1],
                    "company_id": row[2],
                    "security_type": row[3],
                    "primary_ticker": row[4],
                    "primary_exchange": row[5],
                    "symbol": row[6],
                    "exchange": row[7],
                    "mic": row[8],
                    "is_primary": bool(row[9]),
                    "effective_from": row[10].isoformat() if row[10] is not None else None,
                    "effective_to": row[11].isoformat() if row[11] is not None else None,
                    "observed_at": row[12].isoformat() if row[12] is not None else None,
                    "ingested_at": row[13].isoformat() if row[13] is not None else None,
                    "source_name": row[14],
                    "record_status": row[15],
                }
            )

        return items

    def get_symbol_as_of(self, symbol: str, as_of_date: str) -> dict[str, Any] | None:
        """
        Return the single active historical symbol mapping for a given date.

        PIT rule used here:
            effective_from <= as_of_date
            AND (effective_to IS NULL OR effective_to > as_of_date)
        """
        row = self._connection.execute(
            """
            SELECT
                symbol_reference_history_id,
                instrument_id,
                company_id,
                security_type,
                primary_ticker,
                primary_exchange,
                symbol,
                exchange,
                mic,
                is_primary,
                effective_from,
                effective_to,
                observed_at,
                ingested_at,
                source_name,
                record_status
            FROM api.symbol_history
            WHERE UPPER(symbol) = UPPER(?)
              AND effective_from <= CAST(? AS DATE)
              AND (
                    effective_to IS NULL
                    OR effective_to > CAST(? AS DATE)
                  )
            ORDER BY
                effective_from DESC,
                symbol_reference_history_id DESC
            LIMIT 1
            """,
            [symbol, as_of_date, as_of_date],
        ).fetchone()

        if row is None:
            return None

        return {
            "symbol_reference_history_id": int(row[0]),
            "instrument_id": row[1],
            "company_id": row[2],
            "security_type": row[3],
            "primary_ticker": row[4],
            "primary_exchange": row[5],
            "symbol": row[6],
            "exchange": row[7],
            "mic": row[8],
            "is_primary": bool(row[9]),
            "effective_from": row[10].isoformat() if row[10] is not None else None,
            "effective_to": row[11].isoformat() if row[11] is not None else None,
            "observed_at": row[12].isoformat() if row[12] is not None else None,
            "ingested_at": row[13].isoformat() if row[13] is not None else None,
            "source_name": row[14],
            "record_status": row[15],
        }
