"""
Listings repository.

This repository provides explicit read access to published listing history.
"""

from __future__ import annotations

from typing import Any

import duckdb


class ListingsRepository:
    """
    Thin read repository for listing-history endpoints.
    """

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        """
        Store an already-open DuckDB connection.
        """
        self._connection = connection

    def get_listing_status_history(self, symbol: str) -> list[dict[str, Any]]:
        """
        Return the full listing lifecycle history for a symbol.
        """
        rows = self._connection.execute(
            """
            SELECT
                listing_status_history_id,
                instrument_id,
                company_id,
                security_type,
                primary_ticker,
                primary_exchange,
                symbol,
                exchange,
                listing_status,
                event_type,
                effective_from,
                effective_to,
                observed_at,
                ingested_at,
                source_name
            FROM api.listing_status_history
            WHERE UPPER(symbol) = UPPER(?)
            ORDER BY effective_from, listing_status_history_id
            """,
            [symbol],
        ).fetchall()

        items: list[dict[str, Any]] = []

        for row in rows:
            items.append(
                {
                    "listing_status_history_id": int(row[0]),
                    "instrument_id": row[1],
                    "company_id": row[2],
                    "security_type": row[3],
                    "primary_ticker": row[4],
                    "primary_exchange": row[5],
                    "symbol": row[6],
                    "exchange": row[7],
                    "listing_status": row[8],
                    "event_type": row[9],
                    "effective_from": row[10].isoformat() if row[10] is not None else None,
                    "effective_to": row[11].isoformat() if row[11] is not None else None,
                    "observed_at": row[12].isoformat() if row[12] is not None else None,
                    "ingested_at": row[13].isoformat() if row[13] is not None else None,
                    "source_name": row[14],
                }
            )

        return items
