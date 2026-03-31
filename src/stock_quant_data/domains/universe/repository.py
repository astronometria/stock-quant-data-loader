"""
Universe repository.

This module intentionally keeps SQL access small and explicit.
The goal is to make it obvious which queries power the serving API.
"""

from __future__ import annotations

from typing import Any

import duckdb


class UniverseRepository:
    """
    Small repository focused on read access for universe-related queries.
    """

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        """
        Store the active DuckDB connection.
        """
        self._connection = connection

    def list_universes(self) -> list[dict[str, Any]]:
        """
        Return the list of API universes.
        """
        rows = self._connection.execute(
            """
            SELECT
                universe_id,
                universe_name,
                description,
                created_at,
                historical_instrument_count
            FROM api.universes
            ORDER BY universe_name
            """
        ).fetchall()

        items: list[dict[str, Any]] = []

        for row in rows:
            items.append(
                {
                    "universe_id": row[0],
                    "universe_name": row[1],
                    "description": row[2],
                    "created_at": row[3].isoformat() if row[3] is not None else None,
                    "historical_instrument_count": int(row[4]) if row[4] is not None else 0,
                }
            )

        return items

    def get_universe_members_as_of(
        self,
        universe_name: str,
        as_of_date: str,
    ) -> list[dict[str, Any]]:
        """
        Return PIT-active members of a universe at a given as_of date.

        PIT rule:
            effective_from <= as_of_date
            AND (effective_to IS NULL OR effective_to > as_of_date)
        """
        rows = self._connection.execute(
            """
            SELECT
                universe_membership_history_id,
                universe_id,
                universe_name,
                universe_description,
                instrument_id,
                company_id,
                security_type,
                primary_ticker,
                primary_exchange,
                membership_status,
                effective_from,
                effective_to,
                observed_at,
                ingested_at,
                source_name
            FROM api.universe_membership_history
            WHERE UPPER(universe_name) = UPPER(?)
              AND effective_from <= CAST(? AS DATE)
              AND (
                    effective_to IS NULL
                    OR effective_to > CAST(? AS DATE)
                  )
            ORDER BY
                primary_ticker,
                instrument_id,
                universe_membership_history_id
            """,
            [universe_name, as_of_date, as_of_date],
        ).fetchall()

        items: list[dict[str, Any]] = []

        for row in rows:
            items.append(
                {
                    "universe_membership_history_id": int(row[0]),
                    "universe_id": row[1],
                    "universe_name": row[2],
                    "universe_description": row[3],
                    "instrument_id": row[4],
                    "company_id": row[5],
                    "security_type": row[6],
                    "primary_ticker": row[7],
                    "primary_exchange": row[8],
                    "membership_status": row[9],
                    "effective_from": row[10].isoformat() if row[10] is not None else None,
                    "effective_to": row[11].isoformat() if row[11] is not None else None,
                    "observed_at": row[12].isoformat() if row[12] is not None else None,
                    "ingested_at": row[13].isoformat() if row[13] is not None else None,
                    "source_name": row[14],
                }
            )

        return items
