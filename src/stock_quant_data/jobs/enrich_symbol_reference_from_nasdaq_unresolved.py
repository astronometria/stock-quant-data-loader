"""
Enrich symbol_reference_history from Nasdaq raw coverage for symbols that are still
unresolved in the normalized Stooq price layer.

Design goals:
- SQL-first
- append-only / idempotent behavior
- do not rebuild or delete symbol_reference_history
- only add missing open symbol references
- reuse existing instrument rows when possible
- create new instrument rows only when necessary

Resolution strategy:
1. Find distinct unresolved raw_symbol values from price_source_daily_normalized.
2. Normalize Stooq-style symbols into candidate listed symbols:
   - remove trailing ".US"
   - replace "_" with "-"
3. Match those symbols against nasdaq_symbol_directory_raw.
4. Prefer the newest snapshot row per symbol.
5. Prefer nasdaqlisted over otherlisted when both exist on same recency rank.
6. Insert missing instrument rows.
7. Insert missing open symbol_reference_history rows.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Add missing open symbol references for currently unresolved Stooq symbols
    when the normalized probe symbol is present in nasdaq_symbol_directory_raw.
    """
    configure_logging()
    LOGGER.info("enrich-symbol-reference-from-nasdaq-unresolved started")

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Stage unresolved distinct symbols from normalized prices.
        # We use the normalized table because it already captures which rows
        # remain unresolved after current symbol-reference + mapping logic.
        # ------------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_unresolved_stooq_symbols")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_unresolved_stooq_symbols AS
            SELECT
                raw_symbol,
                UPPER(
                    REPLACE(
                        REPLACE(TRIM(raw_symbol), '.US', ''),
                        '_',
                        '-'
                    )
                ) AS normalized_probe_symbol,
                MIN(price_date) AS min_price_date,
                MAX(price_date) AS max_price_date,
                COUNT(*) AS unresolved_price_row_count
            FROM price_source_daily_normalized
            WHERE UPPER(COALESCE(symbol_resolution_status, '')) = 'UNRESOLVED'
              AND raw_symbol IS NOT NULL
              AND TRIM(raw_symbol) <> ''
            GROUP BY 1, 2
            """
        )

        staged_unresolved_symbol_count = conn.execute(
            "SELECT COUNT(*) FROM tmp_unresolved_stooq_symbols"
        ).fetchone()[0]

        # ------------------------------------------------------------------
        # Pick the best Nasdaq raw row per normalized unresolved symbol.
        #
        # Priority:
        # - newest snapshot_id first
        # - nasdaqlisted preferred over otherlisted when tied
        #
        # We only keep symbols that do not already exist as an open reference.
        # ------------------------------------------------------------------
        conn.execute("DROP TABLE IF EXISTS tmp_nasdaq_unresolved_matches")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_nasdaq_unresolved_matches AS
            WITH ranked_matches AS (
                SELECT
                    u.raw_symbol,
                    u.normalized_probe_symbol,
                    u.min_price_date,
                    u.max_price_date,
                    u.unresolved_price_row_count,
                    n.symbol,
                    n.security_name,
                    n.exchange_code,
                    n.etf_flag,
                    n.source_kind,
                    n.snapshot_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY u.normalized_probe_symbol
                        ORDER BY
                            n.snapshot_id DESC,
                            CASE
                                WHEN n.source_kind = 'nasdaqlisted' THEN 1
                                WHEN n.source_kind = 'otherlisted' THEN 2
                                ELSE 3
                            END,
                            n.symbol
                    ) AS rn
                FROM tmp_unresolved_stooq_symbols u
                JOIN nasdaq_symbol_directory_raw n
                  ON UPPER(TRIM(n.symbol)) = u.normalized_probe_symbol
                LEFT JOIN symbol_reference_history srh
                  ON srh.symbol = u.normalized_probe_symbol
                 AND srh.effective_to IS NULL
                WHERE srh.symbol IS NULL
                  AND n.symbol IS NOT NULL
                  AND TRIM(n.symbol) <> ''
                  AND COALESCE(n.test_issue_flag, 'N') <> 'Y'
            )
            SELECT
                raw_symbol,
                normalized_probe_symbol,
                min_price_date,
                max_price_date,
                unresolved_price_row_count,
                symbol,
                security_name,
                exchange_code,
                etf_flag,
                source_kind,
                snapshot_id,
                CASE
                    WHEN COALESCE(etf_flag, 'N') = 'Y' THEN 'ETF'
                    WHEN UPPER(COALESCE(security_name, '')) LIKE '%WARRANT%' THEN 'WARRANT'
                    WHEN UPPER(COALESCE(security_name, '')) LIKE '%RIGHT%' THEN 'RIGHT'
                    WHEN UPPER(COALESCE(security_name, '')) LIKE '%UNIT%' THEN 'UNIT'
                    WHEN UPPER(COALESCE(security_name, '')) LIKE '%PREFERRED%' THEN 'PREFERRED_STOCK'
                    WHEN source_kind = 'otherlisted' AND exchange_code = 'N' THEN 'COMMON_STOCK'
                    WHEN source_kind = 'otherlisted' AND exchange_code = 'A' THEN 'COMMON_STOCK'
                    WHEN source_kind = 'otherlisted' AND exchange_code = 'P' THEN 'ETF'
                    ELSE 'COMMON_STOCK'
                END AS security_type,
                CASE
                    WHEN source_kind = 'nasdaqlisted' AND exchange_code = 'Q' THEN 'NASDAQ'
                    WHEN source_kind = 'nasdaqlisted' AND exchange_code = 'G' THEN 'NASDAQ'
                    WHEN source_kind = 'nasdaqlisted' AND exchange_code = 'S' THEN 'NASDAQ'
                    WHEN source_kind = 'otherlisted' AND exchange_code = 'N' THEN 'NYSE'
                    WHEN source_kind = 'otherlisted' AND exchange_code = 'A' THEN 'NYSEMKT'
                    WHEN source_kind = 'otherlisted' AND exchange_code = 'P' THEN 'NYSEARCA'
                    WHEN source_kind = 'otherlisted' AND exchange_code = 'Z' THEN 'BATS'
                    WHEN source_kind = 'otherlisted' AND exchange_code = 'V' THEN 'IEX'
                    ELSE COALESCE(NULLIF(exchange_code, ''), 'UNKNOWN')
                END AS exchange_name
            FROM ranked_matches
            WHERE rn = 1
            """
        )

        matched_symbol_count = conn.execute(
            "SELECT COUNT(*) FROM tmp_nasdaq_unresolved_matches"
        ).fetchone()[0]

        # ------------------------------------------------------------------
        # Insert missing instrument rows.
        #
        # Reuse existing instrument rows when primary_ticker already exists.
        # Only create new instruments for genuinely missing primary tickers.
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
                    m.symbol,
                    m.security_type,
                    m.exchange_name,
                    ROW_NUMBER() OVER (ORDER BY m.symbol) AS rn
                FROM tmp_nasdaq_unresolved_matches m
                LEFT JOIN instrument i
                  ON i.primary_ticker = m.symbol
                WHERE i.instrument_id IS NULL
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS instrument_id,
                security_type,
                'NASDAQ_UNRESOLVED_' || symbol AS company_id,
                symbol AS primary_ticker,
                exchange_name AS primary_exchange
            FROM missing
            """
        )

        instrument_count_after = conn.execute(
            "SELECT COUNT(*) FROM instrument"
        ).fetchone()[0]

        # ------------------------------------------------------------------
        # Insert missing open symbol references.
        #
        # effective_from uses earliest unresolved price date seen for that raw
        # symbol so the resulting open reference backfills the usable price span.
        # ------------------------------------------------------------------
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
            staged AS (
                SELECT
                    i.instrument_id,
                    m.symbol,
                    m.exchange_name,
                    m.min_price_date AS effective_from,
                    ROW_NUMBER() OVER (ORDER BY m.symbol, i.instrument_id) AS rn
                FROM tmp_nasdaq_unresolved_matches m
                JOIN instrument i
                  ON i.primary_ticker = m.symbol
                LEFT JOIN symbol_reference_history srh
                  ON srh.symbol = m.symbol
                 AND srh.instrument_id = i.instrument_id
                 AND srh.effective_to IS NULL
                WHERE srh.symbol IS NULL
            )
            SELECT
                (SELECT max_id FROM current_max) + rn AS symbol_reference_history_id,
                instrument_id,
                symbol,
                exchange_name AS exchange,
                TRUE AS is_primary,
                effective_from,
                NULL AS effective_to
            FROM staged
            """
        )

        symbol_reference_history_count_after = conn.execute(
            "SELECT COUNT(*) FROM symbol_reference_history"
        ).fetchone()[0]

        # ------------------------------------------------------------------
        # Useful reporting.
        # ------------------------------------------------------------------
        rows_by_source_kind = conn.execute(
            """
            SELECT source_kind, COUNT(*)
            FROM tmp_nasdaq_unresolved_matches
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchall()

        rows_by_security_type = conn.execute(
            """
            SELECT security_type, COUNT(*)
            FROM tmp_nasdaq_unresolved_matches
            GROUP BY 1
            ORDER BY 2 DESC, 1
            """
        ).fetchall()

        sample_rows = conn.execute(
            """
            SELECT
                raw_symbol,
                normalized_probe_symbol,
                symbol,
                exchange_name,
                security_type,
                source_kind,
                snapshot_id,
                min_price_date,
                unresolved_price_row_count
            FROM tmp_nasdaq_unresolved_matches
            ORDER BY unresolved_price_row_count DESC, symbol
            LIMIT 100
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "enrich-symbol-reference-from-nasdaq-unresolved",
                "staged_unresolved_symbol_count": staged_unresolved_symbol_count,
                "matched_symbol_count": matched_symbol_count,
                "instrument_count": instrument_count_after,
                "symbol_reference_history_count": symbol_reference_history_count_after,
                "rows_by_source_kind": rows_by_source_kind,
                "rows_by_security_type": rows_by_security_type,
                "sample_rows": sample_rows,
            }
        )

    finally:
        conn.close()
        LOGGER.info("enrich-symbol-reference-from-nasdaq-unresolved finished")


if __name__ == "__main__":
    run()
