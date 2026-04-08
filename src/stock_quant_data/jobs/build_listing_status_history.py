"""
Build listing_status_history from the canonical symbol identity layer.

Design goals:
- use symbol_reference_history as the primary identity history source
- keep closed symbol reference intervals as the only direct INACTIVE source
- use the latest complete Nasdaq snapshot day only as confirmation for open refs
- do not force-close open refs merely because they are absent from the latest snapshot
- classify non-confirmed open refs into more useful reason buckets
- suppress obvious very-recent snapshot artifacts/test symbols only when they have
  no resolved price coverage and no SEC identity support
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """Rebuild canonical listing_status_history."""
    configure_logging()
    LOGGER.info("build-listing-status-history started")

    conn = connect_build_db()
    try:
        required_counts = {
            "symbol_reference_history": conn.execute(
                "SELECT COUNT(*) FROM symbol_reference_history"
            ).fetchone()[0],
            "price_source_daily_normalized": conn.execute(
                "SELECT COUNT(*) FROM price_source_daily_normalized"
            ).fetchone()[0],
            "nasdaq_symbol_directory_raw": conn.execute(
                "SELECT COUNT(*) FROM nasdaq_symbol_directory_raw"
            ).fetchone()[0],
        }

        latest_complete_snapshot_day_row = conn.execute(
            """
            WITH snapshot_days AS (
                SELECT
                    CAST(substr(snapshot_id, 1, 10) AS DATE) AS snapshot_day,
                    COUNT(
                        DISTINCT CASE
                            WHEN lower(snapshot_id) LIKE %nasdaqlisted% THEN nasdaqlisted
                            WHEN lower(snapshot_id) LIKE %otherlisted% THEN otherlisted
                            ELSE NULL
                        END
                    ) AS file_family_count
                FROM nasdaq_symbol_directory_raw
                GROUP BY CAST(substr(snapshot_id, 1, 10) AS DATE)
            )
            SELECT snapshot_day
            FROM snapshot_days
            WHERE file_family_count >= 2
            ORDER BY snapshot_day DESC
            LIMIT 1
            """
        ).fetchone()

        if latest_complete_snapshot_day_row is None:
            raise RuntimeError(
                "No complete Nasdaq snapshot day found in nasdaq_symbol_directory_raw"
            )

        latest_complete_snapshot_day = latest_complete_snapshot_day_row[0]

        conn.execute("DELETE FROM listing_status_history")

        conn.execute("DROP TABLE IF EXISTS tmp_latest_complete_nasdaq_symbols")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_latest_complete_nasdaq_symbols AS
            SELECT DISTINCT
                upper(trim(symbol)) AS symbol
            FROM nasdaq_symbol_directory_raw
            WHERE CAST(substr(snapshot_id, 1, 10) AS DATE) = ?
              AND symbol IS NOT NULL
              AND trim(symbol) <> 
              AND COALESCE(test_issue_flag, N) = N
            """,
            [latest_complete_snapshot_day],
        )

        conn.execute("DROP TABLE IF EXISTS tmp_resolved_price_coverage_by_instrument")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_resolved_price_coverage_by_instrument AS
            SELECT
                instrument_id,
                MIN(price_date) AS first_resolved_price_date,
                MAX(price_date) AS last_resolved_price_date,
                COUNT(*) AS resolved_price_row_count
            FROM price_source_daily_normalized
            WHERE upper(COALESCE(symbol_resolution_status, )) = RESOLVED
              AND instrument_id IS NOT NULL
            GROUP BY instrument_id
            """
        )

        conn.execute("DROP TABLE IF EXISTS tmp_sec_identity_symbols")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_sec_identity_symbols AS
            SELECT DISTINCT upper(trim(symbol)) AS symbol
            FROM (
                SELECT symbol FROM sec_symbol_company_map
                UNION ALL
                SELECT symbol FROM sec_symbol_company_map_targeted
            )
            WHERE symbol IS NOT NULL
              AND trim(symbol) <> 
            """
        )

        conn.execute("DROP TABLE IF EXISTS tmp_recent_snapshot_artifact_symbols")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_recent_snapshot_artifact_symbols AS
            WITH open_refs AS (
                SELECT
                    srh.instrument_id,
                    srh.symbol,
                    srh.effective_from
                FROM symbol_reference_history srh
                WHERE srh.effective_to IS NULL
            )
            SELECT
                o.instrument_id,
                o.symbol
            FROM open_refs o
            LEFT JOIN tmp_latest_complete_nasdaq_symbols latest
              ON latest.symbol = upper(trim(o.symbol))
            LEFT JOIN tmp_resolved_price_coverage_by_instrument rpc
              ON rpc.instrument_id = o.instrument_id
            LEFT JOIN tmp_sec_identity_symbols sec
              ON sec.symbol = upper(trim(o.symbol))
            WHERE latest.symbol IS NULL
              AND rpc.instrument_id IS NULL
              AND sec.symbol IS NULL
              AND o.effective_from >= DATE 2026-03-29
              AND (
                    upper(o.symbol) LIKE %TEST%
                 OR upper(o.symbol) LIKE ZTEST%
                 OR upper(o.symbol) LIKE ATEST%
                 OR upper(o.symbol) LIKE CTEST%
                 OR upper(o.symbol) LIKE MTEST%
                 OR upper(o.symbol) LIKE NTEST%
                 OR upper(o.symbol) LIKE ZEXIT%
                 OR upper(o.symbol) LIKE ZIEXT%
                 OR upper(o.symbol) LIKE ZXIET%
                 OR upper(o.symbol) LIKE ZBZX%
                 OR upper(o.symbol) LIKE ZVV%
              )
            """
        )

        conn.execute(
            """
            INSERT INTO listing_status_history (
                listing_status_history_id,
                instrument_id,
                symbol,
                listing_status,
                status_reason,
                effective_from,
                effective_to,
                source_name
            )
            WITH params AS (
                SELECT CAST(? AS DATE) AS latest_complete_snapshot_day
            ),
            base AS (
                SELECT
                    srh.symbol_reference_history_id AS listing_status_history_id,
                    srh.instrument_id,
                    srh.symbol,
                    srh.effective_from,
                    srh.effective_to,
                    i.security_type,
                    i.primary_exchange,
                    latest.symbol AS latest_confirmed_symbol
                FROM symbol_reference_history srh
                LEFT JOIN instrument i
                  ON i.instrument_id = srh.instrument_id
                LEFT JOIN tmp_latest_complete_nasdaq_symbols latest
                  ON latest.symbol = upper(trim(srh.symbol))
                LEFT JOIN tmp_recent_snapshot_artifact_symbols artifact
                  ON artifact.instrument_id = srh.instrument_id
                 AND artifact.symbol = srh.symbol
                WHERE artifact.symbol IS NULL
            ),
            classified AS (
                SELECT
                    b.listing_status_history_id,
                    b.instrument_id,
                    b.symbol,
                    b.effective_from,
                    b.effective_to,
                    CASE
                        WHEN b.effective_to IS NOT NULL THEN INACTIVE
                        ELSE ACTIVE
                    END AS listing_status,
                    CASE
                        WHEN b.effective_to IS NOT NULL THEN closed_symbol_reference_interval
                        WHEN b.latest_confirmed_symbol IS NOT NULL THEN present_in_latest_complete_nasdaq_snapshot_day
                        WHEN upper(COALESCE(b.primary_exchange, )) = OTC
                            THEN open_symbol_reference_otc_not_confirmed_by_latest_complete_nasdaq_snapshot_day
                        WHEN upper(COALESCE(b.security_type, )) = ETF
                            THEN open_symbol_reference_etf_not_confirmed_by_latest_complete_nasdaq_snapshot_day
                        WHEN upper(b.symbol) LIKE %-WS
                          OR upper(b.symbol) LIKE %WS
                          OR upper(b.symbol) LIKE %-W
                          OR upper(b.symbol) LIKE %-R
                          OR upper(b.symbol) LIKE %RT
                            THEN open_symbol_reference_warrant_or_right_not_confirmed_by_latest_complete_nasdaq_snapshot_day
                        WHEN upper(b.symbol) LIKE %-U
                          OR upper(b.symbol) LIKE %U
                            THEN open_symbol_reference_unit_not_confirmed_by_latest_complete_nasdaq_snapshot_day
                        WHEN b.symbol LIKE %-%
                            THEN open_symbol_reference_class_or_series_not_confirmed_by_latest_complete_nasdaq_snapshot_day
                        WHEN b.effective_from >= p.latest_complete_snapshot_day - INTERVAL 180 DAY
                            THEN open_symbol_reference_recent_plain_symbol_not_confirmed_by_latest_complete_nasdaq_snapshot_day
                        ELSE open_symbol_reference_long_lived_plain_symbol_not_confirmed_by_latest_complete_nasdaq_snapshot_day
                    END AS status_reason
                FROM base b
                CROSS JOIN params p
            )
            SELECT
                listing_status_history_id,
                instrument_id,
                symbol,
                listing_status,
                status_reason,
                effective_from,
                effective_to,
                build-listing-status-history AS source_name
            FROM classified
            """,
            [latest_complete_snapshot_day],
        )

        listing_status_history_count = conn.execute(
            "SELECT COUNT(*) FROM listing_status_history"
        ).fetchone()[0]

        open_rows = conn.execute(
            """
            SELECT COUNT(*)
            FROM listing_status_history
            WHERE effective_to IS NULL
            """
        ).fetchone()[0]

        rows_by_status = conn.execute(
            """
            SELECT listing_status, COUNT(*)
            FROM listing_status_history
            GROUP BY listing_status
            ORDER BY listing_status
            """
        ).fetchall()

        rows_by_reason = conn.execute(
            """
            SELECT status_reason, COUNT(*)
            FROM listing_status_history
            GROUP BY status_reason
            ORDER BY status_reason
            """
        ).fetchall()

        date_range = conn.execute(
            """
            SELECT
                MIN(effective_from),
                MAX(COALESCE(effective_to, DATE 2026-03-31)),
                MIN(effective_from),
                MAX(COALESCE(effective_to, DATE 2026-03-31))
            FROM listing_status_history
            """
        ).fetchone()

        print(
            {
                "status": "ok",
                "job": "build-listing-status-history",
                "required_counts": required_counts,
                "latest_complete_snapshot_day": str(latest_complete_snapshot_day),
                "listing_status_history_count": listing_status_history_count,
                "open_rows": open_rows,
                "rows_by_status": rows_by_status,
                "rows_by_reason": rows_by_reason,
                "date_range": tuple(str(v) if v is not None else None for v in date_range),
            }
        )
    finally:
        conn.close()
        LOGGER.info("build-listing-status-history finished")


if __name__ == "__main__":
    run()
