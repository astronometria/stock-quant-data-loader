"""
Raw prices CSV ingestion service.

Design goals:
- keep Python thin
- let DuckDB parse the CSV
- preserve provenance
- support repeatable local ingestion
"""

from __future__ import annotations

from pathlib import Path
import logging

from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def ingest_raw_prices_csv(csv_path: str) -> dict:
    """
    Ingest a CSV file into raw.price_source_daily.

    The CSV is expected to contain columns:
    source_symbol, source_exchange, price_date, open, high, low, close,
    adj_close, volume, currency, source_name, observed_at
    """
    csv_file = Path(csv_path).expanduser().resolve()

    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    connection = connect_build_db()

    try:
        before_count = connection.execute(
            "SELECT COUNT(*) FROM raw.price_source_daily"
        ).fetchone()[0]

        LOGGER.info("Ingesting raw prices CSV: %s", csv_file)

        connection.execute(
            f"""
            INSERT INTO raw.price_source_daily (
                raw_price_source_daily_id,
                source_symbol,
                source_exchange,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                currency,
                source_name,
                source_file_path,
                observed_at
            )
            SELECT
                COALESCE(
                    (
                        SELECT COALESCE(MAX(raw_price_source_daily_id), 0)
                        FROM raw.price_source_daily
                    ),
                    0
                ) + ROW_NUMBER() OVER () AS raw_price_source_daily_id,
                TRIM(source_symbol) AS source_symbol,
                source_exchange,
                CAST(price_date AS DATE) AS price_date,
                CAST(open AS DOUBLE) AS open,
                CAST(high AS DOUBLE) AS high,
                CAST(low AS DOUBLE) AS low,
                CAST(close AS DOUBLE) AS close,
                CAST(adj_close AS DOUBLE) AS adj_close,
                CAST(volume AS BIGINT) AS volume,
                currency,
                source_name,
                '{str(csv_file).replace("'", "''")}' AS source_file_path,
                CAST(observed_at AS TIMESTAMP) AS observed_at
            FROM read_csv_auto(
                '{str(csv_file).replace("'", "''")}',
                header = true
            )
            """
        )

        after_count = connection.execute(
            "SELECT COUNT(*) FROM raw.price_source_daily"
        ).fetchone()[0]

        written_rows = int(after_count - before_count)

        return {
            "csv_path": str(csv_file),
            "rows_written": written_rows,
            "raw_total_rows": int(after_count),
        }
    finally:
        connection.close()
