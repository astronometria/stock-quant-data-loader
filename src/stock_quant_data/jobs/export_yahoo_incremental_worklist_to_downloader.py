"""Export yahoo_incremental_worklist to the sibling downloader repo."""
from __future__ import annotations
import csv
import logging
from stock_quant_data.config.logging import configure_logging
from stock_quant_data.config.settings import get_settings
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)

def run() -> None:
    configure_logging()
    LOGGER.info("export-yahoo-incremental-worklist-to-downloader started")
    settings = get_settings()
    conn = connect_build_db()
    try:
        output_dir = settings.downloader_data_dir / "worklists"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "yahoo_incremental_worklist.csv"
        rows = conn.execute("""
            SELECT
                run_date,
                instrument_id,
                provider_name,
                provider_symbol,
                fetch_mode,
                fetch_reason,
                fetch_start_date,
                fetch_end_date,
                stooq_max_price_date,
                yahoo_max_price_date,
                canonical_max_price_date,
                expected_latest_trade_date,
                listing_status,
                listing_status_reason,
                security_type,
                primary_exchange,
                priority_rank,
                is_active_listing,
                allow_overlap_redownload
            FROM yahoo_incremental_worklist
            WHERE run_date = CURRENT_DATE
            ORDER BY priority_rank, provider_symbol, instrument_id
        """).fetchall()
        headers = [
            "run_date","instrument_id","provider_name","provider_symbol","fetch_mode","fetch_reason",
            "fetch_start_date","fetch_end_date","stooq_max_price_date","yahoo_max_price_date",
            "canonical_max_price_date","expected_latest_trade_date","listing_status","listing_status_reason",
            "security_type","primary_exchange","priority_rank","is_active_listing","allow_overlap_redownload",
        ]
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(headers)
            writer.writerows(rows)
        print({"status": "ok", "job": "export-yahoo-incremental-worklist-to-downloader", "output_path": str(output_path), "row_count": len(rows)})
    finally:
        conn.close()
        LOGGER.info("export-yahoo-incremental-worklist-to-downloader finished")

if __name__ == "__main__":
    run()
