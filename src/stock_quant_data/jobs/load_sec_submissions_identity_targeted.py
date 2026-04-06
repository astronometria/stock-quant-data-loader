"""
Build the targeted SEC submissions identity layer from the current worklist.

This module materializes the subset of broad SEC company rows associated with
symbols appearing in the unresolved_symbol_worklist, using sec_symbol_company_map
as the bridge because sec_submissions_company_raw itself does not contain a
direct symbol column in the current canonical schema.
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """Materialize a targeted subset of SEC submissions identity rows."""
    configure_logging()
    LOGGER.info("load-sec-submissions-identity-targeted started")

    conn = connect_build_db()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_submissions_company_raw_targeted AS
            SELECT *
            FROM sec_submissions_company_raw
            WHERE 1 = 0
            """
        )

        conn.execute("DELETE FROM sec_submissions_company_raw_targeted")

        conn.execute(
            """
            INSERT INTO sec_submissions_company_raw_targeted
            SELECT DISTINCT raw.*
            FROM sec_submissions_company_raw raw
            JOIN sec_symbol_company_map map
              ON map.cik = raw.cik
            JOIN unresolved_symbol_worklist work
              ON map.symbol = work.raw_symbol
            """
        )

        latest_zip = conn.execute(
            """
            SELECT MAX(source_zip_path)
            FROM sec_submissions_company_raw
            """
        ).fetchone()[0]

        worklist_symbol_count = conn.execute(
            """
            SELECT COUNT(DISTINCT raw_symbol)
            FROM unresolved_symbol_worklist
            """
        ).fetchone()[0]

        company_row_count = conn.execute(
            """
            SELECT COUNT(DISTINCT cik)
            FROM sec_submissions_company_raw_targeted
            """
        ).fetchone()[0]

        symbol_row_count = conn.execute(
            """
            SELECT COUNT(DISTINCT map.symbol)
            FROM sec_submissions_company_raw_targeted raw
            JOIN sec_symbol_company_map map
              ON map.cik = raw.cik
            JOIN unresolved_symbol_worklist work
              ON map.symbol = work.raw_symbol
            """
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "load-sec-submissions-identity-targeted",
                "latest_zip": latest_zip,
                "worklist_symbol_count": worklist_symbol_count,
                "company_row_count": company_row_count,
                "symbol_row_count": symbol_row_count,
            }
        )
    finally:
        conn.close()
        LOGGER.info("load-sec-submissions-identity-targeted finished")


if __name__ == "__main__":
    run()
