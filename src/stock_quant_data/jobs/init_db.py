"""
Initialize the canonical loader build database schema.

Design:
- SQL-first
- one place to create stable base tables
- downstream jobs may TRUNCATE / REBUILD content, but they should not drift
  fundamental table names
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Create the base database objects required by the loader pipeline.

    This job creates only foundational objects.
    Content population belongs to downstream jobs.
    """
    configure_logging()
    LOGGER.info("init-db started")

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Simple metadata tables.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version VARCHAR,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS release_metadata (
                key VARCHAR,
                value VARCHAR,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # ------------------------------------------------------------------
        # Canonical master data tables.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS instrument (
                instrument_id BIGINT PRIMARY KEY,
                security_type VARCHAR,
                company_id VARCHAR,
                primary_ticker VARCHAR,
                primary_exchange VARCHAR
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_reference_history (
                symbol_reference_history_id BIGINT PRIMARY KEY,
                instrument_id BIGINT,
                symbol VARCHAR,
                exchange VARCHAR,
                is_primary BOOLEAN,
                effective_from DATE,
                effective_to DATE
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS listing_status_history (
                listing_status_history_id BIGINT PRIMARY KEY,
                instrument_id BIGINT,
                symbol VARCHAR,
                listing_status VARCHAR,
                status_reason VARCHAR,
                effective_from DATE,
                effective_to DATE,
                source_name VARCHAR
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS universe_definition (
                universe_name VARCHAR PRIMARY KEY,
                universe_description VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS universe_membership_history (
                universe_membership_history_id BIGINT PRIMARY KEY,
                universe_name VARCHAR,
                instrument_id BIGINT,
                symbol VARCHAR,
                effective_from DATE,
                effective_to DATE,
                source_name VARCHAR
            )
            """
        )

        # ------------------------------------------------------------------
        # Raw source tables.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS nasdaq_symbol_directory_raw (
                snapshot_id VARCHAR,
                source_kind VARCHAR,
                symbol VARCHAR,
                security_name VARCHAR,
                exchange_code VARCHAR,
                test_issue_flag VARCHAR,
                etf_flag VARCHAR,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_companyfacts_raw (
                raw_id BIGINT,
                cik VARCHAR,
                json_text VARCHAR,
                source_zip_path VARCHAR,
                json_member_name VARCHAR,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_submissions_company_raw (
                raw_id BIGINT,
                cik VARCHAR,
                company_name VARCHAR,
                ticker VARCHAR,
                exchange VARCHAR,
                sic VARCHAR,
                sic_description VARCHAR,
                entity_type VARCHAR,
                source_zip_path VARCHAR,
                json_member_name VARCHAR,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_submissions_company_raw_targeted (
                raw_id BIGINT,
                cik VARCHAR,
                company_name VARCHAR,
                ticker VARCHAR,
                exchange VARCHAR,
                sic VARCHAR,
                sic_description VARCHAR,
                entity_type VARCHAR,
                source_zip_path VARCHAR,
                json_member_name VARCHAR,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_symbol_company_map (
                raw_id BIGINT,
                cik VARCHAR,
                symbol VARCHAR,
                company_name VARCHAR,
                exchange VARCHAR,
                source_zip_path VARCHAR,
                json_member_name VARCHAR,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_symbol_company_map_targeted (
                raw_id BIGINT,
                cik VARCHAR,
                symbol VARCHAR,
                company_name VARCHAR,
                exchange VARCHAR,
                source_zip_path VARCHAR,
                json_member_name VARCHAR,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_manual_override_map (
                raw_symbol VARCHAR,
                mapped_symbol VARCHAR,
                source_name VARCHAR,
                rationale VARCHAR,
                review_status VARCHAR
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stooq_symbol_normalization_map (
                raw_symbol VARCHAR,
                normalized_symbol VARCHAR,
                rule_name VARCHAR,
                built_at TIMESTAMP WITH TIME ZONE
            )
            """
        )

        # ------------------------------------------------------------------
        # Derived / triage / serving-adjacent build tables.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_source_daily_raw_stooq (
                source_row_id BIGINT,
                raw_symbol VARCHAR,
                price_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_source_daily_raw_yahoo (
                source_row_id BIGINT,
                raw_symbol VARCHAR,
                price_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                adj_close DOUBLE,
                volume BIGINT
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_source_daily_normalized (
                normalized_price_id BIGINT PRIMARY KEY,
                source_name VARCHAR NOT NULL,
                source_row_id BIGINT NOT NULL,
                raw_symbol VARCHAR NOT NULL,
                instrument_id BIGINT,
                price_date DATE NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                adj_close DOUBLE,
                volume BIGINT NOT NULL,
                symbol_resolution_status VARCHAR NOT NULL,
                normalization_notes VARCHAR,
                normalized_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_history (
                price_history_id BIGINT PRIMARY KEY,
                instrument_id BIGINT,
                symbol VARCHAR,
                source_name VARCHAR,
                source_row_id BIGINT,
                price_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                adj_close DOUBLE,
                volume BIGINT,
                built_at TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_reference_candidates_from_unresolved_stooq (
                raw_symbol VARCHAR,
                unresolved_row_count BIGINT,
                min_price_date DATE,
                max_price_date DATE,
                first_source_row_id BIGINT,
                last_source_row_id BIGINT,
                candidate_family VARCHAR,
                suggested_action VARCHAR,
                recency_bucket VARCHAR,
                normalization_notes_example VARCHAR,
                built_at TIMESTAMP WITH TIME ZONE
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS unresolved_symbol_worklist (
                raw_symbol VARCHAR,
                unresolved_row_count BIGINT,
                min_price_date DATE,
                max_price_date DATE,
                candidate_family VARCHAR,
                suggested_action VARCHAR,
                recency_bucket VARCHAR,
                built_at TIMESTAMP WITH TIME ZONE
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS high_priority_unresolved_symbol_probe (
                raw_symbol VARCHAR,
                unresolved_row_count BIGINT,
                min_price_date DATE,
                max_price_date DATE,
                candidate_family VARCHAR,
                suggested_action VARCHAR,
                recency_bucket VARCHAR,
                exact_current_instrument_id BIGINT,
                exact_current_symbol VARCHAR,
                exact_current_exchange VARCHAR,
                nearby_reference_matches VARCHAR,
                in_latest_nasdaq_raw INTEGER,
                nasdaq_exact_matches VARCHAR,
                nasdaq_nearby_matches VARCHAR,
                in_targeted_sec_symbols INTEGER,
                sec_exact_matches VARCHAR,
                probe_recommendation VARCHAR,
                built_at TIMESTAMP WITH TIME ZONE
            )
            """
        )

        # ------------------------------------------------------------------
        # Helper views used by probes / invariants.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE OR REPLACE VIEW v_symbol_reference_history_open_intervals AS
            SELECT *
            FROM symbol_reference_history
            WHERE effective_to IS NULL
            """
        )

        conn.execute(
            """
            CREATE OR REPLACE VIEW v_universe_membership_history_open_intervals AS
            SELECT *
            FROM universe_membership_history
            WHERE effective_to IS NULL
            """
        )

        print(
            {
                "status": "ok",
                "job": "init-db",
                "tables_initialized": [
                    "schema_migrations",
                    "release_metadata",
                    "instrument",
                    "symbol_reference_history",
                    "listing_status_history",
                    "universe_definition",
                    "universe_membership_history",
                    "nasdaq_symbol_directory_raw",
                    "sec_companyfacts_raw",
                    "sec_submissions_company_raw",
                    "sec_submissions_company_raw_targeted",
                    "sec_symbol_company_map",
                    "sec_symbol_company_map_targeted",
                    "symbol_manual_override_map",
                    "stooq_symbol_normalization_map",
                    "price_source_daily_raw_stooq",
                    "price_source_daily_raw_yahoo",
                    "price_source_daily_normalized",
                    "price_history",
                    "symbol_reference_candidates_from_unresolved_stooq",
                    "unresolved_symbol_worklist",
                    "high_priority_unresolved_symbol_probe",
                ],
                "views_initialized": [
                    "v_symbol_reference_history_open_intervals",
                    "v_universe_membership_history_open_intervals",
                ],
            }
        )
    finally:
        conn.close()
        LOGGER.info("init-db finished")


if __name__ == "__main__":
    run()
