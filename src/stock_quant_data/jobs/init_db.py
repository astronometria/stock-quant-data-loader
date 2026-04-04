"""
Initialize the canonical build database schema for stock-quant-data-loader.

This file is the single source of truth for the CURRENT loader repo schema.
Every job in this repo must target these exact table names.

Design goals:
- stable canonical table names
- idempotent creation
- no legacy aliases
- comments kept verbose for future maintainers
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """
    Create all current canonical tables required by the loader workflow.

    Important:
    - We do not create deprecated tables.
    - We do not create alternate legacy naming variants.
    - All downstream jobs must read/write only the tables declared here.
    """
    configure_logging()
    LOGGER.info("init-db started")

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Minimal migration registry.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                migration_name VARCHAR PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # ------------------------------------------------------------------
        # Release metadata for build provenance.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS release_metadata (
                metadata_key VARCHAR PRIMARY KEY,
                metadata_value VARCHAR,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # ------------------------------------------------------------------
        # Core identity tables.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS instrument (
                instrument_id BIGINT PRIMARY KEY,
                security_type VARCHAR NOT NULL,
                company_id VARCHAR NOT NULL,
                primary_ticker VARCHAR NOT NULL,
                primary_exchange VARCHAR NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_reference_history (
                symbol_reference_history_id BIGINT PRIMARY KEY,
                instrument_id BIGINT NOT NULL,
                symbol VARCHAR NOT NULL,
                exchange VARCHAR,
                is_primary BOOLEAN NOT NULL DEFAULT TRUE,
                effective_from DATE NOT NULL,
                effective_to DATE
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS listing_status_history (
                listing_status_history_id BIGINT PRIMARY KEY,
                instrument_id BIGINT NOT NULL,
                symbol VARCHAR NOT NULL,
                listing_status VARCHAR NOT NULL,
                status_reason VARCHAR,
                effective_from DATE NOT NULL,
                effective_to DATE,
                source_name VARCHAR
            )
            """
        )

        # ------------------------------------------------------------------
        # Universe tables.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS universe_definition (
                universe_name VARCHAR PRIMARY KEY,
                description VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS universe_membership_history (
                universe_membership_history_id BIGINT PRIMARY KEY,
                universe_name VARCHAR NOT NULL,
                instrument_id BIGINT NOT NULL,
                effective_from DATE NOT NULL,
                effective_to DATE,
                source_name VARCHAR
            )
            """
        )

        # ------------------------------------------------------------------
        # Current open-interval helper views.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE OR REPLACE VIEW v_symbol_reference_history_open_intervals AS
            SELECT
                symbol_reference_history_id,
                instrument_id,
                symbol,
                exchange,
                is_primary,
                effective_from,
                effective_to
            FROM symbol_reference_history
            WHERE effective_to IS NULL
            """
        )

        conn.execute(
            """
            CREATE OR REPLACE VIEW v_universe_membership_history_open_intervals AS
            SELECT
                universe_membership_history_id,
                universe_name,
                instrument_id,
                effective_from,
                effective_to,
                source_name
            FROM universe_membership_history
            WHERE effective_to IS NULL
            """
        )

        # ------------------------------------------------------------------
        # Raw/current source tables used by the loader jobs.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS nasdaq_symbol_directory_raw (
                raw_id BIGINT PRIMARY KEY,
                snapshot_id VARCHAR NOT NULL,
                source_kind VARCHAR NOT NULL,
                symbol VARCHAR,
                security_name VARCHAR,
                exchange_code VARCHAR,
                etf_flag VARCHAR,
                test_issue_flag VARCHAR,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_manual_override_map (
                raw_symbol VARCHAR PRIMARY KEY,
                mapped_symbol VARCHAR NOT NULL,
                source_name VARCHAR NOT NULL,
                mapping_rationale VARCHAR,
                confidence_level VARCHAR,
                built_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_companyfacts_raw (
                raw_id BIGINT,
                cik VARCHAR,
                fact_namespace VARCHAR,
                fact_name VARCHAR,
                fact_value_double DOUBLE,
                fact_value_text VARCHAR,
                unit_name VARCHAR,
                period_end DATE,
                filing_date DATE,
                accession_number VARCHAR,
                source_zip_path VARCHAR NOT NULL,
                json_member_name VARCHAR NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_submissions_company_raw (
                raw_id BIGINT,
                cik VARCHAR,
                entity_type VARCHAR,
                sic VARCHAR,
                sic_description VARCHAR,
                name VARCHAR,
                tickers_json VARCHAR,
                exchanges_json VARCHAR,
                ein VARCHAR,
                description VARCHAR,
                website VARCHAR,
                investor_website VARCHAR,
                fiscal_year_end VARCHAR,
                source_zip_path VARCHAR NOT NULL,
                json_member_name VARCHAR NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_submissions_company_raw_targeted (
                raw_id BIGINT,
                cik VARCHAR,
                entity_type VARCHAR,
                sic VARCHAR,
                sic_description VARCHAR,
                name VARCHAR,
                tickers_json VARCHAR,
                exchanges_json VARCHAR,
                ein VARCHAR,
                description VARCHAR,
                website VARCHAR,
                investor_website VARCHAR,
                fiscal_year_end VARCHAR,
                source_zip_path VARCHAR NOT NULL,
                json_member_name VARCHAR NOT NULL,
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
                source_zip_path VARCHAR NOT NULL,
                json_member_name VARCHAR NOT NULL,
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
                source_zip_path VARCHAR NOT NULL,
                json_member_name VARCHAR NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # ------------------------------------------------------------------
        # Normalization/worklist/probe tables.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stooq_symbol_normalization_map (
                raw_symbol VARCHAR,
                normalized_symbol VARCHAR,
                rule_name VARCHAR,
                built_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
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
                built_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
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
                built_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
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
                built_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
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
                    "symbol_manual_override_map",
                    "sec_companyfacts_raw",
                    "sec_submissions_company_raw",
                    "sec_submissions_company_raw_targeted",
                    "sec_symbol_company_map",
                    "sec_symbol_company_map_targeted",
                    "stooq_symbol_normalization_map",
                    "symbol_reference_candidates_from_unresolved_stooq",
                    "unresolved_symbol_worklist",
                    "high_priority_unresolved_symbol_probe",
                ],
            }
        )
    finally:
        conn.close()
        LOGGER.info("init-db finished")


if __name__ == "__main__":
    run()
