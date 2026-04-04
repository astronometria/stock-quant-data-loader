"""
Initialize the current canonical loader database schema.

This file is the single source of truth for the tables owned by the loader
repo. The goal is to keep table names and column names stable so that all
downstream jobs can rely on one coherent schema.

Important design choices:
- SQL-first
- only current repo / current schema names
- no legacy table aliases
- no hidden migrations
- heavily commented so future developers can reason about intent quickly
"""

from __future__ import annotations

import logging
from pathlib import Path

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.config.settings import get_settings
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def _ensure_parent_dir() -> Path:
    """
    Ensure the build DB parent directory exists before connecting.

    Returning the directory is useful both for logging and for callers that
    may want to inspect the location later.
    """
    settings = get_settings()
    build_db_path = settings.build_db_path
    build_db_path.parent.mkdir(parents=True, exist_ok=True)
    return build_db_path.parent


def run() -> None:
    """
    Create every canonical table required by the current loader pipeline.

    This job intentionally uses CREATE TABLE IF NOT EXISTS and does not try to
    mutate old incompatible schemas in-place. The user explicitly wanted a
    clean rebuild path, so the rebuild orchestration is expected to start from
    a fresh DB file.
    """
    configure_logging()
    LOGGER.info("init-db started")

    _ensure_parent_dir()

    conn = connect_build_db()
    try:
        # ------------------------------------------------------------------
        # Release metadata / bookkeeping tables.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version VARCHAR PRIMARY KEY,
                applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS release_metadata (
                release_key VARCHAR PRIMARY KEY,
                release_value VARCHAR,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # ------------------------------------------------------------------
        # Core identity table.
        #
        # primary_ticker is the current anchor for many bootstrap jobs.
        # We intentionally keep the table compact and explicit.
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

        # ------------------------------------------------------------------
        # Symbol reference history.
        #
        # Current loader logic relies on:
        # - symbol
        # - instrument_id
        # - exchange
        # - effective_from
        # - effective_to
        #
        # The open-ended row (effective_to IS NULL) is the active/current one.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_reference_history (
                symbol_reference_history_id BIGINT PRIMARY KEY,
                instrument_id BIGINT NOT NULL,
                symbol VARCHAR NOT NULL,
                exchange VARCHAR,
                is_primary BOOLEAN NOT NULL,
                effective_from DATE NOT NULL,
                effective_to DATE
            )
            """
        )

        # ------------------------------------------------------------------
        # Listing status history.
        #
        # Used by future PIT-oriented reconstruction and consistency checks.
        # ------------------------------------------------------------------
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
                source_name VARCHAR NOT NULL
            )
            """
        )

        # ------------------------------------------------------------------
        # Universe metadata tables.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS universe_definition (
                universe_name VARCHAR PRIMARY KEY,
                universe_description VARCHAR,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
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
                source_name VARCHAR NOT NULL
            )
            """
        )

        # ------------------------------------------------------------------
        # Raw Nasdaq Trader symbol directory snapshots.
        # These are loaded from the downloader repo artifacts.
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
                test_issue_flag VARCHAR,
                etf_flag VARCHAR,
                round_lot_size BIGINT,
                raw_payload_json VARCHAR,
                loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # ------------------------------------------------------------------
        # SEC companyfacts raw.
        # This is kept compact at the loader layer: one row per raw companyfacts
        # JSON member staged from the downloader zip.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_companyfacts_raw (
                raw_id BIGINT PRIMARY KEY,
                cik VARCHAR,
                entity_name VARCHAR,
                source_zip_path VARCHAR NOT NULL,
                json_member_name VARCHAR NOT NULL,
                raw_json VARCHAR NOT NULL,
                loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # ------------------------------------------------------------------
        # SEC submissions raw identity tables.
        # We keep both the full table and the targeted one because the current
        # pipeline uses both broad and targeted passes.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_submissions_company_raw (
                raw_id BIGINT PRIMARY KEY,
                cik VARCHAR,
                company_name VARCHAR,
                source_zip_path VARCHAR NOT NULL,
                json_member_name VARCHAR NOT NULL,
                raw_json VARCHAR NOT NULL,
                loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_submissions_company_raw_targeted (
                raw_id BIGINT PRIMARY KEY,
                cik VARCHAR,
                company_name VARCHAR,
                source_zip_path VARCHAR NOT NULL,
                json_member_name VARCHAR NOT NULL,
                raw_json VARCHAR NOT NULL,
                loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # ------------------------------------------------------------------
        # SEC symbol maps extracted from submissions payloads.
        # ------------------------------------------------------------------
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
                loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
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
                loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # ------------------------------------------------------------------
        # Current manual override map used to bootstrap missing identities.
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_manual_override_map (
                raw_symbol VARCHAR NOT NULL,
                mapped_symbol VARCHAR NOT NULL,
                source_name VARCHAR NOT NULL,
                mapping_note VARCHAR,
                priority_level VARCHAR,
                built_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # ------------------------------------------------------------------
        # Candidate / worklist / probe tables for unresolved symbol workflow.
        # ------------------------------------------------------------------
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
        # Mechanical Stooq symbol normalization map.
        # ------------------------------------------------------------------
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
        # Helpful diagnostic views for open intervals.
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

        print(
            {
                "status": "ok",
                "job": "init-db",
                "build_db_parent_dir": str(_ensure_parent_dir()),
            }
        )
    finally:
        conn.close()
        LOGGER.info("init-db finished")


if __name__ == "__main__":
    run()
