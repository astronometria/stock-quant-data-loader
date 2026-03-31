-- ===================================================================
-- Raw Nasdaq symbol directory snapshots
-- ===================================================================
-- Design rules:
-- - raw only
-- - preserve original rows and provenance
-- - no classification guesses here
-- - supports both nasdaqlisted.txt and otherlisted.txt
-- ===================================================================

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.nasdaq_symbol_directory_snapshot (
    raw_nasdaq_symbol_directory_snapshot_id BIGINT PRIMARY KEY,
    source_name VARCHAR NOT NULL,
    source_root_dir VARCHAR NOT NULL,
    source_file_path VARCHAR NOT NULL,
    source_file_name VARCHAR NOT NULL,
    snapshot_file_type VARCHAR NOT NULL,
    source_line_number BIGINT,
    row_raw VARCHAR,
    col_01_raw VARCHAR,
    col_02_raw VARCHAR,
    col_03_raw VARCHAR,
    col_04_raw VARCHAR,
    col_05_raw VARCHAR,
    col_06_raw VARCHAR,
    col_07_raw VARCHAR,
    col_08_raw VARCHAR,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE SEQUENCE IF NOT EXISTS raw_nasdaq_symbol_directory_snapshot_id_seq START 1;
