# Jobs catalog

## src.stock_quant_data.jobs.build_core_prices

CLI job for building canonical core prices from raw landed data.

- `run_build_core_prices() -> dict`

## src.stock_quant_data.jobs.build_high_priority_unresolved_symbol_probe

Build the high-priority unresolved symbol probe.

This probe enriches the current high-priority worklist with nearby matches
from:
- symbol_reference_history
- nasdaq_symbol_directory_raw
- sec_symbol_company_map_targeted

The output table is canonical and its current columns are:
- raw_symbol
- unresolved_row_count
- min_price_date
- max_price_date
- candidate_family
- suggested_action
- recency_bucket
- exact_current_instrument_id
- exact_current_symbol
- exact_current_exchange
- nearby_reference_matches
- in_latest_nasdaq_raw
- nasdaq_exact_matches
- nasdaq_nearby_matches
- in_targeted_sec_symbols
- sec_exact_matches
- probe_recommendation
- built_at

- `run() -> None`

## src.stock_quant_data.jobs.build_listing_status_history

Build listing_status_history from the current canonical identity layer.

Design goals:
- use symbol_reference_history as the base historical identity layer
- closed symbol_reference_history intervals become INACTIVE rows
- latest complete Nasdaq snapshot day is used only as a confirmation layer
- open refs not confirmed by latest snapshot remain ACTIVE, but get a distinct reason
- suppress obvious recent snapshot/test artifacts that have:
  * no latest snapshot confirmation
  * no resolved price coverage
  * no SEC support
- keep the logic explicit and auditable

- `run() -> None`

## src.stock_quant_data.jobs.build_price_history_from_raw

Build the canonical price_history table from the normalized source table.

Design:
- SQL-first
- one canonical serving/build table for downstream consumers
- only resolved rows enter price_history
- keep the schema narrow and stable

Important:
- this table is the canonical price table for downstream joins
- unresolved symbols remain in price_source_daily_normalized for triage only

- `run() -> None`

## src.stock_quant_data.jobs.build_price_normalized_from_raw

Build canonical normalized price rows from the raw price layer.

This job uses:
- price_source_daily_raw_stooq
- symbol_reference_history
- stooq_symbol_normalization_map

Output:
- price_source_daily_normalized

Important schema notes:
- current raw Stooq table uses raw_price_id and raw_symbol
- current raw Stooq table does not contain adj_close
- current normalized table does contain adj_close, so we populate it with close
  for Stooq until a true adjusted source is introduced

Resolution strategy:
1) Direct exact match on raw_symbol against currently open symbol references
2) Generic Stooq normalization:
   - uppercase
   - trim
   - strip trailing .US
   - replace underscore with dash
   Then match against currently open symbol references
3) Explicit normalization-map fallback for special symbols

- `run() -> None`

## src.stock_quant_data.jobs.build_sec_companyfacts_parquet_from_staged_json

Build a Parquet facts dataset from staged SEC companyfacts JSON.

SQL-first, but batch-oriented:
- avoids one giant JSON explosion over all files at once
- writes one Parquet part per file batch
- future rebuilds can load from Parquet much faster than reparsing JSON

Why this exists:
- A single read_json_auto('*.json') over ~19k SEC companyfacts files can
  require huge intermediate state and fail with DuckDB temp/OOM pressure.
- Batching keeps the transformation SQL-first while bounding the working set.

Why this patched version exists:
- The previous batch size was still too large for the deep json_each(...)
  explosion done by companyfacts.
- DuckDB was failing inside conn.execute(sql_text) with OutOfMemoryException.
- This version reduces pressure by:
  1) lowering the default file batch size,
  2) lowering the DuckDB thread count,
  3) creating a fresh DuckDB connection for each batch so memory can be
     released more aggressively between batches.

- `_latest_stage_dir(stage_root: Path) -> Path`
- `_chunked(items: list[Path], chunk_size: int) -> list[list[Path]]`
- `_quote_sql_string(value: str) -> str`
- `_build_batch_sql(batch_files: list[Path], parquet_output_path: Path) -> str`
- `_configure_connection_for_batch(temp_dir: Path)`
- `run() -> None`

## src.stock_quant_data.jobs.build_stooq_symbol_normalization_map

Build deterministic Stooq symbol normalization rules.

Canonical output table:
- stooq_symbol_normalization_map(raw_symbol, normalized_symbol, rule_name, built_at)

This job derives general-format rules from unresolved raw symbols and keeps
the schema aligned with the active codebase.

- `run() -> None`

## src.stock_quant_data.jobs.build_symbol_manual_override_map

Build the explicit manual override map for unresolved Stooq symbols.

Important:
- This table is only a mapping table.
- It does NOT directly create instruments or symbol_reference rows.
- That responsibility belongs to dedicated enrichment jobs.

- `run() -> None`

## src.stock_quant_data.jobs.build_symbol_reference_candidates_from_unresolved_stooq

Build canonical unresolved-symbol candidates from the normalized Stooq layer.

This job is the single source of truth for unresolved symbol triage.
It only reads current canonical tables and writes the canonical
candidate table:
- symbol_reference_candidates_from_unresolved_stooq

- `run() -> None`

## src.stock_quant_data.jobs.build_symbol_reference_from_nasdaq_latest

Build a current-snapshot instrument + symbol reference layer from the latest
complete Nasdaq Trader raw snapshot.

Canonical behavior:
- keep instrument as the identity table
- rebuild only the current open-ended symbol_reference_history layer
- do not inject legacy demo rows
- do not duplicate currently open symbols

- `run() -> None`

## src.stock_quant_data.jobs.build_symbol_reference_history_from_nasdaq_snapshots

Build a broader symbol reference history from all loaded Nasdaq Trader snapshots.

Canonical behavior:
- derive effective_from from first snapshot date seen
- derive open/closed state from presence in latest snapshot
- upsert through instrument.primary_ticker
- rebuild symbol_reference_history from canonical snapshot staging
- do not insert legacy demo rows

- `run() -> None`

## src.stock_quant_data.jobs.build_universe_membership_history_from_listing_status

Build PIT-ready universe_membership_history from listing_status_history.

Design goals:
- use listing_status_history as the canonical status layer
- keep SQL-first logic for the actual history construction
- produce deterministic rebuilds
- separate common stocks and ETFs into different universes
- exclude obvious non-core instruments from the common stock universe
- keep the code very explicit and heavily commented for maintainability

- `run() -> None`

## src.stock_quant_data.jobs.build_unresolved_symbol_worklist

Build the canonical unresolved symbol worklist.

This table is intentionally derived from the canonical candidate table and
contains only the rows that still require identity creation work.

- `run() -> None`

## src.stock_quant_data.jobs.build_yfinance_download_contract

CLI-style job wrapper for building the Yahoo downloader contract.

- `run() -> dict`

## src.stock_quant_data.jobs.check_master_data_invariants

Check master-data invariants for the canonical loader schema.

- `run() -> None`

## src.stock_quant_data.jobs.enrich_stooq_symbol_normalization_map_from_probe

Append deterministic Stooq normalization rules from probe/candidate tables.

This module only inserts rows that match the canonical
stooq_symbol_normalization_map schema:

- raw_symbol
- normalized_symbol
- rule_name
- built_at

It does not assume any legacy columns such as mapped_symbol.

- `run() -> None`

## src.stock_quant_data.jobs.enrich_symbol_reference_from_high_priority_sec_probe

Repair symbol_reference_history directly from the
high_priority_unresolved_symbol_probe table when the probe has already
identified exact SEC-backed symbols that should exist as open references.

Important design choice:
- this job repairs / back-extends symbol_reference_history
- it does not create alternate legacy tables
- it does not create backup rows
- it is idempotent for the same probe result set

- `run() -> None`

## src.stock_quant_data.jobs.enrich_symbol_reference_from_manual_overrides

Enrich instrument and symbol_reference_history from explicit manual symbol overrides.

Canonical behavior:
- only create a new instrument when the mapped symbol does not already exist
  in instrument.primary_ticker
- only create an open-ended symbol_reference_history row when that open symbol
  does not already exist
- deduplicate mapped_symbol values from the manual map before inserting

- `run() -> None`

## src.stock_quant_data.jobs.enrich_symbol_reference_from_nasdaq_unresolved

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

- `run() -> None`

## src.stock_quant_data.jobs.enrich_symbol_reference_from_sec_general

Enrich symbol_reference_history from the general SEC symbol map.

Purpose:
- add missing open symbol references for symbols already present in sec_symbol_company_map
- create missing instrument rows when required
- keep the implementation SQL-first
- remain idempotent for repeated runs

Important:
- this is the bulk/general SEC enrichment step
- it complements the existing Nasdaq-based identity layer
- it should run before more manual/probe-driven repair steps

- `run() -> None`

## src.stock_quant_data.jobs.enrich_symbol_reference_from_sec_targeted

Enrich symbol_reference_history from the targeted SEC symbol table.

This is a targeted identity-enrichment step fed by the unresolved worklist.
It uses the canonical targeted SEC table names only.

- `run() -> None`

## src.stock_quant_data.jobs.ingest_raw_nasdaq_symbol_directory_dir

Job wrapper for raw Nasdaq symbol directory ingest.

- `run_ingest_raw_nasdaq_symbol_directory_dir(root_dir: str) -> dict`

## src.stock_quant_data.jobs.ingest_raw_prices_csv

CLI job for raw local CSV price ingestion.

- `run_ingest_raw_prices_csv(csv_path: str) -> dict`

## src.stock_quant_data.jobs.ingest_raw_prices_stooq_dir

CLI job for raw Stooq directory ingestion.

- `run_ingest_raw_prices_stooq_dir(root_dir: str) -> dict`

## src.stock_quant_data.jobs.ingest_raw_prices_yfinance_dir

CLI job for raw yfinance directory ingestion.

- `run_ingest_raw_prices_yfinance_dir(root_dir: str) -> dict`

## src.stock_quant_data.jobs.init_db

Initialize the canonical build database schema for stock-quant-data-loader.

This file is the single source of truth for the CURRENT loader repo schema.
Every job in this repo must target these exact table names.

Design goals:
- stable canonical table names
- idempotent creation
- no legacy aliases
- comments kept verbose for future maintainers

- `run() -> None`

## src.stock_quant_data.jobs.init_price_raw_tables

Initialize raw/normalized price tables for the current loader repo.

This module only guarantees that the canonical tables exist.
It must not introduce alternate or legacy table names.

- `run() -> None`

## src.stock_quant_data.jobs.insert_invalid_universe_overlap_demo

Insert a deliberate overlapping universe membership interval for testing.

This job is ONLY for controlled validation testing.
It creates a known-bad row that should make:
- sq validate-release detect an overlap
- sq publish-release refuse publication

- `run() -> None`

## src.stock_quant_data.jobs.load_nasdaq_symbol_directory_raw_from_downloader

Load Nasdaq symbol directory raw snapshots from downloader artifacts.

Supports:
- downloader/data/nasdaq/symdir/*.txt
- downloader/data/nasdaq/*.csv
- downloader/data/nasdaq/*.zip

Important:
- nasdaqlisted.txt and otherlisted.txt have different shapes
- this loader preserves enough structure in nasdaq_symbol_directory_raw
  for downstream symbol-reference builders

- `_candidate_paths(root: Path) -> list[Path]`
- `_snapshot_id_from_name(path_name: str) -> str`
- `_iter_reader_rows(reader: csv.DictReader, desc: str)`
- `_txt_source_kind_from_name(file_name: str) -> str`
- `_parse_txt_row(row: dict, source_kind: str) -> tuple[str | None, str | None, str | None, str | None]`
- `run() -> None`

## src.stock_quant_data.jobs.load_price_source_daily_raw_stooq_from_disk

Fast bulk loader for Stooq daily files into price_source_daily_raw_stooq.

Design goals:
- Replace slow Python row-by-row CSV parsing with DuckDB bulk CSV ingestion.
- Keep a resumable incremental mode.
- Use SQL-first loading for much better throughput.
- Keep Python thin: discover files, batch them, execute SQL.

Supported source roots:
- downloader/data/prices/stooq/daily/us
- local data/stooq
- legacy ~/stock-quant-oop-raw/data/raw/stooq

Modes:
- default / --full-refresh:
    rebuild the target raw table from scratch
- --incremental:
    ingest only files whose source_file_path is not already present

- `_discover_stooq_files() -> tuple[Path, list[Path]]`
- `_chunked(items: list[Path], chunk_size: int) -> list[list[Path]]`
- `_quote_sql_string(value: str) -> str`
- `_ensure_checkpoint_table(conn) -> None`
- `_clear_checkpoint_table(conn) -> None`
- `_existing_checkpoint_files(conn) -> set[str]`
- `_next_raw_price_id(conn) -> int`
- `_mark_batch_completed(conn, batch_files: list[Path]) -> None`
- `_build_batch_sql(batch_files: list[Path], start_raw_price_id: int) -> str`
- `parse_args() -> argparse.Namespace`
- `run() -> None`

## src.stock_quant_data.jobs.load_sec_companyfacts_raw_from_downloader

Convenience wrapper that stages companyfacts JSON from downloader artifacts and
then loads them into sec_companyfacts_raw.

This keeps orchestration simple and preserves one canonical target table:
- sec_companyfacts_raw

- `run() -> None`

## src.stock_quant_data.jobs.load_sec_companyfacts_raw_from_parquet

Load canonical SEC companyfacts raw rows from a derived Parquet facts dataset.

Why this version exists:
- Keep the ingestion path SQL-first.
- Keep Python thin and deterministic.
- Reuse the SQL file under sql/etl/sec/ instead of embedding the full query
  inline in Python.
- Preserve the existing target contract into sec_companyfacts_raw.

Runtime flow:
1) Resolve the latest parquet batch directory.
2) Count parquet files for logging.
3) Load SQL template from sql/etl/sec/load_sec_companyfacts_raw_from_parquet.sql
4) Replace the parquet glob placeholder.
5) Execute the SQL in DuckDB.
6) Verify final target row count.

Notes:
- This loader intentionally does not reparse SEC JSON.
- It assumes the parquet builder already produced the canonical flattened
  companyfacts dataset.

- `_latest_parquet_dir(parquet_root: Path) -> Path`
- `_load_sql_template(repo_root: Path) -> str`
- `run() -> None`

## src.stock_quant_data.jobs.load_sec_companyfacts_raw_from_staged_json

SQL-first SEC companyfacts loader, batched by file groups.

Why this version exists:
- One giant read_json_auto('*.json') query over ~20k files can stall inside one
  opaque SQL execution step.
- This version keeps the transformation SQL-first, but executes it in batches of
  file paths so progress is visible and resource usage is bounded.

Design:
- Python is intentionally thin orchestration only.
- DuckDB still performs the JSON reading and explosion work in SQL.
- We append into sec_companyfacts_raw batch by batch.

- `_latest_stage_dir(stage_root: Path) -> Path`
- `_chunked(items: list[Path], chunk_size: int) -> list[list[Path]]`
- `_quote_sql_string(value: str) -> str`
- `_build_batch_sql(batch_files: list[Path]) -> str`
- `run() -> None`

## src.stock_quant_data.jobs.load_sec_submissions_identity_from_downloader

Load broad SEC submissions identity data from downloader artifacts.

Canonical outputs modified:
- sec_submissions_company_raw
- sec_symbol_company_map

The canonical raw company table stores the parsed top-level submission identity
fields, while sec_symbol_company_map stores one row per ticker mapping.

- `_latest_zip_path(root: Path) -> Path`
- `run() -> None`

## src.stock_quant_data.jobs.load_sec_submissions_identity_targeted

Build the targeted SEC submissions identity layer from the current worklist.

This module materializes the subset of broad SEC company rows associated with
symbols appearing in the unresolved_symbol_worklist, using sec_symbol_company_map
as the bridge because sec_submissions_company_raw itself does not contain a
direct symbol column in the current canonical schema.

- `run() -> None`

## src.stock_quant_data.jobs.probe_unknown_instrument_classifications

Probe the current instrument classification layer to extract symbols that are
still unresolved / UNKNOWN.

Design goals:
- SQL-first
- no guessing
- fully auditable output
- easy to use for building manual JSON overrides

Outputs:
- logs/unknown_instrument_classifications.json
- logs/unknown_instrument_classifications_summary.json

- `run_probe_unknown_instrument_classifications() -> dict`

## src.stock_quant_data.jobs.publish_release

Publish an immutable serving release.

Published objects in this version:
- serving_release_metadata
- serving_release_checks
- instrument
- universe_definition
- universe_membership_history
- symbol_reference_history
- listing_status_history
- price_history

Critical rule:
publication is blocked if validation checks fail.

- `detect_git_commit(repo_root: Path) -> str | None`
- `read_table_rows(sql_text: str) -> list[tuple]`
- `table_exists() -> bool`
- `build_manifest(repo_root: Path, release_id: str, instrument_count: int, universe_count: int, membership_count: int, symbol_reference_count: int, listing_status_count: int, price_history_count: int, checks_passed: bool) -> dict`
- `create_serving_db(release_dir: Path, manifest: dict, checks_payload: dict, instrument_rows: list[tuple], universe_rows: list[tuple], membership_rows: list[tuple], symbol_reference_rows: list[tuple], listing_status_rows: list[tuple], price_history_rows: list[tuple]) -> Path`
- `run() -> None`

## src.stock_quant_data.jobs.remove_invalid_universe_overlap_demo

Remove the deliberate invalid overlap test row.

This restores the build DB to a publishable state after the negative test.

- `run() -> None`

## src.stock_quant_data.jobs.seed_instruments

Seed a small deterministic set of instruments into the build database.

These rows are only bootstrap data for the scientific platform scaffold.
They are not intended to be a full market loader.

- `run() -> None`

## src.stock_quant_data.jobs.seed_listing_status_history

Seed a deterministic minimal listing status history dataset.

This bootstrap data demonstrates:
- active listing lifecycle
- a renamed ticker / listing continuity example
- status history published separately from symbol reference history

- `run() -> None`

## src.stock_quant_data.jobs.seed_price_history

Seed a deterministic minimal daily price history dataset.

This bootstrap dataset is only for validating:
- published price history serving
- historical range queries
- latest price queries
- core OHLCV validation checks

- `run() -> None`

## src.stock_quant_data.jobs.seed_price_raw_demo

Seed deterministic raw Stooq and Yahoo price demo data.

This job simulates what a downloader would normally land into raw tables.
It is intentionally small but preserves the source split:
- Stooq raw
- Yahoo raw

- `run() -> None`

## src.stock_quant_data.jobs.seed_price_raw_yahoo_cutover_demo

Seed additional Yahoo raw rows beyond the current Stooq max date.

Purpose:
- prove that the canonical selection policy switches to Yahoo
  after the Stooq coverage horizon
- keep the test deterministic and easy to inspect

Current expectation:
- existing Stooq max date is 2024-06-28
- these rows extend Yahoo to 2024-07-01
- canonical price_history should therefore select Yahoo
  for dates after 2024-06-28

- `run() -> None`

## src.stock_quant_data.jobs.seed_symbol_reference_history

Seed a deterministic minimal symbol reference history.

This job bootstraps historical symbol identity mapping so the platform can:
- resolve a symbol to an instrument
- expose history for a symbol
- prepare for future as-of symbol resolution

- `run() -> None`

## src.stock_quant_data.jobs.seed_universe_membership_history

Seed a deterministic initial set of universe membership history rows.

This is bootstrap data to validate:
- historized universe membership
- as-of querying
- published serving snapshots

- `run() -> None`

## src.stock_quant_data.jobs.seed_universes

Seed initial logical universes into the mutable build database.

Design goals:
- keep seeding explicit and deterministic
- make the initial serving API useful immediately
- avoid hidden bootstrap data

- `run() -> None`

## src.stock_quant_data.jobs.stage_sec_companyfacts_json_from_downloader

Stage SEC companyfacts JSON from downloader zip into local staging directory.

Canonical staging destination:
- data/staging/sec/companyfacts/<zip_stem>/*.json

- `_latest_companyfacts_zip(downloader_root: Path) -> Path`
- `run() -> None`

## src.stock_quant_data.jobs.validate_release

Validate build-database scientific invariants before publication.

Current scope:
- universe_membership_history interval validity + overlap detection
- symbol_reference_history interval validity + overlap detection
- listing_status_history interval validity + overlap detection by instrument
- price_history uniqueness and OHLCV coherence checks

- `fetch_scalar(conn, sql_text: str)`
- `fetch_rows(conn, sql_text: str) -> list[tuple]`
- `table_exists(conn, table_name: str) -> bool`
- `build_checks_payload() -> dict`
- `write_checks_file(path: Path, payload: dict) -> None`
- `run() -> None`
