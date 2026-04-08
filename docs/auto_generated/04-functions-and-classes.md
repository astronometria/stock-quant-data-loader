# Functions and classes

## scripts.generate_db_inventory

### Functions

- `fetch_objects(conn: duckdb.DuckDBPyConnection) -> list[tuple[str, str, str]]`
- `fetch_columns(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[dict]`
- `safe_count(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> int | None`
- `render_markdown(report: dict) -> str`
- `main() -> None`

## scripts.generate_docs_bundle

### Functions

- `write_text(path: Path, content: str) -> None`
- `build_overview(repo_report: dict, db_report: dict) -> str`
- `build_repo_inventory_md(repo_report: dict) -> str`
- `build_jobs_catalog_md(repo_report: dict) -> str`
- `build_functions_classes_md(repo_report: dict) -> str`
- `build_db_inventory_md(db_report: dict) -> str`
- `build_db_tables_md(db_report: dict) -> str`
- `main() -> None`

## scripts.generate_repo_inventory

### Functions

- `safe_unparse(node: ast.AST | None) -> str`
- `format_function_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str`
- `extract_imports(tree: ast.AST) -> list[str]`
- `extract_functions(nodes: list[ast.stmt]) -> list[FunctionInfo]`
- `extract_classes(nodes: list[ast.stmt]) -> list[ClassInfo]`
- `file_to_module(repo_root: Path, path: Path) -> str`
- `scan_python_file(repo_root: Path, path: Path) -> ModuleInfo | None`
- `render_markdown(modules: list[dict[str, Any]]) -> str`
- `main() -> None`

### Classes

- `FunctionInfo`
- `ClassInfo`
- `ModuleInfo`

## scripts.rebuild_loader_db

### Functions

- `_print_json(payload: object) -> None`
- `_load_run(module_name: str) -> Callable[[], None]`
- `_remove_existing_db_files() -> None`
- `_probe_required_tables() -> dict[str, dict[str, object]]`
- `main() -> None`

## src.stock_quant_data.cli.main

### Functions

- `main() -> None`

## src.stock_quant_data.config.logging

### Functions

- `configure_logging(level: int = logging.INFO) -> None`

## src.stock_quant_data.config.settings

### Functions

- `get_settings() -> Settings`

### Classes

- `Settings`
  - `model_post_init(self, __context: object) -> None`
  - `data_root(self) -> Path`
  - `ensure_directories(self) -> None`

## src.stock_quant_data.db.connections

### Functions

- `_ensure_parent_dir(db_path: Path) -> None`
- `connect_build_db(read_only: bool = False) -> duckdb.DuckDBPyConnection`

## src.stock_quant_data.db.engine

### Functions

- `read_sql_file(path: Path) -> str`
- `execute_sql_file(connection: duckdb.DuckDBPyConnection, path: Path) -> None`
- `execute_sql_files_in_order(connection: duckdb.DuckDBPyConnection, paths: Iterable[Path]) -> None`

## src.stock_quant_data.db.publish

### Functions

- `utc_release_id() -> str`
- `create_release_dir(release_id: str | None = None) -> Path`
- `write_manifest(release_dir: Path, payload: dict) -> Path`
- `switch_current_release_symlink(release_dir: Path) -> None`

## src.stock_quant_data.domains.listings.repository

### Classes

- `ListingsRepository`
  - `__init__(self, connection: duckdb.DuckDBPyConnection) -> None`
  - `get_listing_status_history(self, symbol: str) -> list[dict[str, Any]]`

## src.stock_quant_data.domains.prices.repository

### Classes

- `PricesRepository`
  - `__init__(self, connection: duckdb.DuckDBPyConnection) -> None`
  - `get_price_history(self, symbol: str, start_date: str, end_date: str) -> list[dict[str, Any]]`
  - `get_price_as_of(self, symbol: str, as_of_date: str) -> dict[str, Any] | None`

## src.stock_quant_data.domains.symbols.repository

### Classes

- `SymbolsRepository`
  - `__init__(self, connection: duckdb.DuckDBPyConnection) -> None`
  - `get_symbol_history(self, symbol: str) -> list[dict[str, Any]]`
  - `get_symbol_as_of(self, symbol: str, as_of_date: str) -> dict[str, Any] | None`

## src.stock_quant_data.domains.universe.repository

### Classes

- `UniverseRepository`
  - `__init__(self, connection: duckdb.DuckDBPyConnection) -> None`
  - `list_universes(self) -> list[dict[str, Any]]`
  - `get_universe_members_as_of(self, universe_name: str, as_of_date: str) -> list[dict[str, Any]]`

## src.stock_quant_data.jobs.build_core_prices

### Functions

- `run_build_core_prices() -> dict`

## src.stock_quant_data.jobs.build_high_priority_unresolved_symbol_probe

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_listing_status_history

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_price_history_from_raw

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_price_normalized_from_raw

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_sec_companyfacts_parquet_from_staged_json

### Functions

- `_latest_stage_dir(stage_root: Path) -> Path`
- `_chunked(items: list[Path], chunk_size: int) -> list[list[Path]]`
- `_quote_sql_string(value: str) -> str`
- `_build_batch_sql(batch_files: list[Path], parquet_output_path: Path) -> str`
- `_configure_connection_for_batch(temp_dir: Path)`
- `run() -> None`

## src.stock_quant_data.jobs.build_stooq_symbol_normalization_map

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_symbol_manual_override_map

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_symbol_reference_candidates_from_unresolved_stooq

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_symbol_reference_from_nasdaq_latest

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_symbol_reference_history_from_nasdaq_snapshots

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_universe_membership_history_from_listing_status

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_unresolved_symbol_worklist

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_yfinance_download_contract

### Functions

- `run() -> dict`

## src.stock_quant_data.jobs.check_master_data_invariants

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.enrich_stooq_symbol_normalization_map_from_probe

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.enrich_symbol_reference_from_high_priority_sec_probe

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.enrich_symbol_reference_from_manual_overrides

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.enrich_symbol_reference_from_nasdaq_unresolved

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.enrich_symbol_reference_from_sec_general

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.enrich_symbol_reference_from_sec_targeted

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.ingest_raw_nasdaq_symbol_directory_dir

### Functions

- `run_ingest_raw_nasdaq_symbol_directory_dir(root_dir: str) -> dict`

## src.stock_quant_data.jobs.ingest_raw_prices_csv

### Functions

- `run_ingest_raw_prices_csv(csv_path: str) -> dict`

## src.stock_quant_data.jobs.ingest_raw_prices_stooq_dir

### Functions

- `run_ingest_raw_prices_stooq_dir(root_dir: str) -> dict`

## src.stock_quant_data.jobs.ingest_raw_prices_yfinance_dir

### Functions

- `run_ingest_raw_prices_yfinance_dir(root_dir: str) -> dict`

## src.stock_quant_data.jobs.init_db

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.init_price_raw_tables

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.insert_invalid_universe_overlap_demo

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.load_nasdaq_symbol_directory_raw_from_downloader

### Functions

- `_candidate_paths(root: Path) -> list[Path]`
- `_snapshot_id_from_name(path_name: str) -> str`
- `_iter_reader_rows(reader: csv.DictReader, desc: str)`
- `_txt_source_kind_from_name(file_name: str) -> str`
- `_parse_txt_row(row: dict, source_kind: str) -> tuple[str | None, str | None, str | None, str | None]`
- `run() -> None`

## src.stock_quant_data.jobs.load_price_source_daily_raw_stooq_from_disk

### Functions

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

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.load_sec_companyfacts_raw_from_parquet

### Functions

- `_latest_parquet_dir(parquet_root: Path) -> Path`
- `_load_sql_template(repo_root: Path) -> str`
- `run() -> None`

## src.stock_quant_data.jobs.load_sec_companyfacts_raw_from_staged_json

### Functions

- `_latest_stage_dir(stage_root: Path) -> Path`
- `_chunked(items: list[Path], chunk_size: int) -> list[list[Path]]`
- `_quote_sql_string(value: str) -> str`
- `_build_batch_sql(batch_files: list[Path]) -> str`
- `run() -> None`

## src.stock_quant_data.jobs.load_sec_submissions_identity_from_downloader

### Functions

- `_latest_zip_path(root: Path) -> Path`
- `run() -> None`

## src.stock_quant_data.jobs.load_sec_submissions_identity_targeted

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.probe_unknown_instrument_classifications

### Functions

- `run_probe_unknown_instrument_classifications() -> dict`

## src.stock_quant_data.jobs.publish_release

### Functions

- `detect_git_commit(repo_root: Path) -> str | None`
- `read_table_rows(sql_text: str) -> list[tuple]`
- `table_exists() -> bool`
- `build_manifest(repo_root: Path, release_id: str, instrument_count: int, universe_count: int, membership_count: int, symbol_reference_count: int, listing_status_count: int, price_history_count: int, checks_passed: bool) -> dict`
- `create_serving_db(release_dir: Path, manifest: dict, checks_payload: dict, instrument_rows: list[tuple], universe_rows: list[tuple], membership_rows: list[tuple], symbol_reference_rows: list[tuple], listing_status_rows: list[tuple], price_history_rows: list[tuple]) -> Path`
- `run() -> None`

## src.stock_quant_data.jobs.remove_invalid_universe_overlap_demo

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.seed_instruments

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.seed_listing_status_history

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.seed_price_history

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.seed_price_raw_demo

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.seed_price_raw_yahoo_cutover_demo

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.seed_symbol_reference_history

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.seed_universe_membership_history

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.seed_universes

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.stage_sec_companyfacts_json_from_downloader

### Functions

- `_latest_companyfacts_zip(downloader_root: Path) -> Path`
- `run() -> None`

## src.stock_quant_data.jobs.validate_release

### Functions

- `fetch_scalar(conn, sql_text: str)`
- `fetch_rows(conn, sql_text: str) -> list[tuple]`
- `table_exists(conn, table_name: str) -> bool`
- `build_checks_payload() -> dict`
- `write_checks_file(path: Path, payload: dict) -> None`
- `run() -> None`

## src.stock_quant_data.services.contracts.yfinance_contract_builder_service

### Functions

- `_repo_root() -> Path`
- `_contracts_dir() -> Path`
- `_normalize_symbol_for_yahoo(symbol: str) -> tuple[str, bool, str]`
- `build_yfinance_download_contract() -> dict`

## src.stock_quant_data.services.ingest.raw_nasdaq_symbol_directory_dir_ingest_service

### Functions

- `ingest_raw_nasdaq_symbol_directory_dir(root_dir: str) -> dict`

## src.stock_quant_data.services.ingest.raw_prices_csv_ingest_service

### Functions

- `ingest_raw_prices_csv(csv_path: str) -> dict`

## src.stock_quant_data.services.ingest.raw_prices_stooq_dir_ingest_service

### Functions

- `_discover_stooq_subdirs(root_dir: Path) -> list[Path]`
- `_count_txt_files_recursive(path: Path) -> int`
- `ingest_raw_prices_stooq_dir(root_dir: str) -> dict`

## src.stock_quant_data.services.ingest.raw_prices_yfinance_dir_ingest_service

### Functions

- `_discover_symbol_dirs(root_dir: Path) -> list[Path]`
- `_count_csv_files_recursive(path: Path) -> int`
- `ingest_raw_prices_yfinance_dir(root_dir: str) -> dict`

## src.stock_quant_data.services.normalize.core_prices_builder_service

### Functions

- `build_core_prices_from_raw() -> dict`
