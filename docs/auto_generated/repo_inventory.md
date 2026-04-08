# Repo inventory

Modules scannés: **79**

## scripts.generate_db_inventory

- path: `scripts/generate_db_inventory.py`
- imports: **6**
- functions: **5**
- classes: **0**

### Functions

- `fetch_objects(conn: duckdb.DuckDBPyConnection) -> list[tuple[str, str, str]]`
- `fetch_columns(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[dict]`
- `safe_count(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> int | None`
- `render_markdown(report: dict) -> str`
- `main() -> None`

## scripts.generate_docs_bundle

- path: `scripts/generate_docs_bundle.py`
- imports: **4**
- functions: **8**
- classes: **0**

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

- path: `scripts/generate_repo_inventory.py`
- imports: **8**
- functions: **9**
- classes: **3**

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

- path: `scripts/rebuild_loader_db.py`
- imports: **8**
- functions: **5**
- classes: **0**

### Functions

- `_print_json(payload: object) -> None`
- `_load_run(module_name: str) -> Callable[[], None]`
- `_remove_existing_db_files() -> None`
- `_probe_required_tables() -> dict[str, dict[str, object]]`
- `main() -> None`

## src.stock_quant_data.__init__

- path: `src/stock_quant_data/__init__.py`
- imports: **0**
- functions: **0**
- classes: **0**

## src.stock_quant_data.cli.__init__

- path: `src/stock_quant_data/cli/__init__.py`
- imports: **0**
- functions: **0**
- classes: **0**

## src.stock_quant_data.cli.main

- path: `src/stock_quant_data/cli/main.py`
- imports: **24**
- functions: **1**
- classes: **0**

### Functions

- `main() -> None`

## src.stock_quant_data.config.__init__

- path: `src/stock_quant_data/config/__init__.py`
- imports: **0**
- functions: **0**
- classes: **0**

## src.stock_quant_data.config.logging

- path: `src/stock_quant_data/config/logging.py`
- imports: **3**
- functions: **1**
- classes: **0**

### Functions

- `configure_logging(level: int = logging.INFO) -> None`

## src.stock_quant_data.config.settings

- path: `src/stock_quant_data/config/settings.py`
- imports: **4**
- functions: **1**
- classes: **1**

### Functions

- `get_settings() -> Settings`

### Classes

- `Settings`
  - `model_post_init(self, __context: object) -> None`
  - `data_root(self) -> Path`
  - `ensure_directories(self) -> None`

## src.stock_quant_data.db.__init__

- path: `src/stock_quant_data/db/__init__.py`
- imports: **0**
- functions: **0**
- classes: **0**

## src.stock_quant_data.db.connections

- path: `src/stock_quant_data/db/connections.py`
- imports: **4**
- functions: **2**
- classes: **0**

### Functions

- `_ensure_parent_dir(db_path: Path) -> None`
- `connect_build_db(read_only: bool = False) -> duckdb.DuckDBPyConnection`

## src.stock_quant_data.db.engine

- path: `src/stock_quant_data/db/engine.py`
- imports: **4**
- functions: **3**
- classes: **0**

### Functions

- `read_sql_file(path: Path) -> str`
- `execute_sql_file(connection: duckdb.DuckDBPyConnection, path: Path) -> None`
- `execute_sql_files_in_order(connection: duckdb.DuckDBPyConnection, paths: Iterable[Path]) -> None`

## src.stock_quant_data.db.publish

- path: `src/stock_quant_data/db/publish.py`
- imports: **6**
- functions: **4**
- classes: **0**

### Functions

- `utc_release_id() -> str`
- `create_release_dir(release_id: str | None = None) -> Path`
- `write_manifest(release_dir: Path, payload: dict) -> Path`
- `switch_current_release_symlink(release_dir: Path) -> None`

## src.stock_quant_data.domains.listings.__init__

- path: `src/stock_quant_data/domains/listings/__init__.py`
- imports: **0**
- functions: **0**
- classes: **0**

## src.stock_quant_data.domains.listings.repository

- path: `src/stock_quant_data/domains/listings/repository.py`
- imports: **3**
- functions: **0**
- classes: **1**

### Classes

- `ListingsRepository`
  - `__init__(self, connection: duckdb.DuckDBPyConnection) -> None`
  - `get_listing_status_history(self, symbol: str) -> list[dict[str, Any]]`

## src.stock_quant_data.domains.prices.__init__

- path: `src/stock_quant_data/domains/prices/__init__.py`
- imports: **0**
- functions: **0**
- classes: **0**

## src.stock_quant_data.domains.prices.repository

- path: `src/stock_quant_data/domains/prices/repository.py`
- imports: **3**
- functions: **0**
- classes: **1**

### Classes

- `PricesRepository`
  - `__init__(self, connection: duckdb.DuckDBPyConnection) -> None`
  - `get_price_history(self, symbol: str, start_date: str, end_date: str) -> list[dict[str, Any]]`
  - `get_price_as_of(self, symbol: str, as_of_date: str) -> dict[str, Any] | None`

## src.stock_quant_data.domains.symbols.__init__

- path: `src/stock_quant_data/domains/symbols/__init__.py`
- imports: **0**
- functions: **0**
- classes: **0**

## src.stock_quant_data.domains.symbols.repository

- path: `src/stock_quant_data/domains/symbols/repository.py`
- imports: **3**
- functions: **0**
- classes: **1**

### Classes

- `SymbolsRepository`
  - `__init__(self, connection: duckdb.DuckDBPyConnection) -> None`
  - `get_symbol_history(self, symbol: str) -> list[dict[str, Any]]`
  - `get_symbol_as_of(self, symbol: str, as_of_date: str) -> dict[str, Any] | None`

## src.stock_quant_data.domains.universe.__init__

- path: `src/stock_quant_data/domains/universe/__init__.py`
- imports: **0**
- functions: **0**
- classes: **0**

## src.stock_quant_data.domains.universe.repository

- path: `src/stock_quant_data/domains/universe/repository.py`
- imports: **3**
- functions: **0**
- classes: **1**

### Classes

- `UniverseRepository`
  - `__init__(self, connection: duckdb.DuckDBPyConnection) -> None`
  - `list_universes(self) -> list[dict[str, Any]]`
  - `get_universe_members_as_of(self, universe_name: str, as_of_date: str) -> list[dict[str, Any]]`

## src.stock_quant_data.jobs.build_core_prices

- path: `src/stock_quant_data/jobs/build_core_prices.py`
- imports: **2**
- functions: **1**
- classes: **0**

### Functions

- `run_build_core_prices() -> dict`

## src.stock_quant_data.jobs.build_high_priority_unresolved_symbol_probe

- path: `src/stock_quant_data/jobs/build_high_priority_unresolved_symbol_probe.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_listing_status_history

- path: `src/stock_quant_data/jobs/build_listing_status_history.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_price_history_from_raw

- path: `src/stock_quant_data/jobs/build_price_history_from_raw.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_price_normalized_from_raw

- path: `src/stock_quant_data/jobs/build_price_normalized_from_raw.py`
- imports: **5**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_sec_companyfacts_parquet_from_staged_json

- path: `src/stock_quant_data/jobs/build_sec_companyfacts_parquet_from_staged_json.py`
- imports: **8**
- functions: **6**
- classes: **0**

### Functions

- `_latest_stage_dir(stage_root: Path) -> Path`
- `_chunked(items: list[Path], chunk_size: int) -> list[list[Path]]`
- `_quote_sql_string(value: str) -> str`
- `_build_batch_sql(batch_files: list[Path], parquet_output_path: Path) -> str`
- `_configure_connection_for_batch(temp_dir: Path)`
- `run() -> None`

## src.stock_quant_data.jobs.build_stooq_symbol_normalization_map

- path: `src/stock_quant_data/jobs/build_stooq_symbol_normalization_map.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_symbol_manual_override_map

- path: `src/stock_quant_data/jobs/build_symbol_manual_override_map.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_symbol_reference_candidates_from_unresolved_stooq

- path: `src/stock_quant_data/jobs/build_symbol_reference_candidates_from_unresolved_stooq.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_symbol_reference_from_nasdaq_latest

- path: `src/stock_quant_data/jobs/build_symbol_reference_from_nasdaq_latest.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_symbol_reference_history_from_nasdaq_snapshots

- path: `src/stock_quant_data/jobs/build_symbol_reference_history_from_nasdaq_snapshots.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_universe_membership_history_from_listing_status

- path: `src/stock_quant_data/jobs/build_universe_membership_history_from_listing_status.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_unresolved_symbol_worklist

- path: `src/stock_quant_data/jobs/build_unresolved_symbol_worklist.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.build_yfinance_download_contract

- path: `src/stock_quant_data/jobs/build_yfinance_download_contract.py`
- imports: **5**
- functions: **1**
- classes: **0**

### Functions

- `run() -> dict`

## src.stock_quant_data.jobs.check_master_data_invariants

- path: `src/stock_quant_data/jobs/check_master_data_invariants.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.enrich_stooq_symbol_normalization_map_from_probe

- path: `src/stock_quant_data/jobs/enrich_stooq_symbol_normalization_map_from_probe.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.enrich_symbol_reference_from_high_priority_sec_probe

- path: `src/stock_quant_data/jobs/enrich_symbol_reference_from_high_priority_sec_probe.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.enrich_symbol_reference_from_manual_overrides

- path: `src/stock_quant_data/jobs/enrich_symbol_reference_from_manual_overrides.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.enrich_symbol_reference_from_nasdaq_unresolved

- path: `src/stock_quant_data/jobs/enrich_symbol_reference_from_nasdaq_unresolved.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.enrich_symbol_reference_from_sec_general

- path: `src/stock_quant_data/jobs/enrich_symbol_reference_from_sec_general.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.enrich_symbol_reference_from_sec_targeted

- path: `src/stock_quant_data/jobs/enrich_symbol_reference_from_sec_targeted.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.ingest_raw_nasdaq_symbol_directory_dir

- path: `src/stock_quant_data/jobs/ingest_raw_nasdaq_symbol_directory_dir.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run_ingest_raw_nasdaq_symbol_directory_dir(root_dir: str) -> dict`

## src.stock_quant_data.jobs.ingest_raw_prices_csv

- path: `src/stock_quant_data/jobs/ingest_raw_prices_csv.py`
- imports: **2**
- functions: **1**
- classes: **0**

### Functions

- `run_ingest_raw_prices_csv(csv_path: str) -> dict`

## src.stock_quant_data.jobs.ingest_raw_prices_stooq_dir

- path: `src/stock_quant_data/jobs/ingest_raw_prices_stooq_dir.py`
- imports: **2**
- functions: **1**
- classes: **0**

### Functions

- `run_ingest_raw_prices_stooq_dir(root_dir: str) -> dict`

## src.stock_quant_data.jobs.ingest_raw_prices_yfinance_dir

- path: `src/stock_quant_data/jobs/ingest_raw_prices_yfinance_dir.py`
- imports: **2**
- functions: **1**
- classes: **0**

### Functions

- `run_ingest_raw_prices_yfinance_dir(root_dir: str) -> dict`

## src.stock_quant_data.jobs.init_db

- path: `src/stock_quant_data/jobs/init_db.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.init_price_raw_tables

- path: `src/stock_quant_data/jobs/init_price_raw_tables.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.insert_invalid_universe_overlap_demo

- path: `src/stock_quant_data/jobs/insert_invalid_universe_overlap_demo.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.load_nasdaq_symbol_directory_raw_from_downloader

- path: `src/stock_quant_data/jobs/load_nasdaq_symbol_directory_raw_from_downloader.py`
- imports: **10**
- functions: **6**
- classes: **0**

### Functions

- `_candidate_paths(root: Path) -> list[Path]`
- `_snapshot_id_from_name(path_name: str) -> str`
- `_iter_reader_rows(reader: csv.DictReader, desc: str)`
- `_txt_source_kind_from_name(file_name: str) -> str`
- `_parse_txt_row(row: dict, source_kind: str) -> tuple[str | None, str | None, str | None, str | None]`
- `run() -> None`

## src.stock_quant_data.jobs.load_price_source_daily_raw_stooq_from_disk

- path: `src/stock_quant_data/jobs/load_price_source_daily_raw_stooq_from_disk.py`
- imports: **8**
- functions: **11**
- classes: **0**

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

- path: `src/stock_quant_data/jobs/load_sec_companyfacts_raw_from_downloader.py`
- imports: **5**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.load_sec_companyfacts_raw_from_parquet

- path: `src/stock_quant_data/jobs/load_sec_companyfacts_raw_from_parquet.py`
- imports: **7**
- functions: **3**
- classes: **0**

### Functions

- `_latest_parquet_dir(parquet_root: Path) -> Path`
- `_load_sql_template(repo_root: Path) -> str`
- `run() -> None`

## src.stock_quant_data.jobs.load_sec_companyfacts_raw_from_staged_json

- path: `src/stock_quant_data/jobs/load_sec_companyfacts_raw_from_staged_json.py`
- imports: **7**
- functions: **5**
- classes: **0**

### Functions

- `_latest_stage_dir(stage_root: Path) -> Path`
- `_chunked(items: list[Path], chunk_size: int) -> list[list[Path]]`
- `_quote_sql_string(value: str) -> str`
- `_build_batch_sql(batch_files: list[Path]) -> str`
- `run() -> None`

## src.stock_quant_data.jobs.load_sec_submissions_identity_from_downloader

- path: `src/stock_quant_data/jobs/load_sec_submissions_identity_from_downloader.py`
- imports: **9**
- functions: **2**
- classes: **0**

### Functions

- `_latest_zip_path(root: Path) -> Path`
- `run() -> None`

## src.stock_quant_data.jobs.load_sec_submissions_identity_targeted

- path: `src/stock_quant_data/jobs/load_sec_submissions_identity_targeted.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.probe_unknown_instrument_classifications

- path: `src/stock_quant_data/jobs/probe_unknown_instrument_classifications.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run_probe_unknown_instrument_classifications() -> dict`

## src.stock_quant_data.jobs.publish_release

- path: `src/stock_quant_data/jobs/publish_release.py`
- imports: **12**
- functions: **6**
- classes: **0**

### Functions

- `detect_git_commit(repo_root: Path) -> str | None`
- `read_table_rows(sql_text: str) -> list[tuple]`
- `table_exists() -> bool`
- `build_manifest(repo_root: Path, release_id: str, instrument_count: int, universe_count: int, membership_count: int, symbol_reference_count: int, listing_status_count: int, price_history_count: int, checks_passed: bool) -> dict`
- `create_serving_db(release_dir: Path, manifest: dict, checks_payload: dict, instrument_rows: list[tuple], universe_rows: list[tuple], membership_rows: list[tuple], symbol_reference_rows: list[tuple], listing_status_rows: list[tuple], price_history_rows: list[tuple]) -> Path`
- `run() -> None`

## src.stock_quant_data.jobs.remove_invalid_universe_overlap_demo

- path: `src/stock_quant_data/jobs/remove_invalid_universe_overlap_demo.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.seed_instruments

- path: `src/stock_quant_data/jobs/seed_instruments.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.seed_listing_status_history

- path: `src/stock_quant_data/jobs/seed_listing_status_history.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.seed_price_history

- path: `src/stock_quant_data/jobs/seed_price_history.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.seed_price_raw_demo

- path: `src/stock_quant_data/jobs/seed_price_raw_demo.py`
- imports: **5**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.seed_price_raw_yahoo_cutover_demo

- path: `src/stock_quant_data/jobs/seed_price_raw_yahoo_cutover_demo.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.seed_symbol_reference_history

- path: `src/stock_quant_data/jobs/seed_symbol_reference_history.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.seed_universe_membership_history

- path: `src/stock_quant_data/jobs/seed_universe_membership_history.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.seed_universes

- path: `src/stock_quant_data/jobs/seed_universes.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `run() -> None`

## src.stock_quant_data.jobs.stage_sec_companyfacts_json_from_downloader

- path: `src/stock_quant_data/jobs/stage_sec_companyfacts_json_from_downloader.py`
- imports: **7**
- functions: **2**
- classes: **0**

### Functions

- `_latest_companyfacts_zip(downloader_root: Path) -> Path`
- `run() -> None`

## src.stock_quant_data.jobs.validate_release

- path: `src/stock_quant_data/jobs/validate_release.py`
- imports: **6**
- functions: **6**
- classes: **0**

### Functions

- `fetch_scalar(conn, sql_text: str)`
- `fetch_rows(conn, sql_text: str) -> list[tuple]`
- `table_exists(conn, table_name: str) -> bool`
- `build_checks_payload() -> dict`
- `write_checks_file(path: Path, payload: dict) -> None`
- `run() -> None`

## src.stock_quant_data.services.contracts.__init__

- path: `src/stock_quant_data/services/contracts/__init__.py`
- imports: **0**
- functions: **0**
- classes: **0**

## src.stock_quant_data.services.contracts.yfinance_contract_builder_service

- path: `src/stock_quant_data/services/contracts/yfinance_contract_builder_service.py`
- imports: **4**
- functions: **4**
- classes: **0**

### Functions

- `_repo_root() -> Path`
- `_contracts_dir() -> Path`
- `_normalize_symbol_for_yahoo(symbol: str) -> tuple[str, bool, str]`
- `build_yfinance_download_contract() -> dict`

## src.stock_quant_data.services.ingest.__init__

- path: `src/stock_quant_data/services/ingest/__init__.py`
- imports: **0**
- functions: **0**
- classes: **0**

## src.stock_quant_data.services.ingest.raw_nasdaq_symbol_directory_dir_ingest_service

- path: `src/stock_quant_data/services/ingest/raw_nasdaq_symbol_directory_dir_ingest_service.py`
- imports: **6**
- functions: **1**
- classes: **0**

### Functions

- `ingest_raw_nasdaq_symbol_directory_dir(root_dir: str) -> dict`

## src.stock_quant_data.services.ingest.raw_prices_csv_ingest_service

- path: `src/stock_quant_data/services/ingest/raw_prices_csv_ingest_service.py`
- imports: **4**
- functions: **1**
- classes: **0**

### Functions

- `ingest_raw_prices_csv(csv_path: str) -> dict`

## src.stock_quant_data.services.ingest.raw_prices_stooq_dir_ingest_service

- path: `src/stock_quant_data/services/ingest/raw_prices_stooq_dir_ingest_service.py`
- imports: **5**
- functions: **3**
- classes: **0**

### Functions

- `_discover_stooq_subdirs(root_dir: Path) -> list[Path]`
- `_count_txt_files_recursive(path: Path) -> int`
- `ingest_raw_prices_stooq_dir(root_dir: str) -> dict`

## src.stock_quant_data.services.ingest.raw_prices_yfinance_dir_ingest_service

- path: `src/stock_quant_data/services/ingest/raw_prices_yfinance_dir_ingest_service.py`
- imports: **5**
- functions: **3**
- classes: **0**

### Functions

- `_discover_symbol_dirs(root_dir: Path) -> list[Path]`
- `_count_csv_files_recursive(path: Path) -> int`
- `ingest_raw_prices_yfinance_dir(root_dir: str) -> dict`

## src.stock_quant_data.services.normalize.__init__

- path: `src/stock_quant_data/services/normalize/__init__.py`
- imports: **0**
- functions: **0**
- classes: **0**

## src.stock_quant_data.services.normalize.core_prices_builder_service

- path: `src/stock_quant_data/services/normalize/core_prices_builder_service.py`
- imports: **3**
- functions: **1**
- classes: **0**

### Functions

- `build_core_prices_from_raw() -> dict`
