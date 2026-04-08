# DB inventory

DB path: `/home/marty/stock-quant-data-loader/data/build/market_build.duckdb`
Object count: **24**

## main.high_priority_unresolved_symbol_probe

- type: `BASE TABLE`
- row_count: `0`

### Columns

- `raw_symbol` — VARCHAR — nullable=YES
- `unresolved_row_count` — BIGINT — nullable=YES
- `min_price_date` — DATE — nullable=YES
- `max_price_date` — DATE — nullable=YES
- `candidate_family` — VARCHAR — nullable=YES
- `suggested_action` — VARCHAR — nullable=YES
- `recency_bucket` — VARCHAR — nullable=YES
- `exact_current_instrument_id` — BIGINT — nullable=YES
- `exact_current_symbol` — VARCHAR — nullable=YES
- `exact_current_exchange` — VARCHAR — nullable=YES
- `nearby_reference_matches` — VARCHAR — nullable=YES
- `in_latest_nasdaq_raw` — INTEGER — nullable=YES
- `nasdaq_exact_matches` — VARCHAR — nullable=YES
- `nasdaq_nearby_matches` — VARCHAR — nullable=YES
- `in_targeted_sec_symbols` — INTEGER — nullable=YES
- `sec_exact_matches` — VARCHAR — nullable=YES
- `probe_recommendation` — VARCHAR — nullable=YES
- `built_at` — TIMESTAMP WITH TIME ZONE — nullable=YES

## main.instrument

- type: `BASE TABLE`
- row_count: `12677`

### Columns

- `instrument_id` — BIGINT — nullable=NO
- `security_type` — VARCHAR — nullable=NO
- `company_id` — VARCHAR — nullable=NO
- `primary_ticker` — VARCHAR — nullable=NO
- `primary_exchange` — VARCHAR — nullable=NO

## main.listing_status_history

- type: `BASE TABLE`
- row_count: `17877`

### Columns

- `listing_status_history_id` — BIGINT — nullable=NO
- `instrument_id` — BIGINT — nullable=NO
- `symbol` — VARCHAR — nullable=NO
- `listing_status` — VARCHAR — nullable=NO
- `status_reason` — VARCHAR — nullable=YES
- `effective_from` — DATE — nullable=NO
- `effective_to` — DATE — nullable=YES
- `source_name` — VARCHAR — nullable=YES

## main.nasdaq_symbol_directory_raw

- type: `BASE TABLE`
- row_count: `24926`

### Columns

- `raw_id` — BIGINT — nullable=NO
- `snapshot_id` — VARCHAR — nullable=NO
- `source_kind` — VARCHAR — nullable=NO
- `symbol` — VARCHAR — nullable=YES
- `security_name` — VARCHAR — nullable=YES
- `exchange_code` — VARCHAR — nullable=YES
- `etf_flag` — VARCHAR — nullable=YES
- `test_issue_flag` — VARCHAR — nullable=YES
- `loaded_at` — TIMESTAMP — nullable=YES

## main.price_source_daily_normalized

- type: `BASE TABLE`
- row_count: `27397651`

### Columns

- `normalized_price_id` — BIGINT — nullable=NO
- `source_name` — VARCHAR — nullable=NO
- `source_row_id` — BIGINT — nullable=NO
- `raw_symbol` — VARCHAR — nullable=NO
- `instrument_id` — BIGINT — nullable=YES
- `price_date` — DATE — nullable=NO
- `open` — DOUBLE — nullable=NO
- `high` — DOUBLE — nullable=NO
- `low` — DOUBLE — nullable=NO
- `close` — DOUBLE — nullable=NO
- `adj_close` — DOUBLE — nullable=YES
- `volume` — BIGINT — nullable=NO
- `symbol_resolution_status` — VARCHAR — nullable=NO
- `normalization_notes` — VARCHAR — nullable=YES
- `normalized_at` — TIMESTAMP — nullable=NO

## main.price_source_daily_raw_stooq

- type: `BASE TABLE`
- row_count: `27397651`

### Columns

- `raw_price_id` — BIGINT — nullable=NO
- `raw_symbol` — VARCHAR — nullable=NO
- `price_date` — DATE — nullable=NO
- `open` — DOUBLE — nullable=NO
- `high` — DOUBLE — nullable=NO
- `low` — DOUBLE — nullable=NO
- `close` — DOUBLE — nullable=NO
- `volume` — BIGINT — nullable=NO
- `source_file_path` — VARCHAR — nullable=YES
- `loaded_at` — TIMESTAMP — nullable=YES

## main.price_source_daily_raw_yahoo

- type: `BASE TABLE`
- row_count: `0`

### Columns

- `raw_price_id` — BIGINT — nullable=NO
- `raw_symbol` — VARCHAR — nullable=NO
- `price_date` — DATE — nullable=NO
- `open` — DOUBLE — nullable=NO
- `high` — DOUBLE — nullable=NO
- `low` — DOUBLE — nullable=NO
- `close` — DOUBLE — nullable=NO
- `adj_close` — DOUBLE — nullable=YES
- `volume` — BIGINT — nullable=NO
- `source_batch_id` — VARCHAR — nullable=YES
- `loaded_at` — TIMESTAMP — nullable=YES

## main.release_metadata

- type: `BASE TABLE`
- row_count: `0`

### Columns

- `metadata_key` — VARCHAR — nullable=NO
- `metadata_value` — VARCHAR — nullable=YES
- `updated_at` — TIMESTAMP — nullable=YES

## main.schema_migrations

- type: `BASE TABLE`
- row_count: `0`

### Columns

- `migration_name` — VARCHAR — nullable=NO
- `applied_at` — TIMESTAMP — nullable=YES

## main.sec_companyfacts_raw

- type: `BASE TABLE`
- row_count: `121585445`

### Columns

- `raw_id` — BIGINT — nullable=YES
- `cik` — VARCHAR — nullable=YES
- `fact_namespace` — VARCHAR — nullable=YES
- `fact_name` — VARCHAR — nullable=YES
- `fact_value_double` — DOUBLE — nullable=YES
- `fact_value_text` — VARCHAR — nullable=YES
- `unit_name` — VARCHAR — nullable=YES
- `period_end` — DATE — nullable=YES
- `filing_date` — DATE — nullable=YES
- `accession_number` — VARCHAR — nullable=YES
- `source_zip_path` — VARCHAR — nullable=NO
- `json_member_name` — VARCHAR — nullable=NO
- `loaded_at` — TIMESTAMP — nullable=YES

## main.sec_submissions_company_raw

- type: `BASE TABLE`
- row_count: `966363`

### Columns

- `raw_id` — BIGINT — nullable=YES
- `cik` — VARCHAR — nullable=YES
- `entity_type` — VARCHAR — nullable=YES
- `sic` — VARCHAR — nullable=YES
- `sic_description` — VARCHAR — nullable=YES
- `name` — VARCHAR — nullable=YES
- `tickers_json` — VARCHAR — nullable=YES
- `exchanges_json` — VARCHAR — nullable=YES
- `ein` — VARCHAR — nullable=YES
- `description` — VARCHAR — nullable=YES
- `website` — VARCHAR — nullable=YES
- `investor_website` — VARCHAR — nullable=YES
- `fiscal_year_end` — VARCHAR — nullable=YES
- `source_zip_path` — VARCHAR — nullable=NO
- `json_member_name` — VARCHAR — nullable=NO
- `loaded_at` — TIMESTAMP — nullable=YES

## main.sec_submissions_company_raw_targeted

- type: `BASE TABLE`
- row_count: `0`

### Columns

- `raw_id` — BIGINT — nullable=YES
- `cik` — VARCHAR — nullable=YES
- `entity_type` — VARCHAR — nullable=YES
- `sic` — VARCHAR — nullable=YES
- `sic_description` — VARCHAR — nullable=YES
- `name` — VARCHAR — nullable=YES
- `tickers_json` — VARCHAR — nullable=YES
- `exchanges_json` — VARCHAR — nullable=YES
- `ein` — VARCHAR — nullable=YES
- `description` — VARCHAR — nullable=YES
- `website` — VARCHAR — nullable=YES
- `investor_website` — VARCHAR — nullable=YES
- `fiscal_year_end` — VARCHAR — nullable=YES
- `source_zip_path` — VARCHAR — nullable=NO
- `json_member_name` — VARCHAR — nullable=NO
- `loaded_at` — TIMESTAMP — nullable=YES

## main.sec_symbol_company_map

- type: `BASE TABLE`
- row_count: `10711`

### Columns

- `raw_id` — BIGINT — nullable=YES
- `cik` — VARCHAR — nullable=YES
- `symbol` — VARCHAR — nullable=YES
- `company_name` — VARCHAR — nullable=YES
- `exchange` — VARCHAR — nullable=YES
- `source_zip_path` — VARCHAR — nullable=NO
- `json_member_name` — VARCHAR — nullable=NO
- `loaded_at` — TIMESTAMP — nullable=YES

## main.sec_symbol_company_map_targeted

- type: `BASE TABLE`
- row_count: `0`

### Columns

- `raw_id` — BIGINT — nullable=YES
- `cik` — VARCHAR — nullable=YES
- `symbol` — VARCHAR — nullable=YES
- `company_name` — VARCHAR — nullable=YES
- `exchange` — VARCHAR — nullable=YES
- `source_zip_path` — VARCHAR — nullable=NO
- `json_member_name` — VARCHAR — nullable=NO
- `loaded_at` — TIMESTAMP — nullable=YES

## main.stooq_ingested_files

- type: `BASE TABLE`
- row_count: `11978`

### Columns

- `source_file_path` — VARCHAR — nullable=NO
- `file_size_bytes` — BIGINT — nullable=YES
- `loaded_at` — TIMESTAMP — nullable=YES

## main.stooq_symbol_normalization_map

- type: `BASE TABLE`
- row_count: `355`

### Columns

- `raw_symbol` — VARCHAR — nullable=YES
- `normalized_symbol` — VARCHAR — nullable=YES
- `rule_name` — VARCHAR — nullable=YES
- `built_at` — TIMESTAMP WITH TIME ZONE — nullable=YES

## main.symbol_manual_override_map

- type: `BASE TABLE`
- row_count: `54`

### Columns

- `raw_symbol` — VARCHAR — nullable=NO
- `mapped_symbol` — VARCHAR — nullable=NO
- `source_name` — VARCHAR — nullable=NO
- `mapping_rationale` — VARCHAR — nullable=YES
- `confidence_level` — VARCHAR — nullable=YES
- `built_at` — TIMESTAMP WITH TIME ZONE — nullable=YES

## main.symbol_reference_candidates_from_unresolved_stooq

- type: `BASE TABLE`
- row_count: `182`

### Columns

- `raw_symbol` — VARCHAR — nullable=YES
- `unresolved_row_count` — BIGINT — nullable=YES
- `min_price_date` — DATE — nullable=YES
- `max_price_date` — DATE — nullable=YES
- `first_source_row_id` — BIGINT — nullable=YES
- `last_source_row_id` — BIGINT — nullable=YES
- `candidate_family` — VARCHAR — nullable=YES
- `suggested_action` — VARCHAR — nullable=YES
- `recency_bucket` — VARCHAR — nullable=YES
- `normalization_notes_example` — VARCHAR — nullable=YES
- `built_at` — TIMESTAMP WITH TIME ZONE — nullable=YES

## main.symbol_reference_history

- type: `BASE TABLE`
- row_count: `17899`

### Columns

- `symbol_reference_history_id` — BIGINT — nullable=NO
- `instrument_id` — BIGINT — nullable=NO
- `symbol` — VARCHAR — nullable=NO
- `exchange` — VARCHAR — nullable=YES
- `is_primary` — BOOLEAN — nullable=NO
- `effective_from` — DATE — nullable=NO
- `effective_to` — DATE — nullable=YES

## main.universe_definition

- type: `BASE TABLE`
- row_count: `0`

### Columns

- `universe_name` — VARCHAR — nullable=NO
- `description` — VARCHAR — nullable=YES
- `created_at` — TIMESTAMP — nullable=YES

## main.universe_membership_history

- type: `BASE TABLE`
- row_count: `11159`

### Columns

- `universe_membership_history_id` — BIGINT — nullable=NO
- `universe_name` — VARCHAR — nullable=NO
- `instrument_id` — BIGINT — nullable=NO
- `effective_from` — DATE — nullable=NO
- `effective_to` — DATE — nullable=YES
- `source_name` — VARCHAR — nullable=YES

## main.unresolved_symbol_worklist

- type: `BASE TABLE`
- row_count: `154`

### Columns

- `raw_symbol` — VARCHAR — nullable=YES
- `unresolved_row_count` — BIGINT — nullable=YES
- `min_price_date` — DATE — nullable=YES
- `max_price_date` — DATE — nullable=YES
- `candidate_family` — VARCHAR — nullable=YES
- `suggested_action` — VARCHAR — nullable=YES
- `recency_bucket` — VARCHAR — nullable=YES
- `built_at` — TIMESTAMP WITH TIME ZONE — nullable=YES

## main.v_symbol_reference_history_open_intervals

- type: `VIEW`
- row_count: `12478`

### Columns

- `symbol_reference_history_id` — BIGINT — nullable=YES
- `instrument_id` — BIGINT — nullable=YES
- `symbol` — VARCHAR — nullable=YES
- `exchange` — VARCHAR — nullable=YES
- `is_primary` — BOOLEAN — nullable=YES
- `effective_from` — DATE — nullable=YES
- `effective_to` — DATE — nullable=YES

## main.v_universe_membership_history_open_intervals

- type: `VIEW`
- row_count: `11159`

### Columns

- `universe_membership_history_id` — BIGINT — nullable=YES
- `universe_name` — VARCHAR — nullable=YES
- `instrument_id` — BIGINT — nullable=YES
- `effective_from` — DATE — nullable=YES
- `effective_to` — DATE — nullable=YES
- `source_name` — VARCHAR — nullable=YES
