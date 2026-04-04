# stock-quant-data-loader

Repo de **construction** de la base de données marché.

Ce repo est responsable de :

- initialiser la build DB DuckDB
- charger les tables raw locales produites par le downloader
- construire les tables de référence (`instrument`, `symbol_reference_history`, `listing_status_history`)
- construire les tables de normalisation prix
- produire les tables de triage des symboles non résolus
- construire la table canonique `price_history`

Ce repo n'est **pas** responsable de :

- télécharger les fichiers source depuis internet
- exposer une API HTTP
- exécuter des backtests, labels ML, features de recherche ou portefeuille

## Tables principales côté build

### Raw
- `nasdaq_symbol_directory_raw`
- `sec_companyfacts_raw`
- `sec_submissions_company_raw`
- `price_source_daily_raw_stooq`
- `price_source_daily_raw_yahoo`

### Référence / master data
- `instrument`
- `symbol_reference_history`
- `listing_status_history`
- `symbol_manual_override_map`
- `stooq_symbol_normalization_map`

### Triage / résolution
- `price_source_daily_normalized`
- `symbol_reference_candidates_from_unresolved_stooq`
- `unresolved_symbol_worklist`
- `high_priority_unresolved_symbol_probe`

### Canonique
- `price_history`

## Runtime local
- build DB : `data/build/market_build.duckdb`

## Principes
- SQL-first
- point-in-time
- pas de survivor bias
- invariants explicites après les étapes critiques
- séparation stricte entre downloader, loader et api
