# 07 — Matrice jobs / dépendances

| Job | Entrées principales | Sorties principales | Dépend de |
|---|---|---|---|
| init_db | SQL repo | schémas / tables | aucune |
| load_nasdaq_symbol_directory_raw_from_downloader | snapshots downloader | nasdaq_symbol_directory_raw | init_db |
| load_sec_submissions_identity_from_downloader | SEC downloader | tables SEC identité | init_db |
| stage_sec_companyfacts_json_from_downloader | companyfacts downloader | staging SEC | init_db |
| load_sec_companyfacts_raw_from_staged_json | staging SEC | raw SEC | stage companyfacts |
| load_price_source_daily_raw_stooq_from_disk | fichiers Stooq | raw prix | init_db |
| build_symbol_reference_history_from_nasdaq_snapshots | nasdaq_symbol_directory_raw | symbol_reference_history | Nasdaq raw |
| enrich_symbol_reference_from_sec_general | symbol refs + SEC | symbol refs enrichi | symbol refs, SEC |
| build_price_normalized_from_raw | raw prix + symbol refs | price_source_daily_normalized | raw prix, symbol refs |
| build_symbol_reference_candidates_from_unresolved_stooq | normalized price | candidates | normalized price |
| build_unresolved_symbol_worklist | candidates | worklist | candidates |
| enrich_symbol_reference_from_nasdaq_unresolved | worklist + Nasdaq raw | symbol refs enrichi | worklist, Nasdaq raw |
| build_listing_status_history | symbol refs + Nasdaq raw + normalized price | listing_status_history | symbol refs, normalized price |
| build_universe_membership_history_from_listing_status | listing status + instrument | universe_membership_history | listing status |
| validate_release | build DB | checks | pipeline complet |
| publish_release | build DB validée | release servable | validate_release |

## Point important

Le vrai contrat d’orchestration n’est pas un graphe d’objets Python.  
C’est surtout une **chaîne de tables DuckDB**.
