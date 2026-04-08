# 02 — Pipeline opératoire

## Pipeline recommandé

```mermaid
flowchart TD
    A[init_db] --> B[load_nasdaq_symbol_directory_raw_from_downloader]
    B --> C[stage_sec_companyfacts_json_from_downloader]
    C --> D[load_sec_companyfacts_raw_from_staged_json]
    D --> E[load_sec_submissions_identity_from_downloader]
    E --> F[load_price_source_daily_raw_stooq_from_disk]
    F --> G[build_symbol_manual_override_map]
    G --> H[build_symbol_reference_history_from_nasdaq_snapshots]
    H --> I[enrich_symbol_reference_from_sec_general]
    I --> J[build_price_normalized_from_raw]
    J --> K[build_symbol_reference_candidates_from_unresolved_stooq]
    K --> L[build_unresolved_symbol_worklist]
    L --> M[enrich_symbol_reference_from_nasdaq_unresolved]
    M --> N[build_price_normalized_from_raw]
    N --> O[build_listing_status_history]
    O --> P[build_universe_membership_history_from_listing_status]
    P --> Q[validate_release]
    Q --> R[publish_release]
```

## Passage pratique

L’état actuel observé montre qu’on peut raisonnablement enchaîner :
- identité
- prix normalisés
- listing status
- univers
- validation release
- publication
