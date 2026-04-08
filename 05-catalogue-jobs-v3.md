# 05 — Catalogue des jobs v3

## A. Fondation DB

### `init_db`
Rôle :
- créer la build DB
- poser DDL / vues / structures de base

Entrées :
- SQL du repo

Sorties :
- build DB initialisée

---

## B. Ingestion Nasdaq

### `load_nasdaq_symbol_directory_raw_from_downloader`
Rôle :
- charger les snapshots Nasdaq raw

Entrées :
- fichiers du downloader

Sorties :
- `nasdaq_symbol_directory_raw`

### `build_symbol_reference_history_from_nasdaq_snapshots`
Rôle :
- construire la base historique de symboles

Entrées :
- `nasdaq_symbol_directory_raw`

Sorties :
- `symbol_reference_history`

### `enrich_symbol_reference_from_nasdaq_unresolved`
Rôle :
- résoudre le résiduel unresolved repéré après normalisation prix

Entrées :
- unresolved worklist
- `nasdaq_symbol_directory_raw`

Sorties :
- enrichissement `symbol_reference_history`

---

## C. Ingestion SEC

### `stage_sec_companyfacts_json_from_downloader`
### `load_sec_companyfacts_raw_from_staged_json`
### `load_sec_submissions_identity_from_downloader`
### `enrich_symbol_reference_from_sec_general`

Rôle commun :
- alimenter et enrichir la couche identité via SEC

Effet observé :
- ajout de milliers de symboles / mappings
- réduction forte du unresolved prix

---

## D. Prix

### `load_price_source_daily_raw_stooq_from_disk`
Rôle :
- charger le raw Stooq

### `build_price_normalized_from_raw`
Rôle :
- construire la table prix normalisée
- affecter les `instrument_id`
- marquer `RESOLVED` / `UNRESOLVED`

Effet observé :
- table finale = **27397651**
- unresolved fortement réduit après enrichissements

---

## E. Triage qualité

### `build_symbol_reference_candidates_from_unresolved_stooq`
Rôle :
- fabriquer des candidats de mapping

### `build_unresolved_symbol_worklist`
Rôle :
- fabriquer une worklist priorisée

---

## F. Serving interne / PIT

### `build_listing_status_history`
Rôle :
- transformer la couche identité en statut PIT

### `build_universe_membership_history_from_listing_status`
Rôle :
- dériver les univers PIT

### `validate_release`
Rôle :
- vérifier les invariants de cohérence

### `publish_release`
Rôle :
- publier la release servable

## Règle de lecture

La plupart des jobs sont mieux compris comme :
- **SQL-first**
- **state transition jobs**
- **une table cible par job**
