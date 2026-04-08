# 06 — Contrats des tables critiques

## 1. `instrument`

Contrat :
- un `instrument_id` par identité instrument
- décrit le type de sécurité et le ticker principal

Colonnes observées :
- `instrument_id`
- `security_type`
- `company_id`
- `primary_ticker`
- `primary_exchange`

### Invariants pratiques
- `instrument_id` non nul
- `primary_ticker` cohérent avec la couche symbolique principale

---

## 2. `symbol_reference_history`

Contrat :
- historique canonique des symboles par instrument
- supporte l’intervalle PIT

Champs attendus :
- `symbol_reference_history_id`
- `instrument_id`
- `symbol`
- `effective_from`
- `effective_to`

### Invariants pratiques
- un intervalle ouvert max par symbole / instrument si la logique le prévoit
- pas d’overlap illégitime
- base principale pour `listing_status_history`

---

## 3. `price_source_daily_normalized`

Contrat :
- table prix journalière normalisée côté loader

Champs fonctionnels observés :
- `instrument_id`
- `price_date`
- `symbol_resolution_status`

### Invariants pratiques
- `instrument_id` non nul pour les lignes `RESOLVED`
- pas de duplication anormale par ligne source
- statut de résolution explicite

---

## 4. `listing_status_history`

Contrat :
- couche statut PIT dérivée de l’identité

Colonnes confirmées :
- `listing_status_history_id`
- `instrument_id`
- `symbol`
- `listing_status`
- `status_reason`
- `effective_from`
- `effective_to`
- `source_name`

### Invariants pratiques
- `ACTIVE` / `INACTIVE` seulement
- `closed_symbol_reference_interval` doit être `INACTIVE`
- les refs ouvertes non confirmées par snapshot restent **actives** dans l’état actuel

---

## 5. `universe_membership_history`

Contrat :
- appartenance PIT aux univers publiables

Colonnes confirmées :
- `universe_membership_history_id`
- `universe_name`
- `instrument_id`
- `effective_from`
- `effective_to`
- `source_name`

### Invariants pratiques
- les univers dérivent de `listing_status_history`
- pas de duplication ouverte par `universe_name + instrument_id`
