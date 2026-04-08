# 08 — Checklists de validation

## Après `build_price_normalized_from_raw`

Vérifier :
- count total
- count `RESOLVED`
- count `UNRESOLVED`
- duplications de `source_row_id`
- présence d’`instrument_id` pour les résolus

Exemple :

```sql
SELECT symbol_resolution_status, COUNT(*)
FROM price_source_daily_normalized
GROUP BY 1
ORDER BY 1;
```

## Après `build_listing_status_history`

Vérifier :
- total rows
- répartition `ACTIVE` / `INACTIVE`
- répartition des `status_reason`
- volume `open_symbol_reference_not_confirmed_by_latest_complete_nasdaq_snapshot_day`

## Après `build_universe_membership_history_from_listing_status`

Vérifier :
- volumes par univers
- exclusions attendues côté common stocks
- absence de doublons ouverts

## Avant publication

Vérifier :
- cohérence des compteurs
- absence d’erreur sur les tables critiques
- samples plausibles de common stocks et ETFs
- contrôle rapide du résiduel reviewable
