# Référence des jobs par commande

## Objectif

Ce document sert de pense-bête orienté opérateur pour lancer les jobs les plus utiles sans devoir relire tout le repo.

## Forme canonique

```bash
python3 -m stock_quant_data.jobs.NOM_DU_JOB
```

## Jobs clés

### Normalisation prix

```bash
python3 -m stock_quant_data.jobs.build_price_normalized_from_raw
```

### Listing status

```bash
python3 -m stock_quant_data.jobs.build_listing_status_history
```

### Univers depuis listing

```bash
python3 -m stock_quant_data.jobs.build_universe_membership_history_from_listing_status
```

### Candidats unresolved depuis Stooq

```bash
python3 -m stock_quant_data.jobs.build_symbol_reference_candidates_from_unresolved_stooq
```

### Worklist unresolved

```bash
python3 -m stock_quant_data.jobs.build_unresolved_symbol_worklist
```

### Enrichissement Nasdaq unresolved

```bash
python3 -m stock_quant_data.jobs.enrich_symbol_reference_from_nasdaq_unresolved
```

## Séquence recommandée après correction identité/listing

1. relancer le job ciblé amont
2. relancer `build_price_normalized_from_raw` si le mapping instrument a changé
3. relancer `build_listing_status_history`
4. relancer `build_universe_membership_history_from_listing_status`
5. faire les probes DB

## Reconstruction complète

```bash
python3 scripts/rebuild_loader_db.py
```

## Astuce

Quand plusieurs couches sont touchées, ne pas bricoler une séquence ad hoc: revenir au runbook canonique.
