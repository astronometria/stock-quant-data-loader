# stock-quant-data-platform

Plateforme de données de marché orientée **scientifique / PIT / survivor-bias aware**.

## Objectif

Ce repo couvre uniquement la **partie 1** :

- ingestion de données
- normalisation canonique
- reconstruction historique PIT
- publication d'une release immuable de serving
- exposition lecture seule via API

Ce repo **ne contient pas** :

- recherche alpha
- features de recherche
- labels ML
- backtests
- portefeuille
- moteur de ranking

## Architecture

Deux plans physiques séparés :

1. **Build plane**
   - construit la vérité historique
   - valide les invariants scientifiques
   - prépare la publication

2. **Serve plane**
   - lit seulement une release publiée
   - lecture seule
   - stable et reproductible

## Répertoires runtime

- `data/build/market_build.duckdb` : base de travail
- `data/releases/<release_id>/serving.duckdb` : release publiée
- `data/current` : lien symbolique vers la release active

## Principes scientifiques

- snapshots d'univers historisés
- point-in-time
- pas de survivor bias
- publication immuable
- API lecture seule
- séparation stricte entre build et serving
