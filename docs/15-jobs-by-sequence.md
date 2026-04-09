# Jobs par séquence

## Objectif

Ce document regroupe les jobs du repo par grandes séquences logiques, pour éviter de lire seulement une liste plate de scripts.

## Vue d'ensemble

Le pipeline loader se lit mieux en couches successives :

1. initialisation DB
2. chargement des sources brutes
3. normalisation / résolution
4. enrichissement identité
5. construction des statuts de listing
6. construction des univers
7. validation / probes

## 1. Initialisation DB

Exemples de jobs / points d'entrée :

- `init_db`
- scripts de création de schéma
- bootstrap de la build DB

But :

- créer les structures DuckDB nécessaires
- préparer la base pour les étapes suivantes

## 2. Chargement des sources brutes

Exemples typiques :

- loaders Stooq
- loaders Yahoo
- loaders Nasdaq symbol directory
- loaders SEC

But :

- charger dans la build DB ou les tables staging les données brutes déjà téléchargées

## 3. Normalisation / résolution

Exemples typiques :

- `build_price_normalized_from_raw`
- constructions autour de `symbol_reference_history`
- enrichissements de résolution de symboles

But :

- rattacher les observations de prix ou de symboles à des `instrument_id`
- construire une base de travail plus stable pour les couches aval

## 4. Enrichissement identité

Exemples typiques :

- enrichissement depuis Nasdaq unresolved
- enrichissements SEC
- jobs liés aux candidats unresolved Stooq

But :

- réduire le volume d'unresolved
- renforcer la qualité de `symbol_reference_history`
- améliorer la continuité historique des symboles

## 5. Construction des statuts de listing

Job central :

- `build_listing_status_history`

But :

- dériver une couche listing stable à partir de l'identité
- distinguer les refs fermées des refs ouvertes
- utiliser le dernier snapshot Nasdaq complet comme couche de confirmation

## 6. Construction des univers

Job central :

- `build_universe_membership_history_from_listing_status`

But :

- dériver les univers de recherche à partir des listings
- filtrer de manière conservatrice les cas indésirables
- produire au minimum :
  - `US_LISTED_COMMON_STOCKS`
  - `US_LISTED_ETFS`

## 7. Validation / probes

Exemples :

- comptages de tables
- distribution des `listing_status`
- comptages par `status_reason`
- comptages des univers
- plage de dates des prix normalisés

But :

- vérifier qu'une reconstruction a produit un état cohérent
- éviter d'avancer avec un état DB ambigu

## Commande canonique de reconstruction

```bash
python3 scripts/rebuild_loader_db.py
```

## Commandes unitaires utiles

```bash
python3 -m stock_quant_data.jobs.build_price_normalized_from_raw
python3 -m stock_quant_data.jobs.build_listing_status_history
python3 -m stock_quant_data.jobs.build_universe_membership_history_from_listing_status
```

## Règle pratique

- reconstruction complète : utiliser `scripts/rebuild_loader_db.py`
- itération ciblée : relancer un job unitaire
- validation : toujours faire des probes DB après les jobs critiques

## Lecture métier recommandée

Pour comprendre le repo dans le bon sens, lire les jobs selon cette logique :

```text
raw sources
    ->
identity resolution
    ->
listing status
    ->
universe membership
```

## Résumé

Ce repo ne doit pas être lu comme une simple liste de jobs indépendants.

Il doit être lu comme un pipeline en couches, où les jobs aval supposent qu'une partie de l'amont a déjà été reconstruite correctement.
