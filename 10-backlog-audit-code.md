# 10 — Backlog d’audit code

## Priorité 1

### A. Extraire automatiquement tous les jobs réellement présents
Objectif :
- lister tous les modules dans `src/stock_quant_data/jobs`
- comparer avec ce que le CLI expose

### B. Extraire les signatures de fonctions / classes
Objectif :
- générer un vrai index technique
- arrêter de travailler à partir d’inférences

### C. Cartographier les tables créées / modifiées par job
Objectif :
- avoir un contrat explicite job → tables lues / écrites

## Priorité 2

### D. Écrire une doc release
- comment valider
- comment publier
- comment revenir en arrière

### E. Documenter les invariants
- identité
- prix
- listing
- univers

### F. Produire un schéma de la DB actuelle
- tables
- colonnes
- dépendances logiques

## Priorité 3

### G. Générer un rapport automatisé
Un script qui produit :
- compteurs critiques
- anomalies
- samples
- statut de santé du pipeline

## Recommandation

La meilleure prochaine étape documentaire est :

1. script d’inventaire automatique des jobs / fonctions
2. script d’inventaire automatique du schéma DB
3. génération de markdown depuis ces deux sources
