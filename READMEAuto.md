# Documentation v4 — génération automatique repo + DB

Cette v4 ajoute des scripts pour produire un état technique **automatique** du repo et de la base DuckDB.

## Objectif

Au lieu d’écrire uniquement une documentation statique, cette version permet de :

- scanner `src/stock_quant_data/jobs`
- extraire fonctions, classes, signatures, imports, modules
- extraire le schéma réel de la DB DuckDB
- générer des rapports Markdown réutilisables
- reconstruire un bundle documentaire après chaque évolution du repo

## Fichiers

- `scripts/generate_repo_inventory.py`
- `scripts/generate_db_inventory.py`
- `scripts/generate_docs_bundle.py`
- `templates/README_TEMPLATE.md`
- `templates/PIPELINE_TEMPLATE.md`
- `templates/DB_TEMPLATE.md`

## Utilisation recommandée

### 1) Inventaire du repo

```bash
cd ~/stock-quant-data-loader
source .venv/bin/activate
python3 scripts/generate_repo_inventory.py \
  --repo-root ~/stock-quant-data-loader \
  --output-dir docs_autogen/repo
```

### 2) Inventaire de la DB

```bash
cd ~/stock-quant-data-loader
source .venv/bin/activate
python3 scripts/generate_db_inventory.py \
  --db-path ~/stock-quant-data-loader/data/build/market_build.duckdb \
  --output-dir docs_autogen/db
```

### 3) Génération du bundle documentaire

```bash
cd ~/stock-quant-data-loader
source .venv/bin/activate
python3 scripts/generate_docs_bundle.py \
  --repo-report docs_autogen/repo/repo_inventory.json \
  --db-report docs_autogen/db/db_inventory.json \
  --output-dir docs_autogen/final
```

## Résultat attendu

Le dossier `docs_autogen/final/` contiendra normalement :

- `00-overview.md`
- `01-repo-inventory.md`
- `02-db-inventory.md`
- `03-jobs-catalog.md`
- `04-functions-and-classes.md`
- `05-db-tables.md`

## Remarque importante

Les scripts sont faits pour être **lisibles et auditables** :
- beaucoup de commentaires
- structure simple
- pas de dépendance exotique
- sortie JSON + Markdown
