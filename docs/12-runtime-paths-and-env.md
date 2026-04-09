# Runtime paths et variables d'environnement

## Objectif

Ce document résume les chemins runtime importants du repo et les variables d'environnement qui influencent son comportement.

## Référence principale

La logique de configuration est centralisée dans :

- `src/stock_quant_data/config/settings.py`

## Chemins importants

### Repo root

Racine du repo loader :

```text
~/stock-quant-data-loader
```

### Dossier data

```text
data/
```

### Build directory

```text
data/build/
```

### Build DB principale

```text
data/build/market_build.duckdb
```

## Relation avec le downloader

Le loader suppose souvent qu'un repo downloader adjacent existe ou qu'un dossier de données brutes compatible est déjà disponible.

Chemin attendu typique :

```text
~/stock-quant-data-downloader
```

## Variables d'environnement

Selon la configuration du repo, les variables `SQD_*` peuvent rediriger certains chemins runtime.

Exemples de logique à vérifier dans `settings.py` :

- repo root
- data dir
- build dir
- build DB path
- downloader repo root
- downloader data dir

## Commandes de probe utiles

### Voir les chemins résolus depuis Python

```bash
python3 <<'PY2'
from stock_quant_data.config.settings import settings
print("repo_root =", settings.repo_root)
print("data_dir =", settings.data_dir)
print("build_dir =", settings.build_dir)
print("build_db_path =", settings.build_db_path)
print("downloader_repo_root =", getattr(settings, "downloader_repo_root", None))
print("downloader_data_dir =", getattr(settings, "downloader_data_dir", None))
PY2
```

### Vérifier la build DB

```bash
ls -lh ~/stock-quant-data-loader/data/build/market_build.duckdb
```

## Risques fréquents

- repo downloader absent ou mal placé
- build DB manquante
- environnement Python qui ne pointe pas vers le bon repo
- variables d'environnement qui redirigent silencieusement les chemins

## Recommandation

Avant toute exécution importante :

1. vérifier `python3`
2. vérifier les chemins résolus par `settings`
3. vérifier l'existence de la build DB
4. vérifier la présence des données brutes attendues
