# Runbook canonique

## Objectif

Ce document décrit l'ordre réel recommandé pour utiliser `stock-quant-data-loader` sur une build DB locale DuckDB.

Le repo a pour rôle principal de :

- initialiser la base de build
- charger / normaliser les données brutes déjà téléchargées
- reconstruire les couches d'identité, prix, listing et univers
- produire une build DB cohérente pour la suite du pipeline

## Source de vérité opérationnelle

Le point d'entrée canonique pour une reconstruction complète est :

```bash
python3 scripts/rebuild_loader_db.py
```

Ce script représente l'orchestration recommandée quand on veut reconstruire la build DB dans le bon ordre.

Le CLI `sq` est utile pour exécuter des jobs unitaires, mais pour une reconstruction complète il faut privilégier le runbook automatisé ci-dessus.

## Prérequis

- Ubuntu 22.04
- Python 3.10
- environnement virtuel du repo prêt
- DuckDB installé dans l'environnement Python
- repo downloader présent à côté si le pipeline dépend des données déjà téléchargées
- base de build attendue dans `data/build/market_build.duckdb`

## Séquence canonique

### 1. Se placer dans le repo

```bash
cd ~/stock-quant-data-loader
```

### 2. Vérifier l'environnement Python

```bash
which python3
python3 --version
```

### 3. Vérifier la présence de la build DB

```bash
ls -lh data/build/market_build.duckdb
```

### 4. Lancer la reconstruction complète

```bash
python3 scripts/rebuild_loader_db.py
```

## Exécution de jobs unitaires

Quand un job doit être relancé isolément, utiliser soit :

```bash
python3 -m stock_quant_data.jobs.NOM_DU_JOB
```

ou, selon le cas :

```bash
sq NOM_DU_JOB
```

Exemples récents importants :

```bash
python3 -m stock_quant_data.jobs.build_listing_status_history
python3 -m stock_quant_data.jobs.build_universe_membership_history_from_listing_status
python3 -m stock_quant_data.jobs.build_price_normalized_from_raw
```

## Probes recommandés après rebuild

### Vérifier les statuts de listing

```bash
python3 <<'PY2'
import duckdb
conn = duckdb.connect("data/build/market_build.duckdb")
for row in conn.execute("""
    SELECT listing_status, status_reason, COUNT(*)
    FROM listing_status_history
    GROUP BY 1, 2
    ORDER BY 1, 2
""").fetchall():
    print(row)
conn.close()
PY2
```

### Vérifier les univers

```bash
python3 <<'PY2'
import duckdb
conn = duckdb.connect("data/build/market_build.duckdb")
for row in conn.execute("""
    SELECT universe_name, COUNT(*)
    FROM universe_membership_history
    GROUP BY 1
    ORDER BY 1
""").fetchall():
    print(row)
conn.close()
PY2
```

### Vérifier les prix normalisés

```bash
python3 <<'PY2'
import duckdb
conn = duckdb.connect("data/build/market_build.duckdb")
print(
    conn.execute("""
        SELECT COUNT(*), MIN(price_date), MAX(price_date)
        FROM price_source_daily_normalized
    """).fetchone()
)
conn.close()
PY2
```

## Quand utiliser le runbook complet vs un job isolé

Utiliser le rebuild complet si :

- la build DB a été recréée
- plusieurs couches ont changé
- l'ordre global du pipeline doit être respecté
- on a touché l'identité, les prix, les listings ou les univers

Utiliser un job isolé si :

- on corrige seulement une étape précise
- on veut itérer rapidement sur une table donnée
- on a déjà validé l'amont

## Artefacts importants à connaître

- `scripts/rebuild_loader_db.py` : orchestration complète
- `src/stock_quant_data/cli/main.py` : CLI du repo
- `src/stock_quant_data/jobs/` : jobs unitaires
- `data/build/market_build.duckdb` : build DB locale
- `docs/auto_generated/` : inventaire technique généré automatiquement

## Limites actuelles

- certains jobs supposent que les données brutes ont déjà été téléchargées
- la reconstruction historique reste conservatrice sur certaines couches
- les résidus non confirmés par le dernier snapshot Nasdaq complet ne sont pas tous fermés automatiquement

## Recommandation pratique

Pour un opérateur humain, l'ordre recommandé est :

1. vérifier l'environnement
2. lancer `scripts/rebuild_loader_db.py`
3. exécuter les probes de validation
4. seulement ensuite relancer des jobs unitaires ciblés si nécessaire
