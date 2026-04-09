# Troubleshooting

## Objectif

Ce document regroupe les problèmes fréquents observés dans le repo loader et la manière correcte de les diagnostiquer.

## Règle générale

Toujours commencer par **sonder** avant de corriger.

Avant toute correction :

- vérifier le chemin de DB
- vérifier l'environnement Python
- vérifier les compteurs de tables
- vérifier le schéma exact des tables concernées
- vérifier les logs du job

## 1. Erreurs de quoting SQL / Binder / Parser

Symptômes typiques :

- `Parser Error`
- `Binder Error`
- colonne non trouvée
- token non quoté dans SQL
- string SQL cassée par un rewrite

Cause fréquente :

- SQL modifié via shell heredoc fragile
- quotes perdues
- `%...%` non quoté
- `'RESOLVED'`, `'ACTIVE'`, `'N'` oubliés

Bon réflexe :

- ouvrir le fichier Python concerné
- relire la string SQL réellement écrite
- vérifier les quotes littérales DuckDB

Probe utile :

```bash
python3 -m compileall src/stock_quant_data/jobs
```

## 2. Mauvais chemin de DB

Symptômes typiques :

- table absente
- compteurs inattendus
- build DB différente de celle attendue

Probe utile :

```bash
python3 <<'PY2'
from stock_quant_data.config.settings import settings
print(settings.build_db_path)
PY2
```

Puis :

```bash
ls -lh data/build/market_build.duckdb
```

## 3. Un job fonctionne mais produit des compteurs inattendus

Exemples :

- `listing_status_history_count` inattendu
- `unresolved_symbol_count` anormal
- univers trop petits ou trop grands

Bon réflexe :

- sonder la table amont
- sonder la distribution par statut / raison
- sonder les exclusions appliquées

## 4. Build DB cohérente mais univers inattendus

Cas typique :

- `listing_status_history` semble correcte
- `universe_membership_history` paraît trop restrictive

Cause fréquente :

- logique d'exclusion conservatrice
- exclusions OTC
- exclusions dash series
- exclusions warrant / right like
- filtre security type

Probe utile :

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

## 5. Markdown ou fichiers docs cassés

Symptômes typiques :

- fichier `.md` sur une seule ligne
- quotes perdues
- contenu tronqué
- heredoc shell cassé

Bonne pratique :

- écrire les `.md` avec `python3 <<'PY'`
- éviter les gros `bash -lc " ... "` multi-lignes
- faire une backup `.bak` avant réécriture

## 6. Relance de job après modification de code

Procédure recommandée :

1. backup du fichier
2. réécriture
3. vérification du contenu du fichier
4. `python3 -m compileall`
5. exécution du job
6. probe DB

## Commande de diagnostic minimal

```bash
cd ~/stock-quant-data-loader && \
python3 -m compileall src/stock_quant_data/jobs && \
python3 <<'PY2'
import duckdb
conn = duckdb.connect("data/build/market_build.duckdb")
for table_name in [
    "instrument",
    "symbol_reference_history",
    "price_source_daily_normalized",
    "listing_status_history",
    "universe_membership_history",
]:
    try:
        print(table_name, conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
    except Exception as e:
        print(table_name, "ERROR", e)
conn.close()
PY2
```

## Ce qu'il ne faut pas faire

- corriger avant de sonder
- supposer le schéma d'une table
- supposer le chemin réel de la DB
- réécrire un gros fichier `.md` via une chaîne shell fragile
- mélanger correction de code et correction de doc dans un même bloc non vérifié

## Résumé

Le troubleshooting du repo doit rester simple :

```text
sonder
-> lire le fichier réel
-> compiler
-> exécuter
-> vérifier la DB
```
