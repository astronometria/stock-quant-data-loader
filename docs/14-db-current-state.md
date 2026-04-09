# État actuel de la build DB

## Objectif

Ce document décrit l'état réel courant de la build DB locale du repo `stock-quant-data-loader`.

La base de référence attendue est :

```text
data/build/market_build.duckdb
```

## Tables et couches importantes

Les couches actuellement importantes dans l'état du repo sont :

- `instrument`
- `symbol_reference_history`
- `price_source_daily_normalized`
- `listing_status_history`
- `universe_membership_history`

## Lecture conceptuelle de l'état courant

### 1. Couche identité

La couche identité repose principalement sur :

- `instrument`
- `symbol_reference_history`

Cette couche sert de base canonique pour rattacher les symboles historiques à des instruments stables.

### 2. Couche prix

La couche prix normalisée repose principalement sur :

- `price_source_daily_normalized`

Cette table représente la couche de prix résolue vers `instrument_id` quand le mapping est disponible.

### 3. Couche listing

La couche listing repose principalement sur :

- `listing_status_history`

Statuts attendus :

- `ACTIVE`
- `INACTIVE`

Raisons typiques observées :

- `present_in_latest_complete_nasdaq_snapshot_day`
- `open_symbol_reference_not_confirmed_by_latest_complete_nasdaq_snapshot_day`
- `closed_symbol_reference_interval`

### 4. Couche univers

La couche univers repose principalement sur :

- `universe_membership_history`

Univers observés :

- `US_LISTED_COMMON_STOCKS`
- `US_LISTED_ETFS`

## Interprétation actuelle

L'état courant est construit avec une logique conservatrice :

- les symboles historiquement fermés produisent des lignes `INACTIVE`
- les références ouvertes confirmées par le dernier snapshot Nasdaq complet produisent des lignes `ACTIVE`
- les références ouvertes non confirmées restent visibles avec une raison explicite au lieu d'être fermées agressivement
- les univers finaux filtrent une partie des cas ambigus

## Commandes de probe recommandées

### Vérifier les objets DB principaux

```bash
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
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(table_name, count)
    except Exception as e:
        print(table_name, "ERROR", e)
conn.close()
PY2
```

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

### Vérifier la plage des prix normalisés

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

## Ce qu'il faut garder en tête

- cette build DB est une base de travail loader, pas la couche finale API
- l'état DB doit être interprété avec la logique `identity -> listing -> univers`
- `status_reason` est indispensable pour comprendre la différence entre un `ACTIVE` confirmé et un `ACTIVE` non confirmé par le dernier snapshot complet

## Résumé

L'état actuel de la DB est déjà exploitable pour :

- l'audit des couches loader
- la validation des statuts de listing
- la construction des univers
- la préparation d'une couche aval plus stable
