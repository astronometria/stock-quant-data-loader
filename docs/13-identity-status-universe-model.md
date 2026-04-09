# Modèle identité -> listing -> univers

## Objectif

Ce document décrit la logique métier actuelle qui relie :

- l'identité instrument / symbole
- le statut de listing
- l'appartenance à un univers

## Vue d'ensemble

Le pipeline conceptuel recommandé est :

```text
instrument
    ->
symbol_reference_history
    ->
listing_status_history
    ->
universe_membership_history
```

## 1. Couche identité

### `instrument`

Table d'identité principale des instruments.

Rôle :

- identifiant stable de l'instrument
- type de sécurité
- ticker principal
- bourse principale

### `symbol_reference_history`

Historique des références symbole -> instrument.

Rôle :

- conserver les intervalles de validité des symboles
- permettre une couche historique exploitable
- servir de base à la construction des listings

## 2. Couche listing

### `listing_status_history`

Cette table dérive principalement de `symbol_reference_history`.

Logique actuelle :

- si l'intervalle symbole est fermé, le statut est `INACTIVE`
- si l'intervalle symbole est ouvert et confirmé par le dernier snapshot Nasdaq complet, le statut est `ACTIVE`
- si l'intervalle symbole est ouvert mais non confirmé par le dernier snapshot complet, le statut reste `ACTIVE` avec une raison explicite de non-confirmation
- certains artefacts récents évidents peuvent être supprimés de façon conservatrice

Exemples de `status_reason` :

- `closed_symbol_reference_interval`
- `present_in_latest_complete_nasdaq_snapshot_day`
- `open_symbol_reference_not_confirmed_by_latest_complete_nasdaq_snapshot_day`

## 3. Couche univers

### `universe_membership_history`

Cette table dérive de `listing_status_history`.

Objectif :

- construire des univers de recherche exploitables
- filtrer de façon conservatrice les instruments actifs selon leur type et leur forme de symbole

Univers observés actuellement :

- `US_LISTED_COMMON_STOCKS`
- `US_LISTED_ETFS`

## Exclusions conservatrices typiques

Pour les actions ordinaires US listées, certaines exclusions sont appliquées :

- OTC
- dash series
- warrant / right like
- certaines formes non désirées

## Philosophie actuelle

Le pipeline est conservateur :

- on évite de fermer agressivement des références ouvertes juste parce qu'un dernier snapshot ne les confirme pas
- on préfère signaler les cas douteux dans une catégorie distincte
- les univers finaux filtrent une partie des cas les plus ambigus

## Limite importante

`ACTIVE` ne veut pas toujours dire :

```text
confirmé par le dernier snapshot Nasdaq complet
```

Il peut aussi vouloir dire :

```text
référence encore ouverte mais non confirmée par la dernière couche snapshot
```

C'est pour cela que `status_reason` est essentiel.

## Probes utiles

### Répartition des listings

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

### Répartition des univers

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

## Conclusion

La logique actuelle sépare bien :

- l'identité historique
- le statut de listing
- les univers de recherche

Cette séparation améliore la lisibilité du pipeline et limite les raccourcis dangereux entre symbole, existence du listing et appartenance à un univers.
