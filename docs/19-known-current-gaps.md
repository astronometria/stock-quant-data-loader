# Gaps connus à l'état actuel

## Objectif

Ce document liste les zones encore imparfaites du repo ou de la build DB, pour que l'état courant soit documenté explicitement.

## 1. Références ouvertes non confirmées

Une fraction des lignes `ACTIVE` dans `listing_status_history` n'est pas confirmée par le dernier snapshot Nasdaq complet. Elles restent visibles avec le `status_reason` :

```text
open_symbol_reference_not_confirmed_by_latest_complete_nasdaq_snapshot_day
```

C'est un choix conservateur, pas forcément une erreur.

## 2. Cas ambigus exclus des univers

Les univers finaux excluent encore des formes de symboles ou des cas ambigus:

- OTC
- dash series
- warrants / rights like
- units
- résidus non confirmés selon la logique du job

## 3. Documentation auto-generated

`docs/auto_generated/` est utile comme inventaire, mais reste moins pédagogique que les docs manuelles.

## 4. README racine

Le README doit rester synchronisé avec la séparation loader / API / downloader, sinon il redevient ambigu.

## 5. Build DB locale

L'état de la build DB documentée ici est un instantané local. Les compteurs peuvent changer après une nouvelle rebuild.

## 6. Résolution historique

La qualité historique dépend encore de la richesse des sources brutes et des enrichissements identité. Certains symboles restent dans une zone grise tant qu'ils ne sont pas mieux corroborés.

## Conclusion

Le repo est déjà exploitable sérieusement, mais il faut garder une lecture conservatrice des couches listing et univers.
