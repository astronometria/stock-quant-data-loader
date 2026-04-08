# Documentation v3 — stock-quant-data-loader

Cette v3 ajoute une couche d’audit technique plus exploitable pour travailler dans le repo sans se perdre.

## Ce que contient cette version

- catalogue des jobs par famille
- matrice job → entrées / sorties / dépendances
- contrats de tables critiques
- checklists de validation après exécution
- backlog des zones encore à vérifier dans le code
- guide de debug des erreurs déjà rencontrées

## Index

1. [01-bootstrap-install.md](01-bootstrap-install.md)
2. [02-pipeline-operatoire.md](02-pipeline-operatoire.md)
3. [03-architecture-uml.md](03-architecture-uml.md)
4. [04-etat-reel-db.md](04-etat-reel-db.md)
5. [05-catalogue-jobs-v3.md](05-catalogue-jobs-v3.md)
6. [06-contrats-tables-critiques.md](06-contrats-tables-critiques.md)
7. [07-matrice-jobs-dependances.md](07-matrice-jobs-dependances.md)
8. [08-checklists-validation.md](08-checklists-validation.md)
9. [09-debug-incidents-rencontres.md](09-debug-incidents-rencontres.md)
10. [10-backlog-audit-code.md](10-backlog-audit-code.md)

## Niveau de confiance

- **Confirmé par runs / probes** : chiffres, tables, statuts, volumes, séquences exécutées
- **Fortement probable** : patterns de code et conventions du repo
- **À vérifier dans le code** : signatures exactes de certaines fonctions non relues ligne à ligne ici
