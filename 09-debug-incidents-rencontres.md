# 09 — Debug des incidents rencontrés

## 1. Erreurs SQL de type Binder / Parser

Exemples rencontrés :
- chaînes non quotées dans SQL
- `LIKE %foo%` au lieu de `LIKE '%foo%'`
- `RESOLVED` non quoté
- `ACTIVE` non quoté

### Cause
Le shell / rewrite a parfois corrompu les quotes SQL.

### Réponse
- re-vérifier le fichier source après rewrite
- relancer `python3 -m compileall`
- imprimer un snippet exact du fichier avant exécution

---

## 2. Erreurs shell avec heredoc

Exemples :
- `tee: '': No such file or directory`
- commandes mangées par un quoting cassé
- fragments Python interprétés comme commandes bash

### Cause
Guillemets externes mal fermés ou heredoc mal encapsulé.

### Réponse
- préférer un seul bloc `bash -lc '...'`
- éviter de mélanger trop de niveaux de quotes
- vérifier `LOG=...` bien défini dans le même shell

---

## 3. Confusion entre schéma réel et hypothèse

Exemples :
- supposer une colonne `symbol` dans `instrument`
- supposer une colonne `symbol` dans `universe_membership_history`

### Réponse
Toujours sonder le schéma réel avant correction :

```sql
DESCRIBE instrument;
DESCRIBE universe_membership_history;
```

---

## 4. Résiduel listing status

Constat :
- il reste un petit groupe `ACTIVE` non confirmé par le dernier snapshot complet

Lecture actuelle :
- c’est un backlog reviewable
- cela ne bloque plus la suite
