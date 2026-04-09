# stock-quant-data-loader

Repo de chargement et reconstruction de la build DB pour le stack stock-quant-data.

## Rôle du repo

Ce repo est centré sur la couche **loader / build database**.

Il sert à :

- initialiser la build DB DuckDB
- charger et normaliser des données brutes déjà téléchargées
- reconstruire les couches d'identité
- construire les statuts de listing
- construire les univers historiques

Ce repo n'est pas la couche API de serving finale.

## Point d'entrée canonique

Pour une reconstruction complète, utiliser :

```bash
python3 scripts/rebuild_loader_db.py
```

## Structure utile

- `src/stock_quant_data/jobs/` : jobs unitaires
- `src/stock_quant_data/cli/` : CLI du repo
- `scripts/` : orchestration et outils
- `data/build/market_build.duckdb` : build DB locale
- `docs/` : documentation manuelle
- `docs/auto_generated/` : inventaire technique généré

## Documentation recommandée

Lire dans cet ordre :

1. `docs/11-runbook-canonique.md`
2. `docs/12-runtime-paths-and-env.md`
3. `docs/13-identity-status-universe-model.md`
4. `docs/auto_generated/00-overview.md`

## Commandes utiles

### Vérifier l'environnement

```bash
python3 --version
```

### Lancer la reconstruction complète

```bash
python3 scripts/rebuild_loader_db.py
```

### Lancer un job isolé

```bash
python3 -m stock_quant_data.jobs.build_listing_status_history
python3 -m stock_quant_data.jobs.build_universe_membership_history_from_listing_status
```

## Notes

- la build DB est reconstruite de façon conservatrice
- les couches identité / listing / univers sont séparées explicitement
- la doc auto-générée sert d'inventaire, mais la doc manuelle décrit l'ordre d'utilisation réel
