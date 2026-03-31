stock-quant-data-platform
Aide-memoire de l'etat actuel du projet
Derniere mise a jour: 2026-03-29

======================================================================
1. OBJECTIF DU REPO
======================================================================

Ce repo couvre uniquement la partie 1 du split du projet stock-quant:

- ingestion de donnees
- construction d'une base historique scientifique
- publication de releases immuables
- exposition lecture seule via API
- point-in-time (PIT)
- controle du survivor bias
- snapshots d'univers

Ce repo ne contient pas:

- recherche alpha
- features de recherche
- labels ML
- backtests
- ranking
- portefeuille
- moteur de strategie

Le principe fondateur est:

    build DB mutable
    -> validation scientifique
    -> publication d'une release immuable
    -> API lecture seule sur la release publiee

L'API ne doit jamais lire directement la build DB.

======================================================================
2. ARCHITECTURE GENERALE
======================================================================

Le projet suit une architecture Option B:

A. Build plane
- base de travail mutable
- ingestion / seed / normalisation
- validations scientifiques
- preparation de publication

B. Serve plane
- release publiee
- immuable
- lecture seule
- l'API lit seulement cette release

Chemins importants:

- build DB:
  data/build/market_build.duckdb

- releases publiees:
  data/releases/<release_id>/serving.duckdb

- lien vers release courante:
  data/current

Chaque publication cree une nouvelle release versionnee, puis bascule
atomiquement data/current vers cette release.

======================================================================
3. PRINCIPE SCIENTIFIQUE
======================================================================

Les regles actuelles du projet sont:

1) Separation stricte build / serving
   La build DB peut changer.
   La serving DB publiee est immuable.

2) Point-in-time
   Les requetes "as_of_date" doivent reconstruire l'etat a une date
   precise a partir des intervalles historiques.

3) Survivor bias control
   On n'infere pas un univers historique depuis l'etat actuel.
   On historise explicitement les memberships.

4) Publication bloquante
   Une release ne doit pas etre publiee si les checks scientifiques
   echouent.

5) Checks publies
   Les checks d'une release sont eux aussi exposes a l'utilisateur/API.

======================================================================
4. TABLES ACTUELLEMENT EN PLACE
======================================================================

Dans la build DB, le schema de fondation inclut actuellement:

- schema_migrations
- instrument
- symbol_reference_history
- listing_status_history
- universe_definition
- universe_membership_history
- release_metadata

Tables actuellement vraiment utilisees dans le flux present:

A. instrument
Identite stable d'un instrument.

Colonnes principales:
- instrument_id
- security_type
- company_id
- primary_ticker
- primary_exchange
- created_at

B. universe_definition
Definition logique d'un univers.

Colonnes principales:
- universe_id
- universe_name
- description
- created_at

C. universe_membership_history
Historique d'appartenance a un univers.

Colonnes principales:
- universe_membership_history_id
- universe_id
- instrument_id
- membership_status
- effective_from
- effective_to
- source_name
- observed_at
- ingested_at

Règle PIT actuelle pour les memberships:
- effective_from <= as_of_date
- effective_to IS NULL OR effective_to > as_of_date

Remarque:
symbol_reference_history et listing_status_history existent deja dans le
schema, mais ne sont pas encore exploites dans les endpoints actuels.

======================================================================
5. DATA DE DEMO / SEEDS ACTUELS
======================================================================

Des seeds deterministes ont ete ajoutes pour valider le flux.

A. universe_definition
Univers seedes:
- US_LISTED_COMMON_STOCKS
- NASDAQ_LISTED
- NYSE_LISTED
- US_LISTED_ETFS

B. instrument
Instruments seedes:
- 1001 AAPL NASDAQ COMMON_STOCK
- 1002 MSFT NASDAQ COMMON_STOCK
- 1003 IBM  NYSE   COMMON_STOCK
- 1004 SPY  NYSE   ETF

C. universe_membership_history
Regles seed actuelles:
- US_LISTED_COMMON_STOCKS contient AAPL, MSFT
- US_LISTED_COMMON_STOCKS a aussi IBM jusqu'au 2022-12-31
- NASDAQ_LISTED contient AAPL, MSFT
- NYSE_LISTED contient IBM, SPY
- US_LISTED_ETFS contient SPY

Conséquence PIT verifiee:
- au 2021-06-30, IBM est encore membre de US_LISTED_COMMON_STOCKS
- au 2024-06-30, IBM n'en fait plus partie

======================================================================
6. VALIDATION SCIENTIFIQUE ACTUELLE
======================================================================

Le job de validation actuel verifie universe_membership_history.

Checks actuellement en place:

1) invalid_interval_count
Compte les lignes ou:
- effective_to IS NOT NULL
- ET effective_to < effective_from

2) overlap_count
Compte les chevauchements pour un meme:
- universe_id
- instrument_id

Regle de chevauchement:
Deux intervalles sont en conflit s'ils se recouvrent dans le temps.

Si un de ces checks echoue:
- checks_passed = false
- la publication doit etre refusee

======================================================================
7. PUBLICATION
======================================================================

Le job de publication:

- lit la build DB
- lance les validations
- refuse de publier si les validations echouent
- cree une nouvelle release sous data/releases/<release_id>/
- cree serving.duckdb
- ecrit manifest.json
- ecrit checks.json
- bascule data/current vers la nouvelle release

Objets publies actuellement dans serving.duckdb:
- serving_release_metadata
- serving_release_checks
- instrument
- universe_definition
- universe_membership_history

checks.json est aussi ecrit dans le dossier de release.

======================================================================
8. API ACTUELLE
======================================================================

L'API FastAPI lit uniquement:
- data/current/serving.duckdb
- checks.json de la release courante

Endpoints actuellement disponibles:

A. Sante / metadata
- GET /api/v1/health
- GET /api/v1/ready
- GET /api/v1/release
- GET /api/v1/release/checks

B. Univers
- GET /api/v1/universes
- GET /api/v1/universes/{universe_name}
- GET /api/v1/universes/{universe_name}/members?as_of_date=YYYY-MM-DD

Comportements verifies:
- /health retourne liveness
- /ready retourne ready si une release publiee existe
- /release/checks expose les validations publiees
- /universes retourne les univers publies
- /universes/{name}/members applique la logique PIT sur les intervalles

======================================================================
9. COMMANDES CLI ACTUELLES
======================================================================

Commande generale:
- sq

Commandes actuellement disponibles:
- sq init-db
- sq seed-instruments
- sq seed-universes
- sq seed-universe-membership-history
- sq validate-release
- sq publish-release

Role de chaque commande:

sq init-db
- initialise le schema de fondation dans la build DB

sq seed-instruments
- ajoute les instruments seed

sq seed-universes
- ajoute les univers seed

sq seed-universe-membership-history
- ajoute les memberships historiques seed

sq validate-release
- calcule les checks scientifiques sur la build DB

sq publish-release
- refuse si validation echoue
- sinon publie une nouvelle release immutable

======================================================================
10. FLUX DE TRAVAIL ACTUEL
======================================================================

Flux type pour reconstruire l'etat minimal actuel:

1. initialiser le schema
   sq init-db

2. seed les univers
   sq seed-universes

3. seed les instruments
   sq seed-instruments

4. seed les memberships historiques
   sq seed-universe-membership-history

5. valider
   sq validate-release

6. publier
   sq publish-release

7. lancer l'API
   uvicorn stock_quant_data.api.app:app --host 127.0.0.1 --port 8000 --reload

8. tester les endpoints
   /api/v1/health
   /api/v1/ready
   /api/v1/release
   /api/v1/release/checks
   /api/v1/universes
   /api/v1/universes/US_LISTED_COMMON_STOCKS
   /api/v1/universes/US_LISTED_COMMON_STOCKS/members?as_of_date=2021-06-30
   /api/v1/universes/US_LISTED_COMMON_STOCKS/members?as_of_date=2024-06-30

======================================================================
11. COMPORTEMENT PIT DEJA VALIDE
======================================================================

Exemple deja verifie:

GET /api/v1/universes/US_LISTED_COMMON_STOCKS/members?as_of_date=2021-06-30

Retour attendu:
- AAPL
- MSFT
- IBM

GET /api/v1/universes/US_LISTED_COMMON_STOCKS/members?as_of_date=2024-06-30

Retour attendu:
- AAPL
- MSFT

Cela valide deja:
- intervals historises
- filtering as_of_date
- publication correcte dans la serving DB
- lecture API depuis la release publiee

======================================================================
12. FICHIERS / MODULES IMPORTANTS
======================================================================

Fichiers importants du socle actuel:

Configuration / DB
- src/stock_quant_data/config/settings.py
- src/stock_quant_data/config/logging.py
- src/stock_quant_data/db/connections.py
- src/stock_quant_data/db/publish.py

Jobs
- src/stock_quant_data/jobs/init_db.py
- src/stock_quant_data/jobs/seed_universes.py
- src/stock_quant_data/jobs/seed_instruments.py
- src/stock_quant_data/jobs/seed_universe_membership_history.py
- src/stock_quant_data/jobs/validate_release.py
- src/stock_quant_data/jobs/publish_release.py

API
- src/stock_quant_data/api/app.py
- src/stock_quant_data/api/v1/health.py
- src/stock_quant_data/api/v1/universes.py

DDL
- sql/ddl/001_foundation.sql

CLI
- src/stock_quant_data/cli/main.py

======================================================================
13. CE QUI EST DEJA DECIDE AU NIVEAU ARCHITECTURE
======================================================================

Decisions deja prises:

- on focus uniquement sur la partie 1
- Option B retenue
- build plane separe du serve plane
- releases immuables
- API lecture seule
- approche scientifique / PIT / survivor bias aware
- pas de logique research dans ce repo
- pas de backtest
- pas de feature alpha
- pas de lecture directe de la build DB par l'API

======================================================================
14. PROCHAINES ETAPES RECOMMANDEES
======================================================================

Ordre recommande pour continuer:

1) test negatif controle
- inserer volontairement un intervalle chevauchant
- verifier que sq validate-release le detecte
- verifier que sq publish-release refuse de publier

2) symbol_reference_history
- mapping ticker -> instrument dans le temps
- futurs endpoints /symbols

3) listing_status_history
- cycle de vie listing
- fondation supplementaire contre le survivor bias

4) endpoints symbols
- resolution historique des tickers
- aliases
- mapping temporel

5) puis seulement apres:
- prices
- snapshots de marche plus larges
- FINRA
- SEC

======================================================================
15. MESSAGE COURT POUR RE-PROMPTER UNE AUTRE IA
======================================================================

Si un nouveau chat doit reprendre ce projet, lui donner ce resume:

"Je travaille sur un repo nomme stock-quant-data-platform.
Ce repo couvre uniquement la partie data platform, pas la recherche.
L'architecture est Option B:
- build DB mutable dans data/build/market_build.duckdb
- releases immuables dans data/releases/<release_id>/serving.duckdb
- symlink data/current vers la release active
- API FastAPI lecture seule sur la release publiee

Le schema actuel inclut instrument, universe_definition,
universe_membership_history, symbol_reference_history,
listing_status_history.

Le flux valide aujourd'hui:
- sq init-db
- sq seed-universes
- sq seed-instruments
- sq seed-universe-membership-history
- sq validate-release
- sq publish-release

L'API expose:
- /api/v1/health
- /api/v1/ready
- /api/v1/release
- /api/v1/release/checks
- /api/v1/universes
- /api/v1/universes/{universe_name}
- /api/v1/universes/{universe_name}/members?as_of_date=YYYY-MM-DD

Les checks actuels bloquent la publication si universe_membership_history
contient:
- effective_to < effective_from
- des chevauchements pour un meme (universe_id, instrument_id)

La prochaine etape recommandee est:
- faire un test negatif controle de validation
- puis implementer symbol_reference_history et listing_status_history."

======================================================================
FIN
======================================================================
