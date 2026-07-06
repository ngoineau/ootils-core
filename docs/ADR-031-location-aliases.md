# ADR-031 — Alias de site : résolution multi-code d'un même entrepôt

**Statut :** Accepté — chantier #414.
**Date :** 2026-07-06
**Auteurs :** ootils-core team
**Contexte mesuré :** démo #408 sur base pilote (les mesures ci-dessous) ; header de la migration `070_location_aliases.sql` et le helper de résolution `pyramide/repository.py:_warehouse_codes_subquery`, qui référencent tous deux cet ADR.

---

## Contexte

La démo #408 sur base pilote a chiffré le fossé entre les codes portés par l'ERP et les codes que le référentiel `locations` connaît. Sur **3 761 748 lignes** de `demand_history`, **0 des 15 codes `warehouse_id`** distincts se résolvaient à un site : l'ERP émet des codes DC **numériques** (`'87'`, `'286'`) tandis que `locations.external_id` porte des codes **alphabétiques** (`'DAL'`, `'CAN'`). Résultat : la jointure `demand_history.warehouse_id = locations.external_id` — le contrat de résolution posé en migration 047 (`warehouse_id` est un texte libre, non-FK, résolu au niveau lecture) — ne rapprochait **aucune** ligne. La couche demande par site (Pyramide, DRP) tournait à vide sur données réelles, faute d'un pont entre les deux codifications.

Le constat est **générique, pas pilote-spécifique** : tout ERP réel porte plusieurs codifications par site — un code alpha dans un flux, un code numérique dans un autre, des codes hérités d'une migration de système. `locations.external_id` (migration 007) tient exactement **un** code canonique par site, sous contrainte `UNIQUE` : il ne peut structurellement pas exprimer cette réalité many-to-one. Un seul `external_id` par site est structurellement insuffisant dès qu'on branche un ERP de production.

Trois alternatives ont été écartées d'emblée (voir §Alternatives rejetées) : ré-ingérer une `demand_history` traduite (3,76 M lignes réécrites, et le problème revient à chaque delta ERP) ; écraser `locations.external_id` avec les codes ERP (casse la clé naturelle utilisée partout ailleurs) ; un connecteur/CDC générique de mapping (hors périmètre V1). Le choix retenu est une table de correspondance dédiée, additive, lue à travers un point de résolution unique — même philosophie de résolveur unique que l'overlay #347 (ADR-025).

## Décision

### 1. Table `location_aliases` — N codes par site (migration 070)

`location_aliases` associe **0..N** codes alternatifs à un `location_id`, chaque code étiqueté par le système source dont il provient. C'est la table de correspondance générique dont le cas pilote (`warehouse_id` numérique côté ERP, `external_id` alpha côté référentiel) n'est que le premier consommateur.

- **`source_system NOT NULL DEFAULT '_default'` — jamais nullable, décision critique.** En PostgreSQL, deux `NULL` ne collisionnent **pas** sous une contrainte `UNIQUE` (`NULL <> NULL`). Un `source_system` nullable laisserait donc le **même** alias être inséré deux fois avec `source_system NULL`, les deux lignes survivant à la clé `UNIQUE (alias, source_system)` — soit un alias résolvant silencieusement vers deux sites, exactement la corruption que la table existe pour empêcher. Le sentinelle `'_default'` fait du cas « système non spécifié » une valeur réelle, collisionnable : deux lignes `'_default'` pour le même alias **entrent en conflit** et la seconde est rejetée. Ce trou des `NULL` sous `UNIQUE` est documenté dans le header de la migration.
- **`UNIQUE (alias, source_system)` — clé par système, invariant fort à l'ingest.** La clé DB porte sur `(alias, source_system)` pour permettre à **un même site** de déclarer le même code depuis plusieurs systèmes source (deux lignes `'87'`→DAL, une par système, sont légitimes). Elle ne suffit **pas** à l'invariant dont la résolution a besoin : `demand_history.warehouse_id` ne porte aucune information de système, la résolution est donc **agnostique au système** — un même code pointant deux sites (quel que soit le système) double-compterait la demande. L'invariant fort — **un code → exactement un site, tous systèmes confondus** — est porté par la couche ingest (point 3), pas par une contrainte DB inter-tables (un CHECK enjambant `locations.external_id` et cette table à chaque écriture est trop coûteux et n'est pas exprimable en une seule `UNIQUE`).
- **FK `ON DELETE RESTRICT` — explicite (convention repo).** Supprimer un site qui possède encore des alias est refusé, donc un alias vivant ne peut jamais pointer vers un site disparu. Le défaut Postgres est `NO ACTION` ; le `RESTRICT` est écrit noir sur blanc — même discipline que le garde-fou `test_scenario_fk_retention` sur les FK `scenarios` (migration 032) : l'intention de rétention est toujours explicitée, jamais laissée implicite.
- **Hygiène d'alias — `CHECK (alias <> '' AND btrim(alias) = alias)`.** Les codes ERP arrivent de flux TSV où un espace parasite en tête/queue ou une cellule vide est un risque réel. Un alias vide n'a pas de sens ; un alias espacé (`' 87'`) ne matcherait jamais la clé de lookup trimmée (`'87'`) et ne résoudrait jamais silencieusement. Le CHECK rejette les deux à l'écriture — un code malformé échoue bruyamment à l'ingest plutôt que de devenir une ligne fantôme qui ne matche jamais (fail-loudly plutôt que réponse silencieusement fausse).

### 2. Résolution single-point — `external_id ∪ aliases` défini à UN endroit

La sémantique « quels codes appartiennent à ce site » est l'**union** de son `external_id` et de ses alias, définie à **un seul endroit** : le helper module-privé `_warehouse_codes_subquery()` (`src/ootils_core/pyramide/repository.py`). Il émet un fragment SQL — `SELECT locations.external_id UNION SELECT location_aliases.alias`, paramétré par le seul placeholder `%(location_id)s` — que tout lecteur par site embarque dans un prédicat `dh.warehouse_id IN (...)`.

- **Même philosophie que `resolved_params_sql()` de l'overlay #347 (ADR-025).** Un seul point de vérité pour la sémantique de résolution, une seule requête où l'union `external_id ∪ aliases` est arbitrée. Règle de contribution miroir de celle d'ADR-025 : **aucune union `external_id`/`aliases` écrite à la main ailleurs** — un lecteur qui rejointerait `demand_history.warehouse_id = locations.external_id` en dur (l'égalité single-code d'avant cette table) rate les alias et rouvre le fossé #408. Les deux lecteurs par site de production — le leaf reader de `get_historical_demand` et `get_demand_freshness` — **sont branchés sur ce helper dans cette PR** ; tout futur lecteur par site doit l'être aussi.
- **Rétro-compatibilité stricte.** L'`UNION` avec un côté droit vide se réduit à `external_id` seul : un site **sans** alias résout **exactement** à l'ensemble mono-code qu'il résolvait avant l'existence de la table. Aucun site existant ne change de comportement tant qu'aucun alias ne lui est ajouté.

### 3. Anti-collision cross-site — validée à l'INGEST, pas en contrainte cross-table

Puisque la résolution lit l'`UNION external_id ∪ aliases`, tout code résolvant vers deux `location_id` distincts est une ambiguïté que l'union rendrait silencieusement double. Le garde-fou couvre donc **les deux sens** : un alias entrant qui égale l'`external_id` ou un alias d'un **autre** site (payload ou DB, tous systèmes confondus), ET un `external_id` entrant qui égale un alias existant d'un autre site. Il est porté **à l'ingest**, pas par une contrainte DB inter-tables (coût d'un CHECK enjambant deux tables à chaque écriture, non exprimable en une `UNIQUE`).

- **Ingestion via extension rétro-compatible du payload `POST /v1/ingest/locations`** (`src/ootils_core/api/routers/ingest.py`, `LocationRow` / `ingest_locations`) : un champ `aliases` **optionnel**. Une charge qui ne le porte pas se comporte exactement comme avant (aucun alias créé). C'est la couche ingest qui valide l'absence de collision cross-site avant d'écrire, comme elle valide déjà l'existence du `parent_external_id`.
- **Conséquence à documenter : la validation ingest est le seul rempart cross-site.** Un chargement **hors-API** — SQL direct dans la table — peut créer une ambiguïté (un alias égal à l'`external_id` d'un autre site) que la résolution `UNION` rendrait alors silencieusement double, sans qu'aucune contrainte DB ne l'ait arrêté. D'où la règle opérationnelle : **toujours passer par l'ingest** pour créer un alias ; ne jamais `INSERT` en direct dans `location_aliases`.

## Portée

- **Master data, invariante par scénario.** Un alias est une donnée de référentiel, au même titre que `demand_history` : il ne varie pas d'un scénario à l'autre. **Aucune forkabilité requise** — un alias n'est **pas** un paramètre de planification (contrairement aux 15 champs de l'overlay #347, qui eux sont forkables parce qu'ils changent le calcul). La table ne porte donc **aucune** colonne `scenario_id`, et n'entre pas dans le périmètre de forkabilité du North Star : c'est une correspondance de codification, pas un levier de simulation.
- **Lecture seule côté moteurs.** MRP, DRP et détection de pénurie joignent sur des `location_id` **déjà résolus** et ne touchent **jamais** les alias : la traduction code→`location_id` a lieu en amont, à la lecture de la demande (le helper single-point). Les moteurs de calcul ignorent l'existence de la table — elle ne fait que peupler le pont code-ERP → site que la couche demande consomme.

## Alternatives rejetées

- **Ré-ingérer une `demand_history` traduite.** Rejeté : réécrire les 3,76 M lignes pour y substituer le code alpha au code numérique est coûteux **et** ne résout rien durablement — le problème revient à **chaque delta ERP** (chaque nouvelle extraction rapporte des `warehouse_id` numériques à re-traduire). On corrigerait la donnée une fois pour la re-casser au prochain chargement.
- **Écraser `locations.external_id` avec les codes ERP.** Rejeté : `external_id` est la **clé naturelle** du site, utilisée partout ailleurs dans le système (ingest par upsert, résolution `resolve_location_uuid`, hiérarchies `parent_external_id`, staging). La remplacer par le code ERP casserait tous ces chemins pour réparer un seul. La bonne réponse est d'**ajouter** des codes, pas de substituer le canonique.
- **Connecteur / CDC générique de mapping.** Rejeté : hors périmètre V1. Un pipeline de synchronisation de correspondances (change-data-capture sur les codifications ERP) est de l'ingénierie d'intégration qui dépasse le besoin — une table de correspondance chargée par l'ingest existant suffit pour la démo et le pilote.

## Conséquences

- **Positif :** le mapping pilote devient une **donnée à charger** (~15 lignes, 🎯 pilote), pas du code — on branche l'ERP en peuplant une table, pas en patchant une requête. Le step 2 du runbook (forecast + FVA) **s'allume sur données réelles** dès la table chargée, puisque `get_historical_demand` et les autres lecteurs par site cessent alors de retomber à zéro sur les `warehouse_id` numériques. Le fossé #408 (0/15 codes résolus) se ferme par de la donnée, sans réécrire les 3,76 M lignes de `demand_history`.
- **Négatif / dette assumée en V1 :**
  - **La validation ingest est le seul rempart cross-site** (§Décision point 3). Un chargement hors-API (SQL direct) peut créer une ambiguïté que la résolution `UNION` rendrait silencieusement double — d'où la règle « toujours passer par l'ingest ». Aucune contrainte DB ne rattrape un `INSERT` direct malformé côté cross-site.
  - **🎯 Durcissement DB — arbitrage pilote ouvert.** L'invariant fort « un code → un site, tous systèmes » est appliqué par l'ingest (les collisions cross-site sont rejetées en 422, dans les deux sens : alias entrant vs external_id/alias existants, external_id entrant vs alias existants). Il n'est **pas** doublé d'une contrainte DB (une exclusion « même alias, sites différents » demanderait `btree_gist` — jugé disproportionné en V1). Un déploiement qui veut la ceinture ET les bretelles peut l'ajouter plus tard — le schéma est écrit pour que ce durcissement soit **additif**.
- **Reste à faire :** charger le mapping pilote (~15 lignes) via l'ingest — le code, lui, est complet (helper + deux lecteurs branchés + validations symétriques).

## Références

- **#414** — chantier alias de site (table + résolution + extension ingest).
- **#408** — la démo pilote qui a chiffré le fossé (3 761 748 lignes `demand_history`, 0/15 codes `warehouse_id` résolus).
- `src/ootils_core/db/migrations/070_location_aliases.sql` — table `location_aliases`, source de vérité du schéma : `UNIQUE (alias, source_system)`, `source_system NOT NULL DEFAULT '_default'` (le trou des `NULL` sous `UNIQUE` documenté dans le header), FK `ON DELETE RESTRICT` explicite, CHECK d'hygiène `alias <> '' AND btrim(alias) = alias`.
- `src/ootils_core/pyramide/repository.py` — `_warehouse_codes_subquery()`, le point de résolution unique `external_id ∪ aliases` (#414/ADR-031) ; `get_historical_demand()` et `get_demand_freshness()`, les deux lecteurs par site branchés sur ce helper dans cette PR.
- `src/ootils_core/api/routers/ingest.py` — `LocationRow` / `ingest_locations` (`POST /v1/ingest/locations`), point d'extension du payload (champ `aliases` optionnel) et couche portant l'anti-collision cross-site.
- `src/ootils_core/db/migrations/047_demand_foundation.sql` — le contrat `warehouse_id` (TEXT, non-FK, résolu à la lecture au niveau DRP contre `locations.external_id`) dont cet ADR lève la limite mono-code.
- `src/ootils_core/db/migrations/007_import_pipeline.sql` — `locations.external_id` (UNIQUE, un seul code canonique par site), la contrainte que `location_aliases` complète.
- `docs/ADR-025-scenario-param-overlay.md` — le pattern **single-resolver** (`resolved_params_sql()`, « aucun `COALESCE` divergent ») dont la résolution alias reprend la philosophie ; et le contraste de portée (l'overlay est forkable car paramétrique, l'alias ne l'est pas car master data).
- `docs/ADR-011-scenario-retention.md` — la convention FK `RESTRICT` sur les suppressions, appliquée ici à la FK `location_id`.
